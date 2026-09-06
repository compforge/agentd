package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/cloudwego/hertz/pkg/common/ut"
	"github.com/cloudwego/hertz/pkg/route"
	"github.com/compforge/agentd/agentd/internal/api/view"
	managedevent "github.com/compforge/agentd/internal/event"
	"gorm.io/gorm"
)

var userMessageBody = []byte(`{"events":[{"type":"user.message","content":[{"type":"text","text":"follow up"}]}]}`)

func (f *idempotencyFixture) session(t *testing.T, engine *route.Engine) string {
	t.Helper()
	status, body := createIdempotentSession(engine, "", f.body)
	if status != 200 {
		t.Fatalf("create Session status=%d body=%s", status, body)
	}
	var session view.SessionResponse
	if err := json.Unmarshal(body, &session); err != nil {
		t.Fatal(err)
	}
	return session.ID
}

func sendKeyedMessage(engine *route.Engine, sessionID, key string, body []byte) (int, []byte) {
	response := ut.PerformRequest(engine, "POST", "/v1/sessions/"+sessionID+"/events",
		&ut.Body{Body: bytes.NewReader(body), Len: len(body)},
		ut.Header{Key: "Content-Type", Value: "application/json"},
		ut.Header{Key: "x-api-key", Value: "test"},
		ut.Header{Key: idempotencyKeyHeader, Value: key},
		// Explicit keys must override the content-based SDK retry heuristic.
		ut.Header{Key: stainlessRetryCountHeader, Value: "1"}).Result()
	return response.StatusCode(), bytes.Clone(response.Body())
}

func TestEventIdempotencyReplaysOriginalLedgerInput(t *testing.T) {
	f := newIdempotencyFixture(t)
	engine := f.engine(t)
	sessionID := f.session(t, engine)
	status, first := sendKeyedMessage(engine, sessionID, "event-key", userMessageBody)
	if status != 200 {
		t.Fatalf("status=%d body=%s", status, first)
	}
	var accepted view.Page[managedevent.ManagedEvent]
	if err := json.Unmarshal(first, &accepted); err != nil {
		t.Fatal(err)
	}
	eventID := accepted.Data[0]["id"].(string)
	ctx := context.Background()
	if err := f.events.MarkProcessed(ctx, sessionID, eventID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.control.ArchiveSession(ctx, sessionID); err != nil {
		t.Fatal(err)
	}
	status, replay := sendKeyedMessage(f.engine(t), sessionID, "event-key", userMessageBody)
	if status != 200 || !bytes.Equal(first, replay) {
		t.Fatalf("replay status=%d body=%s; original=%s", status, replay, first)
	}
	if bytes.Contains(replay, []byte("request_digest")) {
		t.Fatal("private request metadata leaked")
	}
	if accepted.Data[0]["processed_at"] != nil {
		t.Fatal("acceptance must retain original processed_at")
	}
	f.count(t, "idempotency_keys", 0)
	f.count(t, "ledger_events", 3) // initial input, keyed input, processed marker
	status, body := sendKeyedMessage(engine, sessionID, "event-key", bytes.Replace(userMessageBody, []byte("follow up"), []byte("changed"), 1))
	if status != 409 {
		t.Fatalf("changed content status=%d body=%s", status, body)
	}
}

func TestEventIdempotencyConcurrentRequestsAndKeyIdentity(t *testing.T) {
	f := newIdempotencyFixture(t)
	engines := []*route.Engine{f.engine(t), f.engine(t)}
	sessionID := f.session(t, engines[0])
	const requests = 8
	statuses := make([]int, requests)
	bodies := make([][]byte, requests)
	var wg sync.WaitGroup
	for i := range requests {
		wg.Add(1)
		go func() {
			defer wg.Done()
			statuses[i], bodies[i] = sendKeyedMessage(engines[i%2], sessionID, "key", userMessageBody)
		}()
	}
	wg.Wait()
	for i := range requests {
		if statuses[i] != 200 || !bytes.Equal(bodies[0], bodies[i]) {
			t.Fatalf("request %d status=%d body=%s", i, statuses[i], bodies[i])
		}
	}
	status, body := sendKeyedMessage(engines[0], sessionID, "Key", userMessageBody)
	if status != 200 || bytes.Equal(bodies[0], body) {
		t.Fatalf("distinct key status=%d body=%s", status, body)
	}
	f.count(t, "ledger_events", 3)
	f.count(t, "idempotency_keys", 0)
	otherID := f.session(t, engines[0])
	status, body = sendKeyedMessage(engines[0], otherID, "key", userMessageBody)
	if status != 409 {
		t.Fatalf("key reused in another Session status=%d body=%s", status, body)
	}
	f.count(t, "ledger_events", 4) // no input accepted for the conflicting request
}

func TestEventIdempotencyRollbackCanRetry(t *testing.T) {
	f := newIdempotencyFixture(t)
	engine := f.engine(t)
	sessionID := f.session(t, engine)
	wakes := f.wakes.Load()
	if err := f.db.Callback().Create().Before("gorm:create").Register("test:fail-append", func(tx *gorm.DB) {
		if tx.Statement.Table == "ledger_appends" {
			tx.AddError(errors.New("injected append receipt failure"))
		}
	}); err != nil {
		t.Fatal(err)
	}
	status, body := sendKeyedMessage(engine, sessionID, "retry", userMessageBody)
	if status != 500 {
		t.Fatalf("status=%d body=%s", status, body)
	}
	f.count(t, "ledger_events", 1)
	f.count(t, "ledger_appends", 1)
	if f.wakes.Load() != wakes {
		t.Fatal("notified for a rolled back append")
	}
	if err := f.db.Callback().Create().Remove("test:fail-append"); err != nil {
		t.Fatal(err)
	}
	status, body = sendKeyedMessage(engine, sessionID, "retry", userMessageBody)
	if status != 200 {
		t.Fatalf("retry status=%d body=%s", status, body)
	}
	f.count(t, "ledger_events", 2)
}

func TestEventIdempotencyRejectsUnsupportedKindsAndBatches(t *testing.T) {
	f := newIdempotencyFixture(t)
	engine := f.engine(t)
	sessionID := f.session(t, engine)
	for _, body := range []string{
		`{"events":[{"type":"user.interrupt"}]}`,
		`{"events":[{"type":"user.message","content":[{"type":"text","text":"one"}]},{"type":"user.message","content":[{"type":"text","text":"two"}]}]}`,
	} {
		status, response := sendKeyedMessage(engine, sessionID, "unsupported", []byte(body))
		if status != 400 || !bytes.Contains(response, []byte("unsupported_feature")) {
			t.Fatalf("status=%d body=%s", status, response)
		}
	}
	f.count(t, "ledger_events", 1)
}
