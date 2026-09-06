package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/common/config"
	"github.com/cloudwego/hertz/pkg/common/ut"
	"github.com/cloudwego/hertz/pkg/route"
	ledgergorm "github.com/compforge/agent-ledger/go/stores/gorm"
	"github.com/compforge/agentd/agentd/internal/api/view"
	"github.com/compforge/agentd/agentd/internal/model"
	gormrepo "github.com/compforge/agentd/agentd/internal/repo/gorm"
	"github.com/compforge/agentd/agentd/internal/service"
	managedevent "github.com/compforge/agentd/internal/event"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type idempotencyFixture struct {
	db      *gorm.DB
	control *service.Service
	events  *managedevent.Log
	wakes   atomic.Int64
	body    []byte
}

func (f *idempotencyFixture) Notify() { f.wakes.Add(1) }

func newIdempotencyFixture(t *testing.T) *idempotencyFixture {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "test.db")), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	sqlDB.SetMaxOpenConns(1)
	sqlDB.SetMaxIdleConns(1)
	t.Cleanup(func() { _ = sqlDB.Close() })
	repository, err := gormrepo.NewGORM(db)
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := ledgergorm.New(db, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := ledger.Initialize(ctx); err != nil {
		t.Fatal(err)
	}
	control, err := service.New(repository, time.Minute, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := control.CreateModel(ctx, model.Model{ID: "test", Provider: "anthropic", APIKey: "test"}); err != nil {
		t.Fatal(err)
	}
	agent, err := control.CreateAgent(ctx, model.Agent{Name: "test", ModelID: "test"})
	if err != nil {
		t.Fatal(err)
	}
	environment, err := control.CreateEnvironment(ctx, model.Environment{Name: "test"})
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(map[string]any{
		"agent": agent.ID, "environment_id": environment.ID, "title": "original",
		"initial_events": []any{map[string]any{"type": "user.message", "content": []any{map[string]any{"type": "text", "text": "hello"}}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return &idempotencyFixture{db: db, control: control, events: managedevent.NewLog(ledger), body: body}
}

func (f *idempotencyFixture) engine(t *testing.T) *route.Engine {
	t.Helper()
	store, err := gormrepo.NewIdempotencyStore(f.db, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	engine := route.NewEngine(config.NewOptions(nil))
	New(f.control, service.NewIdempotency(store, f.control), f.events, nil, f,
		slog.New(slog.NewTextHandler(io.Discard, nil)), WithAPIKey("test")).Register(engine)
	return engine
}

func createIdempotentSession(engine *route.Engine, key string, body []byte) (int, []byte) {
	response := ut.PerformRequest(engine, "POST", "/v1/sessions",
		&ut.Body{Body: bytes.NewReader(body), Len: len(body)},
		ut.Header{Key: "Content-Type", Value: "application/json"},
		ut.Header{Key: "x-api-key", Value: "test"},
		ut.Header{Key: "Idempotency-Key", Value: key}).Result()
	return response.StatusCode(), bytes.Clone(response.Body())
}

func (f *idempotencyFixture) count(t *testing.T, table string, want int64) {
	t.Helper()
	var count int64
	if err := f.db.Table(table).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != want {
		t.Fatalf("%s count=%d want=%d", table, count, want)
	}
}

func TestSessionIdempotencyReplaysOriginalAfterResourceChanges(t *testing.T) {
	f := newIdempotencyFixture(t)
	status, first := createIdempotentSession(f.engine(t), "create-1", f.body)
	if status != 200 {
		t.Fatalf("status=%d body=%s", status, first)
	}
	var created view.SessionResponse
	if err := json.Unmarshal(first, &created); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	title := "changed"
	if _, err := f.control.UpdateSession(ctx, created.ID, service.SessionUpdate{Title: &title}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.control.ArchiveSession(ctx, created.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.control.ArchiveAgent(ctx, created.Agent.ID); err != nil {
		t.Fatal(err)
	}
	// A new handler/store has no process-local receipt cache.
	status, replay := createIdempotentSession(f.engine(t), "create-1", f.body)
	if status != 200 || !bytes.Equal(first, replay) {
		t.Fatalf("replay status=%d body=%s; original=%s", status, replay, first)
	}
	f.count(t, "sessions", 1)
	f.count(t, "idempotency_keys", 1)
	f.count(t, "ledger_events", 1)
	changed := bytes.Replace(f.body, []byte("original"), []byte("different"), 1)
	status, body := createIdempotentSession(f.engine(t), "create-1", changed)
	if status != 409 {
		t.Fatalf("conflicting request status=%d body=%s", status, body)
	}
}

func TestSessionIdempotencyConcurrentHandlersCreateOnce(t *testing.T) {
	f := newIdempotencyFixture(t)
	engines := []*route.Engine{f.engine(t), f.engine(t)}
	const requests = 8
	bodies := make([][]byte, requests)
	statuses := make([]int, requests)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range requests {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			statuses[i], bodies[i] = createIdempotentSession(engines[i%2], "same", f.body)
		}()
	}
	close(start)
	wg.Wait()
	for i := range requests {
		if statuses[i] != 200 || !bytes.Equal(bodies[0], bodies[i]) {
			t.Fatalf("request %d status=%d body=%s", i, statuses[i], bodies[i])
		}
	}
	f.count(t, "sessions", 1)
	f.count(t, "idempotency_keys", 1)
	f.count(t, "ledger_events", 1)
}

func TestSessionIdempotencyRollbackIncludesLedgerAndReceipt(t *testing.T) {
	for _, failureTable := range []string{"ledger_events", "idempotency_keys"} {
		t.Run(failureTable, func(t *testing.T) {
			f := newIdempotencyFixture(t)
			engine := f.engine(t)
			fail := func(tx *gorm.DB) {
				if tx.Statement.Table == failureTable {
					tx.AddError(errors.New("injected persistence failure"))
				}
			}
			if failureTable == "ledger_events" {
				if err := f.db.Callback().Create().Before("gorm:create").Register("test:fail", fail); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := f.db.Callback().Update().Before("gorm:update").Register("test:fail", fail); err != nil {
					t.Fatal(err)
				}
			}
			status, body := createIdempotentSession(engine, "retry", f.body)
			if status != 500 {
				t.Fatalf("status=%d body=%s", status, body)
			}
			for _, table := range []string{"sessions", "idempotency_keys", "ledger_events", "ledger_appends", "ledger_lanes"} {
				f.count(t, table, 0)
			}
			if f.wakes.Load() != 0 {
				t.Fatal("notified before commit")
			}
			if failureTable == "ledger_events" {
				if err := f.db.Callback().Create().Remove("test:fail"); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := f.db.Callback().Update().Remove("test:fail"); err != nil {
					t.Fatal(err)
				}
			}
			status, body = createIdempotentSession(engine, "retry", f.body)
			if status != 200 {
				t.Fatalf("retry status=%d body=%s", status, body)
			}
			f.count(t, "sessions", 1)
			f.count(t, "idempotency_keys", 1)
		})
	}
}

func TestSessionIdempotencyKeysAreOptionalAndCaseSensitive(t *testing.T) {
	f := newIdempotencyFixture(t)
	engine := f.engine(t)
	for _, key := range []string{"", "", "Key", "key"} {
		status, body := createIdempotentSession(engine, key, f.body)
		if status != 200 {
			t.Fatalf("status=%d body=%s", status, body)
		}
	}
	f.count(t, "sessions", 4)
	f.count(t, "idempotency_keys", 2)
}

func TestSessionIdempotencyFingerprint(t *testing.T) {
	first, err := sessionIdempotencyIdentity([]byte("key"), []byte(`{"a":1,"b":[2,3]}`))
	if err != nil {
		t.Fatal(err)
	}
	second, err := sessionIdempotencyIdentity([]byte("key"), []byte(`{ "b": [2,3], "a": 1 }`))
	if err != nil || first != second {
		t.Fatalf("equivalent JSON identity: %v %v", second, err)
	}
	third, err := sessionIdempotencyIdentity([]byte("key"), []byte(`{"a":1,"b":[3,2]}`))
	if err != nil || first.RequestDigest == third.RequestDigest {
		t.Fatal("array order must affect digest")
	}
	for _, key := range []string{"has space", "has\tcontrol", "中文", fmt.Sprintf("%0256d", 1)} {
		if _, err := sessionIdempotencyIdentity([]byte(key), []byte(`{}`)); !errors.Is(err, service.ErrInvalid) {
			t.Fatalf("invalid key accepted: %v", err)
		}
	}
}
