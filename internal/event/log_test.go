package event

import (
	"context"
	"testing"

	agentledger "github.com/compforge/agent-ledger/go"
)

func TestLogSeparatesIngressAndExecutionWriters(t *testing.T) {
	ctx := context.Background()
	log := NewLog(agentledger.NewMemoryEventStore())

	if err := log.AppendIngress(ctx, "session-1", New("agent.message", nil)); err == nil {
		t.Fatal("AppendIngress accepted an execution Event")
	}
	if err := log.AppendExecution(ctx, "session-1", New("user.message", nil)); err == nil {
		t.Fatal("AppendExecution accepted an ingress Event")
	}
}

func TestAppendIngressBatchValidatesBeforeAtomicOrderedAppend(t *testing.T) {
	ctx := context.Background()
	log := NewLog(agentledger.NewMemoryEventStore())
	first := New("user.message", map[string]any{"content": []any{map[string]any{"type": "text", "text": "first"}}})
	second := New("user.message", map[string]any{"content": []any{map[string]any{"type": "text", "text": "second"}}})
	invalid := New("agent.message", nil)
	if err := log.AppendIngressBatch(ctx, "session-1", []ManagedEvent{first, invalid}); err == nil {
		t.Fatal("AppendIngressBatch accepted an invalid mixed-writer batch")
	}
	events, err := log.List(ctx, "session-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 0 {
		t.Fatalf("invalid ingress batch partially persisted: %#v", events)
	}

	batch := []ManagedEvent{first, second}
	if err := log.AppendIngressBatch(ctx, "session-1", batch); err != nil {
		t.Fatal(err)
	}
	if err := log.AppendIngressBatch(ctx, "session-1", batch); err != nil {
		t.Fatalf("idempotent batch retry: %v", err)
	}
	events, err = log.List(ctx, "session-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0]["id"] != first["id"] || events[1]["id"] != second["id"] {
		t.Fatalf("ordered ingress batch = %#v", events)
	}
}

func TestLogProjectsEventsAcrossProcesses(t *testing.T) {
	ctx := context.Background()
	store := agentledger.NewMemoryEventStore()
	ingress := NewLog(store)
	execution := NewLog(store)

	input := New("user.message", map[string]any{
		"content": []map[string]any{{"type": "text", "text": "hello"}},
	})
	input["processed_at"] = nil
	if err := ingress.AppendIngress(ctx, "session-1", input); err != nil {
		t.Fatal(err)
	}
	if err := execution.AppendIngress(ctx, "session-1", New("user.message", map[string]any{
		"content": []map[string]any{{"type": "text", "text": "follow-up"}},
	})); err != nil {
		t.Fatal(err)
	}
	if err := execution.AppendExecution(ctx, "session-1", NewTurn(
		input["id"].(string), "agent.message",
		map[string]any{"content": []map[string]any{{"type": "text", "text": "done"}}},
	)); err != nil {
		t.Fatal(err)
	}
	if err := execution.MarkProcessed(ctx, "session-1", input["id"].(string)); err != nil {
		t.Fatal(err)
	}

	events, err := ingress.List(ctx, "session-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 3 || events[0]["type"] != "user.message" || events[1]["type"] != "user.message" ||
		events[2]["type"] != "agent.message" {
		t.Fatalf("projected Events = %#v", events)
	}
	if events[0]["processed_at"] == nil {
		t.Fatal("processed ingress Event remained pending")
	}
	view, err := store.LoadSession(ctx, "session-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Actors) != 2 {
		t.Fatalf("actors = %#v, want one stable ingress Actor and one execution Actor", view.Actors)
	}
}

func TestPendingToolResolutionUsesInternalConsumptionMarker(t *testing.T) {
	ctx := context.Background()
	log := NewLog(agentledger.NewMemoryEventStore())
	toolUse := New("agent.tool_use", map[string]any{"name": "write", "input": map[string]any{}})
	toolUse["id"] = "event_attempt-1"
	if err := log.AppendExecution(ctx, "session-1", toolUse); err != nil {
		t.Fatal(err)
	}
	if err := log.AppendExecution(ctx, "session-1", New("session.status_idle", map[string]any{
		"stop_reason": map[string]any{"type": "requires_action", "event_ids": []string{"event_attempt-1"}},
	})); err != nil {
		t.Fatal(err)
	}
	queued := New("user.message", map[string]any{"content": []map[string]any{{"type": "text", "text": "later"}}})
	queued["processed_at"] = nil
	if err := log.AppendIngress(ctx, "session-1", queued); err != nil {
		t.Fatal(err)
	}
	pending, err := log.PendingInputs(ctx, "session-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 {
		t.Fatalf("queued message crossed required action: %#v", pending)
	}
	confirmation := New("user.tool_confirmation", map[string]any{
		"tool_use_id": "event_attempt-1", "result": "allow",
	})
	if err := log.AppendIngress(ctx, "session-1", confirmation); err != nil {
		t.Fatal(err)
	}
	pending, err = log.PendingInputs(ctx, "session-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0]["id"] != confirmation["id"] || pending[0]["processed_at"] == nil {
		t.Fatalf("pending resolution = %#v", pending)
	}
	if err := log.MarkProcessed(ctx, "session-1", confirmation["id"].(string)); err != nil {
		t.Fatal(err)
	}
	pending, err = log.PendingInputs(ctx, "session-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0]["id"] != queued["id"] {
		t.Fatalf("queued input was not released after resolution: %#v", pending)
	}
}

func TestSessionUsageProjectsTerminalModelAttempts(t *testing.T) {
	ctx := context.Background()
	store := agentledger.NewMemoryEventStore()
	log := NewLog(store)
	actor, err := store.EnsureActor(ctx, agentledger.NewActorWithKey("test/harness", "agent", "test"))
	if err != nil {
		t.Fatal(err)
	}
	recorder, err := agentledger.OpenRecorder(ctx, agentledger.RecorderOptions{
		Store: store, SessionID: "session-1", RunID: "run-1", LaneName: "main", Actor: actor,
	})
	if err != nil {
		t.Fatal(err)
	}
	turn, err := recorder.StartTurn(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	modelAttempt, err := recorder.BeforeModelCall(ctx, turn.ID, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := recorder.ModelCompleted(ctx, modelAttempt, map[string]any{"usage": map[string]any{
		"input_tokens": 12, "output_tokens": 5,
		"cache_read_input_tokens": 7, "cache_write_input_tokens": 3,
	}}); err != nil {
		t.Fatal(err)
	}
	failedAttempt, err := recorder.BeforeModelCall(ctx, turn.ID, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := recorder.ModelFailed(ctx, failedAttempt, context.DeadlineExceeded, map[string]any{
		"usage": map[string]any{"input_tokens": 2, "cache_read_input_tokens": 1},
	}); err != nil {
		t.Fatal(err)
	}
	toolAttempt, err := recorder.BeforeToolCall(ctx, turn.ID, "tool-call-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := recorder.ToolCompleted(ctx, toolAttempt, map[string]any{"usage": map[string]any{
		"input_tokens": 100, "output_tokens": 100,
	}}); err != nil {
		t.Fatal(err)
	}

	usage, err := log.SessionUsage(ctx, "session-1")
	if err != nil {
		t.Fatal(err)
	}
	want := SessionUsage{
		InputTokens: 14, OutputTokens: 5, CacheReadInputTokens: 8, CacheWriteInputTokens: 3,
	}
	if usage != want {
		t.Fatalf("Session usage = %#v, want %#v", usage, want)
	}
}
