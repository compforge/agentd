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
