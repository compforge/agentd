package api

import (
	"context"
	"errors"
	"testing"

	agentledger "github.com/compforge/agent-ledger/go"
	"github.com/compforge/agentd/agentd/internal/api/view"
	"github.com/compforge/agentd/agentd/internal/service"
	managedevent "github.com/compforge/agentd/internal/event"
)

func TestParseStainlessRetryCount(t *testing.T) {
	for _, test := range []struct {
		name    string
		raw     string
		want    int
		wantErr bool
	}{
		{name: "absent", want: 0},
		{name: "first attempt", raw: "0", want: 0},
		{name: "retry", raw: "2", want: 2},
		{name: "negative", raw: "-1", wantErr: true},
		{name: "invalid", raw: "retry", wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := parseStainlessRetryCount([]byte(test.raw))
			if (err != nil) != test.wantErr || got != test.want {
				t.Fatalf("parseStainlessRetryCount(%q) = %d, %v; want %d, error=%t", test.raw, got, err, test.want, test.wantErr)
			}
		})
	}
}

func TestLatestRetriedUserMessageMatchesOnlyMostRecentMessage(t *testing.T) {
	ctx := context.Background()
	events := managedevent.NewLog(agentledger.NewMemoryEventStore())
	server := &Server{events: events}
	first := managedevent.New("user.message", map[string]any{
		"content": []map[string]any{{"type": "text", "text": "same"}},
	})
	latest := managedevent.New("user.message", map[string]any{
		"content": []map[string]any{{"type": "text", "text": "latest"}},
	})
	if err := events.AppendIngressBatch(ctx, "session-1", []managedevent.ManagedEvent{first, latest}); err != nil {
		t.Fatal(err)
	}

	reused, ok, err := server.latestRetriedUserMessage(ctx, "session-1", []view.IngressEvent{{
		Type: "user.message", Content: []map[string]any{{"type": "text", "text": "latest"}},
	}})
	if err != nil || !ok || reused["id"] != latest["id"] {
		t.Fatalf("latest retry = %#v, %t, %v; want Event %q", reused, ok, err, latest["id"])
	}

	_, ok, err = server.latestRetriedUserMessage(ctx, "session-1", []view.IngressEvent{{
		Type: "user.message", Content: []map[string]any{{"type": "text", "text": "same"}},
	}})
	if err != nil || ok {
		t.Fatalf("older matching message was reused: ok=%t err=%v", ok, err)
	}
	_, ok, err = server.latestRetriedUserMessage(ctx, "session-1", []view.IngressEvent{
		{Type: "user.message", Content: []map[string]any{{"type": "text", "text": "latest"}}},
		{Type: "user.message", Content: []map[string]any{{"type": "text", "text": "second"}}},
	})
	if err != nil || ok {
		t.Fatalf("batch retry was reused: ok=%t err=%v", ok, err)
	}
}

func TestValidateIngressSequenceAllowsMessageAfterResolution(t *testing.T) {
	err := validateIngressSequence([]view.IngressEvent{
		{Type: "user.tool_confirmation", ToolUseID: "event_tool-1", Result: "allow"},
		{Type: "user.message", Content: []map[string]any{{"type": "text", "text": "continue"}}},
	}, map[string]bool{"event_tool-1": true})
	if err != nil {
		t.Fatalf("confirmation followed by message was rejected: %v", err)
	}
}

func TestValidateIngressSequenceRejectsMessageBeforeResolution(t *testing.T) {
	err := validateIngressSequence([]view.IngressEvent{
		{Type: "user.message", Content: []map[string]any{{"type": "text", "text": "continue"}}},
		{Type: "user.tool_confirmation", ToolUseID: "event_tool-1", Result: "allow"},
	}, map[string]bool{"event_tool-1": true})
	if !errors.Is(err, service.ErrConflict) {
		t.Fatalf("error = %v, want ErrConflict", err)
	}
}
