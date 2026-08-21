package api

import (
	"errors"
	"testing"

	"github.com/compforge/agentd/agentd/internal/api/view"
	"github.com/compforge/agentd/agentd/internal/service"
)

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
