package view

import (
	"encoding/json"
	"testing"
)

func TestDecodeToolResolutionEvents(t *testing.T) {
	events, err := DecodeIngressEvents([]json.RawMessage{
		json.RawMessage(`{"type":"user.tool_confirmation","tool_use_id":"event_attempt-1","result":"allow"}`),
		json.RawMessage(`{"type":"user.tool_result","tool_use_id":"event_attempt-2","content":[{"type":"text","text":"ok"}],"is_error":false}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].ToolUseID != "event_attempt-1" || events[0].Result != "allow" {
		t.Fatalf("confirmation = %#v", events)
	}
	content, ok := events[1].Content.([]map[string]any)
	if !ok || len(content) != 1 || content[0]["text"] != "ok" {
		t.Fatalf("tool result content = %#v", events[1].Content)
	}
}

func TestDecodeToolConfirmationRejectsInvalidDecision(t *testing.T) {
	_, err := DecodeIngressEvents([]json.RawMessage{
		json.RawMessage(`{"type":"user.tool_confirmation","tool_use_id":"event_attempt-1","result":"retry"}`),
	})
	if err == nil {
		t.Fatal("invalid tool confirmation was accepted")
	}
}
