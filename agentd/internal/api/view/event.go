// Package view defines the Claude Managed Agents API representation.
// These types are transport contracts, not agentd domain or database models.
package view

import (
	"encoding/json"
	"errors"
	"fmt"
)

const MaxEventsPerRequest = 50

var (
	ErrInvalid     = errors.New("invalid API view")
	ErrUnsupported = errors.New("unsupported API view")
)

type SendEventsRequest struct {
	Events []json.RawMessage `json:"events"`
}

type IngressEvent struct {
	Type        string
	Content     any
	ToolUseID   string
	Result      string
	DenyMessage string
	IsError     bool
}

type Page[T any] struct {
	Data     []T `json:"data"`
	NextPage any `json:"next_page"`
}

func DecodeIngressEvents(rawEvents []json.RawMessage) ([]IngressEvent, error) {
	if len(rawEvents) == 0 {
		return nil, fmt.Errorf("%w: events must not be empty", ErrInvalid)
	}
	if len(rawEvents) > MaxEventsPerRequest {
		return nil, fmt.Errorf("%w: at most %d events may be sent at once", ErrInvalid, MaxEventsPerRequest)
	}
	events := make([]IngressEvent, 0, len(rawEvents))
	for _, raw := range rawEvents {
		var value struct {
			Type        string          `json:"type"`
			Content     json.RawMessage `json:"content"`
			ToolUseID   string          `json:"tool_use_id"`
			Result      string          `json:"result"`
			DenyMessage string          `json:"deny_message"`
			IsError     bool            `json:"is_error"`
		}
		if err := json.Unmarshal(raw, &value); err != nil {
			return nil, fmt.Errorf("%w: decode session event: %v", ErrInvalid, err)
		}
		switch value.Type {
		case "user.message":
			var content []map[string]any
			if len(value.Content) == 0 || json.Unmarshal(value.Content, &content) != nil || len(content) == 0 {
				return nil, fmt.Errorf("%w: user.message content must not be empty", ErrInvalid)
			}
			for _, block := range content {
				if block["type"] != "text" {
					return nil, fmt.Errorf("%w: user.message content type %q", ErrUnsupported, block["type"])
				}
			}
			events = append(events, IngressEvent{Type: value.Type, Content: content})
		case "user.interrupt":
			events = append(events, IngressEvent{Type: value.Type})
		case "user.tool_confirmation":
			if value.ToolUseID == "" || (value.Result != "allow" && value.Result != "deny") {
				return nil, fmt.Errorf("%w: tool confirmation requires tool_use_id and result allow or deny", ErrInvalid)
			}
			if value.Result == "allow" && value.DenyMessage != "" {
				return nil, fmt.Errorf("%w: deny_message is only valid for a deny result", ErrInvalid)
			}
			events = append(events, IngressEvent{
				Type: value.Type, ToolUseID: value.ToolUseID, Result: value.Result, DenyMessage: value.DenyMessage,
			})
		case "user.tool_result":
			if value.ToolUseID == "" {
				return nil, fmt.Errorf("%w: tool result requires tool_use_id", ErrInvalid)
			}
			var content []map[string]any
			if len(value.Content) > 0 && string(value.Content) != "null" {
				if err := json.Unmarshal(value.Content, &content); err != nil {
					return nil, fmt.Errorf("%w: tool result content must be an array of content blocks: %v", ErrInvalid, err)
				}
			}
			events = append(events, IngressEvent{
				Type: value.Type, ToolUseID: value.ToolUseID, Content: content, IsError: value.IsError,
			})
		default:
			return nil, fmt.Errorf("%w: event type %q", ErrUnsupported, value.Type)
		}
	}
	return events, nil
}
