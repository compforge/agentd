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
	Type    string
	Content []map[string]any
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
			Type    string           `json:"type"`
			Content []map[string]any `json:"content"`
		}
		if err := json.Unmarshal(raw, &value); err != nil {
			return nil, fmt.Errorf("%w: decode session event: %v", ErrInvalid, err)
		}
		switch value.Type {
		case "user.message":
			if len(value.Content) == 0 {
				return nil, fmt.Errorf("%w: user.message content must not be empty", ErrInvalid)
			}
			for _, block := range value.Content {
				if block["type"] != "text" {
					return nil, fmt.Errorf("%w: user.message content type %q", ErrUnsupported, block["type"])
				}
			}
		case "user.interrupt":
		default:
			return nil, fmt.Errorf("%w: event type %q", ErrUnsupported, value.Type)
		}
		events = append(events, IngressEvent{Type: value.Type, Content: value.Content})
	}
	return events, nil
}
