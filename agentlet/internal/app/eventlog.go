package app

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	agentledger "github.com/compforge/agent-ledger/go"
	"github.com/compforge/agentd/agentlet/internal/execution"
)

const managedEventStream = "managed/events"

type EventLog struct {
	store agentledger.EventStore

	mu          sync.Mutex
	subscribers map[string]map[chan ManagedEvent]struct{}
}

func NewEventLog(store agentledger.EventStore) *EventLog {
	return &EventLog{store: store, subscribers: make(map[string]map[chan ManagedEvent]struct{})}
}

func (l *EventLog) Append(ctx context.Context, sessionID string, event ManagedEvent) error {
	cloned, err := cloneEvent(event)
	if err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	stream := agentledger.EventStream{SessionID: sessionID, StreamID: managedEventStream}
	expected := int64(-1)
	for stored, loadErr := range l.store.Load(ctx, stream, -1) {
		if loadErr != nil {
			return fmt.Errorf("load managed event stream: %w", loadErr)
		}
		expected = stored.StreamVersion
		if eventID, _ := cloned["id"].(string); eventID != "" {
			raw, ok := stored.Payload["event"]
			if !ok {
				continue
			}
			existing, mapErr := mapEvent(raw)
			if mapErr != nil {
				return mapErr
			}
			if existingID, _ := existing["id"].(string); existingID == eventID {
				return nil
			}
		}
	}
	record := agentledger.NewEvent("managed.api_event", sessionID, "managed-control", actorFor(cloned))
	record.Payload = map[string]any{"event": map[string]any(cloned)}
	appendID, _ := cloned["id"].(string)
	if appendID == "" {
		appendID = agentledger.NewID()
	}
	if _, err := l.store.Append(ctx, stream, expected, "api/"+appendID, record); err != nil {
		return fmt.Errorf("append managed event: %w", err)
	}
	if cloned["type"] != "managed.event_processed" {
		for channel := range l.subscribers[sessionID] {
			select {
			case channel <- cloned:
			default:
			}
		}
	}
	return nil
}

func (l *EventLog) MarkProcessed(ctx context.Context, sessionID, eventID string) error {
	return l.Append(ctx, sessionID, NewManagedEvent("managed.event_processed", map[string]any{
		"event_id": eventID,
	}))
}

func (l *EventLog) List(ctx context.Context, sessionID string) ([]ManagedEvent, error) {
	stream := agentledger.EventStream{SessionID: sessionID, StreamID: managedEventStream}
	var events []ManagedEvent
	processed := make(map[string]string)
	for stored, err := range l.store.Load(ctx, stream, -1) {
		if err != nil {
			return nil, fmt.Errorf("list managed events: %w", err)
		}
		raw, ok := stored.Payload["event"]
		if !ok {
			continue
		}
		event, err := mapEvent(raw)
		if err != nil {
			return nil, err
		}
		if event["type"] == "managed.event_processed" {
			if target, ok := event["event_id"].(string); ok {
				processed[target], _ = event["processed_at"].(string)
			}
			continue
		}
		events = append(events, event)
	}
	for _, event := range events {
		if id, ok := event["id"].(string); ok {
			if timestamp, found := processed[id]; found {
				event["processed_at"] = timestamp
			}
		}
	}
	return events, nil
}

func (l *EventLog) UnprocessedUserMessages(ctx context.Context, sessionID string) ([]ManagedEvent, error) {
	events, err := l.List(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	var pending []ManagedEvent
	for _, event := range events {
		if event["type"] == "user.message" && event["processed_at"] == nil {
			pending = append(pending, event)
		}
	}
	return pending, nil
}

func (l *EventLog) Subscribe(sessionID string) (<-chan ManagedEvent, func()) {
	channel := make(chan ManagedEvent, 32)
	l.mu.Lock()
	if l.subscribers[sessionID] == nil {
		l.subscribers[sessionID] = make(map[chan ManagedEvent]struct{})
	}
	l.subscribers[sessionID][channel] = struct{}{}
	l.mu.Unlock()
	return channel, func() {
		l.mu.Lock()
		delete(l.subscribers[sessionID], channel)
		close(channel)
		l.mu.Unlock()
	}
}

func NewManagedEvent(eventType string, fields map[string]any) ManagedEvent {
	return execution.NewManagedEvent(eventType, fields)
}

// NewTurnEvent gives replayable control events a stable identity. EventLog
// suppresses an already persisted ID, so recovery can safely repeat projection
// before it marks the source input processed.
func NewTurnEvent(inputID, eventType string, fields map[string]any) ManagedEvent {
	event := NewManagedEvent(eventType, fields)
	digest := sha256.Sum256([]byte(inputID + "\x00" + eventType))
	event["id"] = fmt.Sprintf("event_%x", digest[:12])
	return event
}

func actorFor(event ManagedEvent) agentledger.Actor {
	eventType, _ := event["type"].(string)
	if strings.HasPrefix(eventType, "user.") {
		return agentledger.Actor{Type: "human", ID: "api-client"}
	}
	return agentledger.Actor{Type: "orchestrator", ID: "agentd"}
}

func mapEvent(value any) (ManagedEvent, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("encode managed event: %w", err)
	}
	var event ManagedEvent
	if err := json.Unmarshal(encoded, &event); err != nil {
		return nil, fmt.Errorf("decode managed event: %w", err)
	}
	return event, nil
}

func cloneEvent(event ManagedEvent) (ManagedEvent, error) {
	return mapEvent(event)
}
