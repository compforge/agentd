package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	agentledger "github.com/compforge/agent-ledger/go"
	"github.com/compforge/agentd/agentlet/internal/harness"
)

const (
	managedEventRun  = "managed-control"
	managedEventLane = "managed/events"
)

type EventLog struct {
	store agentledger.EventStore

	mu                sync.Mutex
	userActor         agentledger.Actor
	orchestratorActor agentledger.Actor
}

func NewEventLog(store agentledger.EventStore) *EventLog {
	return &EventLog{
		store:     store,
		userActor: agentledger.NewActor("user", ""), orchestratorActor: agentledger.NewActor("orchestrator", "agentd"),
	}
}

func (l *EventLog) Append(ctx context.Context, sessionID string, event ManagedEvent) error {
	cloned, err := cloneEvent(event)
	if err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	recorder, err := agentledger.OpenRecorder(ctx, agentledger.RecorderOptions{
		Store: l.store, SessionID: sessionID, RunID: managedEventRun,
		LaneName: managedEventLane, Actor: l.actorFor(cloned),
	})
	if err != nil {
		return fmt.Errorf("open managed event recorder: %w", err)
	}
	for stored, loadErr := range l.store.LoadLane(ctx, recorder.Lane().ID, 0) {
		if loadErr != nil {
			return fmt.Errorf("load managed event lane: %w", loadErr)
		}
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
	if _, err := recorder.Record(ctx, "lane.managed.api_event", recorder.Lane().ID, map[string]any{
		"event": map[string]any(cloned),
	}, ""); err != nil {
		return fmt.Errorf("append managed event: %w", err)
	}
	return nil
}

func (l *EventLog) MarkProcessed(ctx context.Context, sessionID, eventID string) error {
	return l.Append(ctx, sessionID, NewManagedEvent("managed.event_processed", map[string]any{
		"event_id": eventID,
	}))
}

func (l *EventLog) List(ctx context.Context, sessionID string) ([]ManagedEvent, error) {
	events, _, err := l.Load(ctx, sessionID, 0)
	return events, err
}

// Load returns public Events committed after a durable lane sequence. The
// cursor advances across internal projection records as well as public Events.
func (l *EventLog) Load(ctx context.Context, sessionID string, afterSeq int64) ([]ManagedEvent, int64, error) {
	lane, exists, err := l.store.FindLane(ctx, sessionID, managedEventRun, managedEventLane)
	if err != nil {
		return nil, afterSeq, fmt.Errorf("find managed event lane: %w", err)
	}
	if !exists {
		return []ManagedEvent{}, afterSeq, nil
	}
	events := make([]ManagedEvent, 0)
	processed := make(map[string]string)
	nextSeq := afterSeq
	for stored, err := range l.store.LoadLane(ctx, lane.ID, afterSeq) {
		if err != nil {
			return nil, afterSeq, fmt.Errorf("load managed events: %w", err)
		}
		nextSeq = stored.Seq
		raw, ok := stored.Payload["event"]
		if !ok {
			continue
		}
		event, err := mapEvent(raw)
		if err != nil {
			return nil, afterSeq, err
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
	return events, nextSeq, nil
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

func NewManagedEvent(eventType string, fields map[string]any) ManagedEvent {
	return harness.NewManagedEvent(eventType, fields)
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

func (l *EventLog) actorFor(event ManagedEvent) agentledger.Actor {
	eventType, _ := event["type"].(string)
	if strings.HasPrefix(eventType, "user.") {
		return l.userActor
	}
	return l.orchestratorActor
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
