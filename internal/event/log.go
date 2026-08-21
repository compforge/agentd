// Package event owns the durable Managed Event projection shared by agentd and Agentlet.
package event

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	agentledger "github.com/compforge/agent-ledger/go"
)

const (
	managedEventRun   = "managed-control"
	managedEventLane  = "managed/events"
	ingressActorKey   = "agentd/managed-api/ingress"
	executionActorKey = "agentd/managed-api/execution"
	maxAppendAttempts = 4
)

type ManagedEvent map[string]any

// Log is the only owner of the Managed Event lane schema and projection rules.
// Callers retain ownership of facts through the explicit ingress and execution
// append methods.
type Log struct {
	store agentledger.EventStore

	mu             sync.Mutex
	ingressActor   agentledger.Actor
	executionActor agentledger.Actor
}

func NewLog(store agentledger.EventStore) *Log {
	return &Log{
		store:          store,
		ingressActor:   agentledger.NewActorWithKey(ingressActorKey, "user", "managed-api"),
		executionActor: agentledger.NewActorWithKey(executionActorKey, "orchestrator", "agentd"),
	}
}

func New(eventType string, fields map[string]any) ManagedEvent {
	value := ManagedEvent{
		"id":           "event_" + agentledger.NewID(),
		"type":         eventType,
		"processed_at": time.Now().UTC().Format(time.RFC3339Nano),
	}
	for key, field := range fields {
		value[key] = field
	}
	return value
}

// NewTurn gives replayable execution output a stable identity.
func NewTurn(inputID, eventType string, fields map[string]any) ManagedEvent {
	value := New(eventType, fields)
	digest := sha256.Sum256([]byte(inputID + "\x00" + eventType))
	value["id"] = fmt.Sprintf("event_%x", digest[:12])
	return value
}

// AppendIngress records only facts accepted by agentd's public API boundary.
// +spec=`agentd writes user ingress facts and Agentlet writes execution facts even when both share one Ledger store`
// +link=`agentd/docs/kernel.md`
func (l *Log) AppendIngress(ctx context.Context, sessionID string, value ManagedEvent) error {
	if !hasTypePrefix(value, "user.") {
		return errors.New("append ingress event: event type must start with user.")
	}
	return l.append(ctx, sessionID, value, l.ingressActor)
}

// AppendExecution rejects ingress facts so Agentlet cannot impersonate agentd.
func (l *Log) AppendExecution(ctx context.Context, sessionID string, value ManagedEvent) error {
	if hasTypePrefix(value, "user.") {
		return errors.New("append execution event: event type must not start with user.")
	}
	return l.append(ctx, sessionID, value, l.executionActor)
}

func (l *Log) MarkProcessed(ctx context.Context, sessionID, eventID string) error {
	return l.AppendExecution(ctx, sessionID, New("managed.event_processed", map[string]any{
		"event_id": eventID,
	}))
}

func (l *Log) List(ctx context.Context, sessionID string) ([]ManagedEvent, error) {
	events, _, err := l.Load(ctx, sessionID, 0)
	return events, err
}

// Load returns public Events committed after a durable lane sequence. The
// cursor advances across internal projection records as well as public Events.
func (l *Log) Load(ctx context.Context, sessionID string, afterSeq int64) ([]ManagedEvent, int64, error) {
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
	for stored, loadErr := range l.store.LoadLane(ctx, lane.ID, afterSeq) {
		if loadErr != nil {
			return nil, afterSeq, fmt.Errorf("load managed events: %w", loadErr)
		}
		nextSeq = stored.Seq
		raw, ok := stored.Payload["event"]
		if !ok {
			continue
		}
		value, mapErr := mapEvent(raw)
		if mapErr != nil {
			return nil, afterSeq, mapErr
		}
		if value["type"] == "managed.event_processed" {
			if target, ok := value["event_id"].(string); ok {
				processed[target], _ = value["processed_at"].(string)
			}
			continue
		}
		events = append(events, value)
	}
	for _, value := range events {
		if id, ok := value["id"].(string); ok {
			if timestamp, found := processed[id]; found {
				value["processed_at"] = timestamp
			}
		}
	}
	return events, nextSeq, nil
}

func (l *Log) UnprocessedUserMessages(ctx context.Context, sessionID string) ([]ManagedEvent, error) {
	events, err := l.List(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	pending := make([]ManagedEvent, 0)
	for _, value := range events {
		if value["type"] == "user.message" && value["processed_at"] == nil {
			pending = append(pending, value)
		}
	}
	return pending, nil
}

func (l *Log) append(ctx context.Context, sessionID string, value ManagedEvent, actor agentledger.Actor) error {
	cloned, err := cloneEvent(value)
	if err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	for attempt := 0; attempt < maxAppendAttempts; attempt++ {
		err = l.appendOnce(ctx, sessionID, cloned, actor)
		if !errors.Is(err, agentledger.ErrLaneConflict) {
			return err
		}
	}
	return fmt.Errorf("append managed event after %d lane conflicts: %w", maxAppendAttempts, err)
}

func (l *Log) appendOnce(ctx context.Context, sessionID string, value ManagedEvent, actor agentledger.Actor) error {
	recorder, err := agentledger.OpenRecorder(ctx, agentledger.RecorderOptions{
		Store: l.store, SessionID: sessionID, RunID: managedEventRun,
		LaneName: managedEventLane, Actor: actor,
	})
	if err != nil {
		return fmt.Errorf("open managed event recorder: %w", err)
	}
	for stored, loadErr := range l.store.LoadLane(ctx, recorder.Lane().ID, 0) {
		if loadErr != nil {
			return fmt.Errorf("load managed event lane: %w", loadErr)
		}
		eventID, _ := value["id"].(string)
		if eventID == "" {
			continue
		}
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
	if _, err := recorder.Record(ctx, "lane.managed.api_event", recorder.Lane().ID, map[string]any{
		"event": map[string]any(value),
	}, ""); err != nil {
		return fmt.Errorf("append managed event: %w", err)
	}
	return nil
}

func hasTypePrefix(value ManagedEvent, prefix string) bool {
	eventType, _ := value["type"].(string)
	return strings.HasPrefix(eventType, prefix)
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

func cloneEvent(value ManagedEvent) (ManagedEvent, error) {
	return mapEvent(value)
}
