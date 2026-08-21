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
	events, processed, nextSeq, err := l.loadProjection(ctx, sessionID, afterSeq)
	if err != nil {
		return nil, afterSeq, err
	}
	for _, value := range events {
		if id, ok := value["id"].(string); ok {
			if timestamp, found := processed[id]; found && value["processed_at"] == nil {
				value["processed_at"] = timestamp
			}
		}
	}
	return events, nextSeq, nil
}

func (l *Log) loadProjection(
	ctx context.Context,
	sessionID string,
	afterSeq int64,
) ([]ManagedEvent, map[string]string, int64, error) {
	lane, exists, err := l.store.FindLane(ctx, sessionID, managedEventRun, managedEventLane)
	if err != nil {
		return nil, nil, afterSeq, fmt.Errorf("find managed event lane: %w", err)
	}
	if !exists {
		return []ManagedEvent{}, map[string]string{}, afterSeq, nil
	}

	events := make([]ManagedEvent, 0)
	processed := make(map[string]string)
	nextSeq := afterSeq
	for stored, loadErr := range l.store.LoadLane(ctx, lane.ID, afterSeq) {
		if loadErr != nil {
			return nil, nil, afterSeq, fmt.Errorf("load managed events: %w", loadErr)
		}
		nextSeq = stored.Seq
		raw, ok := stored.Payload["event"]
		if !ok {
			continue
		}
		value, mapErr := mapEvent(raw)
		if mapErr != nil {
			return nil, nil, afterSeq, mapErr
		}
		if value["type"] == "managed.event_processed" {
			if target, ok := value["event_id"].(string); ok {
				processed[target], _ = value["processed_at"].(string)
			}
			continue
		}
		events = append(events, value)
	}
	return events, processed, nextSeq, nil
}

// PendingInputs returns accepted ingress Events not yet consumed by Agentlet.
// Public processed_at and internal consumption are deliberately independent:
// confirmation/result Events are acknowledged on receipt but remain executable
// until managed.event_processed is durable.
func (l *Log) PendingInputs(ctx context.Context, sessionID string) ([]ManagedEvent, error) {
	events, processed, _, err := l.loadProjection(ctx, sessionID, 0)
	if err != nil {
		return nil, err
	}
	pending := make([]ManagedEvent, 0)
	resolutions := make([]ManagedEvent, 0)
	for _, value := range events {
		id, _ := value["id"].(string)
		eventType, _ := value["type"].(string)
		if !strings.HasPrefix(eventType, "user.") || eventType == "user.interrupt" || processed[id] != "" {
			continue
		}
		if eventType == "user.tool_confirmation" || eventType == "user.tool_result" {
			resolutions = append(resolutions, value)
		} else {
			pending = append(pending, value)
		}
	}
	if len(resolutions) > 0 {
		return resolutions, nil
	}
	blocking := unresolvedToolUses(events)
	if len(blocking) > 0 {
		return []ManagedEvent{}, nil
	}
	return pending, nil
}

// UnprocessedUserMessages remains as a source-compatible alias for callers
// migrating to the broader PendingInputs ingress contract.
func (l *Log) UnprocessedUserMessages(ctx context.Context, sessionID string) ([]ManagedEvent, error) {
	return l.PendingInputs(ctx, sessionID)
}

// UnresolvedToolUses projects the latest requires_action stop into its still
// unanswered agent.tool_use Events. Resolution receipt, rather than Agentlet
// consumption, removes a blocker from the public protocol view.
func (l *Log) UnresolvedToolUses(ctx context.Context, sessionID string) ([]ManagedEvent, error) {
	events, err := l.List(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	return unresolvedToolUses(events), nil
}

func unresolvedToolUses(events []ManagedEvent) []ManagedEvent {
	toolUses := make(map[string]ManagedEvent)
	resolved := make(map[string]bool)
	var required []string
	for _, value := range events {
		eventType, _ := value["type"].(string)
		switch eventType {
		case "agent.tool_use":
			id, _ := value["id"].(string)
			toolUses[id] = value
		case "user.tool_confirmation", "user.tool_result":
			toolUseID, _ := value["tool_use_id"].(string)
			resolved[toolUseID] = true
		case "session.status_idle":
			required = requiredEventIDs(value["stop_reason"])
		case "session.status_running", "session.status_terminated":
			required = nil
		}
	}
	result := make([]ManagedEvent, 0, len(required))
	for _, id := range required {
		if value, ok := toolUses[id]; ok && !resolved[id] {
			result = append(result, value)
		}
	}
	return result
}

func requiredEventIDs(raw any) []string {
	stopReason, ok := raw.(map[string]any)
	if !ok || stopReason["type"] != "requires_action" {
		return nil
	}
	values, ok := stopReason["event_ids"].([]any)
	if !ok {
		if typed, typedOK := stopReason["event_ids"].([]string); typedOK {
			return typed
		}
		return nil
	}
	result := make([]string, 0, len(values))
	for _, value := range values {
		if id, ok := value.(string); ok {
			result = append(result, id)
		}
	}
	return result
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
