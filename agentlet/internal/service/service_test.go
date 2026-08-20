package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	agentledger "github.com/compforge/agent-ledger/go"
	"github.com/compforge/agentd/agentlet/internal/harness"
	"github.com/compforge/agentd/internal/executionapi"
)

func TestEventLogAppendIsIdempotentByEventID(t *testing.T) {
	events := NewEventLog(agentledger.NewMemoryEventStore())
	event := NewTurnEvent("input-1", "agent.message", map[string]any{"content": "done"})
	if err := events.Append(context.Background(), "session-1", event); err != nil {
		t.Fatal(err)
	}
	if err := events.Append(context.Background(), "session-1", event); err != nil {
		t.Fatal(err)
	}
	stored, err := events.List(context.Background(), "session-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(stored) != 1 {
		t.Fatalf("stored events = %d, want 1", len(stored))
	}
}

func TestApplyWorkSpecCachesSnapshotAndEnforcesAssignmentFence(t *testing.T) {
	ctx := context.Background()
	repository := NewMemoryRepository()
	application := New(repository, NewEventLog(agentledger.NewMemoryEventStore()), recordingHarness{})
	now := time.Now().UTC()
	spec := executionapi.WorkSpec{
		AssignmentID: "assignment-1",
		WorkerID:     "worker-1",
		Session: executionapi.SessionSnapshot{
			ID: "session-1", EnvironmentID: "env-1", Status: "rescheduling",
			Harness: "recording", HarnessVersion: "test", CreatedAt: now, UpdatedAt: now,
		},
		Agent: executionapi.AgentSnapshot{
			ID: "agent-1", Name: "test", ModelID: "model-1", Version: 1,
		},
		Environment: executionapi.EnvironmentSnapshot{
			ID: "env-1", Config: map[string]any{"type": "cloud"},
		},
	}

	session, err := application.ApplyWorkSpec(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	if session.Control.AssignmentID != "assignment-1" || session.Control.WorkerID != "worker-1" ||
		session.Control.ResumeRef != "recording/session-1" {
		t.Fatalf("cached Session = %#v", session)
	}
	if err := application.ValidateAssignment(ctx, "session-1", "worker-1", "assignment-1"); err != nil {
		t.Fatalf("validate current Assignment: %v", err)
	}
	if err := application.ValidateAssignment(ctx, "session-1", "worker-1", "stale"); !errors.Is(err, ErrConflict) {
		t.Fatalf("validate stale Assignment error = %v, want ErrConflict", err)
	}

	session.Control.Status = "idle"
	session.Control.ResumeRef = "checkpoint-7"
	session.Control.ResumeRevision = 7
	if err := repository.PutSession(ctx, session); err != nil {
		t.Fatal(err)
	}
	repeated, err := application.ApplyWorkSpec(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	if repeated.Control.Status != "idle" || repeated.Control.ResumeRef != "checkpoint-7" ||
		repeated.Control.ResumeRevision != 7 {
		t.Fatalf("idempotent WorkSpec rewound local state = %#v", repeated.Control)
	}
	overCapacity := spec
	overCapacity.AssignmentID = "assignment-2"
	overCapacity.Session.ID = "session-2"
	if _, err := application.ApplyWorkSpec(ctx, overCapacity); !errors.Is(err, ErrCapacity) {
		t.Fatalf("ApplyWorkSpec(over capacity) error = %v, want ErrCapacity", err)
	}

	replacement := spec
	replacement.AssignmentID = "assignment-2"
	replacement.Session.Status = repeated.Control.Status
	replacement.Session.ResumeRef = repeated.Control.ResumeRef
	replacement.Session.ResumeRevision = repeated.Control.ResumeRevision
	replaced, err := application.ApplyWorkSpec(ctx, replacement)
	if err != nil {
		t.Fatalf("replace inactive Assignment: %v", err)
	}
	if replaced.Control.AssignmentID != "assignment-2" {
		t.Fatalf("replacement Control = %#v", replaced.Control)
	}
	if err := application.ValidateAssignment(ctx, spec.Session.ID, spec.WorkerID, spec.AssignmentID); !errors.Is(err, ErrConflict) {
		t.Fatalf("old Assignment validation error = %v, want ErrConflict", err)
	}
}

func TestUnsafeRecoveryTerminatesSession(t *testing.T) {
	ctx := context.Background()
	repository := newMemoryRepository()
	events := NewEventLog(agentledger.NewMemoryEventStore())
	application := New(repository, events, unsafeHarness{})
	agent, err := application.CreateAgent(ctx, Agent{Name: "test", ModelID: "test-model"})
	if err != nil {
		t.Fatal(err)
	}
	environment, err := application.CreateEnvironment(ctx, Environment{Name: "test"})
	if err != nil {
		t.Fatal(err)
	}
	session, err := application.CreateSession(ctx, agent.ID, agent.Version, environment.ID, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	input := NewManagedEvent("user.message", map[string]any{
		"content": []map[string]any{{"type": "text", "text": "resume"}},
	})
	input["processed_at"] = nil
	if err := events.Append(ctx, session.ID, input); err != nil {
		t.Fatal(err)
	}
	queued := NewManagedEvent("user.message", map[string]any{
		"content": []map[string]any{{"type": "text", "text": "do not run"}},
	})
	queued["processed_at"] = nil
	if err := events.Append(ctx, session.ID, queued); err != nil {
		t.Fatal(err)
	}
	resident, _, err := application.works.Ensure(WorkSpec{
		AssignmentID: session.Control.AssignmentID,
		Session:      executionSession(session),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, start, err := application.works.Wake(session.ID, session.Control.AssignmentID); err != nil || !start {
		t.Fatalf("wake Work = start %t, error %v", start, err)
	}
	keepRunning, err := application.process(session.ID, session.Control.AssignmentID, resident, input)
	if err != nil {
		t.Fatal(err)
	}
	if err := resident.Stop(session.Control.AssignmentID); err != nil {
		t.Fatal(err)
	}
	if keepRunning {
		t.Fatal("unsafe recovery allowed the worker to continue")
	}
	current, err := repository.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Control.Status != "terminated" || current.Control.ResumeRevision != 3 {
		t.Fatalf("control state = %#v", current.Control)
	}
	pending, err := events.UnprocessedUserMessages(ctx, session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0]["id"] != queued["id"] {
		t.Fatalf("pending inputs = %#v, want only queued input", pending)
	}
}

func TestReconcileRetriesInputAfterWorkerPersistenceFailure(t *testing.T) {
	ctx := context.Background()
	repository := newMemoryRepository()
	store := &failOnceEventStore{
		EventStore: agentledger.NewMemoryEventStore(),
		eventType:  "agent.message",
	}
	events := NewEventLog(store)
	harness := recordingHarness{inputs: make(chan TurnInput, 2)}
	var logs bytes.Buffer
	application := New(
		repository,
		events,
		harness,
		WithLogger(slog.New(slog.NewJSONHandler(&logs, nil))),
		WithReconcileInterval(10*time.Millisecond),
	)
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = application.Shutdown(shutdownCtx)
	})
	agent, err := application.CreateAgent(ctx, Agent{Name: "test", ModelID: "test-model"})
	if err != nil {
		t.Fatal(err)
	}
	environment, err := application.CreateEnvironment(ctx, Environment{Name: "test"})
	if err != nil {
		t.Fatal(err)
	}
	session, err := application.CreateSession(ctx, agent.ID, agent.Version, environment.ID, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(ctx); err != nil {
		t.Fatal(err)
	}
	accepted, err := application.SendEvents(ctx, session.ID, []IncomingEvent{{
		Type: "user.message", Content: []map[string]any{{"type": "text", "text": "retry me"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	inputID, _ := accepted[0]["id"].(string)
	for attempt := 0; attempt < 2; attempt++ {
		select {
		case input := <-harness.inputs:
			if input.ID != inputID {
				t.Fatalf("harness input id = %q, want %q", input.ID, inputID)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for harness attempt %d", attempt+1)
		}
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		current, getErr := repository.GetSession(ctx, session.ID)
		if getErr != nil {
			t.Fatal(getErr)
		}
		pending, pendingErr := events.UnprocessedUserMessages(ctx, session.ID)
		if pendingErr != nil {
			t.Fatal(pendingErr)
		}
		if current.Control.Status == "idle" && len(pending) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("reconciliation did not settle: status=%s pending=%d", current.Control.Status, len(pending))
		}
		time.Sleep(10 * time.Millisecond)
	}
	stored, err := events.List(ctx, session.ID)
	if err != nil {
		t.Fatal(err)
	}
	counts := make(map[string]int)
	for _, event := range stored {
		if eventType, ok := event["type"].(string); ok {
			counts[eventType]++
		}
	}
	if counts["agent.message"] != 1 || counts["session.status_rescheduled"] != 1 {
		t.Fatalf("event counts = %#v", counts)
	}
	if !strings.Contains(logs.String(), "session worker stopped") || !strings.Contains(logs.String(), session.ID) {
		t.Fatalf("worker failure log = %q", logs.String())
	}
}

func TestRecoverProcessesDurableUserMessage(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repository := newMemoryRepository()
	events := NewEventLog(agentledger.NewMemoryEventStore())
	seed := New(repository, events, recordingHarness{})
	agent, err := seed.CreateAgent(ctx, Agent{Name: "test", ModelID: "test-model"})
	if err != nil {
		t.Fatal(err)
	}
	environment, err := seed.CreateEnvironment(ctx, Environment{Name: "test", Config: map[string]any{"type": "cloud"}})
	if err != nil {
		t.Fatal(err)
	}
	session, err := seed.CreateSession(ctx, agent.ID, agent.Version, environment.ID, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	if session.Control.Status != "idle" || session.Control.Harness != "recording" ||
		session.Control.HarnessVersion != "test" || session.Control.ResumeRef != "recording/"+session.ID ||
		session.Control.ResumeRevision != 0 {
		t.Fatalf("control state = %#v", session.Control)
	}
	session.Control.Status = "running"
	if err := repository.PutSession(ctx, session); err != nil {
		t.Fatal(err)
	}
	input := NewManagedEvent("user.message", map[string]any{
		"content": []map[string]any{{"type": "text", "text": "resume me"}},
	})
	input["processed_at"] = nil
	if err := events.Append(ctx, session.ID, input); err != nil {
		t.Fatal(err)
	}

	harness := recordingHarness{inputs: make(chan TurnInput, 1)}
	recovered := New(repository, events, harness)
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = recovered.Shutdown(shutdownCtx)
	})
	if err := recovered.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case value := <-harness.inputs:
		if value.ID != input["id"] || value.Text != "resume me" {
			t.Fatalf("harness input = %#v", value)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for recovered message")
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		current, getErr := repository.GetSession(ctx, session.ID)
		if getErr != nil {
			t.Fatal(getErr)
		}
		pending, pendingErr := events.UnprocessedUserMessages(ctx, session.ID)
		if pendingErr != nil {
			t.Fatal(pendingErr)
		}
		if current.Control.Status == "idle" && current.Control.ResumeRef == "checkpoint-7" &&
			current.Control.ResumeRevision == 7 && len(pending) == 0 {
			if len(recovered.works.Snapshots()) == 0 {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf(
				"recovery did not settle: status=%s resume_revision=%d pending=%d",
				current.Control.Status,
				current.Control.ResumeRevision,
				len(pending),
			)
		}
		time.Sleep(10 * time.Millisecond)
	}
	state, err := recovered.ExecutionState(ctx, session.ID)
	if err != nil {
		t.Fatalf("read settled Session after Work eviction: %v", err)
	}
	if state.Status != "idle" || state.ResumeRef != "checkpoint-7" || state.ResumeRevision != 7 {
		t.Fatalf("settled execution state = %#v", state)
	}
}

type recordingHarness struct {
	inputs chan TurnInput
}

func (recordingHarness) Name() string { return "recording" }

func (recordingHarness) Version() string { return "test" }

func (recordingHarness) PrepareSession(_ context.Context, session harness.Session) (string, error) {
	return "recording/" + session.ID, nil
}

func (h recordingHarness) Run(
	_ context.Context,
	_ harness.Session,
	input TurnInput,
	emit func(ManagedEvent) error,
) (TurnResult, error) {
	if h.inputs != nil {
		h.inputs <- input
	}
	err := emit(NewTurnEvent(input.ID, "agent.message", map[string]any{
		"content": []map[string]any{{"type": "text", "text": "done"}},
	}))
	return TurnResult{ResumeRef: "checkpoint-7", ResumeRevision: 7}, err
}

func (recordingHarness) Interrupt(string) {}

type unsafeHarness struct{}

func (unsafeHarness) Name() string    { return "unsafe" }
func (unsafeHarness) Version() string { return "test" }

func (unsafeHarness) PrepareSession(_ context.Context, session harness.Session) (string, error) {
	return "unsafe/" + session.ID, nil
}

func (unsafeHarness) Run(
	context.Context,
	harness.Session,
	TurnInput,
	func(ManagedEvent) error,
) (TurnResult, error) {
	return TurnResult{ResumeRevision: 3}, fmt.Errorf("%w: pending tool call", ErrUnsafeRecovery)
}

func (unsafeHarness) Interrupt(string) {}

type failOnceEventStore struct {
	agentledger.EventStore

	mu        sync.Mutex
	eventType string
	failed    bool
}

func (s *failOnceEventStore) Append(
	ctx context.Context,
	laneID string,
	expectedLastSeq int64,
	appendID string,
	events ...agentledger.ProposedEvent,
) (agentledger.AppendReceipt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, event := range events {
		managed, _ := event.Payload["event"].(map[string]any)
		if managed["type"] == s.eventType && !s.failed {
			s.failed = true
			return agentledger.AppendReceipt{}, fmt.Errorf("injected %s append failure", s.eventType)
		}
	}
	return s.EventStore.Append(ctx, laneID, expectedLastSeq, appendID, events...)
}

type memoryRepository struct {
	mu           sync.Mutex
	agents       map[string]Agent
	environments map[string]Environment
	sessions     map[string]Session
}

func newMemoryRepository() *memoryRepository {
	return &memoryRepository{
		agents: make(map[string]Agent), environments: make(map[string]Environment), sessions: make(map[string]Session),
	}
}

func (r *memoryRepository) PutAgent(_ context.Context, value Agent) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.agents[value.ID] = value
	return nil
}

func (r *memoryRepository) GetAgent(_ context.Context, id string) (Agent, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	value, ok := r.agents[id]
	if !ok {
		return Agent{}, ErrNotFound
	}
	return value, nil
}

func (r *memoryRepository) ListAgents(context.Context) ([]Agent, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	values := make([]Agent, 0, len(r.agents))
	for _, value := range r.agents {
		values = append(values, value)
	}
	return values, nil
}

func (r *memoryRepository) PutEnvironment(_ context.Context, value Environment) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.environments[value.ID] = value
	return nil
}

func (r *memoryRepository) GetEnvironment(_ context.Context, id string) (Environment, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	value, ok := r.environments[id]
	if !ok {
		return Environment{}, ErrNotFound
	}
	return value, nil
}

func (r *memoryRepository) ListEnvironments(context.Context) ([]Environment, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	values := make([]Environment, 0, len(r.environments))
	for _, value := range r.environments {
		values = append(values, value)
	}
	return values, nil
}

func (r *memoryRepository) PutSession(_ context.Context, value Session) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sessions[value.ID] = value
	return nil
}

func (r *memoryRepository) GetSession(_ context.Context, id string) (Session, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	value, ok := r.sessions[id]
	if !ok {
		return Session{}, ErrNotFound
	}
	return value, nil
}

func (r *memoryRepository) ListSessions(context.Context) ([]Session, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	values := make([]Session, 0, len(r.sessions))
	for _, value := range r.sessions {
		values = append(values, value)
	}
	return values, nil
}
