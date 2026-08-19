package app

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	agentledger "github.com/compforge/agent-ledger/go"
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
	if !application.process(session.ID, input) {
		t.Fatal("unsafe recovery did not reach a durable terminal state")
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
	if len(pending) != 0 {
		t.Fatalf("pending inputs = %d, want 0", len(pending))
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
		session.Control.ResumeRevision != -1 {
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
		if current.Control.Status == "idle" && current.Control.ResumeRevision == 7 && len(pending) == 0 {
			recovered.mu.Lock()
			activeWorkers := len(recovered.workers)
			recovered.mu.Unlock()
			if activeWorkers == 0 {
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
}

type recordingHarness struct {
	inputs chan TurnInput
}

func (recordingHarness) Name() string { return "recording" }

func (recordingHarness) Version() string { return "test" }

func (recordingHarness) PrepareSession(_ context.Context, session Session) (string, error) {
	return "recording/" + session.ID, nil
}

func (h recordingHarness) Run(
	_ context.Context,
	_ Session,
	input TurnInput,
	emit func(ManagedEvent) error,
) (TurnResult, error) {
	if h.inputs != nil {
		h.inputs <- input
	}
	err := emit(NewTurnEvent(input.ID, "agent.message", map[string]any{
		"content": []map[string]any{{"type": "text", "text": "done"}},
	}))
	return TurnResult{ResumeRevision: 7}, err
}

func (recordingHarness) Interrupt(string) {}

type unsafeHarness struct{}

func (unsafeHarness) Name() string    { return "unsafe" }
func (unsafeHarness) Version() string { return "test" }

func (unsafeHarness) PrepareSession(_ context.Context, session Session) (string, error) {
	return "unsafe/" + session.ID, nil
}

func (unsafeHarness) Run(
	context.Context,
	Session,
	TurnInput,
	func(ManagedEvent) error,
) (TurnResult, error) {
	return TurnResult{ResumeRevision: 3}, fmt.Errorf("%w: pending tool call", ErrUnsafeRecovery)
}

func (unsafeHarness) Interrupt(string) {}

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
