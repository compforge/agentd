package app

import (
	"context"
	"sync"
	"testing"
	"time"

	agentledger "github.com/compforge/agent-ledger/go"
)

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
	session.Status = "running"
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

	harness := recordingHarness{inputs: make(chan string, 1)}
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
		if value != "resume me" {
			t.Fatalf("harness input = %q", value)
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
		if current.Status == "idle" && len(pending) == 0 {
			recovered.mu.Lock()
			activeWorkers := len(recovered.workers)
			recovered.mu.Unlock()
			if activeWorkers == 0 {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("recovery did not settle: status=%s pending=%d", current.Status, len(pending))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

type recordingHarness struct {
	inputs chan string
}

func (recordingHarness) Name() string { return "recording" }

func (h recordingHarness) Run(_ context.Context, _ Session, input string, emit func(ManagedEvent) error) error {
	if h.inputs != nil {
		h.inputs <- input
	}
	return emit(NewManagedEvent("agent.message", map[string]any{
		"content": []map[string]any{{"type": "text", "text": "done"}},
	}))
}

func (recordingHarness) Interrupt(string) {}

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
