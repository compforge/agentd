package service

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	agentledger "github.com/compforge/agent-ledger/go"
	"github.com/compforge/agentd/agentlet/internal/harness"
	"github.com/compforge/agentd/internal/executionapi"
)

func TestShutdownDrainsActiveWorkWithoutInterrupt(t *testing.T) {
	draining := &drainHarness{
		started: make(chan struct{}), release: make(chan struct{}), finished: make(chan struct{}),
	}
	application, events, repository, session, _ := startDrainTestWork(t, draining)

	shutdownResult := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		shutdownResult <- application.Shutdown(ctx)
	}()
	waitForServiceClosing(t, application)
	if _, err := application.ApplyWorkSpec(context.Background(), executionapi.WorkSpec{}); !errors.Is(err, ErrConflict) {
		t.Fatalf("ApplyWorkSpec while draining error = %v, want ErrConflict", err)
	}
	if err := application.Wake(context.Background(), session.ID); !errors.Is(err, ErrConflict) {
		t.Fatalf("Wake while draining error = %v, want ErrConflict", err)
	}
	select {
	case err := <-shutdownResult:
		t.Fatalf("Shutdown returned before active Work settled: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	close(draining.release)
	if err := <-shutdownResult; err != nil {
		t.Fatal(err)
	}
	if got := draining.interrupts.Load(); got != 0 {
		t.Fatalf("Harness interrupts = %d, want 0 during graceful drain", got)
	}
	pending, err := events.PendingInputs(context.Background(), session.ID)
	if err != nil {
		t.Fatal(err)
	}
	current, err := repository.GetSession(context.Background(), session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 || current.Control.Status != "idle" {
		t.Fatalf("drained Session = status %q, pending %d", current.Control.Status, len(pending))
	}
}

func TestShutdownDeadlineCancelsWorkWithoutConsumingInput(t *testing.T) {
	draining := &drainHarness{started: make(chan struct{}), finished: make(chan struct{})}
	application, events, _, session, input := startDrainTestWork(t, draining)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := application.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown after forced cancellation: %v", err)
	}
	select {
	case <-draining.finished:
	case <-time.After(time.Second):
		t.Fatal("Harness did not stop after forced drain cancellation")
	}
	waitForServiceWorks(t, application)
	if got := draining.interrupts.Load(); got != 1 {
		t.Fatalf("Harness interrupts = %d, want 1 after drain deadline", got)
	}
	pending, err := events.PendingInputs(context.Background(), session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0]["id"] != input["id"] {
		t.Fatalf("pending inputs after forced drain = %#v", pending)
	}
	stored, err := events.List(context.Background(), session.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range stored {
		if event["type"] == "session.status_idle" || event["type"] == "session.error" {
			t.Fatalf("forced drain recorded terminal Turn event %#v", event)
		}
	}
}

type drainHarness struct {
	started    chan struct{}
	release    chan struct{}
	finished   chan struct{}
	interrupts atomic.Int64
}

func (*drainHarness) Name() string    { return "drain" }
func (*drainHarness) Version() string { return "test" }

func (*drainHarness) PrepareSession(_ context.Context, session harness.Session) (string, error) {
	return "drain/" + session.ID, nil
}

func (h *drainHarness) Run(
	ctx context.Context,
	_ harness.Session,
	input TurnInput,
	emit func(ManagedEvent) error,
) (TurnResult, error) {
	close(h.started)
	defer close(h.finished)
	select {
	case <-h.release:
		err := emit(NewTurnEvent(input.ID, "agent.message", map[string]any{
			"content": []map[string]any{{"type": "text", "text": "done"}},
		}))
		return TurnResult{ResumeRef: "checkpoint-drained", ResumeRevision: 1}, err
	case <-ctx.Done():
		return TurnResult{}, ctx.Err()
	}
}

func (h *drainHarness) Interrupt(string) { h.interrupts.Add(1) }

func startDrainTestWork(
	t *testing.T,
	draining *drainHarness,
) (*Service, *EventLog, *memoryRepository, Session, ManagedEvent) {
	t.Helper()
	ctx := context.Background()
	repository := newMemoryRepository()
	events := NewEventLog(agentledger.NewMemoryEventStore())
	application := New(repository, events, draining)
	agent, err := application.CreateAgent(ctx, Agent{Name: "test", Model: testModel("test-model")})
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
		"content": []map[string]any{{"type": "text", "text": "drain me"}},
	})
	input["processed_at"] = nil
	if err := events.AppendIngress(ctx, session.ID, input); err != nil {
		t.Fatal(err)
	}
	if err := application.Wake(ctx, session.ID); err != nil {
		t.Fatal(err)
	}
	select {
	case <-draining.started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Harness Work to start")
	}
	return application, events, repository, session, input
}

func waitForServiceClosing(t *testing.T, application *Service) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		application.mu.Lock()
		closing := application.closing
		application.mu.Unlock()
		if closing {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for Service drain admission fence")
		}
		time.Sleep(time.Millisecond)
	}
}

func waitForServiceWorks(t *testing.T, application *Service) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		application.workSet.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Service Work to stop")
	}
}
