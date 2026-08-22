//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	agentledger "github.com/compforge/agent-ledger/go"
	"github.com/compforge/agentd/agentlet/internal/harness"
	"github.com/compforge/agentd/agentlet/internal/service"
	"github.com/compforge/agentd/internal/executionapi"
)

func TestAssignmentHandoffFencesStaleAgentletRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sharedEvents := service.NewEventLog(agentledger.NewMemoryEventStore())

	staleHarness := &assignmentHandoffHarness{executed: make(chan string, 2)}
	stale := service.New(service.NewMemoryRepository(), sharedEvents, staleHarness)
	installAssignmentHandoffWork(t, ctx, stale, "worker-old", "assignment-old", "", 0)
	firstInput := appendAssignmentHandoffInput(t, ctx, sharedEvents, "first")
	if err := stale.Wake(ctx, assignmentHandoffSessionID); err != nil {
		t.Fatalf("wake stale Agentlet for its current Assignment: %v", err)
	}
	waitAssignmentHandoffExecution(t, ctx, staleHarness.executed, firstInput)
	waitAssignmentHandoffIdle(t, ctx, stale, sharedEvents)

	secondInput := appendAssignmentHandoffInput(t, ctx, sharedEvents, "second")
	if err := stale.Recover(ctx); err != nil {
		t.Fatalf("reconcile stale Agentlet: %v", err)
	}
	select {
	case inputID := <-staleHarness.executed:
		t.Fatalf("stale Agentlet executed replacement input %q", inputID)
	case <-time.After(100 * time.Millisecond):
	}

	previous, err := stale.GetSession(ctx, assignmentHandoffSessionID)
	if err != nil {
		t.Fatalf("read previous Session state: %v", err)
	}
	replacementHarness := &assignmentHandoffHarness{executed: make(chan string, 1)}
	replacement := service.New(service.NewMemoryRepository(), sharedEvents, replacementHarness)
	installAssignmentHandoffWork(
		t,
		ctx,
		replacement,
		"worker-new",
		"assignment-new",
		previous.Control.ResumeRef,
		previous.Control.ResumeRevision,
	)
	if err := replacement.Wake(ctx, assignmentHandoffSessionID); err != nil {
		t.Fatalf("wake replacement Agentlet: %v", err)
	}
	waitAssignmentHandoffExecution(t, ctx, replacementHarness.executed, secondInput)
	waitAssignmentHandoffIdle(t, ctx, replacement, sharedEvents)

	events, err := sharedEvents.List(ctx, assignmentHandoffSessionID)
	if err != nil {
		t.Fatalf("list shared Ledger Events: %v", err)
	}
	agentMessages := 0
	for _, event := range events {
		if event["type"] == "agent.message" {
			agentMessages++
		}
	}
	if agentMessages != 2 {
		t.Fatalf("agent.message count = %d, want 2", agentMessages)
	}
}

const assignmentHandoffSessionID = "session-assignment-handoff"

func installAssignmentHandoffWork(
	t *testing.T,
	ctx context.Context,
	application *service.Service,
	workerID string,
	assignmentID string,
	resumeRef string,
	resumeRevision int64,
) {
	t.Helper()
	_, err := application.ApplyWorkSpec(ctx, executionapi.WorkSpec{
		AssignmentID: assignmentID,
		WorkerID:     workerID,
		Session: executionapi.SessionSnapshot{
			ID: assignmentHandoffSessionID, EnvironmentID: "environment-1", Status: "rescheduling",
			Harness: "assignment-handoff", HarnessVersion: "test",
			ResumeRef: resumeRef, ResumeRevision: resumeRevision,
		},
		Agent: executionapi.AgentSnapshot{
			ID: "agent-1", Name: "test", Version: 1,
			Model: executionapi.ModelSnapshot{
				ID: "model-1", Provider: "anthropic", UpstreamID: "model-1", APIKey: "secret",
			},
		},
		Environment: executionapi.EnvironmentSnapshot{
			ID: "environment-1", Config: map[string]any{"type": "cloud"},
		},
	})
	if err != nil {
		t.Fatalf("install Assignment %q: %v", assignmentID, err)
	}
}

func appendAssignmentHandoffInput(t *testing.T, ctx context.Context, events *service.EventLog, text string) string {
	t.Helper()
	input := service.NewManagedEvent("user.message", map[string]any{
		"content": []map[string]any{{"type": "text", "text": text}},
	})
	input["processed_at"] = nil
	if err := events.AppendIngress(ctx, assignmentHandoffSessionID, input); err != nil {
		t.Fatalf("append input: %v", err)
	}
	inputID, _ := input["id"].(string)
	return inputID
}

func waitAssignmentHandoffExecution(t *testing.T, ctx context.Context, executed <-chan string, want string) {
	t.Helper()
	select {
	case got := <-executed:
		if got != want {
			t.Fatalf("executed input = %q, want %q", got, want)
		}
	case <-ctx.Done():
		t.Fatalf("wait for input %q execution: %v", want, ctx.Err())
	}
}

func waitAssignmentHandoffIdle(
	t *testing.T,
	ctx context.Context,
	application *service.Service,
	events *service.EventLog,
) {
	t.Helper()
	for {
		session, sessionErr := application.GetSession(ctx, assignmentHandoffSessionID)
		pending, pendingErr := events.PendingInputs(ctx, assignmentHandoffSessionID)
		if sessionErr == nil && pendingErr == nil && session.Control.Status == "idle" && len(pending) == 0 {
			// Let the execution goroutine evict its inactive process-local Work.
			time.Sleep(10 * time.Millisecond)
			return
		}
		if sessionErr != nil || pendingErr != nil {
			t.Fatalf("observe Assignment handoff: %v", fmt.Errorf("session: %w; pending: %v", sessionErr, pendingErr))
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for Assignment handoff idle: %v", ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

type assignmentHandoffHarness struct {
	executed chan string
}

func (*assignmentHandoffHarness) Name() string    { return "assignment-handoff" }
func (*assignmentHandoffHarness) Version() string { return "test" }

func (*assignmentHandoffHarness) PrepareSession(_ context.Context, session harness.Session) (string, error) {
	return "assignment-handoff/" + session.ID, nil
}

func (h *assignmentHandoffHarness) Run(
	_ context.Context,
	session harness.Session,
	input service.TurnInput,
	emit func(service.ManagedEvent) error,
) (service.TurnResult, error) {
	if err := emit(service.NewTurnEvent(input.ID, "agent.message", map[string]any{
		"content": []map[string]any{{"type": "text", "text": "done"}},
	})); err != nil {
		return service.TurnResult{}, err
	}
	h.executed <- input.ID
	return service.TurnResult{
		ResumeRef: "assignment-handoff/" + session.ID, ResumeRevision: session.ResumeRevision + 1,
	}, nil
}

func (*assignmentHandoffHarness) Interrupt(string) {}
