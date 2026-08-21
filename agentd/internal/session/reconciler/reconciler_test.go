package reconciler

import (
	"context"
	"testing"
	"time"

	"github.com/compforge/agentd/agentd/internal/model"
	"github.com/compforge/agentd/agentd/internal/service"
	"github.com/compforge/agentd/agentd/internal/session/connector"
	managedevent "github.com/compforge/agentd/internal/event"
	"github.com/compforge/agentd/internal/executionapi"
)

type fakeControl struct {
	sessions     []model.Session
	prepare      service.ExecutionTarget
	current      service.ExecutionTarget
	prepareErr   error
	currentErr   error
	prepareCalls []string
	currentCalls []string
}

func (f *fakeControl) ListSessions(context.Context) ([]model.Session, error) {
	return f.sessions, nil
}

func (f *fakeControl) PrepareExecution(_ context.Context, sessionID string) (service.ExecutionTarget, error) {
	f.prepareCalls = append(f.prepareCalls, sessionID)
	return f.prepare, f.prepareErr
}

func (f *fakeControl) CurrentExecution(_ context.Context, sessionID string) (service.ExecutionTarget, error) {
	f.currentCalls = append(f.currentCalls, sessionID)
	return f.current, f.currentErr
}

type fakeEvents struct {
	pending map[string][]managedevent.ManagedEvent
}

func (f *fakeEvents) UnprocessedUserMessages(
	_ context.Context,
	sessionID string,
) ([]managedevent.ManagedEvent, error) {
	return f.pending[sessionID], nil
}

type fakeDataPlane struct {
	ensured []connector.Target
	woken   []connector.Target
}

func (f *fakeDataPlane) Ensure(_ context.Context, target connector.Target) error {
	f.ensured = append(f.ensured, target)
	return nil
}

func (f *fakeDataPlane) Wake(_ context.Context, target connector.Target) error {
	f.woken = append(f.woken, target)
	return nil
}

func TestReconcilerAssignsAndWakesPendingSession(t *testing.T) {
	target := executionTarget("session-1", "assignment-1")
	control := &fakeControl{sessions: []model.Session{{ID: "session-1"}}, prepare: target}
	events := &fakeEvents{pending: map[string][]managedevent.ManagedEvent{
		"session-1": {managedevent.New("user.message", nil)},
	}}
	dataPlane := &fakeDataPlane{}
	reconciler := newTestReconciler(t, control, events, dataPlane)

	if err := reconciler.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(control.prepareCalls) != 1 || len(control.currentCalls) != 0 {
		t.Fatalf("execution calls = prepare %v, current %v", control.prepareCalls, control.currentCalls)
	}
	if len(dataPlane.ensured) != 1 || len(dataPlane.woken) != 1 ||
		dataPlane.woken[0].Work.AssignmentID != "assignment-1" {
		t.Fatalf("data-plane calls = ensure %#v, wake %#v", dataPlane.ensured, dataPlane.woken)
	}
}

func TestReconcilerRetriesExistingAssignmentInPlace(t *testing.T) {
	target := executionTarget("session-1", "assignment-1")
	control := &fakeControl{
		sessions: []model.Session{{
			ID: "session-1", AssignmentID: "assignment-1", WorkerID: "worker-1",
		}},
		current: target,
	}
	events := &fakeEvents{pending: map[string][]managedevent.ManagedEvent{
		"session-1": {managedevent.New("user.message", nil)},
	}}
	dataPlane := &fakeDataPlane{}
	reconciler := newTestReconciler(t, control, events, dataPlane)

	if err := reconciler.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(control.prepareCalls) != 0 || len(control.currentCalls) != 1 {
		t.Fatalf("execution calls = prepare %v, current %v", control.prepareCalls, control.currentCalls)
	}
	if len(dataPlane.woken) != 1 || dataPlane.woken[0].Work.AssignmentID != "assignment-1" {
		t.Fatalf("wake calls = %#v", dataPlane.woken)
	}
}

func TestReconcilerLeavesNoCapacityDemandPending(t *testing.T) {
	control := &fakeControl{
		sessions: []model.Session{{ID: "session-1"}}, prepareErr: service.ErrNoCapacity,
	}
	events := &fakeEvents{pending: map[string][]managedevent.ManagedEvent{
		"session-1": {managedevent.New("user.message", nil)},
	}}
	dataPlane := &fakeDataPlane{}
	reconciler := newTestReconciler(t, control, events, dataPlane)

	if err := reconciler.Reconcile(context.Background()); err != nil {
		t.Fatalf("no capacity surfaced as reconciliation failure: %v", err)
	}
	if len(dataPlane.ensured) != 0 || len(dataPlane.woken) != 0 {
		t.Fatalf("data plane called without capacity: ensure %#v, wake %#v", dataPlane.ensured, dataPlane.woken)
	}
}

func TestReconcilerSkipsSessionsWithoutPendingInput(t *testing.T) {
	control := &fakeControl{sessions: []model.Session{{ID: "session-1"}}}
	dataPlane := &fakeDataPlane{}
	reconciler := newTestReconciler(t, control, &fakeEvents{pending: map[string][]managedevent.ManagedEvent{}}, dataPlane)

	if err := reconciler.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(control.prepareCalls) != 0 || len(control.currentCalls) != 0 {
		t.Fatalf("execution resolved without pending input: prepare %v, current %v", control.prepareCalls, control.currentCalls)
	}
}

func TestNewRequiresValidConfiguration(t *testing.T) {
	_, err := New(nil, nil, nil, Config{})
	if err == nil {
		t.Fatal("New() error = nil")
	}
	_, err = New(&fakeControl{}, &fakeEvents{}, &fakeDataPlane{}, Config{
		Interval: time.Second, RequestTimeout: time.Second, Concurrency: 0,
	})
	if err == nil {
		t.Fatal("New() accepted zero concurrency")
	}
}

func newTestReconciler(
	t *testing.T,
	control Control,
	events EventSource,
	dataPlane DataPlane,
) *Reconciler {
	t.Helper()
	reconciler, err := New(control, events, dataPlane, Config{
		Interval: time.Second, RequestTimeout: time.Second, Concurrency: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	return reconciler
}

func executionTarget(sessionID, assignmentID string) service.ExecutionTarget {
	return service.ExecutionTarget{
		Endpoint: "http://agentlet",
		Work: executionapi.WorkSpec{
			AssignmentID: assignmentID, WorkerID: "worker-1",
			Session: executionapi.SessionSnapshot{ID: sessionID},
		},
	}
}
