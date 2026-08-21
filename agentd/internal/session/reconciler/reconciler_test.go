package reconciler

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/compforge/agentd/agentd/internal/model"
	"github.com/compforge/agentd/agentd/internal/service"
	"github.com/compforge/agentd/agentd/internal/session/connector"
	managedevent "github.com/compforge/agentd/internal/event"
	"github.com/compforge/agentd/internal/executionapi"
)

type fakeControl struct {
	sessions       []model.Session
	reconciled     model.Session
	current        service.ExecutionTarget
	reconcileErr   error
	currentErr     error
	reconcileCalls []placementCall
	currentCalls   []string
}

type placementCall struct {
	sessionID string
	hasDemand bool
}

func (f *fakeControl) ListSessions(context.Context) ([]model.Session, error) {
	return f.sessions, nil
}

func (f *fakeControl) ReconcilePlacement(
	_ context.Context,
	sessionID string,
	hasDemand bool,
) (model.Session, error) {
	f.reconcileCalls = append(f.reconcileCalls, placementCall{sessionID: sessionID, hasDemand: hasDemand})
	return f.reconciled, f.reconcileErr
}

func (f *fakeControl) CurrentExecution(_ context.Context, sessionID string) (service.ExecutionTarget, error) {
	f.currentCalls = append(f.currentCalls, sessionID)
	return f.current, f.currentErr
}

type fakeEvents struct {
	pending map[string][]managedevent.ManagedEvent
}

func (f *fakeEvents) PendingInputs(
	_ context.Context,
	sessionID string,
) ([]managedevent.ManagedEvent, error) {
	return f.pending[sessionID], nil
}

type fakeDataPlane struct {
	ensured []connector.Target
	woken   []connector.Target
}

type fakeWorkerNotifier struct {
	calls atomic.Int64
}

func (f *fakeWorkerNotifier) Notify() {
	f.calls.Add(1)
}

func (f *fakeDataPlane) Ensure(_ context.Context, target connector.Target) error {
	f.ensured = append(f.ensured, target)
	return nil
}

func (f *fakeDataPlane) Wake(_ context.Context, target connector.Target) error {
	f.woken = append(f.woken, target)
	return nil
}

func TestReconcilerPlacesAndWakesPendingSession(t *testing.T) {
	target := executionTarget("session-1", "assignment-1")
	control := &fakeControl{
		sessions: []model.Session{{ID: "session-1"}},
		reconciled: model.Session{ID: "session-1", Placement: model.SessionPlacement{
			Fence: "assignment-1", WorkerID: "worker-1",
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
	if len(control.reconcileCalls) != 1 || !control.reconcileCalls[0].hasDemand || len(control.currentCalls) != 1 {
		t.Fatalf("execution calls = reconcile %+v, current %v", control.reconcileCalls, control.currentCalls)
	}
	if len(dataPlane.ensured) != 1 || len(dataPlane.woken) != 1 ||
		dataPlane.woken[0].Work.AssignmentID != "assignment-1" {
		t.Fatalf("data-plane calls = ensure %#v, wake %#v", dataPlane.ensured, dataPlane.woken)
	}
}

func TestReconcilerNotifiesWorkerAfterPlacementChanges(t *testing.T) {
	control := &fakeControl{
		sessions: []model.Session{{ID: "session-1"}},
		reconciled: model.Session{ID: "session-1", Placement: model.SessionPlacement{
			Fence: "assignment-1", WorkerID: "worker-creating",
		}},
		currentErr: service.ErrUnavailable,
	}
	events := &fakeEvents{pending: map[string][]managedevent.ManagedEvent{
		"session-1": {managedevent.New("user.message", nil)},
	}}
	notifier := &fakeWorkerNotifier{}
	reconciler, err := New(control, events, &fakeDataPlane{}, notifier, Config{
		Interval: time.Hour, RequestTimeout: time.Second, Concurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := reconciler.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := notifier.calls.Load(); got != 1 {
		t.Fatalf("Worker notifications = %d, want 1", got)
	}
}

func TestReconcilerRetriesExistingAssignmentInPlace(t *testing.T) {
	target := executionTarget("session-1", "assignment-1")
	control := &fakeControl{
		sessions: []model.Session{{
			ID: "session-1", Placement: model.SessionPlacement{Fence: "assignment-1", WorkerID: "worker-1"},
		}},
		reconciled: model.Session{
			ID: "session-1", Placement: model.SessionPlacement{Fence: "assignment-1", WorkerID: "worker-1"},
		},
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
	if len(control.reconcileCalls) != 1 || len(control.currentCalls) != 1 {
		t.Fatalf("execution calls = reconcile %+v, current %v", control.reconcileCalls, control.currentCalls)
	}
	if len(dataPlane.woken) != 1 || dataPlane.woken[0].Work.AssignmentID != "assignment-1" {
		t.Fatalf("wake calls = %#v", dataPlane.woken)
	}
}

func TestReconcilerLeavesNoCapacityDemandPending(t *testing.T) {
	control := &fakeControl{
		sessions: []model.Session{{ID: "session-1"}}, reconcileErr: service.ErrNoCapacity,
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

func TestReconcilerReleasesStablePlacementWithoutPendingInput(t *testing.T) {
	control := &fakeControl{
		sessions:   []model.Session{{ID: "session-1"}},
		reconciled: model.Session{ID: "session-1", Status: model.SessionStatusIdle},
	}
	dataPlane := &fakeDataPlane{}
	reconciler := newTestReconciler(t, control, &fakeEvents{pending: map[string][]managedevent.ManagedEvent{}}, dataPlane)

	if err := reconciler.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(control.reconcileCalls) != 1 || control.reconcileCalls[0].hasDemand || len(control.currentCalls) != 0 {
		t.Fatalf("execution calls without pending input: reconcile %+v, current %v", control.reconcileCalls, control.currentCalls)
	}
}

func TestNewRequiresValidConfiguration(t *testing.T) {
	_, err := New(nil, nil, nil, nil, Config{})
	if err == nil {
		t.Fatal("New() error = nil")
	}
	_, err = New(&fakeControl{}, &fakeEvents{}, &fakeDataPlane{}, nil, Config{
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
	reconciler, err := New(control, events, dataPlane, nil, Config{
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
