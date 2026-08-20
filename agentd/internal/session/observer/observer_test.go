package observer

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/compforge/agentd/agentd/internal/model"
	"github.com/compforge/agentd/agentd/internal/service"
	"github.com/compforge/agentd/internal/executionapi"
)

type fakeSource struct {
	lock     sync.Mutex
	states   map[string]executionapi.SessionState
	observed []string
	err      error
}

func (f *fakeSource) ObserveSession(
	_ context.Context,
	session model.Session,
) (executionapi.SessionState, error) {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.observed = append(f.observed, session.ID)
	return f.states[session.ID], f.err
}

type fakeSink struct {
	sessions     []model.Session
	observations map[string]model.SessionObserverStatus
	err          error
}

func (f *fakeSink) ListSessions(context.Context) ([]model.Session, error) {
	return f.sessions, nil
}

func (f *fakeSink) ObserveSession(
	_ context.Context,
	sessionID string,
	status model.SessionObserverStatus,
) (model.Session, error) {
	f.observations[sessionID] = status
	return model.Session{ID: sessionID}, f.err
}

func TestObserverPollsOnlyAssignedSessions(t *testing.T) {
	source := &fakeSource{states: map[string]executionapi.SessionState{
		"assigned": {
			AssignmentID: "assignment-1", Status: "running",
			ResumeRef: "checkpoint-2", ResumeRevision: 2,
		},
	}}
	sink := &fakeSink{
		sessions: []model.Session{
			{ID: "assigned", AssignmentID: "assignment-1", WorkerID: "worker-1"},
			{ID: "idle"},
		},
		observations: map[string]model.SessionObserverStatus{},
	}
	observer, err := New(source, sink, Config{
		Interval: time.Second, RequestTimeout: time.Second, Concurrency: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := observer.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(source.observed) != 1 || source.observed[0] != "assigned" {
		t.Fatalf("observed Sessions = %#v", source.observed)
	}
	status := sink.observations["assigned"]
	if status.ObservedAt.IsZero() || !status.Exists || status.AssignmentID != "assignment-1" ||
		status.Status != model.SessionStatusRunning || status.ResumeRevision != 2 {
		t.Fatalf("persisted observation = %#v", status)
	}
}

func TestObserverIgnoresAssignmentRace(t *testing.T) {
	source := &fakeSource{states: map[string]executionapi.SessionState{
		"session-1": {AssignmentID: "assignment-old", Status: "idle"},
	}}
	sink := &fakeSink{
		sessions: []model.Session{{
			ID: "session-1", AssignmentID: "assignment-old", WorkerID: "worker-1",
		}},
		observations: map[string]model.SessionObserverStatus{},
		err:          service.ErrConflict,
	}
	observer, err := New(source, sink, Config{
		Interval: time.Second, RequestTimeout: time.Second, Concurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := observer.Reconcile(context.Background()); err != nil {
		t.Fatalf("assignment race surfaced as reconciliation failure: %v", err)
	}
}

func TestObserverPreservesFactsWhenSourceFails(t *testing.T) {
	source := &fakeSource{
		states: map[string]executionapi.SessionState{}, err: errors.New("Agentlet unavailable"),
	}
	sink := &fakeSink{
		sessions: []model.Session{{
			ID: "session-1", AssignmentID: "assignment-1", WorkerID: "worker-1",
		}},
		observations: map[string]model.SessionObserverStatus{},
	}
	observer, err := New(source, sink, Config{
		Interval: time.Second, RequestTimeout: time.Second, Concurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := observer.Reconcile(context.Background()); err == nil {
		t.Fatal("Reconcile() error = nil, want source failure")
	}
	if len(sink.observations) != 0 {
		t.Fatalf("observations changed after source failure: %#v", sink.observations)
	}
}
