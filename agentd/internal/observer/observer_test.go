package observer

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	control "github.com/compforge/agentd/agentd/internal/app"
)

type fakeSource struct {
	workers []WorkerSnapshot
}

func (f fakeSource) ListWorkers(context.Context) ([]WorkerSnapshot, error) { return f.workers, nil }

type fakeSink struct {
	workers map[string]control.Worker
}

func (f *fakeSink) ListWorkers(context.Context) ([]control.Worker, error) {
	workers := make([]control.Worker, 0, len(f.workers))
	for _, worker := range f.workers {
		workers = append(workers, worker)
	}
	return workers, nil
}

func (f *fakeSink) ObserveWorker(_ context.Context, worker control.Worker) (control.Worker, error) {
	f.workers[worker.ID] = worker
	return worker, nil
}

func TestReconcileUpdatesPresentAndMissingWorkers(t *testing.T) {
	sink := &fakeSink{workers: map[string]control.Worker{
		"uid-old": {ID: "uid-old", Name: "old", MaxRuns: 2},
	}}
	controller, err := New(fakeSource{workers: []WorkerSnapshot{{
		ID: "uid-new", Name: "new", Endpoint: "http://10.0.0.1:8081", Ready: true, MaxRuns: 4,
	}}}, sink, Config{Interval: time.Second, RequestTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}

	if err := controller.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertStatus(t, sink.workers["uid-new"], true, true)
	assertStatus(t, sink.workers["uid-old"], false, false)
}

func assertStatus(t *testing.T, worker control.Worker, exists, ready bool) {
	t.Helper()
	var status control.WorkerObserverStatus
	if err := json.Unmarshal(worker.ObserverStatus, &status); err != nil {
		t.Fatal(err)
	}
	if status.ObservedAt.IsZero() || status.Exists != exists || status.Ready != ready {
		t.Fatalf("Worker %q status = %+v", worker.ID, status)
	}
}
