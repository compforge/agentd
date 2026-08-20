package gc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/compforge/agentd/agentd/internal/model"
	"github.com/compforge/agentd/agentd/internal/repo"
)

func TestRecordGCSweepsOnlyAgedAbsentRetiredWorkers(t *testing.T) {
	repository := newTestRepository(t)
	ctx := context.Background()
	now := time.Now().UTC()
	oldAbsence := now.Add(-2 * time.Hour)
	freshAbsence := now.Add(-time.Minute)
	workers := []model.Worker{
		{ID: "eligible", Name: "eligible", Capacity: 1, Phase: model.WorkerPhaseRetired,
			AbsentAt: &oldAbsence, CreatedAt: now, UpdatedAt: now},
		{ID: "fresh", Name: "fresh", Capacity: 1, Phase: model.WorkerPhaseRetired,
			AbsentAt: &freshAbsence, CreatedAt: now, UpdatedAt: now},
		{ID: "active", Name: "active", Capacity: 1, Phase: model.WorkerPhaseActive,
			AbsentAt: &oldAbsence, CreatedAt: now, UpdatedAt: now},
		{ID: "present", Name: "present", Capacity: 1, Phase: model.WorkerPhaseRetired,
			CreatedAt: now, UpdatedAt: now},
		{ID: "bound", Name: "bound", Capacity: 1, Phase: model.WorkerPhaseRetired,
			AbsentAt: &oldAbsence, CreatedAt: now, UpdatedAt: now},
	}
	for _, worker := range workers {
		if err := repository.PutWorker(ctx, worker); err != nil {
			t.Fatal(err)
		}
	}
	if err := repository.PutSession(ctx, model.Session{
		ID: "session-bound", Status: model.SessionStatusRunning, WorkerID: "bound",
		Metadata: map[string]string{}, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	controller, err := NewRecordGC(repository, RecordConfig{
		Interval: time.Hour, RequestTimeout: time.Second, Retention: time.Hour, BatchSize: 10,
	})
	if err != nil {
		t.Fatal(err)
	}

	deleted, err := controller.Sweep(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 1 {
		t.Fatalf("deleted Worker records = %d, want 1", deleted)
	}
	if _, err := repository.GetWorker(ctx, "eligible"); !errors.Is(err, repo.ErrNotFound) {
		t.Fatalf("eligible Worker lookup error = %v, want ErrNotFound", err)
	}
	for _, workerID := range []string{"fresh", "active", "present", "bound"} {
		if _, err := repository.GetWorker(ctx, workerID); err != nil {
			t.Fatalf("retained Worker %q: %v", workerID, err)
		}
	}
}
