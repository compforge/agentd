package app_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/compforge/agentd/agentd/internal/app"
	"github.com/compforge/agentd/agentd/internal/store"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestAssignBalancesWorkersAndHonorsCapacity(t *testing.T) {
	application := newTestApp(t)
	ctx := context.Background()
	observeReadyWorker(t, application, "worker-a", 1, time.Now().UTC())
	observeReadyWorker(t, application, "worker-b", 2, time.Now().UTC())

	wants := []struct {
		sessionID string
		workerID  string
	}{
		{"session-1", "worker-a"},
		{"session-2", "worker-b"},
		{"session-3", "worker-b"},
	}
	for _, want := range wants {
		assignment, err := application.Assign(ctx, want.sessionID)
		if err != nil {
			t.Fatalf("Assign(%q): %v", want.sessionID, err)
		}
		if assignment.WorkerID != want.workerID {
			t.Fatalf("Assign(%q).WorkerID = %q, want %q", want.sessionID, assignment.WorkerID, want.workerID)
		}
	}
	if _, err := application.Assign(ctx, "session-4"); !errors.Is(err, app.ErrNoCapacity) {
		t.Fatalf("Assign() error = %v, want ErrNoCapacity", err)
	}

	first, err := application.Assign(ctx, "session-1")
	if err != nil {
		t.Fatalf("reuse assignment: %v", err)
	}
	second, err := application.Assign(ctx, "session-1")
	if err != nil {
		t.Fatalf("reuse assignment again: %v", err)
	}
	if second.ID != first.ID {
		t.Fatalf("assignment ID changed from %q to %q", first.ID, second.ID)
	}

	if err := application.Release(ctx, "session-1"); err != nil {
		t.Fatalf("Release(): %v", err)
	}
	replacement, err := application.Assign(ctx, "session-4")
	if err != nil {
		t.Fatalf("Assign() after release: %v", err)
	}
	if replacement.WorkerID != "worker-a" {
		t.Fatalf("replacement.WorkerID = %q, want worker-a", replacement.WorkerID)
	}
}

func TestAssignReplacesBindingToStaleWorker(t *testing.T) {
	application := newTestApp(t)
	ctx := context.Background()
	observeReadyWorker(t, application, "worker-a", 1, time.Now().UTC())

	first, err := application.Assign(ctx, "session-1")
	if err != nil {
		t.Fatalf("initial Assign(): %v", err)
	}
	observeReadyWorker(t, application, "worker-a", 1, time.Now().UTC().Add(-time.Hour))
	observeReadyWorker(t, application, "worker-b", 1, time.Now().UTC())

	replacement, err := application.Assign(ctx, "session-1")
	if err != nil {
		t.Fatalf("replacement Assign(): %v", err)
	}
	if replacement.WorkerID != "worker-b" {
		t.Fatalf("replacement.WorkerID = %q, want worker-b", replacement.WorkerID)
	}
	if replacement.ID == first.ID {
		t.Fatalf("replacement reused stale assignment ID %q", first.ID)
	}
}

func newTestApp(t *testing.T) *app.App {
	t.Helper()
	database, err := gorm.Open(sqlite.Open(fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name())), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("open SQLite: %v", err)
	}
	repository, err := store.NewGORM(database)
	if err != nil {
		t.Fatalf("create repository: %v", err)
	}
	application, err := app.New(repository, time.Minute)
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	return application
}

func observeReadyWorker(t *testing.T, application *app.App, id string, maxRuns int, observedAt time.Time) {
	t.Helper()
	observerStatus, err := json.Marshal(app.WorkerObserverStatus{
		ObservedAt: observedAt,
		Exists:     true,
		Ready:      true,
		Endpoint:   "http://" + id,
	})
	if err != nil {
		t.Fatalf("marshal observer status: %v", err)
	}
	_, err = application.ObserveWorker(context.Background(), app.Worker{
		ID: id, Name: id, MaxRuns: maxRuns, ObserverStatus: observerStatus,
	})
	if err != nil {
		t.Fatalf("ObserveWorker(%q): %v", id, err)
	}
}
