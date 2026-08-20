package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/compforge/agentd/agentd/internal/model"
	gormrepo "github.com/compforge/agentd/agentd/internal/repo/gorm"
	"github.com/compforge/agentd/agentd/internal/service"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestAssignBalancesWorkersAndHonorsCapacity(t *testing.T) {
	application, repository := newTestControl(t)
	ctx := context.Background()
	for index := 1; index <= 4; index++ {
		putSession(t, repository, fmt.Sprintf("session-%d", index))
	}
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
	if _, err := application.Assign(ctx, "session-4"); !errors.Is(err, service.ErrNoCapacity) {
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

func TestAssignReplacesBindingToUnavailableWorker(t *testing.T) {
	application, repository := newTestControl(t)
	ctx := context.Background()
	putSession(t, repository, "session-1")
	observeReadyWorker(t, application, "worker-a", 1, time.Now().UTC())

	first, err := application.Assign(ctx, "session-1")
	if err != nil {
		t.Fatalf("initial Assign(): %v", err)
	}
	observeWorker(t, application, "worker-a", 1, time.Now().UTC(), false)
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

func TestAssignPersistsPendingSessionUntilRelease(t *testing.T) {
	application, repository := newTestControl(t)
	ctx := context.Background()
	putSession(t, repository, "session-pending")
	if _, err := application.Assign(ctx, "session-pending"); !errors.Is(err, service.ErrNoCapacity) {
		t.Fatalf("Assign() error = %v, want ErrNoCapacity", err)
	}
	session, err := repository.GetSession(ctx, "session-pending")
	if err != nil {
		t.Fatal(err)
	}
	if session.Status != model.SessionStatusRescheduling || session.WorkerID != "" {
		t.Fatalf("pending session = %+v", session)
	}
	count, err := repository.CountPendingSessions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("pending sessions = %d, want 1", count)
	}
	if err := application.Release(ctx, "session-pending"); err != nil {
		t.Fatal(err)
	}
	count, err = repository.CountPendingSessions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("pending sessions after release = %d, want 0", count)
	}
	session, err = repository.GetSession(ctx, "session-pending")
	if err != nil {
		t.Fatal(err)
	}
	if session.Status != model.SessionStatusIdle || session.WorkerID != "" || session.AssignmentID != "" {
		t.Fatalf("released session = %+v", session)
	}
}

func newTestService(t *testing.T) *service.Service {
	application, _ := newTestControl(t)
	return application
}

func putSession(t *testing.T, repository *gormrepo.GORMRepository, id string) {
	t.Helper()
	now := time.Now().UTC()
	if err := repository.PutSession(context.Background(), model.Session{
		ID: id, Metadata: map[string]string{}, Status: model.SessionStatusIdle,
		CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("put Session %q: %v", id, err)
	}
}

func newTestControl(t *testing.T) (*service.Service, *gormrepo.GORMRepository) {
	t.Helper()
	database, err := gorm.Open(sqlite.Open(fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name())), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("open SQLite: %v", err)
	}
	repository, err := gormrepo.NewGORM(database)
	if err != nil {
		t.Fatalf("create repository: %v", err)
	}
	application, err := service.New(repository, time.Minute)
	if err != nil {
		t.Fatalf("create service: %v", err)
	}
	return application, repository
}

func observeReadyWorker(t *testing.T, application *service.Service, id string, capacity int, observedAt time.Time) {
	observeWorker(t, application, id, capacity, observedAt, true)
}

func observeWorker(t *testing.T, application *service.Service, id string, capacity int, observedAt time.Time, ready bool) {
	t.Helper()
	observerStatus, err := json.Marshal(model.WorkerObserverStatus{
		ObservedAt: observedAt,
		Exists:     true,
		Ready:      ready,
		Endpoint:   "http://" + id,
	})
	if err != nil {
		t.Fatalf("marshal observer status: %v", err)
	}
	_, err = application.ObserveWorker(context.Background(), model.Worker{
		ID: id, Name: id, Capacity: capacity, ObserverStatus: observerStatus,
	})
	if err != nil {
		t.Fatalf("ObserveWorker(%q): %v", id, err)
	}
}
