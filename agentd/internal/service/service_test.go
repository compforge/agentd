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

type demandNotifierFunc func()

func (notify demandNotifierFunc) NotifyDemand() { notify() }

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
	released, err := repository.GetSession(ctx, "session-1")
	if err != nil {
		t.Fatal(err)
	}
	if released.LastWorkerID != "worker-a" || released.WorkerID != "" || released.AssignmentID != "" {
		t.Fatalf("released Session affinity = %+v, want last worker-a", released)
	}
	replacement, err := application.Assign(ctx, "session-4")
	if err != nil {
		t.Fatalf("Assign() after release: %v", err)
	}
	if replacement.WorkerID != "worker-a" {
		t.Fatalf("replacement.WorkerID = %q, want worker-a", replacement.WorkerID)
	}
}

func TestAssignPrefersLastWorkerWithoutReservingIt(t *testing.T) {
	application, repository := newTestControl(t)
	ctx := context.Background()
	now := time.Now().UTC()
	observeReadyWorker(t, application, "worker-a", 2, now)
	observeReadyWorker(t, application, "worker-b", 2, now)
	if err := repository.PutSession(ctx, model.Session{
		ID: "session-on-a", Metadata: map[string]string{}, Status: model.SessionStatusRunning,
		AssignmentID: "assignment-on-a", WorkerID: "worker-a", LastWorkerID: "worker-a",
		CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := repository.PutSession(ctx, model.Session{
		ID: "session-on-b", Metadata: map[string]string{}, Status: model.SessionStatusRunning,
		AssignmentID: "assignment-on-b", WorkerID: "worker-b", LastWorkerID: "worker-b",
		CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := repository.PutSession(ctx, model.Session{
		ID: "session-sticky", Metadata: map[string]string{}, Status: model.SessionStatusIdle,
		LastWorkerID: "worker-b", CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	assignedOnB, err := repository.CountWorkerSessions(ctx, "worker-b")
	if err != nil {
		t.Fatal(err)
	}
	if assignedOnB != 1 {
		t.Fatalf("worker-b assigned Sessions = %d, want affinity-only Session not counted", assignedOnB)
	}

	assignment, err := application.Assign(ctx, "session-sticky")
	if err != nil {
		t.Fatal(err)
	}
	if assignment.WorkerID != "worker-b" {
		t.Fatalf("affinity assignment WorkerID = %q, want worker-b", assignment.WorkerID)
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

func TestAssignNotifiesDemandAfterPendingSessionIsDurable(t *testing.T) {
	var repository *gormrepo.GORMRepository
	notifications := 0
	pendingAtNotification := int64(-1)
	application, repository := newTestControlWithNotifier(t, demandNotifierFunc(func() {
		notifications++
		count, err := repository.CountPendingSessions(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		pendingAtNotification = count
	}))
	putSession(t, repository, "session-pending")

	if _, err := application.Assign(context.Background(), "session-pending"); !errors.Is(err, service.ErrNoCapacity) {
		t.Fatalf("Assign() error = %v, want ErrNoCapacity", err)
	}
	if notifications != 1 || pendingAtNotification != 1 {
		t.Fatalf("notifications = %d, pending at notification = %d", notifications, pendingAtNotification)
	}
}

func TestPrepareExecutionBuildsAssignedWorkSnapshot(t *testing.T) {
	application, repository := newTestControl(t)
	ctx := context.Background()
	now := time.Now().UTC()
	if err := repository.PutAgent(ctx, model.Agent{
		ID: "agent-1", Name: "test", ModelID: "model-1", Version: 3,
		System: "be concise", Tools: []map[string]any{{"type": "agent_toolset_20260401"}},
		CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := repository.PutEnvironment(ctx, model.Environment{
		ID: "env-1", Name: "test", Config: map[string]any{"type": "cloud"},
		CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := repository.PutSession(ctx, model.Session{
		ID: "session-1", AgentID: "agent-1", AgentVersion: 3, EnvironmentID: "env-1",
		Metadata: map[string]string{"suite": "contract"}, Status: model.SessionStatusIdle,
		Harness: "agentgo", HarnessVersion: "v1", CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	observeReadyWorker(t, application, "worker-1", 1, now)

	target, err := application.PrepareExecution(ctx, "session-1")
	if err != nil {
		t.Fatal(err)
	}
	if target.Endpoint != "http://worker-1" || target.Work.AssignmentID == "" ||
		target.Work.WorkerID != "worker-1" {
		t.Fatalf("execution target = %#v", target)
	}
	if target.Work.Session.ID != "session-1" || target.Work.Session.Status != "rescheduling" ||
		target.Work.Agent.ID != "agent-1" || target.Work.Agent.Version != 3 ||
		target.Work.Environment.ID != "env-1" {
		t.Fatalf("work snapshot = %#v", target.Work)
	}
}

func TestObserveSessionUsesAssignmentFenceAndMonotonicResumeRevision(t *testing.T) {
	application, repository := newTestControl(t)
	ctx := context.Background()
	now := time.Now().UTC()
	if err := repository.PutSession(ctx, model.Session{
		ID: "session-1", Status: model.SessionStatusRescheduling,
		AssignmentID: "assignment-1", WorkerID: "worker-1", CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}

	observed, err := application.ObserveSession(ctx, "session-1", model.SessionObserverStatus{
		ObservedAt: now.Add(time.Second), AssignmentID: "assignment-1", Exists: true,
		Status: model.SessionStatusRunning, ResumeRef: "checkpoint-0",
	})
	if err != nil {
		t.Fatal(err)
	}
	if observed.Status != model.SessionStatusRunning || observed.ResumeRef != "checkpoint-0" ||
		observed.ResumeRevision != 0 || observed.Revision != 1 {
		t.Fatalf("initial observed state = %#v", observed)
	}

	observed, err = application.ObserveSession(ctx, "session-1", model.SessionObserverStatus{
		ObservedAt: now.Add(2 * time.Second), AssignmentID: "assignment-1", Exists: true,
		Status: model.SessionStatusRunning, ResumeRef: "checkpoint-7", ResumeRevision: 7,
	})
	if err != nil {
		t.Fatal(err)
	}
	if observed.Status != model.SessionStatusRunning || observed.ResumeRef != "checkpoint-7" ||
		observed.ResumeRevision != 7 || observed.Revision != 2 {
		t.Fatalf("advanced observed state = %#v", observed)
	}

	observed, err = application.ObserveSession(ctx, "session-1", model.SessionObserverStatus{
		ObservedAt: now.Add(1500 * time.Millisecond), AssignmentID: "assignment-1", Exists: true,
		Status: model.SessionStatusRunning, ResumeRef: "checkpoint-9", ResumeRevision: 9,
	})
	if err != nil {
		t.Fatal(err)
	}
	if observed.ResumeRef != "checkpoint-7" || observed.ResumeRevision != 7 || observed.Revision != 2 {
		t.Fatalf("older observation replaced current facts = %#v", observed)
	}

	observed, err = application.ObserveSession(ctx, "session-1", model.SessionObserverStatus{
		ObservedAt: now.Add(3 * time.Second), AssignmentID: "assignment-1", Exists: true,
		Status: model.SessionStatusRunning, ResumeRef: "checkpoint-3", ResumeRevision: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	if observed.ResumeRef != "checkpoint-7" || observed.ResumeRevision != 7 || observed.Revision != 2 {
		t.Fatalf("resume state rewound = %#v", observed)
	}

	observed, err = application.ObserveSession(ctx, "session-1", model.SessionObserverStatus{
		ObservedAt: now.Add(4 * time.Second), AssignmentID: "assignment-1", Exists: true,
		Status: model.SessionStatusIdle, ResumeRef: "checkpoint-7", ResumeRevision: 7,
	})
	if err != nil {
		t.Fatal(err)
	}
	if observed.Status != model.SessionStatusIdle || observed.AssignmentID != "" || observed.WorkerID != "" ||
		observed.LastWorkerID != "worker-1" ||
		observed.Revision != 3 {
		t.Fatalf("released observed state = %#v", observed)
	}

	_, err = application.ObserveSession(ctx, "session-1", model.SessionObserverStatus{
		ObservedAt: now.Add(5 * time.Second), AssignmentID: "stale-assignment", Exists: true,
		Status: model.SessionStatusIdle, ResumeRevision: 8,
	})
	if !errors.Is(err, service.ErrConflict) {
		t.Fatalf("stale Assignment error = %v, want ErrConflict", err)
	}
}

func TestIdleSessionReleasesWorkerOnlyAfterLastActiveAssignment(t *testing.T) {
	application, repository := newTestControl(t)
	ctx := context.Background()
	now := time.Now().UTC()
	if err := repository.PutWorker(ctx, model.Worker{
		ID: "worker-1", Name: "worker-1", Capacity: 2, Phase: model.WorkerPhaseActive,
		CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"session-1", "session-2"} {
		if err := repository.PutSession(ctx, model.Session{
			ID: id, Status: model.SessionStatusRunning, AssignmentID: "assignment-" + id,
			WorkerID: "worker-1", AssignedAt: &now, CreatedAt: now, UpdatedAt: now,
		}); err != nil {
			t.Fatal(err)
		}
	}
	observeIdle := func(id string, offset time.Duration) {
		t.Helper()
		if _, err := application.ObserveSession(ctx, id, model.SessionObserverStatus{
			ObservedAt: now.Add(offset), AssignmentID: "assignment-" + id,
			Exists: true, Status: model.SessionStatusIdle,
		}); err != nil {
			t.Fatal(err)
		}
	}
	observeIdle("session-1", time.Second)
	worker, err := repository.GetWorker(ctx, "worker-1")
	if err != nil {
		t.Fatal(err)
	}
	if worker.IdleSince != nil {
		t.Fatalf("Worker became idle while one Session remained assigned: %#v", worker)
	}
	observeIdle("session-2", 2*time.Second)
	worker, err = repository.GetWorker(ctx, "worker-1")
	if err != nil {
		t.Fatal(err)
	}
	if worker.IdleSince == nil {
		t.Fatalf("Worker did not become idle after its last Session released: %#v", worker)
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
	return newTestControlWithNotifier(t, nil)
}

func newTestControlWithNotifier(
	t *testing.T,
	notifier service.DemandNotifier,
) (*service.Service, *gormrepo.GORMRepository) {
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
	application, err := service.New(repository, time.Minute, notifier)
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
