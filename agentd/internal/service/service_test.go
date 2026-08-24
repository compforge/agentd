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

func TestReconcilePlacementBalancesWorkersAndHonorsCapacity(t *testing.T) {
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
		{"session-1", "worker-b"},
		{"session-2", "worker-a"},
		{"session-3", "worker-b"},
	}
	for _, want := range wants {
		session, err := application.ReconcilePlacement(ctx, want.sessionID, true)
		if err != nil {
			t.Fatalf("ReconcilePlacement(%q): %v", want.sessionID, err)
		}
		if session.Placement.WorkerID != want.workerID {
			t.Fatalf("ReconcilePlacement(%q).WorkerID = %q, want %q", want.sessionID, session.Placement.WorkerID, want.workerID)
		}
	}
	if _, err := application.ReconcilePlacement(ctx, "session-4", true); !errors.Is(err, service.ErrNoCapacity) {
		t.Fatalf("ReconcilePlacement() error = %v, want ErrNoCapacity", err)
	}

	first, err := application.ReconcilePlacement(ctx, "session-1", true)
	if err != nil {
		t.Fatalf("reuse placement: %v", err)
	}
	second, err := application.ReconcilePlacement(ctx, "session-1", true)
	if err != nil {
		t.Fatalf("reuse placement again: %v", err)
	}
	if second.Placement.Fence != first.Placement.Fence {
		t.Fatalf("placement fence changed from %q to %q", first.Placement.Fence, second.Placement.Fence)
	}

	if _, err := application.ObserveSession(ctx, "session-1", model.SessionObserverStatus{
		ObservedAt: time.Now().UTC(), PlacementFence: first.Placement.Fence,
		Exists: true, Status: model.SessionStatusIdle,
	}); err != nil {
		t.Fatalf("ObserveSession(): %v", err)
	}
	if _, err := application.ReconcilePlacement(ctx, "session-1", false); err != nil {
		t.Fatalf("release placement: %v", err)
	}
	released, err := repository.GetSession(ctx, "session-1")
	if err != nil {
		t.Fatal(err)
	}
	if released.LastWorkerID != "worker-b" || released.Placement.Bound() {
		t.Fatalf("released Session affinity = %+v, want last worker-b", released)
	}
	replacement, err := application.ReconcilePlacement(ctx, "session-4", true)
	if err != nil {
		t.Fatalf("Assign() after release: %v", err)
	}
	if replacement.Placement.WorkerID != "worker-b" {
		t.Fatalf("replacement.WorkerID = %q, want worker-b", replacement.Placement.WorkerID)
	}
}

func TestReconcilePlacementPrefersLastWorkerWithoutReservingIt(t *testing.T) {
	application, repository := newTestControl(t)
	ctx := context.Background()
	now := time.Now().UTC()
	observeReadyWorker(t, application, "worker-a", 2, now)
	observeReadyWorker(t, application, "worker-b", 2, now)
	if err := repository.PutSession(ctx, model.Session{
		ID: "session-on-a", Metadata: map[string]string{}, Status: model.SessionStatusRunning,
		Placement: model.SessionPlacement{Fence: "placement-on-a", WorkerID: "worker-a"}, LastWorkerID: "worker-a",
		CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := repository.PutSession(ctx, model.Session{
		ID: "session-on-b", Metadata: map[string]string{}, Status: model.SessionStatusRunning,
		Placement: model.SessionPlacement{Fence: "placement-on-b", WorkerID: "worker-b"}, LastWorkerID: "worker-b",
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

	session, err := application.ReconcilePlacement(ctx, "session-sticky", true)
	if err != nil {
		t.Fatal(err)
	}
	if session.Placement.WorkerID != "worker-b" {
		t.Fatalf("affinity placement WorkerID = %q, want worker-b", session.Placement.WorkerID)
	}
}

func TestReconcilePlacementMovesOnlyAfterWorkerIsConfirmedAbsent(t *testing.T) {
	application, repository := newTestControl(t)
	ctx := context.Background()
	putSession(t, repository, "session-1")
	observeReadyWorker(t, application, "worker-a", 1, time.Now().UTC())

	first, err := application.ReconcilePlacement(ctx, "session-1", true)
	if err != nil {
		t.Fatalf("initial placement: %v", err)
	}
	observeWorker(t, application, "worker-a", 1, time.Now().UTC(), false)
	observeReadyWorker(t, application, "worker-b", 1, time.Now().UTC())

	retained, err := application.ReconcilePlacement(ctx, "session-1", true)
	if err != nil {
		t.Fatalf("retain placement: %v", err)
	}
	if retained.Placement.WorkerID != first.Placement.WorkerID ||
		retained.Placement.Fence != first.Placement.Fence {
		t.Fatalf("transient not-ready observation moved placement: before=%+v after=%+v", first.Placement, retained.Placement)
	}
	observeMissingWorker(t, application, "worker-a", 1, time.Now().UTC())
	replacement, err := application.ReconcilePlacement(ctx, "session-1", true)
	if err != nil {
		t.Fatalf("replacement placement: %v", err)
	}
	if replacement.Placement.WorkerID != "worker-b" {
		t.Fatalf("replacement.WorkerID = %q, want worker-b", replacement.Placement.WorkerID)
	}
	if replacement.Placement.Fence == first.Placement.Fence {
		t.Fatalf("replacement reused stale placement fence %q", first.Placement.Fence)
	}
}

func TestReconcilePlacementKeepsPendingStateWhenWorkerManagementIsDisabled(t *testing.T) {
	application, repository := newTestControl(t)
	ctx := context.Background()
	putSession(t, repository, "session-pending")
	if _, err := application.ReconcilePlacement(ctx, "session-pending", true); !errors.Is(err, service.ErrNoCapacity) {
		t.Fatalf("ReconcilePlacement() error = %v, want ErrNoCapacity", err)
	}
	session, err := repository.GetSession(ctx, "session-pending")
	if err != nil {
		t.Fatal(err)
	}
	if session.Status != model.SessionStatusRescheduling || session.Placement.Bound() {
		t.Fatalf("pending session = %+v", session)
	}
	if _, err := application.ReconcilePlacement(ctx, "session-pending", false); err != nil {
		t.Fatal(err)
	}
	session, err = repository.GetSession(ctx, "session-pending")
	if err != nil {
		t.Fatal(err)
	}
	if session.Status != model.SessionStatusIdle || session.Placement.Bound() {
		t.Fatalf("released session = %+v", session)
	}
}

func TestReconcilePlacementPublishesCreatingWorkerAndReservesCapacity(t *testing.T) {
	application, repository := newTestControlWithCapacity(t, 2)
	putSession(t, repository, "session-1")
	putSession(t, repository, "session-2")

	first, err := application.ReconcilePlacement(context.Background(), "session-1", true)
	if err != nil {
		t.Fatal(err)
	}
	second, err := application.ReconcilePlacement(context.Background(), "session-2", true)
	if err != nil {
		t.Fatal(err)
	}
	if first.Placement.WorkerID == "" || second.Placement.WorkerID != first.Placement.WorkerID {
		t.Fatalf("creating Worker reservations = first %+v, second %+v", first.Placement, second.Placement)
	}
	worker, err := repository.GetWorker(context.Background(), first.Placement.WorkerID)
	if err != nil {
		t.Fatal(err)
	}
	if worker.Phase != model.WorkerPhaseCreating || worker.Capacity != 2 {
		t.Fatalf("published Worker = %+v", worker)
	}
}

func TestCurrentExecutionBuildsPlacedWorkSnapshot(t *testing.T) {
	application, repository := newTestControl(t)
	ctx := context.Background()
	now := time.Now().UTC()
	if err := repository.PutModel(ctx, model.Model{
		ID: "model-1", Provider: "anthropic", UpstreamID: "claude-sonnet-4-6",
		BaseURL: "https://model.example.test", APIKey: "secret",
		CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	agent := model.Agent{
		ID: "agent-1", VersionID: "agent-version-3", Name: "test", ModelID: "model-1", Version: 3,
		System: "be concise", Tools: []map[string]any{{"type": "agent_toolset_20260401"}},
		CreatedAt: now, UpdatedAt: now,
	}
	if err := repository.CreateAgentVersion(ctx, agent); err != nil {
		t.Fatal(err)
	}
	if err := repository.PutAgent(ctx, agent); err != nil {
		t.Fatal(err)
	}
	if err := repository.PutEnvironment(ctx, model.Environment{
		ID: "env-1", Name: "test", Config: map[string]any{"type": "cloud"},
		CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := repository.PutSession(ctx, model.Session{
		ID: "session-1", AgentID: "agent-1", AgentVersionID: "agent-version-3", EnvironmentID: "env-1",
		Metadata: map[string]string{"suite": "contract"}, Status: model.SessionStatusIdle,
		Harness: "agentgo", HarnessVersion: "v1", CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	observeReadyWorker(t, application, "worker-1", 1, now)

	placed, err := application.ReconcilePlacement(ctx, "session-1", true)
	if err != nil {
		t.Fatal(err)
	}
	target, err := application.CurrentExecution(ctx, "session-1")
	if err != nil {
		t.Fatal(err)
	}
	if target.Endpoint != "http://worker-1" || target.Work.AssignmentID != placed.Placement.Fence ||
		target.Work.WorkerID != "worker-1" {
		t.Fatalf("execution target = %#v", target)
	}
	if target.Work.Session.ID != "session-1" || target.Work.Session.Status != "rescheduling" ||
		target.Work.Agent.ID != "agent-1" || target.Work.Agent.Version != 3 ||
		target.Work.Environment.ID != "env-1" {
		t.Fatalf("work snapshot = %#v", target.Work)
	}
	if target.Work.Agent.Model.ID != "model-1" || target.Work.Agent.Model.Provider != "anthropic" ||
		target.Work.Agent.Model.UpstreamID != "claude-sonnet-4-6" ||
		target.Work.Agent.Model.BaseURL != "https://model.example.test" || target.Work.Agent.Model.APIKey != "secret" {
		t.Fatalf("model snapshot = %#v", target.Work.Agent.Model)
	}
}

func TestAgentLifecycleKeepsImmutableVersionsAndPinnedSessions(t *testing.T) {
	application, repository := newTestControl(t)
	ctx := context.Background()
	now := time.Now().UTC()
	if err := repository.PutModel(ctx, model.Model{
		ID: "model-1", Provider: "anthropic", UpstreamID: "claude-sonnet-4-6", APIKey: "secret",
		CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	created, err := application.CreateAgent(ctx, model.Agent{
		Name: "reviewer", Description: "reviews code", ModelID: "model-1", System: "be precise",
		Tools: []map[string]any{{"type": "agent_toolset_20260401"}}, Metadata: map[string]string{"team": "quality"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.Version != 1 || created.VersionID == "" {
		t.Fatalf("created Agent = %#v", created)
	}

	expectedVersion := created.Version
	updatedDescription := "reviews and tests code"
	updatedTeam := "platform"
	updated, err := application.UpdateAgent(ctx, created.ID, service.AgentUpdate{
		Version: &expectedVersion, Description: &updatedDescription,
		Metadata: map[string]*string{"team": &updatedTeam, "obsolete": nil},
	})
	if err != nil {
		t.Fatal(err)
	}
	if updated.Version != 2 || updated.VersionID == created.VersionID || updated.Description != updatedDescription ||
		updated.Metadata["team"] != updatedTeam {
		t.Fatalf("updated Agent = %#v", updated)
	}

	noOpVersion := updated.Version
	noOp, err := application.UpdateAgent(ctx, created.ID, service.AgentUpdate{
		Version: &noOpVersion, Description: &updatedDescription,
	})
	if err != nil {
		t.Fatal(err)
	}
	if noOp.Version != updated.Version || noOp.VersionID != updated.VersionID {
		t.Fatalf("no-op update created version: before=%#v after=%#v", updated, noOp)
	}
	if _, err := application.UpdateAgent(ctx, created.ID, service.AgentUpdate{
		Version: &expectedVersion, Description: &updatedDescription,
	}); !errors.Is(err, service.ErrConflict) {
		t.Fatalf("stale update error = %v, want ErrConflict", err)
	}

	versions, err := application.ListAgentVersions(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(versions) != 2 || versions[0].Version != 2 || versions[1].Version != 1 ||
		versions[1].Description != "reviews code" {
		t.Fatalf("Agent versions = %#v", versions)
	}

	environment, err := application.CreateEnvironment(ctx, model.Environment{
		Name: "test", Config: map[string]any{"type": "cloud"},
	})
	if err != nil {
		t.Fatal(err)
	}
	pinned, err := application.CreateSession(ctx, created.ID, 1, environment.ID, "pinned", nil)
	if err != nil {
		t.Fatal(err)
	}
	latest, err := application.CreateSession(ctx, created.ID, 0, environment.ID, "latest", nil)
	if err != nil {
		t.Fatal(err)
	}
	if pinned.AgentVersionID != created.VersionID || latest.AgentVersionID != updated.VersionID {
		t.Fatalf("Session version pins: pinned=%#v latest=%#v", pinned, latest)
	}

	archived, err := application.ArchiveAgent(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if archived.ArchivedAt == nil || archived.Version != 2 {
		t.Fatalf("archived Agent = %#v", archived)
	}
	if _, err := application.UpdateAgent(ctx, created.ID, service.AgentUpdate{}); !errors.Is(err, service.ErrConflict) {
		t.Fatalf("archived update error = %v, want ErrConflict", err)
	}
	if _, err := application.CreateSession(ctx, created.ID, 0, environment.ID, "blocked", nil); !errors.Is(err, service.ErrConflict) {
		t.Fatalf("archived Session create error = %v, want ErrConflict", err)
	}
	resolved, err := application.GetAgentVersion(ctx, pinned.AgentVersionID)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Version != 1 || resolved.Description != "reviews code" || resolved.ArchivedAt == nil {
		t.Fatalf("resolved pinned Agent version = %#v", resolved)
	}
}

func TestObserveSessionUsesPlacementFenceAndMonotonicResumeRevision(t *testing.T) {
	application, repository := newTestControl(t)
	ctx := context.Background()
	now := time.Now().UTC()
	if err := repository.PutSession(ctx, model.Session{
		ID: "session-1", Status: model.SessionStatusRescheduling,
		Placement: model.SessionPlacement{Fence: "placement-1", WorkerID: "worker-1"}, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}

	observed, err := application.ObserveSession(ctx, "session-1", model.SessionObserverStatus{
		ObservedAt: now.Add(time.Second), PlacementFence: "placement-1", Exists: true,
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
		ObservedAt: now.Add(2 * time.Second), PlacementFence: "placement-1", Exists: true,
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
		ObservedAt: now.Add(1500 * time.Millisecond), PlacementFence: "placement-1", Exists: true,
		Status: model.SessionStatusRunning, ResumeRef: "checkpoint-9", ResumeRevision: 9,
	})
	if err != nil {
		t.Fatal(err)
	}
	if observed.ResumeRef != "checkpoint-7" || observed.ResumeRevision != 7 || observed.Revision != 2 {
		t.Fatalf("older observation replaced current facts = %#v", observed)
	}

	observed, err = application.ObserveSession(ctx, "session-1", model.SessionObserverStatus{
		ObservedAt: now.Add(3 * time.Second), PlacementFence: "placement-1", Exists: true,
		Status: model.SessionStatusRunning, ResumeRef: "checkpoint-3", ResumeRevision: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	if observed.ResumeRef != "checkpoint-7" || observed.ResumeRevision != 7 || observed.Revision != 2 {
		t.Fatalf("resume state rewound = %#v", observed)
	}

	observed, err = application.ObserveSession(ctx, "session-1", model.SessionObserverStatus{
		ObservedAt: now.Add(4 * time.Second), PlacementFence: "placement-1", Exists: true,
		Status: model.SessionStatusIdle, ResumeRef: "checkpoint-7", ResumeRevision: 7,
	})
	if err != nil {
		t.Fatal(err)
	}
	if observed.Status != model.SessionStatusIdle || !observed.Placement.Bound() || observed.Revision != 3 {
		t.Fatalf("observed state changed placement = %#v", observed)
	}
	observed, err = application.ReconcilePlacement(ctx, "session-1", false)
	if err != nil {
		t.Fatal(err)
	}
	if observed.Placement.Bound() || observed.LastWorkerID != "worker-1" || observed.Revision != 4 {
		t.Fatalf("released placement = %#v", observed)
	}

	_, err = application.ObserveSession(ctx, "session-1", model.SessionObserverStatus{
		ObservedAt: now.Add(5 * time.Second), PlacementFence: "stale-placement", Exists: true,
		Status: model.SessionStatusIdle, ResumeRevision: 8,
	})
	if !errors.Is(err, service.ErrConflict) {
		t.Fatalf("stale Assignment error = %v, want ErrConflict", err)
	}
}

func TestReconcilePlacementMarksWorkerIdleOnlyAfterLastSessionRelease(t *testing.T) {
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
			ID: id, Status: model.SessionStatusRunning,
			Placement: model.SessionPlacement{Fence: "placement-" + id, WorkerID: "worker-1", PlacedAt: &now},
			CreatedAt: now, UpdatedAt: now,
		}); err != nil {
			t.Fatal(err)
		}
	}
	observeIdle := func(id string, offset time.Duration) {
		t.Helper()
		if _, err := application.ObserveSession(ctx, id, model.SessionObserverStatus{
			ObservedAt: now.Add(offset), PlacementFence: "placement-" + id,
			Exists: true, Status: model.SessionStatusIdle,
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := application.ReconcilePlacement(ctx, id, false); err != nil {
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
	return newTestControlWithCapacity(t, 0)
}

func newTestControlWithCapacity(
	t *testing.T,
	workerCapacity int,
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
	application, err := service.New(repository, time.Minute, workerCapacity)
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

func observeMissingWorker(t *testing.T, application *service.Service, id string, capacity int, observedAt time.Time) {
	t.Helper()
	observerStatus, err := json.Marshal(model.WorkerObserverStatus{
		ObservedAt: observedAt,
		Exists:     false,
	})
	if err != nil {
		t.Fatalf("marshal observer status: %v", err)
	}
	if _, err := application.ObserveWorker(context.Background(), model.Worker{
		ID: id, Name: id, Capacity: capacity, ObserverStatus: observerStatus,
	}); err != nil {
		t.Fatalf("ObserveWorker(%q missing): %v", id, err)
	}
}
