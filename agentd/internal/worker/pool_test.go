package worker

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	controllock "github.com/compforge/agentd/agentd/internal/lock"
	"github.com/compforge/agentd/agentd/internal/model"
	gormrepo "github.com/compforge/agentd/agentd/internal/repo/gorm"
	controlgc "github.com/compforge/agentd/agentd/internal/worker/gc"
	controlk8s "github.com/compforge/agentd/agentd/internal/worker/k8s"
	workerreconciler "github.com/compforge/agentd/agentd/internal/worker/reconciler"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type contendedLocker struct {
	mu                 sync.Mutex
	remainingConflicts int
	lockCalls          int
}

func (l *contendedLocker) Lock(
	context.Context,
	string,
	time.Duration,
) (*controllock.Token, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lockCalls++
	if l.remainingConflicts > 0 {
		l.remainingConflicts--
		return nil, controllock.ErrLocked
	}
	return &controllock.Token{Resource: poolLockResource, LockerID: "test"}, nil
}

func (*contendedLocker) Unlock(context.Context, *controllock.Token) error {
	return nil
}

type fakeWorkerInfrastructure struct {
	pods      []controlk8s.PodSnapshot
	ensured   []string
	destroyed []string
	listCalls chan struct{}
}

func (f *fakeWorkerInfrastructure) ListAgentletPods(context.Context) ([]controlk8s.PodSnapshot, error) {
	if f.listCalls != nil {
		f.listCalls <- struct{}{}
	}
	return f.pods, nil
}

func (f *fakeWorkerInfrastructure) DeleteWorkerPod(_ context.Context, name string) error {
	f.destroyed = append(f.destroyed, name)
	return nil
}

func (f *fakeWorkerInfrastructure) Ensure(_ context.Context, worker model.Worker) error {
	f.ensured = append(f.ensured, worker.ID)
	return nil
}

func (f *fakeWorkerInfrastructure) Destroy(_ context.Context, worker model.Worker) error {
	f.destroyed = append(f.destroyed, worker.ID)
	return nil
}

func TestFullReconcileWaitsForLeaseThenReclaimsAndReplaces(t *testing.T) {
	repository := newPoolTestRepository(t)
	now := time.Now().UTC()
	idleSince := now.Add(-time.Hour)
	oldWorker := model.Worker{
		ID: "worker-idle", Name: "worker-idle", Capacity: 1,
		Phase: model.WorkerPhaseActive, IdleSince: &idleSince,
		CreatedAt: now, UpdatedAt: now,
	}
	if err := repository.PutWorker(context.Background(), oldWorker); err != nil {
		t.Fatal(err)
	}
	infrastructure := &fakeWorkerInfrastructure{pods: []controlk8s.PodSnapshot{{
		ID: oldWorker.ID, Name: oldWorker.Name, Managed: true,
	}}}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	capacity, err := workerreconciler.New(repository, infrastructure, infrastructure, workerreconciler.Config{
		WorkerCapacity: 1, MinWorkers: 1, MinIdleWorkers: 1,
		CreateBatchSize: 10, Logger: logger,
	})
	if err != nil {
		t.Fatal(err)
	}
	reclamation, err := controlgc.NewPodGC(repository, infrastructure, infrastructure, controlgc.PodConfig{
		IdleTTL: time.Minute, MinWorkers: 0, MinIdleWorkers: 0,
		DeleteBatchSize: 10, Logger: logger,
	})
	if err != nil {
		t.Fatal(err)
	}
	locker := &contendedLocker{remainingConflicts: 2}
	pool := &Pool{
		config: Config{
			ControllerLeaseTTL: time.Second,
		},
		logger: logger, locker: locker, reconciler: capacity, podGC: reclamation,
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := pool.Reconcile(ctx, true); err != nil {
		t.Fatal(err)
	}

	retired, err := repository.GetWorker(ctx, oldWorker.ID)
	if err != nil {
		t.Fatal(err)
	}
	if retired.Phase != model.WorkerPhaseRetired {
		t.Fatalf("old Worker phase = %q, want retired", retired.Phase)
	}
	workers, err := repository.ListWorkers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if locker.lockCalls != 3 {
		t.Fatalf("lease attempts = %d, want 3", locker.lockCalls)
	}
	if len(workers) != 2 || len(infrastructure.ensured) != 1 {
		t.Fatalf("replacement = workers %d, ensured %#v", len(workers), infrastructure.ensured)
	}
	if len(infrastructure.destroyed) != 1 || infrastructure.destroyed[0] != oldWorker.ID {
		t.Fatalf("destroyed Workers = %#v", infrastructure.destroyed)
	}
}

func TestNotifyTriggersCapacityPassBeforePeriodicScan(t *testing.T) {
	repository := newPoolTestRepository(t)
	infrastructure := &fakeWorkerInfrastructure{listCalls: make(chan struct{}, 3)}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	capacity, err := workerreconciler.New(repository, infrastructure, infrastructure, workerreconciler.Config{
		WorkerCapacity: 1, CreateBatchSize: 1, Logger: logger,
	})
	if err != nil {
		t.Fatal(err)
	}
	reclamation, err := controlgc.NewPodGC(repository, infrastructure, infrastructure, controlgc.PodConfig{
		IdleTTL: time.Minute, DeleteBatchSize: 1, Logger: logger,
	})
	if err != nil {
		t.Fatal(err)
	}
	pool := &Pool{
		config: Config{
			ReconcilerInterval: time.Hour,
			GCInterval:         time.Hour,
			ControllerTimeout:  time.Second,
			ControllerLeaseTTL: 2 * time.Second,
		},
		logger: logger, locker: &contendedLocker{},
		reconciler: capacity, podGC: reclamation,
		notifications: make(chan struct{}, 1),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pool.run(ctx)

	// Startup is a full pass, so both reclamation and capacity inspect Pods.
	waitForPoolList(t, infrastructure.listCalls)
	waitForPoolList(t, infrastructure.listCalls)
	pool.Notify()
	waitForPoolList(t, infrastructure.listCalls)
}

func waitForPoolList(t *testing.T, calls <-chan struct{}) {
	t.Helper()
	select {
	case <-calls:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Worker Pool pass")
	}
}

func newPoolTestRepository(t *testing.T) *gormrepo.GORMRepository {
	t.Helper()
	database, err := gorm.Open(
		sqlite.Open(fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name())),
		&gorm.Config{Logger: logger.Default.LogMode(logger.Silent)},
	)
	if err != nil {
		t.Fatal(err)
	}
	repository, err := gormrepo.NewGORM(database)
	if err != nil {
		t.Fatal(err)
	}
	return repository
}
