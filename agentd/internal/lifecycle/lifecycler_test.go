package lifecycle

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	controllock "github.com/compforge/agentd/agentd/internal/lock"
	"github.com/compforge/agentd/agentd/internal/model"
	"github.com/compforge/agentd/agentd/internal/repo"
	gormrepo "github.com/compforge/agentd/agentd/internal/repo/gorm"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type fakeLocker struct {
	mu     sync.Mutex
	locked bool
}

func (l *fakeLocker) Lock(context.Context, string, time.Duration) (*controllock.Token, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.locked {
		return nil, controllock.ErrLocked
	}
	l.locked = true
	return &controllock.Token{Resource: poolLockResource, LockerID: "test"}, nil
}

func (l *fakeLocker) Unlock(context.Context, *controllock.Token) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.locked = false
	return nil
}

type fakeProvisioner struct {
	mu      sync.Mutex
	ensured []string
}

func (p *fakeProvisioner) Ensure(_ context.Context, worker model.Worker) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ensured = append(p.ensured, worker.ID)
	return nil
}

func (*fakeProvisioner) Destroy(context.Context, model.Worker) error { return nil }

func TestReconcileCombinesPendingDemandAndWarmCapacity(t *testing.T) {
	repository := newTestRepository(t)
	for i := range 5 {
		now := time.Now().UTC()
		if err := repository.PutSession(context.Background(), model.Session{
			ID: fmt.Sprintf("session-%d", i), Status: model.SessionStatusRescheduling,
			CreatedAt: now, UpdatedAt: now,
		}); err != nil {
			t.Fatal(err)
		}
	}
	provisioner := &fakeProvisioner{}
	lifecycler := newTestLifecycler(t, repository, provisioner, Config{
		WorkerMaxRuns: 2, MinIdleWorkers: 1, CreateBatchSize: 10,
	})

	if err := lifecycler.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	workers, err := repository.ListWorkers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// Five pending slots plus two warm slots require four max_runs=2 Workers.
	if len(workers) != 4 {
		t.Fatalf("Workers = %d, want 4", len(workers))
	}
	for _, worker := range workers {
		if worker.Phase != model.WorkerPhaseCreating || worker.MaxRuns != 2 {
			t.Fatalf("Worker = %+v", worker)
		}
	}
}

func TestReconcileDoesNotGrowBehindPendingWorker(t *testing.T) {
	repository := newTestRepository(t)
	now := time.Now().UTC()
	status, err := json.Marshal(model.WorkerObserverStatus{
		ObservedAt: now, Exists: true, PodPhase: "Pending",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.PutWorker(context.Background(), model.Worker{
		ID: "worker-pending", Name: "worker-pending", MaxRuns: 1,
		Phase: model.WorkerPhaseCreating, ObserverStatus: status, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := repository.PutSession(context.Background(), model.Session{
		ID: "session-1", Status: model.SessionStatusRescheduling,
		CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	lifecycler := newTestLifecycler(t, repository, &fakeProvisioner{}, Config{
		WorkerMaxRuns: 1, CreateBatchSize: 10,
	})

	if err := lifecycler.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	workers, err := repository.ListWorkers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(workers) != 1 {
		t.Fatalf("Workers = %d, want existing pending Worker only", len(workers))
	}
}

func newTestLifecycler(
	t *testing.T,
	repository repo.Repository,
	provisioner Provisioner,
	config Config,
) *Lifecycler {
	t.Helper()
	config.Interval = time.Second
	config.RequestTimeout = time.Second
	config.LeaseTTL = time.Second
	lifecycler, err := New(repository, &fakeLocker{}, provisioner, config)
	if err != nil {
		t.Fatal(err)
	}
	return lifecycler
}

func newTestRepository(t *testing.T) *gormrepo.GORMRepository {
	t.Helper()
	database, err := gorm.Open(sqlite.Open(fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name())), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatal(err)
	}
	repository, err := gormrepo.NewGORM(database)
	if err != nil {
		t.Fatal(err)
	}
	return repository
}
