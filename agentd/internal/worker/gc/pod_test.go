package gc

import (
	"context"
	"fmt"
	"testing"
	"time"

	controllock "github.com/compforge/agentd/agentd/internal/lock"
	"github.com/compforge/agentd/agentd/internal/model"
	gormrepo "github.com/compforge/agentd/agentd/internal/repo/gorm"
	controlk8s "github.com/compforge/agentd/agentd/internal/worker/k8s"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type testLocker struct{}

func (testLocker) Lock(context.Context, string, time.Duration) (*controllock.Token, error) {
	return &controllock.Token{Resource: workerPoolLock, LockerID: "test"}, nil
}

func (testLocker) Unlock(context.Context, *controllock.Token) error { return nil }

type testPodSource struct {
	pods    []controlk8s.PodSnapshot
	deleted []string
}

func (s *testPodSource) ListAgentletPods(context.Context) ([]controlk8s.PodSnapshot, error) {
	return s.pods, nil
}

func (s *testPodSource) DeleteWorkerPod(_ context.Context, name string) error {
	s.deleted = append(s.deleted, name)
	return nil
}

type testProvisioner struct {
	destroyed []string
}

func (*testProvisioner) Ensure(context.Context, model.Worker) error { return nil }

func (p *testProvisioner) Destroy(_ context.Context, worker model.Worker) error {
	p.destroyed = append(p.destroyed, worker.ID)
	return nil
}

func TestPodGCReclaimsIdleWorker(t *testing.T) {
	repository := newTestRepository(t)
	now := time.Now().UTC()
	idleSince := now.Add(-time.Hour)
	if err := repository.PutWorker(context.Background(), model.Worker{
		ID: "worker-idle", Name: "worker-idle", Capacity: 1,
		Phase: model.WorkerPhaseActive, IdleSince: &idleSince, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	pods := &testPodSource{pods: []controlk8s.PodSnapshot{{
		ID: "worker-idle", Name: "worker-idle", Managed: true,
	}}}
	provisioner := &testProvisioner{}
	controller := newTestPodGC(t, repository, pods, provisioner)

	if err := controller.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	worker, err := repository.GetWorker(context.Background(), "worker-idle")
	if err != nil {
		t.Fatal(err)
	}
	if worker.Phase != model.WorkerPhaseRetired {
		t.Fatalf("worker phase = %q, want retired", worker.Phase)
	}
	if len(provisioner.destroyed) != 1 || provisioner.destroyed[0] != "worker-idle" {
		t.Fatalf("destroyed Workers = %#v", provisioner.destroyed)
	}
}

func TestPodGCRetainsMinimumWorkerCount(t *testing.T) {
	repository := newTestRepository(t)
	now := time.Now().UTC()
	idleSince := now.Add(-time.Hour)
	if err := repository.PutWorker(context.Background(), model.Worker{
		ID: "worker-floor", Name: "worker-floor", Capacity: 1,
		Phase: model.WorkerPhaseActive, IdleSince: &idleSince, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	pods := &testPodSource{pods: []controlk8s.PodSnapshot{{
		ID: "worker-floor", Name: "worker-floor", Managed: true,
	}}}
	provisioner := &testProvisioner{}
	controller, err := NewPodGC(repository, testLocker{}, pods, provisioner, PodConfig{
		Interval: time.Minute, RequestTimeout: time.Second, LeaseTTL: 2 * time.Second,
		IdleTTL: time.Minute, MinWorkers: 1, DeleteBatchSize: 10,
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := controller.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	worker, err := repository.GetWorker(context.Background(), "worker-floor")
	if err != nil {
		t.Fatal(err)
	}
	if worker.Phase != model.WorkerPhaseActive || len(provisioner.destroyed) != 0 {
		t.Fatalf("minimum Worker was reclaimed: phase %q, destroyed %#v", worker.Phase, provisioner.destroyed)
	}
}

func TestPodGCDeletesZombiePod(t *testing.T) {
	repository := newTestRepository(t)
	pods := &testPodSource{pods: []controlk8s.PodSnapshot{{
		ID: "worker-zombie", Name: "worker-zombie", Managed: true,
	}}}
	controller := newTestPodGC(t, repository, pods, &testProvisioner{})

	if err := controller.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(pods.deleted) != 1 || pods.deleted[0] != "worker-zombie" {
		t.Fatalf("deleted Pods = %#v", pods.deleted)
	}
}

func newTestPodGC(
	t *testing.T,
	repository *gormrepo.GORMRepository,
	pods *testPodSource,
	provisioner *testProvisioner,
) *PodGC {
	t.Helper()
	controller, err := NewPodGC(repository, testLocker{}, pods, provisioner, PodConfig{
		Interval: time.Minute, RequestTimeout: time.Second, LeaseTTL: 2 * time.Second,
		IdleTTL: time.Minute, DeleteBatchSize: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	return controller
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
