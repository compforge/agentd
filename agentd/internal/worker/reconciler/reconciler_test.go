package reconciler

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/compforge/agentd/agentd/internal/model"
	"github.com/compforge/agentd/agentd/internal/repo"
	gormrepo "github.com/compforge/agentd/agentd/internal/repo/gorm"
	controlk8s "github.com/compforge/agentd/agentd/internal/worker/k8s"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	corev1 "k8s.io/api/core/v1"
)

type fakePods struct {
	values []controlk8s.PodSnapshot
}

func (f fakePods) ListAgentletPods(context.Context) ([]controlk8s.PodSnapshot, error) {
	return f.values, nil
}

type fakeProvisioner struct {
	ensured []string
}

func (p *fakeProvisioner) Ensure(_ context.Context, worker model.Worker) error {
	p.ensured = append(p.ensured, worker.ID)
	return nil
}

func TestReconcileRealizesCreatingWorkerRow(t *testing.T) {
	repository := newTestRepository(t)
	now := time.Now().UTC()
	putWorker(t, repository, model.Worker{
		ID: "worker-demand", Name: "worker-demand", Capacity: 2,
		Phase: model.WorkerPhaseCreating, CreatedAt: now, UpdatedAt: now,
	})
	provisioner := &fakeProvisioner{}
	controller := newTestReconciler(t, repository, fakePods{}, provisioner, Config{
		WorkerCapacity: 2, CreateBatchSize: 10,
	})

	if err := planAndApply(context.Background(), controller); err != nil {
		t.Fatal(err)
	}
	if len(provisioner.ensured) != 1 || provisioner.ensured[0] != "worker-demand" {
		t.Fatalf("ensured Workers = %#v", provisioner.ensured)
	}
}

func TestReconcileMaintainsWarmWorkerFloor(t *testing.T) {
	repository := newTestRepository(t)
	provisioner := &fakeProvisioner{}
	controller := newTestReconciler(t, repository, fakePods{}, provisioner, Config{
		WorkerCapacity: 2, MinWorkers: 1, MinIdleWorkers: 2, CreateBatchSize: 10,
	})

	if err := planAndApply(context.Background(), controller); err != nil {
		t.Fatal(err)
	}
	workers, err := repository.ListWorkers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(workers) != 2 || len(provisioner.ensured) != 2 {
		t.Fatalf("warm reconcile = workers %d, ensured %#v", len(workers), provisioner.ensured)
	}
}

func TestReconcileStopsCreatingUnderKubernetesBackpressure(t *testing.T) {
	repository := newTestRepository(t)
	now := time.Now().UTC()
	putWorker(t, repository, model.Worker{
		ID: "worker-demand", Name: "worker-demand", Capacity: 1,
		Phase: model.WorkerPhaseCreating, CreatedAt: now, UpdatedAt: now,
	})
	provisioner := &fakeProvisioner{}
	controller := newTestReconciler(t, repository, fakePods{values: []controlk8s.PodSnapshot{{
		ID: "worker-pending", Name: "worker-pending", Phase: corev1.PodPending,
	}}}, provisioner, Config{
		WorkerCapacity: 1, MinIdleWorkers: 2, CreateBatchSize: 10,
	})

	if err := planAndApply(context.Background(), controller); err != nil {
		t.Fatal(err)
	}
	workers, err := repository.ListWorkers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(workers) != 1 || len(provisioner.ensured) != 0 {
		t.Fatalf("backpressured reconcile = workers %d, ensured %#v", len(workers), provisioner.ensured)
	}
}

func newTestReconciler(
	t *testing.T,
	repository repo.Repository,
	pods PodSource,
	provisioner Provisioner,
	config Config,
) *Reconciler {
	t.Helper()
	controller, err := New(repository, pods, provisioner, config)
	if err != nil {
		t.Fatal(err)
	}
	return controller
}

func planAndApply(ctx context.Context, controller *Reconciler) error {
	workers, err := controller.Plan(ctx)
	if err != nil {
		return err
	}
	return controller.Apply(ctx, workers)
}

func putWorker(t *testing.T, repository repo.Repository, worker model.Worker) {
	t.Helper()
	if err := repository.PutWorker(context.Background(), worker); err != nil {
		t.Fatal(err)
	}
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
