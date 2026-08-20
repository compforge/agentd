//go:build e2e

package e2e

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	controllock "github.com/compforge/agentd/agentd/internal/lock"
	"github.com/compforge/agentd/agentd/internal/model"
	"github.com/compforge/agentd/agentd/internal/repo"
	gormrepo "github.com/compforge/agentd/agentd/internal/repo/gorm"
	control "github.com/compforge/agentd/agentd/internal/service"
	controlgc "github.com/compforge/agentd/agentd/internal/worker/gc"
	controlk8s "github.com/compforge/agentd/agentd/internal/worker/k8s"
	"github.com/compforge/agentd/agentd/internal/worker/observer"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type podSource struct {
	pods      []controlk8s.PodSnapshot
	destroyed []string
}

func (s *podSource) ListAgentletPods(context.Context) ([]controlk8s.PodSnapshot, error) {
	return s.pods, nil
}

func (s *podSource) DeleteWorkerPod(_ context.Context, name string) error {
	for index, pod := range s.pods {
		if pod.Name == name {
			s.pods = append(s.pods[:index], s.pods[index+1:]...)
			break
		}
	}
	s.destroyed = append(s.destroyed, name)
	return nil
}

func (*podSource) Ensure(context.Context, model.Worker) error { return nil }

func (s *podSource) Destroy(ctx context.Context, worker model.Worker) error {
	return s.DeleteWorkerPod(ctx, worker.Name)
}

type poolLocker struct{}

func (poolLocker) Lock(context.Context, string, time.Duration) (*controllock.Token, error) {
	return &controllock.Token{Resource: "worker-pool:capacity", LockerID: "e2e"}, nil
}

func (poolLocker) Unlock(context.Context, *controllock.Token) error { return nil }

func TestRetiredWorkerRecordOutlivesPodThenExpires(t *testing.T) {
	ctx := context.Background()
	repository := openRepository(t)
	now := time.Now().UTC()
	idleSince := now.Add(-time.Hour)
	worker := model.Worker{
		ID: "worker-e2e", Name: "worker-e2e", Capacity: 1,
		Phase: model.WorkerPhaseActive, IdleSince: &idleSince, CreatedAt: now, UpdatedAt: now,
	}
	if err := repository.PutWorker(ctx, worker); err != nil {
		t.Fatal(err)
	}
	pods := &podSource{pods: []controlk8s.PodSnapshot{{
		ID: worker.ID, Name: worker.Name, Managed: true, IP: "127.0.0.1", Ready: true,
	}}}
	podGC, err := controlgc.NewPodGC(repository, poolLocker{}, pods, pods, controlgc.PodConfig{
		Interval: time.Minute, RequestTimeout: time.Second, LeaseTTL: 2 * time.Second,
		IdleTTL: time.Minute, DeleteBatchSize: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := podGC.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if len(pods.destroyed) != 1 || pods.destroyed[0] != worker.Name {
		t.Fatalf("destroyed Pods = %#v", pods.destroyed)
	}

	controlService, err := control.New(repository, time.Minute, nil)
	if err != nil {
		t.Fatal(err)
	}
	workerSource, err := observer.NewKubernetesSource(pods, 8019, 1)
	if err != nil {
		t.Fatal(err)
	}
	workerObserver, err := observer.New(workerSource, controlService, observer.Config{
		Interval: time.Minute, RequestTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := workerObserver.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	retired, err := repository.GetWorker(ctx, worker.ID)
	if err != nil {
		t.Fatal(err)
	}
	if retired.Phase != model.WorkerPhaseRetired || retired.AbsentAt == nil {
		t.Fatalf("retired Worker = %#v", retired)
	}

	recordGC, err := controlgc.NewRecordGC(repository, controlgc.RecordConfig{
		Interval: time.Minute, RequestTimeout: time.Second,
		Retention: time.Hour, BatchSize: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if deleted, err := recordGC.Sweep(ctx); err != nil || deleted != 0 {
		t.Fatalf("initial Record GC = deleted %d, error %v", deleted, err)
	}
	if _, err := repository.GetWorker(ctx, worker.ID); err != nil {
		t.Fatalf("Worker record was not retained: %v", err)
	}
	expiredAt := time.Now().UTC().Add(-2 * time.Hour)
	retired.AbsentAt = &expiredAt
	retired.UpdatedAt = time.Now().UTC()
	if err := repository.PutWorker(ctx, retired); err != nil {
		t.Fatal(err)
	}
	if deleted, err := recordGC.Sweep(ctx); err != nil || deleted != 1 {
		t.Fatalf("expired Record GC = deleted %d, error %v", deleted, err)
	}
	if _, err := repository.GetWorker(ctx, worker.ID); !errors.Is(err, repo.ErrNotFound) {
		t.Fatalf("expired Worker lookup error = %v, want ErrNotFound", err)
	}
}

func openRepository(t *testing.T) *gormrepo.GORMRepository {
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
