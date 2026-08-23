// Package reconciler plans durable Worker capacity and realizes it as
// Kubernetes Pods. The parent worker Pool owns scheduling and serialization.
package reconciler

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	agentledger "github.com/compforge/agent-ledger/go"
	"github.com/compforge/agentd/agentd/internal/model"
	"github.com/compforge/agentd/agentd/internal/repo"
	controlk8s "github.com/compforge/agentd/agentd/internal/worker/k8s"
)

type PodSource interface {
	ListAgentletPods(context.Context) ([]controlk8s.PodSnapshot, error)
}

type Provisioner interface {
	Ensure(context.Context, model.Worker) error
}

type Config struct {
	WorkerCapacity  int
	MinWorkers      int
	MinIdleWorkers  int
	CreateBatchSize int
	Logger          *slog.Logger
}

type Reconciler struct {
	repository  repo.Repository
	pods        PodSource
	provisioner Provisioner
	config      Config
}

func New(
	repository repo.Repository,
	pods PodSource,
	provisioner Provisioner,
	config Config,
) (*Reconciler, error) {
	if repository == nil || pods == nil || provisioner == nil {
		return nil, fmt.Errorf("create Worker Reconciler: repository, Pod source, and provisioner are required")
	}
	if config.WorkerCapacity <= 0 || config.CreateBatchSize <= 0 || config.MinWorkers < 0 || config.MinIdleWorkers < 0 {
		return nil, fmt.Errorf("create Worker Reconciler: invalid capacity configuration")
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	return &Reconciler{
		repository:  repository,
		pods:        pods,
		provisioner: provisioner,
		config:      config,
	}, nil
}

// Plan publishes missing Worker rows while the worker Pool holds the capacity
// lease. Kubernetes Pending and Unschedulable Pods provide immediate
// backpressure, so this pass publishes no additional Pod when either exists.
func (r *Reconciler) Plan(ctx context.Context) ([]model.Worker, error) {
	pods, err := r.pods.ListAgentletPods(ctx)
	if err != nil {
		return nil, fmt.Errorf("list Worker Pods before creation: %w", err)
	}
	for _, pod := range pods {
		if string(pod.Phase) == "Pending" || pod.Unschedulable {
			r.config.Logger.WarnContext(ctx, "Kubernetes backpressure blocks Worker creation",
				"worker_id", pod.ID, "pod_name", pod.Name,
				"pod_phase", pod.Phase, "unschedulable", pod.Unschedulable)
			return nil, nil
		}
	}

	workers, err := r.repository.ListWorkers(ctx)
	if err != nil {
		return nil, fmt.Errorf("list Workers: %w", err)
	}
	actions := make([]model.Worker, 0, r.config.CreateBatchSize)
	for _, worker := range workers {
		if len(actions) >= r.config.CreateBatchSize {
			break
		}
		if worker.Phase != model.WorkerPhaseCreating {
			continue
		}
		actions = append(actions, worker)
	}

	needed, err := r.warmDeficit(ctx, workers)
	if err != nil {
		return nil, err
	}
	remaining := r.config.CreateBatchSize - len(actions)
	if needed > remaining {
		needed = remaining
	}
	now := time.Now().UTC()
	for range needed {
		id := agentledger.NewID()
		worker := model.Worker{
			ID: id, Name: "agentd-worker-" + id, Capacity: r.config.WorkerCapacity,
			Phase: model.WorkerPhaseCreating, CreatedAt: now, UpdatedAt: now,
		}
		if err := r.repository.PutWorker(ctx, worker); err != nil {
			return actions, fmt.Errorf("persist warm Worker %q: %w", worker.ID, err)
		}
		actions = append(actions, worker)
		r.config.Logger.InfoContext(ctx, "published warm Worker", "worker_id", worker.ID)
	}
	return actions, nil
}

// Apply realizes a previously persisted capacity plan. Ensure is deliberately
// outside the lease: the Worker row makes retries idempotent and avoids holding
// a database lease during a potentially slow Kubernetes call.
func (r *Reconciler) Apply(ctx context.Context, workers []model.Worker) error {
	for _, worker := range workers {
		if err := r.provisioner.Ensure(ctx, worker); err != nil {
			return fmt.Errorf("ensure Worker %q: %w", worker.ID, err)
		}
		r.config.Logger.InfoContext(ctx, "ensured Worker Pod", "worker_id", worker.ID)
	}
	return nil
}

func (r *Reconciler) warmDeficit(ctx context.Context, workers []model.Worker) (int, error) {
	planned := 0
	idle := 0
	for _, worker := range workers {
		if worker.Phase != model.WorkerPhaseCreating && worker.Phase != model.WorkerPhaseActive {
			continue
		}
		planned++
		assigned, err := r.repository.CountWorkerSessions(ctx, worker.ID)
		if err != nil {
			return 0, fmt.Errorf("count Sessions for Worker %q: %w", worker.ID, err)
		}
		if assigned == 0 {
			idle++
		}
	}
	needed := r.config.MinIdleWorkers - idle
	if floor := r.config.MinWorkers - planned; floor > needed {
		needed = floor
	}
	if needed < 0 {
		return 0, nil
	}
	return needed, nil
}
