// Package reconciler converges durable Worker rows into Kubernetes Pods and
// maintains the configured warm Worker floor.
package reconciler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	agentledger "github.com/compforge/agent-ledger/go"
	controllock "github.com/compforge/agentd/agentd/internal/lock"
	"github.com/compforge/agentd/agentd/internal/model"
	"github.com/compforge/agentd/agentd/internal/repo"
	controlk8s "github.com/compforge/agentd/agentd/internal/worker/k8s"
)

const poolLockResource = "worker-pool:capacity"

type PodSource interface {
	ListAgentletPods(context.Context) ([]controlk8s.PodSnapshot, error)
}

type Provisioner interface {
	Ensure(context.Context, model.Worker) error
}

type Config struct {
	Interval        time.Duration
	RequestTimeout  time.Duration
	LeaseTTL        time.Duration
	WorkerCapacity  int
	MinWorkers      int
	MinIdleWorkers  int
	CreateBatchSize int
	Logger          *slog.Logger
}

type Reconciler struct {
	repository    repo.Repository
	locker        controllock.Locker
	pods          PodSource
	provisioner   Provisioner
	config        Config
	notifications chan struct{}
}

func New(
	repository repo.Repository,
	locker controllock.Locker,
	pods PodSource,
	provisioner Provisioner,
	config Config,
) (*Reconciler, error) {
	if repository == nil || locker == nil || pods == nil || provisioner == nil {
		return nil, fmt.Errorf("create Worker Reconciler: repository, locker, Pod source, and provisioner are required")
	}
	if config.Interval <= 0 || config.RequestTimeout <= 0 || config.LeaseTTL <= 0 {
		return nil, fmt.Errorf("create Worker Reconciler: intervals and lease TTL must be positive")
	}
	if config.WorkerCapacity <= 0 || config.CreateBatchSize <= 0 || config.MinWorkers < 0 || config.MinIdleWorkers < 0 {
		return nil, fmt.Errorf("create Worker Reconciler: invalid capacity configuration")
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	return &Reconciler{
		repository: repository, locker: locker, pods: pods,
		provisioner: provisioner, config: config,
		notifications: make(chan struct{}, 1),
	}, nil
}

// Notify requests an early reconciliation. Notifications are deliberately
// coalesced because the Worker rows in the database remain the source of truth.
func (r *Reconciler) Notify() {
	select {
	case r.notifications <- struct{}{}:
	default:
	}
}

func (r *Reconciler) Run(ctx context.Context) {
	r.reconcileWithTimeout(ctx)
	ticker := time.NewTicker(r.config.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.reconcileWithTimeout(ctx)
		case <-r.notifications:
			r.reconcileWithTimeout(ctx)
		}
	}
}

// Reconcile realizes Worker rows and maintains warm capacity. The Kubernetes
// list is an intentional live admission check: Pending or Unschedulable Pods
// are backpressure from the cluster, so this pass publishes no additional Pod.
func (r *Reconciler) Reconcile(ctx context.Context) error {
	token, err := r.locker.Lock(ctx, poolLockResource, r.config.LeaseTTL)
	if errors.Is(err, controllock.ErrLocked) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("lock Worker reconcile: %w", err)
	}
	workers, planErr := r.plan(ctx)
	unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	unlockErr := r.locker.Unlock(unlockCtx, token)
	cancel()
	if planErr != nil || unlockErr != nil {
		return errors.Join(planErr, unlockErr)
	}
	for _, worker := range workers {
		if err := r.provisioner.Ensure(ctx, worker); err != nil {
			return fmt.Errorf("ensure Worker %q: %w", worker.ID, err)
		}
		r.config.Logger.InfoContext(ctx, "ensured Worker Pod", "worker_id", worker.ID)
	}
	return nil
}

func (r *Reconciler) plan(ctx context.Context) ([]model.Worker, error) {
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

func (r *Reconciler) reconcileWithTimeout(ctx context.Context) {
	reconcileCtx, cancel := context.WithTimeout(ctx, r.config.RequestTimeout)
	defer cancel()
	if err := r.Reconcile(reconcileCtx); err != nil && ctx.Err() == nil {
		r.config.Logger.ErrorContext(ctx, "reconcile Workers", "error", err)
	}
}
