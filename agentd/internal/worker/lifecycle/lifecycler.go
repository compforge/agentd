// Package lifecycle reconciles durable Session demand into Worker capacity.
package lifecycle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	agentledger "github.com/compforge/agent-ledger/go"
	controllock "github.com/compforge/agentd/agentd/internal/lock"
	"github.com/compforge/agentd/agentd/internal/model"
	"github.com/compforge/agentd/agentd/internal/repo"
)

const poolLockResource = "worker-pool:capacity"

type Provisioner interface {
	Ensure(context.Context, model.Worker) error
	Destroy(context.Context, model.Worker) error
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

type Lifecycler struct {
	repository  repo.Repository
	locker      controllock.Locker
	provisioner Provisioner
	config      Config
	trigger     chan struct{}
}

func New(
	repository repo.Repository,
	locker controllock.Locker,
	provisioner Provisioner,
	config Config,
) (*Lifecycler, error) {
	if repository == nil || locker == nil || provisioner == nil {
		return nil, fmt.Errorf("create Worker Lifecycler: repository, locker, and provisioner are required")
	}
	if config.Interval <= 0 || config.RequestTimeout <= 0 || config.LeaseTTL <= 0 {
		return nil, fmt.Errorf("create Worker Lifecycler: intervals and lease TTL must be positive")
	}
	if config.WorkerCapacity <= 0 || config.CreateBatchSize <= 0 || config.MinWorkers < 0 || config.MinIdleWorkers < 0 {
		return nil, fmt.Errorf("create Worker Lifecycler: invalid capacity configuration")
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	return &Lifecycler{
		repository: repository, locker: locker, provisioner: provisioner,
		config: config, trigger: make(chan struct{}, 1),
	}, nil
}

func (l *Lifecycler) NotifyDemand() {
	select {
	case l.trigger <- struct{}{}:
	default:
	}
}

func (l *Lifecycler) Run(ctx context.Context) {
	l.reconcileWithTimeout(ctx)
	ticker := time.NewTicker(l.config.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-l.trigger:
			l.reconcileWithTimeout(ctx)
		case <-ticker.C:
			l.reconcileWithTimeout(ctx)
		}
	}
}

func (l *Lifecycler) Reconcile(ctx context.Context) error {
	if err := l.ensureCreating(ctx); err != nil {
		return err
	}
	token, err := l.locker.Lock(ctx, poolLockResource, l.config.LeaseTTL)
	if errors.Is(err, controllock.ErrLocked) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("lock Worker capacity reconcile: %w", err)
	}
	created, planErr := l.planCapacity(ctx)
	unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	unlockErr := l.locker.Unlock(unlockCtx, token)
	cancel()
	if planErr != nil || unlockErr != nil {
		return errors.Join(planErr, unlockErr)
	}
	for _, worker := range created {
		if err := l.provisioner.Ensure(ctx, worker); err != nil {
			return fmt.Errorf("ensure Worker %q: %w", worker.ID, err)
		}
	}
	return nil
}

func (l *Lifecycler) planCapacity(ctx context.Context) ([]model.Worker, error) {
	workers, err := l.repository.ListWorkers(ctx)
	if err != nil {
		return nil, fmt.Errorf("list Workers for capacity: %w", err)
	}
	if creationBlocked(workers) {
		return nil, nil
	}
	pending, err := l.repository.CountPendingSessions(ctx)
	if err != nil {
		return nil, err
	}
	plannedFreeSlots := int64(0)
	plannedWorkers := 0
	for _, worker := range workers {
		switch worker.Phase {
		case model.WorkerPhaseCreating:
			plannedWorkers++
			plannedFreeSlots += int64(worker.Capacity)
		case model.WorkerPhaseActive:
			plannedWorkers++
			assigned, err := l.repository.CountWorkerSessions(ctx, worker.ID)
			if err != nil {
				return nil, err
			}
			if assigned < int64(worker.Capacity) {
				plannedFreeSlots += int64(worker.Capacity) - assigned
			}
		}
	}
	requiredFreeSlots := pending + int64(l.config.MinIdleWorkers*l.config.WorkerCapacity)
	deficit := requiredFreeSlots - plannedFreeSlots
	count := 0
	if deficit > 0 {
		count = int((deficit + int64(l.config.WorkerCapacity) - 1) / int64(l.config.WorkerCapacity))
	}
	if floorDeficit := l.config.MinWorkers - plannedWorkers; floorDeficit > count {
		count = floorDeficit
	}
	if count <= 0 {
		return nil, nil
	}
	if count > l.config.CreateBatchSize {
		count = l.config.CreateBatchSize
	}
	now := time.Now().UTC()
	created := make([]model.Worker, 0, count)
	for range count {
		id := agentledger.NewID()
		worker := model.Worker{
			ID: id, Name: "agentd-worker-" + id, Capacity: l.config.WorkerCapacity,
			Phase: model.WorkerPhaseCreating, CreatedAt: now, UpdatedAt: now,
		}
		if err := l.repository.PutWorker(ctx, worker); err != nil {
			return created, fmt.Errorf("persist creating Worker %q: %w", worker.ID, err)
		}
		created = append(created, worker)
	}
	return created, nil
}

func (l *Lifecycler) ensureCreating(ctx context.Context) error {
	workers, err := l.repository.ListWorkers(ctx)
	if err != nil {
		return fmt.Errorf("list creating Workers: %w", err)
	}
	for _, worker := range workers {
		if worker.Phase != model.WorkerPhaseCreating {
			continue
		}
		if err := l.provisioner.Ensure(ctx, worker); err != nil {
			return fmt.Errorf("ensure creating Worker %q: %w", worker.ID, err)
		}
	}
	return nil
}

func creationBlocked(workers []model.Worker) bool {
	for _, worker := range workers {
		if worker.Phase != model.WorkerPhaseCreating || len(worker.ObserverStatus) == 0 {
			continue
		}
		var status model.WorkerObserverStatus
		if err := json.Unmarshal(worker.ObserverStatus, &status); err != nil {
			continue
		}
		if status.Unschedulable || status.PodPhase == "Pending" {
			return true
		}
	}
	return false
}

func (l *Lifecycler) reconcileWithTimeout(ctx context.Context) {
	reconcileCtx, cancel := context.WithTimeout(ctx, l.config.RequestTimeout)
	defer cancel()
	if err := l.Reconcile(reconcileCtx); err != nil && ctx.Err() == nil {
		l.config.Logger.ErrorContext(ctx, "reconcile Worker capacity", "error", err)
	}
}
