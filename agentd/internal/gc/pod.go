// Package gc reconciles Worker runtime resources and retained database records.
package gc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	controlk8s "github.com/compforge/agentd/agentd/internal/k8s"
	controllifecycle "github.com/compforge/agentd/agentd/internal/lifecycle"
	controllock "github.com/compforge/agentd/agentd/internal/lock"
	"github.com/compforge/agentd/agentd/internal/model"
	"github.com/compforge/agentd/agentd/internal/repo"
)

const workerPoolLock = "worker-pool:capacity"

type PodSource interface {
	ListAgentletPods(context.Context) ([]controlk8s.PodSnapshot, error)
	DeleteWorkerPod(context.Context, string) error
}

type PodConfig struct {
	Interval        time.Duration
	RequestTimeout  time.Duration
	LeaseTTL        time.Duration
	IdleTTL         time.Duration
	MinIdleWorkers  int
	DeleteBatchSize int
	Logger          *slog.Logger
}

type PodGC struct {
	repository  repo.Repository
	locker      controllock.Locker
	pods        PodSource
	provisioner controllifecycle.Provisioner
	config      PodConfig
}

func NewPodGC(
	repository repo.Repository,
	locker controllock.Locker,
	pods PodSource,
	provisioner controllifecycle.Provisioner,
	config PodConfig,
) (*PodGC, error) {
	if repository == nil || locker == nil || pods == nil || provisioner == nil {
		return nil, fmt.Errorf("create Worker Pod GC: dependencies are required")
	}
	if config.Interval <= 0 || config.RequestTimeout <= 0 || config.LeaseTTL <= 0 || config.IdleTTL <= 0 {
		return nil, fmt.Errorf("create Worker Pod GC: durations must be positive")
	}
	if config.MinIdleWorkers < 0 || config.DeleteBatchSize <= 0 {
		return nil, fmt.Errorf("create Worker Pod GC: invalid capacity configuration")
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	return &PodGC{
		repository: repository, locker: locker, pods: pods, provisioner: provisioner, config: config,
	}, nil
}

func (g *PodGC) Run(ctx context.Context) {
	g.reconcileWithTimeout(ctx)
	ticker := time.NewTicker(g.config.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			g.reconcileWithTimeout(ctx)
		}
	}
}

func (g *PodGC) Reconcile(ctx context.Context) error {
	workers, err := g.repository.ListWorkers(ctx)
	if err != nil {
		// Fail closed: without the complete owner set no Pod can be classified
		// as a zombie safely.
		return fmt.Errorf("list Workers for Pod GC: %w", err)
	}
	pods, err := g.pods.ListAgentletPods(ctx)
	if err != nil {
		return fmt.Errorf("list Worker Pods for GC: %w", err)
	}
	known := make(map[string]model.Worker, len(workers))
	for _, worker := range workers {
		known[worker.ID] = worker
	}
	deleted := 0
	for _, pod := range pods {
		if !pod.Managed {
			continue
		}
		worker, exists := known[pod.ID]
		if exists && worker.Name == pod.Name {
			continue
		}
		if deleted == g.config.DeleteBatchSize {
			break
		}
		if err := g.pods.DeleteWorkerPod(ctx, pod.Name); err != nil {
			return fmt.Errorf("delete zombie Worker Pod %q: %w", pod.Name, err)
		}
		deleted++
	}

	token, err := g.locker.Lock(ctx, workerPoolLock, g.config.LeaseTTL)
	if errors.Is(err, controllock.ErrLocked) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("lock Worker Pod GC: %w", err)
	}
	destroy, reconcileErr := g.reconcileKnownWorkers(ctx, workers, pods, g.config.DeleteBatchSize-deleted)
	unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	unlockErr := g.locker.Unlock(unlockCtx, token)
	cancel()
	if reconcileErr != nil || unlockErr != nil {
		return errors.Join(reconcileErr, unlockErr)
	}
	for _, worker := range destroy {
		if err := g.provisioner.Destroy(ctx, worker); err != nil {
			return fmt.Errorf("destroy retired Worker %q: %w", worker.ID, err)
		}
	}
	return nil
}

func (g *PodGC) reconcileKnownWorkers(
	ctx context.Context,
	workers []model.Worker,
	pods []controlk8s.PodSnapshot,
	limit int,
) ([]model.Worker, error) {
	if limit <= 0 {
		return nil, nil
	}
	known := make(map[string]model.Worker, len(workers))
	for _, worker := range workers {
		known[worker.ID] = worker
	}
	present := make(map[string]struct{}, len(pods))
	for _, pod := range pods {
		worker, exists := known[pod.ID]
		if pod.Managed && exists && worker.Name == pod.Name {
			present[pod.ID] = struct{}{}
		}
	}
	now := time.Now().UTC()
	idle := make([]model.Worker, 0)
	destroy := make([]model.Worker, 0, limit)
	for _, worker := range workers {
		_, exists := present[worker.ID]
		switch worker.Phase {
		case model.WorkerPhaseCreating:
			continue
		case model.WorkerPhaseActive:
			if !exists {
				if _, _, err := g.movePhase(
					ctx, worker.ID, model.WorkerPhaseActive, model.WorkerPhaseRetired, false, true, now,
				); err != nil {
					return nil, err
				}
				continue
			}
			assigned, err := g.repository.CountWorkerSessions(ctx, worker.ID)
			if err != nil {
				return nil, err
			}
			if assigned == 0 && worker.IdleSince != nil && !worker.IdleSince.Add(g.config.IdleTTL).After(now) {
				idle = append(idle, worker)
			}
		case model.WorkerPhaseDraining:
			retired, ok, err := g.movePhase(
				ctx, worker.ID, model.WorkerPhaseDraining, model.WorkerPhaseRetired, true, false, now,
			)
			if err != nil {
				return nil, err
			}
			if ok && exists {
				destroy = append(destroy, retired)
			}
		case model.WorkerPhaseRetired:
			if exists {
				destroy = append(destroy, worker)
			}
		}
	}
	sort.Slice(idle, func(i, j int) bool { return idle[i].IdleSince.Before(*idle[j].IdleSince) })
	reclaim := len(idle) - g.config.MinIdleWorkers
	for i := 0; i < reclaim && len(destroy) < limit; i++ {
		draining, ok, err := g.movePhase(
			ctx, idle[i].ID, model.WorkerPhaseActive, model.WorkerPhaseDraining, true, false, now,
		)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		retired, ok, err := g.movePhase(
			ctx, draining.ID, model.WorkerPhaseDraining, model.WorkerPhaseRetired, true, false, now,
		)
		if err != nil {
			return nil, err
		}
		if ok {
			destroy = append(destroy, retired)
		}
	}
	if len(destroy) > limit {
		destroy = destroy[:limit]
	}
	return destroy, nil
}

func (g *PodGC) movePhase(
	ctx context.Context,
	workerID string,
	from model.WorkerPhase,
	to model.WorkerPhase,
	requireIdle bool,
	markAbsent bool,
	now time.Time,
) (model.Worker, bool, error) {
	var moved model.Worker
	ok := false
	err := g.repository.Transaction(ctx, func(repository repo.Repository) error {
		worker, err := repository.GetWorkerForUpdate(ctx, workerID)
		if err != nil {
			if err == repo.ErrNotFound {
				return nil
			}
			return err
		}
		if worker.Phase != from {
			return nil
		}
		if requireIdle {
			assigned, err := repository.CountWorkerSessions(ctx, workerID)
			if err != nil {
				return err
			}
			if assigned != 0 {
				return nil
			}
		}
		worker.Phase = to
		worker.UpdatedAt = now
		if markAbsent {
			worker.AbsentAt = &now
		}
		if err := repository.PutWorker(ctx, worker); err != nil {
			return err
		}
		moved = worker
		ok = true
		return nil
	})
	return moved, ok, err
}

func (g *PodGC) reconcileWithTimeout(ctx context.Context) {
	reconcileCtx, cancel := context.WithTimeout(ctx, g.config.RequestTimeout)
	defer cancel()
	if err := g.Reconcile(reconcileCtx); err != nil && ctx.Err() == nil {
		g.config.Logger.ErrorContext(ctx, "reconcile Worker Pod GC", "error", err)
	}
}
