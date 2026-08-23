// Package gc plans and applies Worker Pod reclamation. The parent worker Pool
// owns scheduling and serialization with capacity creation.
package gc

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/compforge/agentd/agentd/internal/model"
	"github.com/compforge/agentd/agentd/internal/repo"
	controlk8s "github.com/compforge/agentd/agentd/internal/worker/k8s"
)

type PodSource interface {
	ListAgentletPods(context.Context) ([]controlk8s.PodSnapshot, error)
	DeleteWorkerPod(context.Context, string) error
}

type Provisioner interface {
	Destroy(context.Context, model.Worker) error
}

type PodConfig struct {
	IdleTTL         time.Duration
	MinWorkers      int
	MinIdleWorkers  int
	DeleteBatchSize int
	Logger          *slog.Logger
}

type OrphanPod struct {
	WorkerID string
	Name     string
}

type Actions struct {
	OrphanPods []OrphanPod
	Workers    []model.Worker
}

type PodGC struct {
	repository  repo.Repository
	pods        PodSource
	provisioner Provisioner
	config      PodConfig
}

func NewPodGC(
	repository repo.Repository,
	pods PodSource,
	provisioner Provisioner,
	config PodConfig,
) (*PodGC, error) {
	if repository == nil || pods == nil || provisioner == nil {
		return nil, fmt.Errorf("create Worker Pod GC: dependencies are required")
	}
	if config.IdleTTL <= 0 {
		return nil, fmt.Errorf("create Worker Pod GC: idle TTL must be positive")
	}
	if config.MinWorkers < 0 || config.MinIdleWorkers < 0 || config.DeleteBatchSize <= 0 {
		return nil, fmt.Errorf("create Worker Pod GC: invalid capacity configuration")
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	return &PodGC{
		repository:  repository,
		pods:        pods,
		provisioner: provisioner,
		config:      config,
	}, nil
}

// Plan persists Worker phase changes while the parent Pool holds the capacity
// lease. Destructive Kubernetes calls are returned as actions and happen only
// after the lease is released.
func (g *PodGC) Plan(ctx context.Context) (Actions, error) {
	workers, err := g.repository.ListWorkers(ctx)
	if err != nil {
		// Fail closed: without the complete owner set no Pod can be classified
		// as an orphan safely.
		return Actions{}, fmt.Errorf("list Workers for Pod GC: %w", err)
	}
	pods, err := g.pods.ListAgentletPods(ctx)
	if err != nil {
		return Actions{}, fmt.Errorf("list Worker Pods for GC: %w", err)
	}

	known := make(map[string]model.Worker, len(workers))
	for _, worker := range workers {
		known[worker.ID] = worker
	}
	actions := Actions{
		OrphanPods: make([]OrphanPod, 0, g.config.DeleteBatchSize),
	}
	for _, pod := range pods {
		if !pod.Managed {
			continue
		}
		worker, exists := known[pod.ID]
		if exists && worker.Name == pod.Name {
			continue
		}
		if len(actions.OrphanPods) == g.config.DeleteBatchSize {
			return actions, nil
		}
		actions.OrphanPods = append(actions.OrphanPods, OrphanPod{WorkerID: pod.ID, Name: pod.Name})
	}

	actions.Workers, err = g.reconcileKnownWorkers(
		ctx, workers, pods, g.config.DeleteBatchSize-len(actions.OrphanPods),
	)
	return actions, err
}

// Apply performs a previously persisted reclamation plan. Delete and Destroy
// are idempotent, so an interrupted pass can safely repeat them.
func (g *PodGC) Apply(ctx context.Context, actions Actions) error {
	for _, pod := range actions.OrphanPods {
		if err := g.pods.DeleteWorkerPod(ctx, pod.Name); err != nil {
			return fmt.Errorf("delete orphan Worker Pod %q: %w", pod.Name, err)
		}
		g.config.Logger.InfoContext(ctx, "deleted orphan Worker Pod",
			"worker_id", pod.WorkerID, "pod_name", pod.Name)
	}
	for _, worker := range actions.Workers {
		if err := g.provisioner.Destroy(ctx, worker); err != nil {
			return fmt.Errorf("destroy retired Worker %q: %w", worker.ID, err)
		}
		g.config.Logger.InfoContext(ctx, "destroyed Worker Pod",
			"worker_id", worker.ID, "worker_name", worker.Name)
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
	availableWorkers := 0
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
			availableWorkers++
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
	if floorLimit := availableWorkers - g.config.MinWorkers; reclaim > floorLimit {
		reclaim = floorLimit
	}
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
