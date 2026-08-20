package app

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	agentledger "github.com/compforge/agent-ledger/go"
	"github.com/compforge/agentd/agentd/internal/scheduler"
)

type App struct {
	repository Repository
	scheduler  *scheduler.Scheduler
}

func New(repository Repository, observationTimeout time.Duration) (*App, error) {
	if repository == nil {
		return nil, fmt.Errorf("create control plane: repository is required")
	}
	if observationTimeout <= 0 {
		return nil, fmt.Errorf("create control plane: observation timeout must be positive")
	}
	return &App{repository: repository, scheduler: scheduler.New(observationTimeout)}, nil
}

func (a *App) ObserveWorker(ctx context.Context, worker Worker) (Worker, error) {
	if strings.TrimSpace(worker.ID) == "" || strings.TrimSpace(worker.Name) == "" {
		return Worker{}, fmt.Errorf("%w: worker id and name are required", ErrInvalid)
	}
	if worker.MaxRuns <= 0 {
		return Worker{}, fmt.Errorf("%w: worker %q max_runs must be positive", ErrInvalid, worker.ID)
	}
	status, err := parseWorkerObserverStatus(worker.ObserverStatus)
	if err != nil {
		return Worker{}, fmt.Errorf("%w: worker %q observer_status: %v", ErrInvalid, worker.ID, err)
	}
	if status.ObservedAt.IsZero() {
		return Worker{}, fmt.Errorf("%w: worker %q observer_status.observed_at is required", ErrInvalid, worker.ID)
	}
	if status.Ready && strings.TrimSpace(status.Endpoint) == "" {
		return Worker{}, fmt.Errorf("%w: ready worker %q observer_status.endpoint is required", ErrInvalid, worker.ID)
	}
	now := time.Now().UTC()
	existing, err := a.repository.GetWorker(ctx, worker.ID)
	if err != nil && err != ErrNotFound {
		return Worker{}, fmt.Errorf("load worker %q: %w", worker.ID, err)
	}
	worker.UpdatedAt = now
	if err == ErrNotFound {
		worker.CreatedAt = now
	} else {
		worker.CreatedAt = existing.CreatedAt
	}
	if err := a.repository.PutWorker(ctx, worker); err != nil {
		return Worker{}, fmt.Errorf("persist worker %q: %w", worker.ID, err)
	}
	return worker, nil
}

func (a *App) ListWorkers(ctx context.Context) ([]Worker, error) {
	workers, err := a.repository.ListWorkers(ctx)
	if err != nil {
		return nil, fmt.Errorf("list workers: %w", err)
	}
	return workers, nil
}

// Assign returns the live Assignment for a Session or selects a Worker with
// free Run capacity. The transaction locks the schedulable Worker set so two
// concurrent schedulers cannot consume the same final slot.
//
// +spec=`A Session reuses its live Assignment; otherwise agentd persists a new Assignment on the least-loaded live Worker whose current Assignment count is below max_runs`
// +link=agentd/docs/control-plane.md
func (a *App) Assign(ctx context.Context, sessionID string) (Assignment, error) {
	if strings.TrimSpace(sessionID) == "" {
		return Assignment{}, fmt.Errorf("%w: session id is required", ErrInvalid)
	}
	var selected Assignment
	err := a.repository.Transaction(ctx, func(repository Repository) error {
		now := time.Now().UTC()
		workers, err := repository.ListWorkersForUpdate(ctx)
		if err != nil {
			return fmt.Errorf("list workers: %w", err)
		}
		existing, err := repository.GetAssignment(ctx, sessionID)
		hasExisting := err == nil
		if err != nil && err != ErrNotFound {
			return fmt.Errorf("load assignment for session %q: %w", sessionID, err)
		}

		candidates := make([]scheduler.Candidate, 0, len(workers))
		for _, worker := range workers {
			count, err := repository.CountAssignments(ctx, worker.ID)
			if err != nil {
				return fmt.Errorf("count assignments for worker %q: %w", worker.ID, err)
			}
			candidates = append(candidates, schedulingCandidate(worker, count))
		}
		existingWorkerID := ""
		if hasExisting {
			existingWorkerID = existing.WorkerID
		}
		decision := a.scheduler.Schedule(now, existingWorkerID, candidates)
		if decision.Reason == scheduler.ReasonExisting {
			selected = existing
			return nil
		}
		if hasExisting {
			if err := repository.DeleteAssignment(ctx, sessionID); err != nil {
				return fmt.Errorf("release stale assignment for session %q: %w", sessionID, err)
			}
		}
		if decision.Reason == scheduler.ReasonNoCapacity {
			return ErrNoCapacity
		}
		selected = Assignment{
			ID: agentledger.NewID(), SessionID: sessionID, WorkerID: decision.WorkerID,
			CreatedAt: now, UpdatedAt: now,
		}
		if err := repository.PutAssignment(ctx, selected); err != nil {
			return fmt.Errorf("persist assignment for session %q: %w", sessionID, err)
		}
		return nil
	})
	if err != nil {
		return Assignment{}, err
	}
	return selected, nil
}

func schedulingCandidate(worker Worker, assignedRuns int64) scheduler.Candidate {
	candidate := scheduler.Candidate{
		WorkerID: worker.ID, MaxRuns: worker.MaxRuns, AssignedRuns: assignedRuns,
	}
	status, err := parseWorkerObserverStatus(worker.ObserverStatus)
	if err != nil {
		return candidate
	}
	candidate.Observation = scheduler.Observation{
		ObservedAt: status.ObservedAt,
		Exists:     status.Exists,
		Ready:      status.Ready,
		Endpoint:   status.Endpoint,
	}
	return candidate
}

func parseWorkerObserverStatus(raw json.RawMessage) (WorkerObserverStatus, error) {
	var status WorkerObserverStatus
	if len(raw) == 0 {
		return status, fmt.Errorf("is required")
	}
	if err := json.Unmarshal(raw, &status); err != nil {
		return status, err
	}
	return status, nil
}

func (a *App) Release(ctx context.Context, sessionID string) error {
	if strings.TrimSpace(sessionID) == "" {
		return fmt.Errorf("%w: session id is required", ErrInvalid)
	}
	if err := a.repository.DeleteAssignment(ctx, sessionID); err != nil {
		return fmt.Errorf("release session %q: %w", sessionID, err)
	}
	return nil
}
