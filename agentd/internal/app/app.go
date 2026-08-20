package app

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	agentledger "github.com/compforge/agent-ledger/go"
	"github.com/compforge/agentd/agentd/internal/model"
	"github.com/compforge/agentd/agentd/internal/repo"
	"github.com/compforge/agentd/agentd/internal/scheduler"
)

type App struct {
	repository repo.Repository
	scheduler  *scheduler.Scheduler
}

func New(repository repo.Repository, observationTimeout time.Duration) (*App, error) {
	if repository == nil {
		return nil, fmt.Errorf("create control plane: repository is required")
	}
	if observationTimeout <= 0 {
		return nil, fmt.Errorf("create control plane: observation timeout must be positive")
	}
	return &App{repository: repository, scheduler: scheduler.New(observationTimeout)}, nil
}

func (a *App) ObserveWorker(ctx context.Context, worker model.Worker) (model.Worker, error) {
	if strings.TrimSpace(worker.ID) == "" || strings.TrimSpace(worker.Name) == "" {
		return model.Worker{}, fmt.Errorf("%w: worker id and name are required", ErrInvalid)
	}
	if worker.Capacity <= 0 {
		return model.Worker{}, fmt.Errorf("%w: worker %q capacity must be positive", ErrInvalid, worker.ID)
	}
	status, err := parseWorkerObserverStatus(worker.ObserverStatus)
	if err != nil {
		return model.Worker{}, fmt.Errorf("%w: worker %q observer_status: %v", ErrInvalid, worker.ID, err)
	}
	if status.ObservedAt.IsZero() {
		return model.Worker{}, fmt.Errorf("%w: worker %q observer_status.observed_at is required", ErrInvalid, worker.ID)
	}
	if status.Ready && strings.TrimSpace(status.Endpoint) == "" {
		return model.Worker{}, fmt.Errorf("%w: ready worker %q observer_status.endpoint is required", ErrInvalid, worker.ID)
	}
	var observed model.Worker
	err = a.repository.Transaction(ctx, func(repository repo.Repository) error {
		now := time.Now().UTC()
		existing, loadErr := repository.GetWorkerForUpdate(ctx, worker.ID)
		if loadErr != nil && loadErr != repo.ErrNotFound {
			return fmt.Errorf("load worker %q: %w", worker.ID, loadErr)
		}
		if loadErr == nil {
			currentStatus, parseErr := parseWorkerObserverStatus(existing.ObserverStatus)
			if parseErr == nil && currentStatus.ObservedAt.After(status.ObservedAt) {
				observed = existing
				return nil
			}
			worker.CreatedAt = existing.CreatedAt
			worker.Phase = existing.Phase
			worker.IdleSince = existing.IdleSince
			worker.AbsentAt = existing.AbsentAt
		} else {
			worker.CreatedAt = now
			if worker.Phase == "" {
				worker.Phase = model.WorkerPhaseCreating
			}
		}
		if worker.Phase == model.WorkerPhaseCreating && status.Ready {
			worker.Phase = model.WorkerPhaseActive
			worker.IdleSince = &now
		}
		if status.Exists {
			worker.AbsentAt = nil
		} else if worker.AbsentAt == nil {
			worker.AbsentAt = &now
		}
		worker.UpdatedAt = now
		if err := repository.PutWorker(ctx, worker); err != nil {
			return fmt.Errorf("persist worker %q: %w", worker.ID, err)
		}
		observed = worker
		return nil
	})
	if err != nil {
		return model.Worker{}, err
	}
	return observed, nil
}

func (a *App) ListWorkers(ctx context.Context) ([]model.Worker, error) {
	workers, err := a.repository.ListWorkers(ctx)
	if err != nil {
		return nil, fmt.Errorf("list workers: %w", err)
	}
	return workers, nil
}

// Assign returns the live Assignment for a Session or selects a Worker with
// free Work capacity. The transaction locks the schedulable Worker set so two
// concurrent schedulers cannot consume the same final slot.
//
// +spec=`A Session reuses its live Assignment; otherwise agentd persists a new Assignment on the least-loaded live Worker whose current Assignment count is below capacity`
// +link=agentd/docs/agentd.md
func (a *App) Assign(ctx context.Context, sessionID string) (model.Assignment, error) {
	if strings.TrimSpace(sessionID) == "" {
		return model.Assignment{}, fmt.Errorf("%w: session id is required", ErrInvalid)
	}
	var (
		selected   model.Assignment
		noCapacity bool
	)
	err := a.repository.Transaction(ctx, func(repository repo.Repository) error {
		now := time.Now().UTC()
		workers, err := repository.ListWorkersForUpdate(ctx)
		if err != nil {
			return fmt.Errorf("list workers: %w", err)
		}
		session, err := repository.GetSessionForUpdate(ctx, sessionID)
		if err != nil {
			return fmt.Errorf("load session %q: %w", sessionID, err)
		}

		candidates := make([]scheduler.Candidate, 0, len(workers))
		for _, worker := range workers {
			count, err := repository.CountWorkerSessions(ctx, worker.ID)
			if err != nil {
				return fmt.Errorf("count sessions for worker %q: %w", worker.ID, err)
			}
			candidates = append(candidates, schedulingCandidate(worker, count))
		}
		decision := a.scheduler.Schedule(now, session.WorkerID, candidates)
		if decision.Reason == scheduler.ReasonExisting {
			selected = assignmentFromSession(session)
			return nil
		}
		session.Status = model.SessionStatusRescheduling
		session.AssignmentID = ""
		session.WorkerID = ""
		session.AssignedAt = nil
		session.UpdatedAt = now
		if decision.Reason == scheduler.ReasonNoCapacity {
			if err := repository.PutSession(ctx, session); err != nil {
				return fmt.Errorf("persist pending session %q: %w", sessionID, err)
			}
			noCapacity = true
			return nil
		}
		session.AssignmentID = agentledger.NewID()
		session.WorkerID = decision.WorkerID
		session.AssignedAt = &now
		if err := repository.PutSession(ctx, session); err != nil {
			return fmt.Errorf("persist binding for session %q: %w", sessionID, err)
		}
		selected = assignmentFromSession(session)
		worker, err := repository.GetWorkerForUpdate(ctx, decision.WorkerID)
		if err != nil {
			return fmt.Errorf("load assigned worker %q: %w", decision.WorkerID, err)
		}
		worker.IdleSince = nil
		worker.UpdatedAt = now
		if err := repository.PutWorker(ctx, worker); err != nil {
			return fmt.Errorf("mark assigned worker %q busy: %w", decision.WorkerID, err)
		}
		return nil
	})
	if err != nil {
		return model.Assignment{}, err
	}
	if noCapacity {
		return model.Assignment{}, ErrNoCapacity
	}
	return selected, nil
}

func assignmentFromSession(session model.Session) model.Assignment {
	createdAt := session.UpdatedAt
	if session.AssignedAt != nil {
		createdAt = *session.AssignedAt
	}
	return model.Assignment{
		ID: session.AssignmentID, SessionID: session.ID, WorkerID: session.WorkerID,
		CreatedAt: createdAt, UpdatedAt: session.UpdatedAt,
	}
}

func schedulingCandidate(worker model.Worker, assignedCount int64) scheduler.Candidate {
	candidate := scheduler.Candidate{
		WorkerID: worker.ID, Capacity: worker.Capacity, AssignedCount: assignedCount,
	}
	if worker.Phase != model.WorkerPhaseActive {
		return candidate
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

func parseWorkerObserverStatus(raw json.RawMessage) (model.WorkerObserverStatus, error) {
	var status model.WorkerObserverStatus
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
	return a.repository.Transaction(ctx, func(repository repo.Repository) error {
		session, err := repository.GetSessionForUpdate(ctx, sessionID)
		if err != nil && err != repo.ErrNotFound {
			return fmt.Errorf("load session %q: %w", sessionID, err)
		}
		if err == nil {
			workerID := session.WorkerID
			worker, workerErr := repository.GetWorkerForUpdate(ctx, workerID)
			if workerErr != nil && workerErr != repo.ErrNotFound {
				return workerErr
			}
			now := time.Now().UTC()
			session.Status = model.SessionStatusIdle
			session.AssignmentID = ""
			session.WorkerID = ""
			session.AssignedAt = nil
			session.UpdatedAt = now
			if err := repository.PutSession(ctx, session); err != nil {
				return fmt.Errorf("release session %q: %w", sessionID, err)
			}
			if workerID == "" {
				return nil
			}
			remaining, err := repository.CountWorkerSessions(ctx, workerID)
			if err != nil {
				return err
			}
			if remaining == 0 && workerErr == nil {
				worker.IdleSince = &now
				worker.UpdatedAt = now
				if err := repository.PutWorker(ctx, worker); err != nil {
					return err
				}
			}
		}
		return nil
	})
}
