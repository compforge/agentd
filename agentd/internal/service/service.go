package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	agentledger "github.com/compforge/agent-ledger/go"
	"github.com/compforge/agentd/agentd/internal/model"
	"github.com/compforge/agentd/agentd/internal/repo"
	"github.com/compforge/agentd/agentd/internal/session/scheduler"
)

type Service struct {
	repository         repo.Repository
	scheduler          *scheduler.Scheduler
	observationTimeout time.Duration
	workerCapacity     int
}

func New(
	repository repo.Repository,
	observationTimeout time.Duration,
	workerCapacity int,
) (*Service, error) {
	if repository == nil {
		return nil, fmt.Errorf("create control plane: repository is required")
	}
	if observationTimeout <= 0 {
		return nil, fmt.Errorf("create control plane: observation timeout must be positive")
	}
	if workerCapacity < 0 {
		return nil, fmt.Errorf("create control plane: Worker capacity must not be negative")
	}
	return &Service{
		repository: repository, scheduler: scheduler.New(observationTimeout),
		observationTimeout: observationTimeout, workerCapacity: workerCapacity,
	}, nil
}

func (a *Service) ObserveWorker(ctx context.Context, worker model.Worker) (model.Worker, error) {
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

func (a *Service) ListWorkers(ctx context.Context) ([]model.Worker, error) {
	workers, err := a.repository.ListWorkers(ctx)
	if err != nil {
		return nil, fmt.Errorf("list workers: %w", err)
	}
	return workers, nil
}

// ReconcilePlacement converges one Session's durable execution demand and
// observed execution facts into its current Worker placement. Placement may
// move only after the old execution reached a stable boundary or its Worker is
// confirmed gone; a transiently unreachable Worker keeps its fence.
//
// +spec=`Session Reconciler is the only owner of placement changes; Scheduler scores eligible Workers only at a safe placement boundary, and unavailable capacity is published as a creating Worker placement`
// +link=agentd/docs/agentd.md
func (a *Service) ReconcilePlacement(
	ctx context.Context,
	sessionID string,
	hasDemand bool,
) (model.Session, error) {
	if strings.TrimSpace(sessionID) == "" {
		return model.Session{}, fmt.Errorf("%w: session id is required", ErrInvalid)
	}
	var (
		reconciled      model.Session
		noCapacity      bool
		publishedWorker bool
		placementAction string
		previousWorker  string
		placementReason scheduler.Reason
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
		oldPlacement := session.Placement
		currentWorker, currentWorkerFound := findWorker(workers, oldPlacement.WorkerID)
		stable := placementStable(session)
		gone := oldPlacement.Bound() && placementWorkerGone(currentWorker, currentWorkerFound)

		// A live execution is not preempted merely because another Worker scores
		// better. Soft affinity is evaluated only after a durable safe boundary.
		if oldPlacement.Bound() && !stable && !gone {
			reconciled = session
			return nil
		}
		if session.Status == model.SessionStatusTerminated {
			hasDemand = false
		}
		if !hasDemand {
			if oldPlacement.Bound() && (stable || gone) {
				placementAction = "released"
				previousWorker = oldPlacement.WorkerID
				releasePlacement(&session, now, false)
				if err := repository.PutSession(ctx, session); err != nil {
					return fmt.Errorf("release Session %q placement: %w", sessionID, err)
				}
				if err := markWorkerIdle(ctx, repository, workers, oldPlacement.WorkerID, now); err != nil {
					return err
				}
			} else if !oldPlacement.Bound() && session.Status == model.SessionStatusRescheduling {
				session.Status = model.SessionStatusIdle
				session.Revision++
				session.UpdatedAt = now
				if err := repository.PutSession(ctx, session); err != nil {
					return fmt.Errorf("settle Session %q without demand: %w", sessionID, err)
				}
			}
			reconciled = session
			return nil
		}

		candidates := make([]scheduler.Candidate, 0, len(workers))
		for _, worker := range workers {
			count, err := repository.CountWorkerSessions(ctx, worker.ID)
			if err != nil {
				return fmt.Errorf("count sessions for worker %q: %w", worker.ID, err)
			}
			candidates = append(candidates, schedulingCandidate(worker, count))
		}
		existingWorkerID := ""
		if oldPlacement.Bound() && stable && !gone {
			existingWorkerID = oldPlacement.WorkerID
		}
		decision := a.scheduler.Schedule(now, existingWorkerID, session.LastWorkerID, candidates)
		if decision.Reason == scheduler.ReasonNoCapacity {
			if a.workerCapacity > 0 {
				id := agentledger.NewID()
				worker := model.Worker{
					ID: id, Name: "agentd-worker-" + id, Capacity: a.workerCapacity,
					Phase: model.WorkerPhaseCreating, CreatedAt: now, UpdatedAt: now,
				}
				if err := repository.PutWorker(ctx, worker); err != nil {
					return fmt.Errorf("publish Worker %q for Session %q: %w", id, sessionID, err)
				}
				workers = append(workers, worker)
				publishedWorker = true
				decision = scheduler.Decision{WorkerID: id, Reason: scheduler.ReasonCreating}
			} else {
				releasePlacement(&session, now, true)
				if err := repository.PutSession(ctx, session); err != nil {
					return fmt.Errorf("persist pending session %q: %w", sessionID, err)
				}
				if err := markWorkerIdle(ctx, repository, workers, oldPlacement.WorkerID, now); err != nil {
					return err
				}
				reconciled = session
				noCapacity = true
				return nil
			}
		}

		if decision.WorkerID != oldPlacement.WorkerID || !oldPlacement.Bound() {
			session.Placement = model.SessionPlacement{
				WorkerID: decision.WorkerID, Fence: agentledger.NewID(), PlacedAt: &now,
			}
		}
		previousWorker = oldPlacement.WorkerID
		placementReason = decision.Reason
		switch {
		case !oldPlacement.Bound():
			placementAction = "assigned"
		case oldPlacement.WorkerID != decision.WorkerID:
			placementAction = "moved"
		default:
			placementAction = "reused"
		}
		session.LastWorkerID = decision.WorkerID
		session.ObserverStatus = nil
		session.Status = model.SessionStatusRescheduling
		session.Revision++
		session.UpdatedAt = now
		if err := repository.PutSession(ctx, session); err != nil {
			return fmt.Errorf("persist placement for session %q: %w", sessionID, err)
		}
		if err := markWorkerBusy(ctx, repository, workers, decision.WorkerID, now); err != nil {
			return err
		}
		if oldPlacement.WorkerID != "" && oldPlacement.WorkerID != decision.WorkerID {
			if err := markWorkerIdle(ctx, repository, workers, oldPlacement.WorkerID, now); err != nil {
				return err
			}
		}
		reconciled = session
		return nil
	})
	if err != nil {
		return model.Session{}, err
	}
	if publishedWorker {
		slog.InfoContext(ctx, "published Worker for Session demand",
			"session_id", sessionID, "worker_id", reconciled.Placement.WorkerID,
			"worker_capacity", a.workerCapacity)
	}
	if placementAction != "" {
		slog.InfoContext(ctx, "reconciled Session placement",
			"session_id", sessionID, "action", placementAction,
			"worker_id", reconciled.Placement.WorkerID, "previous_worker_id", previousWorker,
			"placement_fence", reconciled.Placement.Fence, "reason", placementReason)
	}
	if noCapacity {
		slog.WarnContext(ctx, "Session demand has no managed Worker capacity", "session_id", sessionID)
		return reconciled, ErrNoCapacity
	}
	return reconciled, nil
}

func placementStable(session model.Session) bool {
	status, err := parseSessionObserverStatus(session.ObserverStatus)
	if err != nil || status.PlacementFence != session.Placement.Fence {
		return false
	}
	return !status.Exists || status.Status == model.SessionStatusIdle ||
		status.Status == model.SessionStatusTerminated
}

func placementWorkerGone(worker model.Worker, found bool) bool {
	if !found || worker.Phase == model.WorkerPhaseRetired {
		return true
	}
	status, err := parseWorkerObserverStatus(worker.ObserverStatus)
	return err == nil && !status.Exists
}

func releasePlacement(session *model.Session, now time.Time, rescheduling bool) {
	if session.Placement.WorkerID != "" {
		session.LastWorkerID = session.Placement.WorkerID
	}
	session.Placement = model.SessionPlacement{}
	session.ObserverStatus = nil
	if session.Status != model.SessionStatusTerminated {
		if rescheduling {
			session.Status = model.SessionStatusRescheduling
		} else {
			session.Status = model.SessionStatusIdle
		}
	}
	session.Revision++
	session.UpdatedAt = now
}

func findWorker(workers []model.Worker, workerID string) (model.Worker, bool) {
	for _, worker := range workers {
		if worker.ID == workerID {
			return worker, true
		}
	}
	return model.Worker{}, false
}

func markWorkerBusy(
	ctx context.Context,
	repository repo.Repository,
	workers []model.Worker,
	workerID string,
	now time.Time,
) error {
	worker, found := findWorker(workers, workerID)
	if !found {
		return fmt.Errorf("load placed Worker %q: %w", workerID, repo.ErrNotFound)
	}
	worker.IdleSince = nil
	worker.UpdatedAt = now
	if err := repository.PutWorker(ctx, worker); err != nil {
		return fmt.Errorf("mark placed Worker %q busy: %w", workerID, err)
	}
	return nil
}

func markWorkerIdle(
	ctx context.Context,
	repository repo.Repository,
	workers []model.Worker,
	workerID string,
	now time.Time,
) error {
	if workerID == "" {
		return nil
	}
	remaining, err := repository.CountWorkerSessions(ctx, workerID)
	if err != nil {
		return fmt.Errorf("count remaining Sessions for Worker %q: %w", workerID, err)
	}
	worker, found := findWorker(workers, workerID)
	if remaining != 0 || !found {
		return nil
	}
	worker.IdleSince = &now
	worker.UpdatedAt = now
	if err := repository.PutWorker(ctx, worker); err != nil {
		return fmt.Errorf("mark released Worker %q idle: %w", workerID, err)
	}
	return nil
}

func schedulingCandidate(worker model.Worker, assignedCount int64) scheduler.Candidate {
	candidate := scheduler.Candidate{
		WorkerID: worker.ID, Capacity: worker.Capacity, AssignedCount: assignedCount,
	}
	if worker.Phase == model.WorkerPhaseCreating {
		candidate.Reservable = true
		return candidate
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
