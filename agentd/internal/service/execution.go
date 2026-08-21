package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/compforge/agentd/agentd/internal/model"
	"github.com/compforge/agentd/agentd/internal/repo"
	"github.com/compforge/agentd/internal/executionapi"
)

type ExecutionTarget struct {
	Endpoint string
	Work     executionapi.WorkSpec
}

type EventTarget struct {
	Endpoint string
	WorkerID string
}

// PrepareExecution assigns the Session when necessary, then resolves the
// immutable Work snapshot and current Agentlet endpoint for Connector.
func (a *Service) PrepareExecution(ctx context.Context, sessionID string) (ExecutionTarget, error) {
	if _, err := a.Assign(ctx, sessionID); err != nil {
		return ExecutionTarget{}, err
	}
	return a.CurrentExecution(ctx, sessionID)
}

func (a *Service) CurrentExecution(ctx context.Context, sessionID string) (ExecutionTarget, error) {
	session, err := a.repository.GetSession(ctx, sessionID)
	if err != nil {
		return ExecutionTarget{}, fmt.Errorf("load Session %q execution target: %w", sessionID, err)
	}
	if session.AssignmentID == "" || session.WorkerID == "" {
		return ExecutionTarget{}, fmt.Errorf("%w: Session %q", ErrNoAssignment, sessionID)
	}
	worker, err := a.repository.GetWorker(ctx, session.WorkerID)
	if err != nil {
		return ExecutionTarget{}, fmt.Errorf("load assigned Worker %q: %w", session.WorkerID, err)
	}
	status, ready := a.readyWorker(worker, time.Now().UTC())
	if !ready {
		return ExecutionTarget{}, fmt.Errorf("%w: Worker %q has no fresh ready endpoint", ErrUnavailable, worker.ID)
	}
	agent, err := a.repository.GetAgent(ctx, session.AgentID)
	if err != nil {
		return ExecutionTarget{}, fmt.Errorf("load Session %q Agent: %w", sessionID, err)
	}
	environment, err := a.repository.GetEnvironment(ctx, session.EnvironmentID)
	if err != nil {
		return ExecutionTarget{}, fmt.Errorf("load Session %q Environment: %w", sessionID, err)
	}
	return ExecutionTarget{
		Endpoint: status.Endpoint,
		Work: executionapi.WorkSpec{
			AssignmentID: session.AssignmentID,
			WorkerID:     session.WorkerID,
			Session: executionapi.SessionSnapshot{
				ID: session.ID, EnvironmentID: session.EnvironmentID, Title: session.Title,
				Metadata: session.Metadata, Status: string(session.Status), Revision: session.Revision,
				Harness: session.Harness, HarnessVersion: session.HarnessVersion,
				ResumeRef: session.ResumeRef, ResumeRevision: session.ResumeRevision,
				CreatedAt: session.CreatedAt, UpdatedAt: session.UpdatedAt,
			},
			Agent: executionapi.AgentSnapshot{
				ID: agent.ID, Name: agent.Name, Description: agent.Description, ModelID: agent.ModelID,
				System: agent.System, Tools: agent.Tools, Version: agent.Version,
			},
			Environment: executionapi.EnvironmentSnapshot{ID: environment.ID, Config: environment.Config},
		},
	}, nil
}

// ResolveEventTarget selects a data-plane endpoint for durable Event reads.
// Event history belongs to the shared Agentlet database, not to the Worker's
// current in-memory Session assignment.
//
// +spec=`Persisted Event reads use any fresh ready Worker and never create or require an Assignment`
// +link=agentd/docs/agentd.md
func (a *Service) ResolveEventTarget(ctx context.Context, sessionID string) (EventTarget, error) {
	if _, err := a.repository.GetSession(ctx, sessionID); err != nil {
		return EventTarget{}, fmt.Errorf("load Session %q Event target: %w", sessionID, err)
	}
	workers, err := a.repository.ListWorkers(ctx)
	if err != nil {
		return EventTarget{}, fmt.Errorf("list Event read Workers: %w", err)
	}
	now := time.Now().UTC()
	for _, worker := range workers {
		status, ready := a.readyWorker(worker, now)
		if ready {
			return EventTarget{Endpoint: status.Endpoint, WorkerID: worker.ID}, nil
		}
	}
	return EventTarget{}, fmt.Errorf("%w: no fresh ready Worker for Event reads", ErrUnavailable)
}

func (a *Service) readyWorker(worker model.Worker, now time.Time) (model.WorkerObserverStatus, bool) {
	status, err := parseWorkerObserverStatus(worker.ObserverStatus)
	if err != nil {
		return model.WorkerObserverStatus{}, false
	}
	ready := worker.Phase == model.WorkerPhaseActive && status.Exists && status.Ready &&
		strings.TrimSpace(status.Endpoint) != "" && !status.ObservedAt.IsZero() && !status.ObservedAt.After(now) &&
		now.Sub(status.ObservedAt) <= a.observationTimeout
	return status, ready
}

// ObserveSession persists one Agentlet observation and projects its safe facts
// into Control State. Idle and terminal observations release the Assignment in
// the same transaction, so Worker capacity is not held by inactive Sessions.
//
// +spec=`Session observations apply only to the current Assignment; stale observations cannot rewind ResumeRevision; idle or terminal observations atomically release placement`
// +link=agentd/docs/agentd.md
func (a *Service) ObserveSession(
	ctx context.Context,
	sessionID string,
	status model.SessionObserverStatus,
) (model.Session, error) {
	if strings.TrimSpace(status.AssignmentID) == "" {
		return model.Session{}, fmt.Errorf("%w: assignment id is required", ErrInvalid)
	}
	if status.ObservedAt.IsZero() {
		return model.Session{}, fmt.Errorf("%w: observed_at is required", ErrInvalid)
	}
	if status.ResumeRevision < 0 {
		return model.Session{}, fmt.Errorf("%w: resume revision must not be negative", ErrInvalid)
	}
	if status.Exists && !validSessionStatus(status.Status) {
		return model.Session{}, fmt.Errorf("%w: invalid Session status %q", ErrInvalid, status.Status)
	}
	raw, err := json.Marshal(status)
	if err != nil {
		return model.Session{}, fmt.Errorf("encode Session %q observation: %w", sessionID, err)
	}
	release := !status.Exists || status.Status == model.SessionStatusIdle ||
		status.Status == model.SessionStatusTerminated
	var (
		observed   model.Session
		wakeDemand bool
	)
	err = a.repository.Transaction(ctx, func(repository repo.Repository) error {
		var (
			lockedWorker model.Worker
			workerLocked bool
		)
		if release {
			current, err := repository.GetSession(ctx, sessionID)
			if err != nil {
				return err
			}
			if current.AssignmentID != status.AssignmentID {
				return fmt.Errorf("%w: Session %q Assignment changed", ErrConflict, sessionID)
			}
			if current.WorkerID != "" {
				// Scheduler locks Worker before Session. Keep the same order here so
				// an idle observation cannot deadlock a concurrent placement.
				lockedWorker, err = repository.GetWorkerForUpdate(ctx, current.WorkerID)
				if err != nil && err != repo.ErrNotFound {
					return err
				}
				workerLocked = err == nil
			}
		}
		session, err := repository.GetSessionForUpdate(ctx, sessionID)
		if err != nil {
			return err
		}
		if session.AssignmentID != status.AssignmentID {
			return fmt.Errorf("%w: Session %q Assignment changed", ErrConflict, sessionID)
		}
		if current, parseErr := parseSessionObserverStatus(session.ObserverStatus); parseErr == nil &&
			current.ObservedAt.After(status.ObservedAt) {
			observed = session
			return nil
		}

		now := time.Now().UTC()
		session.ObserverStatus = raw
		projectedStatus := status.Status
		if !status.Exists {
			projectedStatus = model.SessionStatusRescheduling
		}
		changed := session.Status != projectedStatus
		session.Status = projectedStatus
		if status.ResumeRevision > session.ResumeRevision ||
			(status.ResumeRevision == session.ResumeRevision && session.ResumeRef == "" && status.ResumeRef != "") {
			session.ResumeRef = status.ResumeRef
			session.ResumeRevision = status.ResumeRevision
			changed = true
		}

		workerID := session.WorkerID
		if release {
			session.AssignmentID = ""
			session.WorkerID = ""
			session.AssignedAt = nil
			changed = true
			wakeDemand = !status.Exists
		}
		if changed {
			session.Revision++
		}
		session.UpdatedAt = now
		if err := repository.PutSession(ctx, session); err != nil {
			return fmt.Errorf("persist Session %q observation: %w", sessionID, err)
		}
		if release && workerID != "" {
			remaining, err := repository.CountWorkerSessions(ctx, workerID)
			if err != nil {
				return err
			}
			if remaining == 0 {
				if workerLocked && lockedWorker.ID == workerID {
					lockedWorker.IdleSince = &now
					lockedWorker.UpdatedAt = now
					if err := repository.PutWorker(ctx, lockedWorker); err != nil {
						return fmt.Errorf("mark Worker %q idle: %w", workerID, err)
					}
				}
			}
		}
		observed = session
		return nil
	})
	if err == nil && wakeDemand && a.demandNotifier != nil {
		a.demandNotifier.NotifyDemand()
	}
	return observed, err
}

func parseSessionObserverStatus(raw json.RawMessage) (model.SessionObserverStatus, error) {
	var status model.SessionObserverStatus
	if len(raw) == 0 {
		return status, fmt.Errorf("is required")
	}
	if err := json.Unmarshal(raw, &status); err != nil {
		return status, err
	}
	return status, nil
}

func validSessionStatus(status model.SessionStatus) bool {
	switch status {
	case model.SessionStatusIdle, model.SessionStatusRunning,
		model.SessionStatusRescheduling, model.SessionStatusTerminated:
		return true
	default:
		return false
	}
}
