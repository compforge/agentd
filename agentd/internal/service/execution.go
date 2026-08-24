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

func (a *Service) CurrentExecution(ctx context.Context, sessionID string) (ExecutionTarget, error) {
	session, err := a.repository.GetSession(ctx, sessionID)
	if err != nil {
		return ExecutionTarget{}, fmt.Errorf("load Session %q execution target: %w", sessionID, err)
	}
	if !session.Placement.Bound() {
		return ExecutionTarget{}, fmt.Errorf("%w: Session %q", ErrNoAssignment, sessionID)
	}
	worker, err := a.repository.GetWorker(ctx, session.Placement.WorkerID)
	if err != nil {
		return ExecutionTarget{}, fmt.Errorf("load placed Worker %q: %w", session.Placement.WorkerID, err)
	}
	status, ready := a.readyWorker(worker, time.Now().UTC())
	if !ready {
		return ExecutionTarget{}, fmt.Errorf("%w: Worker %q has no fresh ready endpoint", ErrUnavailable, worker.ID)
	}
	agent, err := a.repository.GetAgentVersion(ctx, session.AgentVersionID)
	if err != nil {
		return ExecutionTarget{}, fmt.Errorf("load Session %q Agent: %w", sessionID, err)
	}
	registeredModel, err := a.repository.GetModel(ctx, agent.ModelID)
	if err != nil {
		return ExecutionTarget{}, fmt.Errorf("load Agent %q Model %q: %w", agent.ID, agent.ModelID, err)
	}
	environment, err := a.repository.GetEnvironment(ctx, session.EnvironmentID)
	if err != nil {
		return ExecutionTarget{}, fmt.Errorf("load Session %q Environment: %w", sessionID, err)
	}
	return ExecutionTarget{
		Endpoint: status.Endpoint,
		Work: executionapi.WorkSpec{
			AssignmentID: session.Placement.Fence,
			WorkerID:     session.Placement.WorkerID,
			Session: executionapi.SessionSnapshot{
				ID: session.ID, EnvironmentID: session.EnvironmentID, Title: session.Title,
				Metadata: session.Metadata, Status: string(session.Status), Revision: session.Revision,
				Harness: session.Harness, HarnessVersion: session.HarnessVersion,
				ResumeRef: session.ResumeRef, ResumeRevision: session.ResumeRevision,
				CreatedAt: session.CreatedAt, UpdatedAt: session.UpdatedAt,
			},
			Agent: executionapi.AgentSnapshot{
				ID: agent.ID, Name: agent.Name, Description: agent.Description,
				Model: executionapi.ModelSnapshot{
					ID: registeredModel.ID, Provider: registeredModel.Provider, UpstreamID: registeredModel.UpstreamID,
					BaseURL: registeredModel.BaseURL, APIKey: registeredModel.APIKey,
				},
				System: agent.System, Tools: agent.Tools, Version: agent.Version,
			},
			Environment: executionapi.EnvironmentSnapshot{ID: environment.ID, Config: environment.Config},
		},
	}, nil
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
// into Control State. It does not change placement; Session Reconciler consumes
// the observation and owns release or replacement.
//
// +spec=`Session observations apply only to the current placement fence and cannot rewind ResumeRevision; observation never performs placement actions`
// +link=agentd/docs/agentd.md
func (a *Service) ObserveSession(
	ctx context.Context,
	sessionID string,
	status model.SessionObserverStatus,
) (model.Session, error) {
	if strings.TrimSpace(status.PlacementFence) == "" {
		return model.Session{}, fmt.Errorf("%w: placement fence is required", ErrInvalid)
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
	var observed model.Session
	err = a.repository.Transaction(ctx, func(repository repo.Repository) error {
		session, err := repository.GetSessionForUpdate(ctx, sessionID)
		if err != nil {
			return err
		}
		if session.Placement.Fence != status.PlacementFence {
			return fmt.Errorf("%w: Session %q placement changed", ErrConflict, sessionID)
		}
		if current, parseErr := parseSessionObserverStatus(session.ObserverStatus); parseErr == nil &&
			current.ObservedAt.After(status.ObservedAt) {
			observed = session
			return nil
		}

		now := time.Now().UTC()
		session.ObserverStatus = raw
		projectedStatus := status.Status
		if session.ArchivedAt != nil {
			projectedStatus = model.SessionStatusTerminated
		} else if !status.Exists {
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

		if changed {
			session.Revision++
		}
		session.UpdatedAt = now
		if err := repository.PutSession(ctx, session); err != nil {
			return fmt.Errorf("persist Session %q observation: %w", sessionID, err)
		}
		observed = session
		return nil
	})
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
