package service

import (
	"context"
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
	status, err := parseWorkerObserverStatus(worker.ObserverStatus)
	if err != nil {
		return ExecutionTarget{}, fmt.Errorf("%w: Worker %q observation: %v", ErrUnavailable, worker.ID, err)
	}
	now := time.Now().UTC()
	if worker.Phase != model.WorkerPhaseActive || !status.Exists || !status.Ready ||
		strings.TrimSpace(status.Endpoint) == "" || status.ObservedAt.IsZero() || status.ObservedAt.After(now) ||
		now.Sub(status.ObservedAt) > a.observationTimeout {
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

// ObserveExecutionState conditionally advances Control State using the
// Assignment fence. A delayed Agentlet response cannot mutate a rescheduled
// Session, and an older ResumePoint cannot rewind a newer checkpoint.
//
// +spec=`Agentlet execution state updates apply only to the current Assignment and ResumeRevision never decreases`
// +link=agentd/docs/agentlet.md
func (a *Service) ObserveExecutionState(
	ctx context.Context,
	sessionID string,
	state executionapi.SessionState,
) (model.Session, error) {
	if strings.TrimSpace(state.AssignmentID) == "" {
		return model.Session{}, fmt.Errorf("%w: assignment id is required", ErrInvalid)
	}
	if !validSessionStatus(model.SessionStatus(state.Status)) {
		return model.Session{}, fmt.Errorf("%w: invalid Session status %q", ErrInvalid, state.Status)
	}
	var observed model.Session
	err := a.repository.Transaction(ctx, func(repository repo.Repository) error {
		session, err := repository.GetSessionForUpdate(ctx, sessionID)
		if err != nil {
			return err
		}
		if session.AssignmentID != state.AssignmentID {
			return fmt.Errorf("%w: Session %q Assignment changed", ErrConflict, sessionID)
		}
		changed := session.Status != model.SessionStatus(state.Status)
		session.Status = model.SessionStatus(state.Status)
		if state.ResumeRevision > session.ResumeRevision ||
			(state.ResumeRevision == session.ResumeRevision && session.ResumeRef == "" && state.ResumeRef != "") {
			session.ResumeRef = state.ResumeRef
			session.ResumeRevision = state.ResumeRevision
			changed = true
		}
		if changed {
			session.Revision++
			session.UpdatedAt = time.Now().UTC()
			if err := repository.PutSession(ctx, session); err != nil {
				return fmt.Errorf("persist Session %q execution state: %w", sessionID, err)
			}
		}
		observed = session
		return nil
	})
	return observed, err
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
