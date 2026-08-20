package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	sessionwork "github.com/compforge/agentd/agentlet/internal/work"
	"github.com/compforge/agentd/internal/executionapi"
)

// ApplyWorkSpec installs the current control-plane snapshot in Agentlet's
// process-local cache. Repeating the same Assignment is idempotent; a new
// Assignment replaces only inactive cached state.
func (a *Service) ApplyWorkSpec(ctx context.Context, spec executionapi.WorkSpec) (Session, error) {
	if strings.TrimSpace(spec.AssignmentID) == "" || strings.TrimSpace(spec.WorkerID) == "" ||
		strings.TrimSpace(spec.Session.ID) == "" || strings.TrimSpace(spec.Agent.ID) == "" ||
		strings.TrimSpace(spec.Agent.ModelID) == "" {
		return Session{}, invalid("WorkSpec assignment, worker, session, agent, and model are required")
	}
	if spec.Session.EnvironmentID == "" || spec.Session.EnvironmentID != spec.Environment.ID {
		return Session{}, invalid("WorkSpec environment identity is inconsistent")
	}
	if spec.Session.Harness != "" && (spec.Session.Harness != a.harness.Name() ||
		spec.Session.HarnessVersion != a.harness.Version()) {
		return Session{}, fmt.Errorf(
			"%w: Agentlet provides Harness %s@%s, Work requires %s@%s",
			ErrConflict, a.harness.Name(), a.harness.Version(),
			spec.Session.Harness, spec.Session.HarnessVersion,
		)
	}
	existing, err := a.repository.GetSession(ctx, spec.Session.ID)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return Session{}, err
	}

	agent := Agent{
		ID: spec.Agent.ID, Name: spec.Agent.Name, Description: spec.Agent.Description,
		ModelID: spec.Agent.ModelID, System: spec.Agent.System, Tools: spec.Agent.Tools,
		Version: spec.Agent.Version,
	}
	environment := Environment{ID: spec.Environment.ID, Config: spec.Environment.Config}
	session := Session{
		ID: spec.Session.ID, Agent: agent, EnvironmentID: spec.Session.EnvironmentID,
		Title: spec.Session.Title, Metadata: spec.Session.Metadata,
		Control: ControlState{
			Status: spec.Session.Status, Revision: spec.Session.Revision,
			Harness: a.harness.Name(), HarnessVersion: a.harness.Version(),
			ResumeRef: spec.Session.ResumeRef, ResumeRevision: spec.Session.ResumeRevision,
			AssignmentID: spec.AssignmentID, WorkerID: spec.WorkerID,
		},
		CreatedAt: spec.Session.CreatedAt, UpdatedAt: spec.Session.UpdatedAt,
	}
	if err == nil && existing.Control.AssignmentID == spec.AssignmentID {
		if existing.Control.ResumeRevision >= session.Control.ResumeRevision {
			session.Control = existing.Control
		}
		if existing.CreatedAt.Before(session.CreatedAt) {
			session.CreatedAt = existing.CreatedAt
		}
	}
	if session.CreatedAt.IsZero() {
		session.CreatedAt = time.Now().UTC()
	}
	if session.UpdatedAt.IsZero() {
		session.UpdatedAt = session.CreatedAt
	}
	if session.Control.ResumeRef == "" {
		resumeRef, err := a.harness.PrepareSession(ctx, executionSession(session))
		if err != nil {
			return Session{}, fmt.Errorf("prepare Session %q Harness state: %w", session.ID, err)
		}
		session.Control.ResumeRef = resumeRef
	}
	resident, _, err := a.works.Ensure(WorkSpec{
		AssignmentID: spec.AssignmentID,
		Session:      executionSession(session),
	})
	if err != nil {
		return Session{}, translateWorkError(session.ID, err)
	}
	resume := resident.Snapshot().Spec.Session
	session.Control.ResumeRef = resume.ResumeRef
	session.Control.ResumeRevision = resume.ResumeRevision
	if err := a.repository.PutAgent(ctx, agent); err != nil {
		return Session{}, fmt.Errorf("cache Work Agent %q: %w", agent.ID, err)
	}
	if err := a.repository.PutEnvironment(ctx, environment); err != nil {
		return Session{}, fmt.Errorf("cache Work Environment %q: %w", environment.ID, err)
	}
	if err := a.repository.PutSession(ctx, session); err != nil {
		return Session{}, fmt.Errorf("cache Work Session %q: %w", session.ID, err)
	}
	return session, nil
}

func (a *Service) ValidateAssignment(ctx context.Context, sessionID, workerID, assignmentID string) error {
	session, err := a.repository.GetSession(ctx, sessionID)
	if err != nil {
		return err
	}
	if session.Control.AssignmentID == "" && workerID == "" && assignmentID == "" {
		return nil
	}
	work, err := a.works.Snapshot(sessionID)
	if err != nil {
		return translateWorkError(sessionID, err)
	}
	if work.Spec.AssignmentID != assignmentID || session.Control.AssignmentID != assignmentID ||
		session.Control.WorkerID != workerID {
		return fmt.Errorf("%w: Session %q Assignment fence does not match", ErrConflict, sessionID)
	}
	return nil
}

func (a *Service) ExecutionState(ctx context.Context, sessionID string) (executionapi.SessionState, error) {
	session, err := a.repository.GetSession(ctx, sessionID)
	if err != nil {
		return executionapi.SessionState{}, err
	}
	state := executionapi.SessionState{
		AssignmentID: session.Control.AssignmentID,
		Status:       session.Control.Status, ResumeRef: session.Control.ResumeRef,
		ResumeRevision: session.Control.ResumeRevision,
	}
	work, err := a.works.Snapshot(sessionID)
	if err == nil && work.Spec.AssignmentID == session.Control.AssignmentID &&
		work.Spec.Session.ResumeRevision > state.ResumeRevision {
		state.ResumeRef = work.Spec.Session.ResumeRef
		state.ResumeRevision = work.Spec.Session.ResumeRevision
	}
	if err != nil {
		if !errors.Is(err, sessionwork.ErrNotFound) || session.Control.AssignmentID != "" {
			return executionapi.SessionState{}, translateWorkError(sessionID, err)
		}
	}
	return state, nil
}
