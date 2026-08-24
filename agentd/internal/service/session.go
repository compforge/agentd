package service

import (
	"context"
	"fmt"
	"time"

	"github.com/compforge/agentd/agentd/internal/model"
	"github.com/compforge/agentd/agentd/internal/repo"
)

func (a *Service) CreateSession(
	ctx context.Context,
	agentID string,
	agentVersion int64,
	environmentID string,
	title string,
	metadata map[string]string,
) (model.Session, error) {
	now := time.Now().UTC()
	if metadata == nil {
		metadata = map[string]string{}
	}
	var session model.Session
	if err := a.repository.Transaction(ctx, func(repository repo.Repository) error {
		// Lock the Agent identity through Session creation so archive cannot
		// succeed between validation and persisting the pinned version.
		agent, err := repository.GetAgentForUpdate(ctx, agentID)
		if err != nil {
			return fmt.Errorf("resolve session agent: %w", err)
		}
		if agent.ArchivedAt != nil {
			return fmt.Errorf("%w: agent %q is archived", ErrConflict, agentID)
		}
		if agentVersion != 0 && agentVersion != agent.Version {
			agent, err = repository.FindAgentVersion(ctx, agentID, agentVersion)
			if err != nil {
				return fmt.Errorf("resolve session agent version %d: %w", agentVersion, err)
			}
		}
		if _, err := repository.GetEnvironment(ctx, environmentID); err != nil {
			return fmt.Errorf("resolve session environment: %w", err)
		}
		session = model.Session{
			ID: newID("session"), AgentID: agent.ID, AgentVersionID: agent.VersionID,
			EnvironmentID: environmentID, Title: title, Metadata: metadata,
			Status: model.SessionStatusIdle, CreatedAt: now, UpdatedAt: now,
		}
		return repository.PutSession(ctx, session)
	}); err != nil {
		return model.Session{}, fmt.Errorf("create session: %w", err)
	}
	return session, nil
}

func (a *Service) GetSession(ctx context.Context, id string) (model.Session, error) {
	return a.repository.GetSession(ctx, id)
}

func (a *Service) ListSessions(ctx context.Context) ([]model.Session, error) {
	return a.repository.ListSessions(ctx)
}
