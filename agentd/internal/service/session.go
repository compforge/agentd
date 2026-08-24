package service

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/compforge/agentd/agentd/internal/model"
	"github.com/compforge/agentd/agentd/internal/repo"
	"github.com/qiankunli/go-stdx/uuid"
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
			ID: uuid.NewWithPrefix("session"), AgentID: agent.ID, AgentVersionID: agent.VersionID,
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

// SessionUpdate is a partial public-resource update. Nil fields preserve the
// current value; Metadata is a key-level patch whose nil values delete keys.
type SessionUpdate struct {
	Title    *string
	Metadata map[string]*string
}

// UpdateSession changes only mutable Session resource fields and leaves the
// pinned Agent, Environment, execution state, and placement unchanged.
//
// +spec=`Session updates preserve omitted fields, merge metadata keys, reject archived Sessions, and do not perturb execution placement`
// +link=agentd/docs/kernel.md
func (a *Service) UpdateSession(ctx context.Context, sessionID string, update SessionUpdate) (model.Session, error) {
	var updated model.Session
	err := a.repository.Transaction(ctx, func(repository repo.Repository) error {
		current, err := repository.GetSessionForUpdate(ctx, sessionID)
		if err != nil {
			return err
		}
		if current.ArchivedAt != nil {
			return fmt.Errorf("%w: Session %q is archived", ErrConflict, sessionID)
		}

		next := current
		next.Metadata = make(map[string]string, len(current.Metadata))
		for key, value := range current.Metadata {
			next.Metadata[key] = value
		}
		if update.Title != nil {
			next.Title = *update.Title
		}
		for key, value := range update.Metadata {
			if value == nil {
				delete(next.Metadata, key)
			} else {
				next.Metadata[key] = *value
			}
		}
		if current.Title == next.Title && reflect.DeepEqual(current.Metadata, next.Metadata) {
			updated = current
			return nil
		}
		now := time.Now().UTC()
		next.Revision++
		next.UpdatedAt = now
		if err := repository.PutSession(ctx, next); err != nil {
			return err
		}
		updated = next
		return nil
	})
	if err != nil {
		return model.Session{}, fmt.Errorf("update Session %q: %w", sessionID, err)
	}
	return updated, nil
}

// ArchiveSession terminates an inactive Session while preserving its Control
// State and Ledger history. Session Reconciler remains the only owner of
// placement release.
//
// +spec=`Archiving is idempotent, rejects running or rescheduling Sessions, preserves history, and permanently rejects new ingress Events`
// +link=agentd/docs/kernel.md
func (a *Service) ArchiveSession(ctx context.Context, sessionID string) (model.Session, error) {
	var archived model.Session
	err := a.repository.Transaction(ctx, func(repository repo.Repository) error {
		current, err := repository.GetSessionForUpdate(ctx, sessionID)
		if err != nil {
			return err
		}
		if current.ArchivedAt != nil {
			archived = current
			return nil
		}
		if current.Status == model.SessionStatusRunning || current.Status == model.SessionStatusRescheduling {
			return fmt.Errorf("%w: Session %q is %s", ErrConflict, sessionID, current.Status)
		}
		now := time.Now().UTC()
		current.ArchivedAt = &now
		current.Status = model.SessionStatusTerminated
		current.Revision++
		current.UpdatedAt = now
		if err := repository.PutSession(ctx, current); err != nil {
			return err
		}
		archived = current
		return nil
	})
	if err != nil {
		return model.Session{}, fmt.Errorf("archive Session %q: %w", sessionID, err)
	}
	return archived, nil
}

func (a *Service) ListSessions(ctx context.Context) ([]model.Session, error) {
	return a.repository.ListSessions(ctx)
}

func (a *Service) PageSessions(
	ctx context.Context,
	page PageQuery,
	includeArchived bool,
) (Page[model.Session], error) {
	result, err := a.repository.ListSessionsPage(ctx, repositoryPageQuery(page), includeArchived)
	return servicePage(result), err
}
