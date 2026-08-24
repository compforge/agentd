package service

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/compforge/agentd/agentd/internal/model"
	"github.com/compforge/agentd/agentd/internal/repo"
	"github.com/qiankunli/go-stdx/uuid"
)

func (a *Service) CreateAgent(ctx context.Context, value model.Agent) (model.Agent, error) {
	if strings.TrimSpace(value.Name) == "" || strings.TrimSpace(value.ModelID) == "" {
		return model.Agent{}, fmt.Errorf("%w: agent name and model are required", ErrInvalid)
	}
	if _, err := a.repository.GetModel(ctx, value.ModelID); err != nil {
		return model.Agent{}, fmt.Errorf("resolve agent model %q: %w", value.ModelID, err)
	}
	now := time.Now().UTC()
	value.ID = uuid.NewWithPrefix("agent")
	value.VersionID = uuid.NewWithPrefix("agent_version")
	value.Version = 1
	value.CreatedAt = now
	value.UpdatedAt = now
	if value.Tools == nil {
		value.Tools = []map[string]any{}
	}
	if value.Metadata == nil {
		value.Metadata = map[string]string{}
	}
	if err := a.repository.Transaction(ctx, func(repository repo.Repository) error {
		if err := repository.CreateAgentVersion(ctx, value); err != nil {
			return err
		}
		return repository.PutAgent(ctx, value)
	}); err != nil {
		return model.Agent{}, fmt.Errorf("create agent: %w", err)
	}
	return value, nil
}

// AgentUpdate is a partial update. Nil fields preserve the current value;
// Metadata is a key-level patch whose nil values delete keys.
type AgentUpdate struct {
	Version     *int64
	Name        *string
	Description *string
	ModelID     *string
	System      *string
	Tools       *[]map[string]any
	Metadata    map[string]*string
}

// UpdateAgent persists a new immutable version only when the resolved
// configuration changes. The optional Version is an optimistic concurrency
// precondition, not the version number to create.
//
// +spec=`Agent updates preserve omitted fields, merge metadata keys, reject stale versions, and create no version for a no-op`
// +link=agentd/docs/kernel.md
func (a *Service) UpdateAgent(ctx context.Context, agentID string, update AgentUpdate) (model.Agent, error) {
	if update.Version != nil && *update.Version < 1 {
		return model.Agent{}, fmt.Errorf("%w: agent version must be at least 1", ErrInvalid)
	}
	var updated model.Agent
	err := a.repository.Transaction(ctx, func(repository repo.Repository) error {
		current, err := repository.GetAgentForUpdate(ctx, agentID)
		if err != nil {
			return err
		}
		if current.ArchivedAt != nil {
			return fmt.Errorf("%w: agent %q is archived", ErrConflict, agentID)
		}
		if update.Version != nil && *update.Version != current.Version {
			return fmt.Errorf("%w: agent %q is at version %d, not %d", ErrConflict, agentID, current.Version, *update.Version)
		}

		next := cloneAgent(current)
		if update.Name != nil {
			next.Name = *update.Name
		}
		if update.Description != nil {
			next.Description = *update.Description
		}
		if update.ModelID != nil {
			next.ModelID = *update.ModelID
		}
		if update.System != nil {
			next.System = *update.System
		}
		if update.Tools != nil {
			next.Tools = *update.Tools
			if next.Tools == nil {
				next.Tools = []map[string]any{}
			}
		}
		for key, value := range update.Metadata {
			if value == nil {
				delete(next.Metadata, key)
			} else {
				next.Metadata[key] = *value
			}
		}
		if strings.TrimSpace(next.Name) == "" || strings.TrimSpace(next.ModelID) == "" {
			return fmt.Errorf("%w: agent name and model are required", ErrInvalid)
		}
		if next.ModelID != current.ModelID {
			if _, err := repository.GetModel(ctx, next.ModelID); err != nil {
				return fmt.Errorf("resolve agent model %q: %w", next.ModelID, err)
			}
		}
		if sameAgentConfiguration(current, next) {
			updated = current
			return nil
		}

		now := time.Now().UTC()
		next.VersionID = uuid.NewWithPrefix("agent_version")
		next.Version++
		next.UpdatedAt = now
		if err := repository.CreateAgentVersion(ctx, next); err != nil {
			return err
		}
		if err := repository.PutAgent(ctx, next); err != nil {
			return err
		}
		updated = next
		return nil
	})
	if err != nil {
		return model.Agent{}, fmt.Errorf("update agent %q: %w", agentID, err)
	}
	return updated, nil
}

func (a *Service) ArchiveAgent(ctx context.Context, agentID string) (model.Agent, error) {
	var archived model.Agent
	err := a.repository.Transaction(ctx, func(repository repo.Repository) error {
		current, err := repository.GetAgentForUpdate(ctx, agentID)
		if err != nil {
			return err
		}
		if current.ArchivedAt != nil {
			archived = current
			return nil
		}
		now := time.Now().UTC()
		current.ArchivedAt = &now
		current.UpdatedAt = now
		if err := repository.PutAgent(ctx, current); err != nil {
			return err
		}
		archived = current
		return nil
	})
	if err != nil {
		return model.Agent{}, fmt.Errorf("archive agent %q: %w", agentID, err)
	}
	return archived, nil
}

func (a *Service) GetAgent(ctx context.Context, id string) (model.Agent, error) {
	return a.repository.GetAgent(ctx, id)
}

func (a *Service) ListAgents(ctx context.Context) ([]model.Agent, error) {
	return a.repository.ListAgents(ctx)
}

func (a *Service) GetAgentVersion(ctx context.Context, versionID string) (model.Agent, error) {
	return a.repository.GetAgentVersion(ctx, versionID)
}

func (a *Service) FindAgentVersion(ctx context.Context, agentID string, version int64) (model.Agent, error) {
	if version < 1 {
		return model.Agent{}, fmt.Errorf("%w: agent version must be at least 1", ErrInvalid)
	}
	return a.repository.FindAgentVersion(ctx, agentID, version)
}

func (a *Service) ListAgentVersions(ctx context.Context, agentID string) ([]model.Agent, error) {
	return a.repository.ListAgentVersions(ctx, agentID)
}

func cloneAgent(value model.Agent) model.Agent {
	copy := value
	copy.Tools = append([]map[string]any(nil), value.Tools...)
	copy.Metadata = make(map[string]string, len(value.Metadata))
	for key, item := range value.Metadata {
		copy.Metadata[key] = item
	}
	return copy
}

func sameAgentConfiguration(left, right model.Agent) bool {
	return left.Name == right.Name && left.Description == right.Description &&
		left.ModelID == right.ModelID && left.System == right.System &&
		reflect.DeepEqual(left.Tools, right.Tools) && reflect.DeepEqual(left.Metadata, right.Metadata)
}
