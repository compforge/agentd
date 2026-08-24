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

func (a *Service) CreateEnvironment(ctx context.Context, value model.Environment) (model.Environment, error) {
	if strings.TrimSpace(value.Name) == "" {
		return model.Environment{}, fmt.Errorf("%w: environment name is required", ErrInvalid)
	}
	now := time.Now().UTC()
	value.ID = uuid.NewWithPrefix("env")
	value.CreatedAt = now
	value.UpdatedAt = now
	if value.Config == nil {
		value.Config = map[string]any{}
	}
	if value.Metadata == nil {
		value.Metadata = map[string]string{}
	}
	if err := a.repository.PutEnvironment(ctx, value); err != nil {
		return model.Environment{}, fmt.Errorf("create environment: %w", err)
	}
	return value, nil
}

func (a *Service) GetEnvironment(ctx context.Context, id string) (model.Environment, error) {
	return a.repository.GetEnvironment(ctx, id)
}

// EnvironmentUpdate is a partial update. Nil fields preserve the current
// value; Metadata is a key-level patch whose nil values delete keys.
type EnvironmentUpdate struct {
	Name        *string
	Description *string
	Config      *map[string]any
	Metadata    map[string]*string
}

// UpdateEnvironment changes the shared Environment definition without
// changing Session identity or placement.
//
// +spec=`Environment updates preserve omitted fields, merge metadata keys, reject archived Environments, and leave UpdatedAt unchanged for a no-op`
// +link=agentd/docs/kernel.md
func (a *Service) UpdateEnvironment(
	ctx context.Context,
	environmentID string,
	update EnvironmentUpdate,
) (model.Environment, error) {
	var updated model.Environment
	err := a.repository.Transaction(ctx, func(repository repo.Repository) error {
		current, err := repository.GetEnvironmentForUpdate(ctx, environmentID)
		if err != nil {
			return err
		}
		if current.ArchivedAt != nil {
			return fmt.Errorf("%w: environment %q is archived", ErrConflict, environmentID)
		}
		next := current
		next.Metadata = make(map[string]string, len(current.Metadata))
		for key, value := range current.Metadata {
			next.Metadata[key] = value
		}
		if update.Name != nil {
			next.Name = *update.Name
		}
		if update.Description != nil {
			next.Description = *update.Description
		}
		if update.Config != nil {
			next.Config = *update.Config
			if next.Config == nil {
				next.Config = map[string]any{}
			}
		}
		for key, value := range update.Metadata {
			if value == nil {
				delete(next.Metadata, key)
			} else {
				next.Metadata[key] = *value
			}
		}
		if strings.TrimSpace(next.Name) == "" {
			return fmt.Errorf("%w: environment name is required", ErrInvalid)
		}
		if current.Name == next.Name && current.Description == next.Description &&
			reflect.DeepEqual(current.Config, next.Config) && reflect.DeepEqual(current.Metadata, next.Metadata) {
			updated = current
			return nil
		}
		next.UpdatedAt = time.Now().UTC()
		if err := repository.PutEnvironment(ctx, next); err != nil {
			return err
		}
		updated, err = repository.GetEnvironment(ctx, environmentID)
		if err != nil {
			return fmt.Errorf("read updated environment: %w", err)
		}
		return nil
	})
	if err != nil {
		return model.Environment{}, fmt.Errorf("update environment %q: %w", environmentID, err)
	}
	return updated, nil
}

// ArchiveEnvironment prevents new Sessions from selecting this Environment;
// existing Sessions retain their reference and remain executable.
//
// +spec=`Archiving an Environment is idempotent, blocks new Sessions, and preserves existing Sessions`
// +link=agentd/docs/kernel.md
func (a *Service) ArchiveEnvironment(ctx context.Context, environmentID string) (model.Environment, error) {
	var archived model.Environment
	err := a.repository.Transaction(ctx, func(repository repo.Repository) error {
		current, err := repository.GetEnvironmentForUpdate(ctx, environmentID)
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
		if err := repository.PutEnvironment(ctx, current); err != nil {
			return err
		}
		archived, err = repository.GetEnvironment(ctx, environmentID)
		if err != nil {
			return fmt.Errorf("read archived environment: %w", err)
		}
		return nil
	})
	if err != nil {
		return model.Environment{}, fmt.Errorf("archive environment %q: %w", environmentID, err)
	}
	return archived, nil
}

func (a *Service) ListEnvironments(ctx context.Context) ([]model.Environment, error) {
	return a.repository.ListEnvironments(ctx)
}

func (a *Service) PageEnvironments(
	ctx context.Context,
	page PageQuery,
	includeArchived bool,
) (Page[model.Environment], error) {
	result, err := a.repository.ListEnvironmentsPage(ctx, repositoryPageQuery(page), includeArchived)
	return servicePage(result), err
}
