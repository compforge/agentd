package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/compforge/agentd/agentd/internal/model"
	"github.com/compforge/agentd/agentd/internal/repo"
)

func (a *Service) CreateModel(ctx context.Context, value model.Model) (model.Model, error) {
	value.ID = strings.TrimSpace(value.ID)
	value.Provider = strings.ToLower(strings.TrimSpace(value.Provider))
	value.UpstreamID = strings.TrimSpace(value.UpstreamID)
	value.BaseURL = strings.TrimSpace(value.BaseURL)
	if value.UpstreamID == "" {
		value.UpstreamID = value.ID
	}
	if value.ID == "" || value.Provider == "" || strings.TrimSpace(value.APIKey) == "" {
		return model.Model{}, fmt.Errorf("%w: model id, provider, and api_key are required", ErrInvalid)
	}
	if _, err := a.repository.GetModel(ctx, value.ID); err == nil {
		return model.Model{}, fmt.Errorf("%w: model %q already exists", ErrConflict, value.ID)
	} else if err != repo.ErrNotFound {
		return model.Model{}, fmt.Errorf("check model %q: %w", value.ID, err)
	}
	now := time.Now().UTC()
	value.CreatedAt = now
	value.UpdatedAt = now
	if err := a.repository.PutModel(ctx, value); err != nil {
		return model.Model{}, fmt.Errorf("create model: %w", err)
	}
	return value, nil
}

func (a *Service) GetModel(ctx context.Context, id string) (model.Model, error) {
	return a.repository.GetModel(ctx, id)
}

// ModelUpdate is a partial connection update. Model identity fields may be
// supplied as concurrency-safe assertions, but cannot be changed in place.
type ModelUpdate struct {
	Provider   *string
	UpstreamID *string
	BaseURL    *string
	APIKey     *string
}

// UpdateModel rotates operational connection settings without changing the
// model identity referenced by immutable Agent versions.
//
// +spec=`Model updates may rotate base URL and credentials, preserve omitted fields, reject identity changes, and leave UpdatedAt unchanged for a no-op`
// +why=`Keeping provider and upstream model immutable prevents an existing AgentVersion from silently changing model identity, while mutable connection settings allow endpoint and credential rotation`
// +link=agentd/docs/kernel.md
func (a *Service) UpdateModel(ctx context.Context, modelID string, update ModelUpdate) (model.Model, error) {
	var updated model.Model
	err := a.repository.Transaction(ctx, func(repository repo.Repository) error {
		current, err := repository.GetModelForUpdate(ctx, modelID)
		if err != nil {
			return err
		}
		if update.Provider != nil {
			provider := strings.ToLower(strings.TrimSpace(*update.Provider))
			if provider != current.Provider {
				return fmt.Errorf("%w: model provider is immutable; register a new Model", ErrConflict)
			}
		}
		if update.UpstreamID != nil {
			upstreamID := strings.TrimSpace(*update.UpstreamID)
			if upstreamID != current.UpstreamID {
				return fmt.Errorf("%w: upstream model is immutable; register a new Model", ErrConflict)
			}
		}

		next := current
		if update.BaseURL != nil {
			next.BaseURL = strings.TrimSpace(*update.BaseURL)
		}
		if update.APIKey != nil {
			if strings.TrimSpace(*update.APIKey) == "" {
				return fmt.Errorf("%w: model api_key cannot be cleared", ErrInvalid)
			}
			// Credentials are opaque. Validate blank input without normalizing the
			// value that will be sent to the provider.
			next.APIKey = *update.APIKey
		}
		if current.BaseURL == next.BaseURL && current.APIKey == next.APIKey {
			updated = current
			return nil
		}

		next.UpdatedAt = time.Now().UTC()
		if err := repository.PutModel(ctx, next); err != nil {
			return err
		}
		updated, err = repository.GetModel(ctx, modelID)
		if err != nil {
			return fmt.Errorf("read updated model: %w", err)
		}
		return nil
	})
	if err != nil {
		return model.Model{}, fmt.Errorf("update model %q: %w", modelID, err)
	}
	return updated, nil
}

func (a *Service) ListModels(ctx context.Context) ([]model.Model, error) {
	return a.repository.ListModels(ctx)
}

func (a *Service) PageModels(ctx context.Context, page PageQuery) (Page[model.Model], error) {
	result, err := a.repository.ListModelsPage(ctx, repositoryPageQuery(page))
	return servicePage(result), err
}
