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

func (a *Service) ListModels(ctx context.Context) ([]model.Model, error) {
	return a.repository.ListModels(ctx)
}
