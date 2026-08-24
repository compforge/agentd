package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/compforge/agentd/agentd/internal/model"
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

func (a *Service) ListEnvironments(ctx context.Context) ([]model.Environment, error) {
	return a.repository.ListEnvironments(ctx)
}
