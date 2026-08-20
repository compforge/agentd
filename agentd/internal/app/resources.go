package app

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/compforge/agentd/agentd/internal/model"
)

func (a *App) CreateAgent(ctx context.Context, value model.Agent) (model.Agent, error) {
	if strings.TrimSpace(value.Name) == "" || strings.TrimSpace(value.ModelID) == "" {
		return model.Agent{}, fmt.Errorf("%w: agent name and model are required", ErrInvalid)
	}
	now := time.Now().UTC()
	value.ID = newID("agent")
	value.Version = 1
	value.CreatedAt = now
	value.UpdatedAt = now
	if value.Tools == nil {
		value.Tools = []map[string]any{}
	}
	if value.Metadata == nil {
		value.Metadata = map[string]string{}
	}
	if err := a.repository.PutAgent(ctx, value); err != nil {
		return model.Agent{}, fmt.Errorf("create agent: %w", err)
	}
	return value, nil
}

func (a *App) GetAgent(ctx context.Context, id string) (model.Agent, error) {
	return a.repository.GetAgent(ctx, id)
}

func (a *App) ListAgents(ctx context.Context) ([]model.Agent, error) {
	return a.repository.ListAgents(ctx)
}

func (a *App) CreateEnvironment(ctx context.Context, value model.Environment) (model.Environment, error) {
	if strings.TrimSpace(value.Name) == "" {
		return model.Environment{}, fmt.Errorf("%w: environment name is required", ErrInvalid)
	}
	now := time.Now().UTC()
	value.ID = newID("env")
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

func (a *App) GetEnvironment(ctx context.Context, id string) (model.Environment, error) {
	return a.repository.GetEnvironment(ctx, id)
}

func (a *App) ListEnvironments(ctx context.Context) ([]model.Environment, error) {
	return a.repository.ListEnvironments(ctx)
}

func (a *App) CreateSession(
	ctx context.Context,
	agentID string,
	agentVersion int64,
	environmentID string,
	title string,
	metadata map[string]string,
) (model.Session, error) {
	agent, err := a.repository.GetAgent(ctx, agentID)
	if err != nil {
		return model.Session{}, fmt.Errorf("resolve session agent: %w", err)
	}
	if agentVersion != 0 && agentVersion != agent.Version {
		return model.Session{}, fmt.Errorf("%w: agent version %d is unavailable", ErrConflict, agentVersion)
	}
	if _, err := a.repository.GetEnvironment(ctx, environmentID); err != nil {
		return model.Session{}, fmt.Errorf("resolve session environment: %w", err)
	}
	now := time.Now().UTC()
	if metadata == nil {
		metadata = map[string]string{}
	}
	session := model.Session{
		ID: newID("session"), AgentID: agent.ID, AgentVersion: agent.Version,
		EnvironmentID: environmentID, Title: title, Metadata: metadata,
		Status: model.SessionStatusIdle, CreatedAt: now, UpdatedAt: now,
	}
	if err := a.repository.PutSession(ctx, session); err != nil {
		return model.Session{}, fmt.Errorf("create session: %w", err)
	}
	return session, nil
}

func (a *App) GetSession(ctx context.Context, id string) (model.Session, error) {
	return a.repository.GetSession(ctx, id)
}

func (a *App) ListSessions(ctx context.Context) ([]model.Session, error) {
	return a.repository.ListSessions(ctx)
}

func newID(prefix string) string {
	bytes := make([]byte, 12)
	if _, err := rand.Read(bytes); err != nil {
		panic(fmt.Sprintf("generate %s id: %v", prefix, err))
	}
	return prefix + "_" + hex.EncodeToString(bytes)
}
