package store

import (
	"context"
	"sync"

	"github.com/compforge/agentd/server/internal/app"
)

// MemoryRepository is a test repository. Production persistence is provided
// by the MySQL/GORM provider.
type MemoryRepository struct {
	mu           sync.RWMutex
	agents       map[string]app.Agent
	environments map[string]app.Environment
	sessions     map[string]app.Session
}

func NewMemory() *MemoryRepository {
	return &MemoryRepository{
		agents: make(map[string]app.Agent), environments: make(map[string]app.Environment),
		sessions: make(map[string]app.Session),
	}
}

func (r *MemoryRepository) PutAgent(_ context.Context, value app.Agent) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.agents[value.ID] = value
	return nil
}

func (r *MemoryRepository) GetAgent(_ context.Context, id string) (app.Agent, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	value, ok := r.agents[id]
	if !ok {
		return app.Agent{}, app.ErrNotFound
	}
	return value, nil
}

func (r *MemoryRepository) ListAgents(context.Context) ([]app.Agent, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	values := make([]app.Agent, 0, len(r.agents))
	for _, value := range r.agents {
		values = append(values, value)
	}
	return values, nil
}

func (r *MemoryRepository) PutEnvironment(_ context.Context, value app.Environment) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.environments[value.ID] = value
	return nil
}

func (r *MemoryRepository) GetEnvironment(_ context.Context, id string) (app.Environment, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	value, ok := r.environments[id]
	if !ok {
		return app.Environment{}, app.ErrNotFound
	}
	return value, nil
}

func (r *MemoryRepository) ListEnvironments(context.Context) ([]app.Environment, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	values := make([]app.Environment, 0, len(r.environments))
	for _, value := range r.environments {
		values = append(values, value)
	}
	return values, nil
}

func (r *MemoryRepository) PutSession(_ context.Context, value app.Session) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sessions[value.ID] = value
	return nil
}

func (r *MemoryRepository) GetSession(_ context.Context, id string) (app.Session, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	value, ok := r.sessions[id]
	if !ok {
		return app.Session{}, app.ErrNotFound
	}
	return value, nil
}

func (r *MemoryRepository) ListSessions(context.Context) ([]app.Session, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	values := make([]app.Session, 0, len(r.sessions))
	for _, value := range r.sessions {
		values = append(values, value)
	}
	return values, nil
}
