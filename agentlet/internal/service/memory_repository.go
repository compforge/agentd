package service

import (
	"context"
	"sync"
)

// MemoryRepository holds resource snapshots for the lifetime of one Agentlet
// process. Agentd owns the durable Agent, Environment, and Session resources.
type MemoryRepository struct {
	mu           sync.RWMutex
	agents       map[string]Agent
	environments map[string]Environment
	sessions     map[string]Session
}

func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{
		agents: make(map[string]Agent), environments: make(map[string]Environment),
		sessions: make(map[string]Session),
	}
}

func (r *MemoryRepository) PutAgent(_ context.Context, value Agent) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.agents[value.ID] = value
	return nil
}

func (r *MemoryRepository) GetAgent(_ context.Context, id string) (Agent, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	value, ok := r.agents[id]
	if !ok {
		return Agent{}, ErrNotFound
	}
	return value, nil
}

func (r *MemoryRepository) ListAgents(context.Context) ([]Agent, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	values := make([]Agent, 0, len(r.agents))
	for _, value := range r.agents {
		values = append(values, value)
	}
	return values, nil
}

func (r *MemoryRepository) PutEnvironment(_ context.Context, value Environment) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.environments[value.ID] = value
	return nil
}

func (r *MemoryRepository) GetEnvironment(_ context.Context, id string) (Environment, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	value, ok := r.environments[id]
	if !ok {
		return Environment{}, ErrNotFound
	}
	return value, nil
}

func (r *MemoryRepository) ListEnvironments(context.Context) ([]Environment, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	values := make([]Environment, 0, len(r.environments))
	for _, value := range r.environments {
		values = append(values, value)
	}
	return values, nil
}

func (r *MemoryRepository) PutSession(_ context.Context, value Session) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sessions[value.ID] = value
	return nil
}

func (r *MemoryRepository) GetSession(_ context.Context, id string) (Session, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	value, ok := r.sessions[id]
	if !ok {
		return Session{}, ErrNotFound
	}
	return value, nil
}

func (r *MemoryRepository) ListSessions(context.Context) ([]Session, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	values := make([]Session, 0, len(r.sessions))
	for _, value := range r.sessions {
		values = append(values, value)
	}
	return values, nil
}
