// Package repo defines the persistence contract used by the control plane.
package repo

import (
	"context"
	"errors"
	"time"

	"github.com/compforge/agentd/agentd/internal/model"
)

var ErrNotFound = errors.New("resource not found")

type PageQuery struct {
	Offset     int
	Limit      int
	Descending bool
}

type Page[T any] struct {
	Items   []T
	HasMore bool
}

func NewPage[T any](items []T, limit int) Page[T] {
	page := Page[T]{Items: items}
	if len(page.Items) > limit {
		page.Items = page.Items[:limit]
		page.HasMore = true
	}
	return page
}

type Repository interface {
	Transaction(context.Context, func(Repository) error) error
	PutAgent(context.Context, model.Agent) error
	CreateAgentVersion(context.Context, model.Agent) error
	GetAgent(context.Context, string) (model.Agent, error)
	GetAgentForUpdate(context.Context, string) (model.Agent, error)
	GetAgentVersion(context.Context, string) (model.Agent, error)
	FindAgentVersion(context.Context, string, int64) (model.Agent, error)
	ListAgents(context.Context) ([]model.Agent, error)
	ListAgentVersions(context.Context, string) ([]model.Agent, error)
	ListAgentsPage(context.Context, PageQuery, bool) (Page[model.Agent], error)
	ListAgentVersionsPage(context.Context, string, PageQuery) (Page[model.Agent], error)
	PutModel(context.Context, model.Model) error
	GetModel(context.Context, string) (model.Model, error)
	ListModels(context.Context) ([]model.Model, error)
	ListModelsPage(context.Context, PageQuery) (Page[model.Model], error)
	PutEnvironment(context.Context, model.Environment) error
	GetEnvironment(context.Context, string) (model.Environment, error)
	ListEnvironments(context.Context) ([]model.Environment, error)
	ListEnvironmentsPage(context.Context, PageQuery) (Page[model.Environment], error)
	PutWorker(context.Context, model.Worker) error
	GetWorker(context.Context, string) (model.Worker, error)
	GetWorkerForUpdate(context.Context, string) (model.Worker, error)
	ListWorkers(context.Context) ([]model.Worker, error)
	ListWorkersForUpdate(context.Context) ([]model.Worker, error)
	PutSession(context.Context, model.Session) error
	GetSession(context.Context, string) (model.Session, error)
	GetSessionForUpdate(context.Context, string) (model.Session, error)
	ListSessions(context.Context) ([]model.Session, error)
	ListSessionsPage(context.Context, PageQuery, bool) (Page[model.Session], error)
	CountWorkerSessions(context.Context, string) (int64, error)
	DeleteRetiredWorkersBefore(context.Context, time.Time, int) (int64, error)
}
