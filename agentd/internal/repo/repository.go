// Package repo defines the persistence contract used by the control plane.
package repo

import (
	"context"
	"errors"
	"time"

	"github.com/compforge/agentd/agentd/internal/model"
)

var ErrNotFound = errors.New("resource not found")

type Repository interface {
	Transaction(context.Context, func(Repository) error) error
	PutAgent(context.Context, model.Agent) error
	GetAgent(context.Context, string) (model.Agent, error)
	ListAgents(context.Context) ([]model.Agent, error)
	PutEnvironment(context.Context, model.Environment) error
	GetEnvironment(context.Context, string) (model.Environment, error)
	ListEnvironments(context.Context) ([]model.Environment, error)
	PutWorker(context.Context, model.Worker) error
	GetWorker(context.Context, string) (model.Worker, error)
	GetWorkerForUpdate(context.Context, string) (model.Worker, error)
	ListWorkers(context.Context) ([]model.Worker, error)
	ListWorkersForUpdate(context.Context) ([]model.Worker, error)
	PutSession(context.Context, model.Session) error
	GetSession(context.Context, string) (model.Session, error)
	GetSessionForUpdate(context.Context, string) (model.Session, error)
	ListSessions(context.Context) ([]model.Session, error)
	CountPendingSessions(context.Context) (int64, error)
	CountWorkerSessions(context.Context, string) (int64, error)
	DeleteRetiredWorkersBefore(context.Context, time.Time, int) (int64, error)
}
