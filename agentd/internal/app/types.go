package app

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

var (
	ErrNotFound   = errors.New("resource not found")
	ErrNoCapacity = errors.New("no worker capacity")
	ErrInvalid    = errors.New("invalid request")
)

// Worker is one observed Agentlet Pod and the capacity unit agentd schedules.
type Worker struct {
	ID             string
	Name           string
	MaxRuns        int
	ObserverStatus json.RawMessage
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

type WorkerObserverStatus struct {
	ObservedAt time.Time `json:"observed_at"`
	Exists     bool      `json:"exists"`
	Ready      bool      `json:"ready"`
	Endpoint   string    `json:"endpoint,omitempty"`
}

// Assignment is the current execution ownership of one managed Session.
type Assignment struct {
	ID        string
	SessionID string
	WorkerID  string
	CreatedAt time.Time
	UpdatedAt time.Time
}

type Repository interface {
	Transaction(context.Context, func(Repository) error) error
	PutWorker(context.Context, Worker) error
	GetWorker(context.Context, string) (Worker, error)
	ListWorkers(context.Context) ([]Worker, error)
	ListWorkersForUpdate(context.Context) ([]Worker, error)
	GetAssignment(context.Context, string) (Assignment, error)
	CountAssignments(context.Context, string) (int64, error)
	PutAssignment(context.Context, Assignment) error
	DeleteAssignment(context.Context, string) error
}
