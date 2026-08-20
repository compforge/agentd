package app

import (
	"context"
	"errors"
	"time"

	"github.com/compforge/agentd/agentlet/internal/execution"
)

var (
	ErrNotFound       = errors.New("resource not found")
	ErrConflict       = errors.New("resource conflict")
	ErrUnsupported    = errors.New("unsupported feature")
	ErrUnsafeRecovery = execution.ErrUnsafeRecovery
)

type Agent struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Description string            `json:"description"`
	ModelID     string            `json:"model_id"`
	System      string            `json:"system"`
	Tools       []map[string]any  `json:"tools"`
	Metadata    map[string]string `json:"metadata"`
	Version     int64             `json:"version"`
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
}

type Environment struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Description string            `json:"description"`
	Config      map[string]any    `json:"config"`
	Metadata    map[string]string `json:"metadata"`
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
}

type Session struct {
	ID            string            `json:"id"`
	Agent         Agent             `json:"agent"`
	EnvironmentID string            `json:"environment_id"`
	Title         string            `json:"title"`
	Metadata      map[string]string `json:"metadata"`
	Control       ControlState      `json:"control"`
	CreatedAt     time.Time         `json:"created_at"`
	UpdatedAt     time.Time         `json:"updated_at"`
}

type ControlState struct {
	Status         string `json:"status"`
	Revision       int64  `json:"revision"`
	Harness        string `json:"harness"`
	HarnessVersion string `json:"harness_version"`
	ResumeRef      string `json:"resume_ref"`
	ResumeRevision int64  `json:"resume_revision"`
}

type TurnInput = execution.TurnInput

type TurnResult = execution.TurnResult

type IncomingEvent struct {
	Type    string
	Content []map[string]any
}

type ManagedEvent = execution.ManagedEvent

type Repository interface {
	PutAgent(context.Context, Agent) error
	GetAgent(context.Context, string) (Agent, error)
	ListAgents(context.Context) ([]Agent, error)
	PutEnvironment(context.Context, Environment) error
	GetEnvironment(context.Context, string) (Environment, error)
	ListEnvironments(context.Context) ([]Environment, error)
	PutSession(context.Context, Session) error
	GetSession(context.Context, string) (Session, error)
	ListSessions(context.Context) ([]Session, error)
}

type Harness = execution.Harness

func executionSession(session Session) execution.Session {
	return execution.Session{
		ID: session.ID,
		Agent: execution.Agent{
			ID: session.Agent.ID, ModelID: session.Agent.ModelID, System: session.Agent.System,
			Tools: session.Agent.Tools, Version: session.Agent.Version,
		},
		EnvironmentID:  session.EnvironmentID,
		ResumeRef:      session.Control.ResumeRef,
		ResumeRevision: session.Control.ResumeRevision,
	}
}
