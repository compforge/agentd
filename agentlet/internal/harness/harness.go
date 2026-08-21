package harness

import (
	"context"
	"errors"

	managedevent "github.com/compforge/agentd/internal/event"
)

var ErrUnsafeRecovery = errors.New("automatic recovery is unsafe")

// Agent is the immutable execution definition an Agentlet needs for one turn.
type Agent struct {
	ID      string
	ModelID string
	System  string
	Tools   []map[string]any
	Version int64
}

// Session is the minimal execution context derived from agentd Control State.
// Agentlets receive it with an assignment and do not load or mutate Control State.
type Session struct {
	ID             string
	Agent          Agent
	EnvironmentID  string
	ResumeRef      string
	ResumeRevision int64
}

type TurnInput struct {
	ID   string
	Text string
}

type TurnResult struct {
	ResumeRef      string
	ResumeRevision int64
}

type ManagedEvent = managedevent.ManagedEvent

// Harness runs one durable session turn without owning placement or Control State.
type Harness interface {
	Name() string
	Version() string
	PrepareSession(context.Context, Session) (string, error)
	Run(context.Context, Session, TurnInput, func(ManagedEvent) error) (TurnResult, error)
	Interrupt(string)
}

func NewManagedEvent(eventType string, fields map[string]any) ManagedEvent {
	return managedevent.New(eventType, fields)
}
