package harness

import (
	"context"
	"errors"
	"fmt"

	managedevent "github.com/compforge/agentd/internal/event"
)

var ErrUnsafeRecovery = errors.New("automatic recovery is unsafe")

type BlockingToolUse struct {
	ID      string
	Name    string
	Input   map[string]any
	InputID string
}

// RequiresActionError is a recoverable idle boundary, not a terminal Harness
// failure. Its tool uses are projected through the Managed Agents Event API.
type RequiresActionError struct {
	ToolUses []BlockingToolUse
}

func (e *RequiresActionError) Error() string {
	return fmt.Sprintf("user action required for %d tool use(s)", len(e.ToolUses))
}

// Agent is the immutable execution definition an Agentlet needs for one turn.
type Agent struct {
	ID      string
	Model   Model
	System  string
	Tools   []map[string]any
	Version int64
}

// Model is the control-plane-resolved external model connection for one Work.
// It is execution input, not an Agentlet-owned resource.
type Model struct {
	ID         string
	Provider   string
	UpstreamID string
	BaseURL    string
	APIKey     string
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
	ID             string
	Text           string
	ToolResolution *ToolResolution
	RecoveryInput  *RecoveryInput
}

// RecoveryInput is an accepted ingress message redelivered by the Agentlet
// host when a later tool-resolution event resumes its interrupted execution.
// Harnesses may ignore it when they do not reconstruct native loop execution.
type RecoveryInput struct {
	ID   string
	Text string
}

type ToolResolution struct {
	ToolUseID   string
	Decision    string
	DenyMessage string
	Content     any
	IsError     bool
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
