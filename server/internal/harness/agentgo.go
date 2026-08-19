package harness

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	agentledger "github.com/compforge/agent-ledger/go"
	agentgoadapter "github.com/compforge/agent-ledger/go/adapters/agentgo"
	"github.com/compforge/agentd/server/internal/app"
	"github.com/compforge/agentd/server/internal/harnessstate"
	"github.com/compforge/agentd/server/internal/sandbox"
	"github.com/compforge/agentgo"
	"github.com/compforge/agentgo/llm"
)

type AgentGoRunnerConfig struct {
	APIKey           string
	BaseURL          string
	RequestTimeout   time.Duration
	OperationTimeout time.Duration
	ToolTimeout      time.Duration
	Ledger           agentledger.EventStore
	State            harnessstate.Store
	Sandbox          sandbox.Engine
}

type AgentGoRunner struct {
	config AgentGoRunnerConfig

	mu     sync.Mutex
	active map[string]*agentgo.Agent
}

func NewAgentGoRunner(config AgentGoRunnerConfig) (*AgentGoRunner, error) {
	if config.RequestTimeout <= 0 || config.OperationTimeout <= 0 || config.ToolTimeout <= 0 {
		return nil, fmt.Errorf("create AgentGo runner: request, operation, and tool timeouts must be positive")
	}
	if config.Ledger == nil || config.State == nil || config.Sandbox == nil {
		return nil, fmt.Errorf("create AgentGo runner: ledger, harness state, and sandbox engine are required")
	}
	return &AgentGoRunner{config: config, active: make(map[string]*agentgo.Agent)}, nil
}

func (r *AgentGoRunner) Name() string {
	return "agentgo"
}

func (r *AgentGoRunner) Version() string {
	return "0.0.1"
}

func (r *AgentGoRunner) PrepareSession(ctx context.Context, session app.Session) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return r.Name() + "/" + session.ID, nil
}

func (r *AgentGoRunner) Run(ctx context.Context, session app.Session, input string, emit func(app.ManagedEvent) error) error {
	if r.config.APIKey == "" {
		return fmt.Errorf("run AgentGo session: ANTHROPIC_API_KEY is not configured")
	}
	runID := "run_" + agentledger.NewID()
	actor := agentledger.Actor{Type: "agent", ID: session.Agent.ID, Framework: "agentgo"}
	recorder := agentledger.NewSessionRecorder(agentledger.RecorderOptions{
		Store: r.config.Ledger, SessionID: session.ID, RunID: runID, Actor: actor,
	})
	if _, err := recorder.StartRun(ctx, map[string]any{"agent_version": session.Agent.Version}); err != nil {
		return fmt.Errorf("record AgentGo run start: %w", err)
	}

	adapter, err := agentgoadapter.New(ctx, agentgoadapter.Config{
		Store:            r.config.Ledger,
		SessionID:        session.ID,
		RunID:            runID,
		Actor:            actor,
		NativeSessionID:  session.ID,
		OperationTimeout: r.config.OperationTimeout,
	})
	if err != nil {
		return r.finishRun(ctx, session.ID, runID, actor, fmt.Errorf("create AgentGo ledger adapter: %w", err))
	}
	messages, revision, err := r.loadMessages(ctx, session.Control.ResumeRef)
	if err != nil {
		return r.finishRun(ctx, session.ID, runID, actor, fmt.Errorf("restore AgentGo session: %w", err))
	}
	modelOptions := []llm.ModelOption{
		llm.WithAPIKey(r.config.APIKey),
		llm.WithRequestTimeout(r.config.RequestTimeout),
	}
	if r.config.BaseURL != "" {
		modelOptions = append(modelOptions, llm.WithBaseURL(r.config.BaseURL))
	}
	model, err := llm.NewModel("anthropic", session.Agent.ModelID, modelOptions...)
	if err != nil {
		return r.finishRun(ctx, session.ID, runID, actor, fmt.Errorf("create AgentGo model: %w", err))
	}

	options := []agentgo.AgentOption{
		agentgo.WithModel(adapter.WrapModel(model)),
		agentgo.WithSystemPrompt(session.Agent.System),
		agentgo.WithMaxTurns(100),
	}
	if agentUsesToolset(session.Agent) {
		options = append(options, agentgo.WithTools(sandbox.NewAgentGoToolset(r.config.Sandbox, session.ID, r.config.ToolTimeout)...))
	}
	// The ledger adapter still owns strict model/tool audit hooks. The final
	// committer is intentionally replaced so recovery state has its own store.
	options = append(options, adapter.Options()...)
	options = append(options, agentgo.WithMessageCommitter(r.messageCommitter(session.Control.ResumeRef, &revision)))
	agent := agentgo.NewAgent(options...)
	if err := agent.SetMessages(messages); err != nil {
		return r.finishRun(ctx, session.ID, runID, actor, fmt.Errorf("restore AgentGo session: %w", err))
	}

	var emitErr error
	agent.Subscribe(func(event agentgo.Event) {
		if emitErr != nil || event.Type != agentgo.EventMessageEnd || event.Message == nil {
			return
		}
		if event.Message.GetRole() != agentgo.RoleAssistant {
			return
		}
		emitErr = emit(app.NewManagedEvent("agent.message", map[string]any{
			"content": []map[string]any{{"type": "text", "text": event.Message.TextContent()}},
		}))
	})
	r.mu.Lock()
	r.active[session.ID] = agent
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		delete(r.active, session.ID)
		r.mu.Unlock()
	}()

	if err := agent.Prompt(ctx, input); err != nil {
		return r.finishRun(ctx, session.ID, runID, actor, fmt.Errorf("prompt AgentGo session: %w", err))
	}
	agent.WaitForIdle()
	if emitErr != nil {
		return r.finishRun(ctx, session.ID, runID, actor, fmt.Errorf("persist managed agent event: %w", emitErr))
	}
	if stateErr := agent.State().Error; stateErr != "" {
		return r.finishRun(ctx, session.ID, runID, actor, fmt.Errorf("AgentGo state: %s", stateErr))
	}
	return r.finishRun(ctx, session.ID, runID, actor, nil)
}

const agentGoMessageFormat = "application/vnd.compforge.agentgo.message+json;version=1"

func (r *AgentGoRunner) loadMessages(ctx context.Context, resumeRef string) ([]agentgo.AgentMessage, int64, error) {
	records, err := r.config.State.Load(ctx, resumeRef)
	if err != nil {
		return nil, -1, err
	}
	messages := make([]agentgo.AgentMessage, 0, len(records))
	revision := int64(-1)
	for _, record := range records {
		if record.Format != agentGoMessageFormat {
			return nil, revision, fmt.Errorf("unsupported AgentGo state format %q", record.Format)
		}
		var message agentgo.Message
		if err := json.Unmarshal(record.Data, &message); err != nil {
			return nil, revision, fmt.Errorf("decode AgentGo state revision %d: %w", record.Revision, err)
		}
		messages = append(messages, message)
		revision = record.Revision
	}
	return messages, revision, nil
}

func (r *AgentGoRunner) messageCommitter(resumeRef string, revision *int64) func(agentgo.AgentMessage) error {
	var mu sync.Mutex
	return func(message agentgo.AgentMessage) error {
		data, err := encodeAgentGoMessage(message)
		if err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), r.config.OperationTimeout)
		defer cancel()
		record, err := r.config.State.Append(ctx, resumeRef, *revision, agentGoMessageFormat, data)
		if err != nil {
			return fmt.Errorf("persist AgentGo message: %w", err)
		}
		*revision = record.Revision
		return nil
	}
}

func encodeAgentGoMessage(message agentgo.AgentMessage) (json.RawMessage, error) {
	switch value := message.(type) {
	case agentgo.Message:
		return json.Marshal(value)
	case *agentgo.Message:
		return json.Marshal(value)
	default:
		return nil, fmt.Errorf("unsupported custom AgentGo message %T", message)
	}
}

func (r *AgentGoRunner) Interrupt(sessionID string) {
	r.mu.Lock()
	agent := r.active[sessionID]
	r.mu.Unlock()
	if agent != nil {
		agent.Abort()
	}
}

func (r *AgentGoRunner) finishRun(ctx context.Context, sessionID, runID string, actor agentledger.Actor, runErr error) error {
	recorder, err := agentledger.ResumeRecorder(ctx, agentledger.RecorderOptions{
		Store: r.config.Ledger, SessionID: sessionID, RunID: runID, Actor: actor,
	})
	if err != nil {
		if runErr != nil {
			return fmt.Errorf("%v; resume run recorder: %w", runErr, err)
		}
		return fmt.Errorf("resume run recorder: %w", err)
	}
	eventType := "run.completed"
	payload := map[string]any{}
	if runErr != nil {
		eventType = "run.failed"
		payload["error"] = runErr.Error()
	}
	if _, err := recorder.Record(ctx, eventType, payload, "", ""); err != nil {
		if runErr != nil {
			return fmt.Errorf("%v; record run outcome: %w", runErr, err)
		}
		return fmt.Errorf("record run outcome: %w", err)
	}
	return runErr
}

func agentUsesToolset(agent app.Agent) bool {
	for _, tool := range agent.Tools {
		if tool["type"] == "agent_toolset_20260401" {
			return true
		}
	}
	return false
}
