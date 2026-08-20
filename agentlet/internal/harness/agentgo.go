package harness

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	agentledger "github.com/compforge/agent-ledger/go"
	agentgoadapter "github.com/compforge/agent-ledger/go/adapters/agentgo"
	"github.com/compforge/agentd/agentlet/internal/execution"
	harnessstate "github.com/compforge/agentd/agentlet/internal/harness/state"
	"github.com/compforge/agentd/agentlet/internal/sandbox"
	"github.com/compforge/agentd/agentlet/internal/sandbox/engine"
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
	Sandbox          engine.Engine
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

func (r *AgentGoRunner) PrepareSession(ctx context.Context, session execution.Session) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return r.Name() + "/" + session.ID, nil
}

// Run executes one AgentGo turn and projects only durable assistant messages.
//
// +spec=`Model calls use the session's model and system prompt; only complete assistant messages become managed events, while every model attempt remains auditable`
// +case:id=model_question_answer,desc=`answer through an Anthropic-compatible streaming model`,expect=`the final answer is persisted once and the model attempt completes in the ledger`
// +case:id=model_stream_timeout,desc=`a model stream times out after partial output and a later user input succeeds`,expect=`the timed-out attempt is audited and only the later complete answer is persisted`,forbid=`persisting partial output or losing the failed model attempt`
// +link=agentd/docs/state-ledger.md
func (r *AgentGoRunner) Run(
	ctx context.Context,
	session execution.Session,
	input execution.TurnInput,
	emit func(execution.ManagedEvent) error,
) (execution.TurnResult, error) {
	if input.ID == "" {
		return execution.TurnResult{}, fmt.Errorf("run AgentGo session: input ID is required")
	}
	messages, revision, err := r.loadMessages(ctx, session.ResumeRef)
	if err != nil {
		return execution.TurnResult{ResumeRevision: revision}, fmt.Errorf("restore AgentGo session: %w", err)
	}
	if err := projectAssistantMessages(session.ResumeRef, messages, input.ID, emit); err != nil {
		return execution.TurnResult{ResumeRevision: revision}, fmt.Errorf("project restored AgentGo messages: %w", err)
	}
	action, err := r.resumeAction(ctx, session.ID, messages, input.ID)
	if err != nil {
		return execution.TurnResult{ResumeRevision: revision}, err
	}
	if action == resumeCompleted {
		return execution.TurnResult{ResumeRevision: revision}, nil
	}
	if r.config.APIKey == "" {
		return execution.TurnResult{ResumeRevision: revision}, fmt.Errorf("run AgentGo session: ANTHROPIC_API_KEY is not configured")
	}
	runID := "run_" + agentledger.NewID()
	actor := agentledger.Actor{Type: "agent", ID: session.Agent.ID, Framework: "agentgo"}
	recorder := agentledger.NewSessionRecorder(agentledger.RecorderOptions{
		Store: r.config.Ledger, SessionID: session.ID, RunID: runID, Actor: actor,
	})
	if _, err := recorder.StartRun(ctx, map[string]any{"agent_version": session.Agent.Version}); err != nil {
		return execution.TurnResult{ResumeRevision: revision}, fmt.Errorf("record AgentGo run start: %w", err)
	}
	finish := func(runErr error) (execution.TurnResult, error) {
		return execution.TurnResult{ResumeRevision: revision}, r.finishRun(ctx, session.ID, runID, actor, runErr)
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
		return finish(fmt.Errorf("create AgentGo ledger adapter: %w", err))
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
		return finish(fmt.Errorf("create AgentGo model: %w", err))
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
	options = append(options, agentgo.WithMessageCommitter(r.messageCommitter(session.ResumeRef, &revision)))
	agent := agentgo.NewAgent(options...)
	if err := agent.SetMessages(messages); err != nil {
		return finish(fmt.Errorf("restore AgentGo session: %w", err))
	}

	var emitErr error
	var emitMu sync.Mutex
	agent.Subscribe(func(event agentgo.Event) {
		emitMu.Lock()
		defer emitMu.Unlock()
		if emitErr != nil || event.Type != agentgo.EventMessageEnd || event.Message == nil {
			return
		}
		if event.Message.GetRole() != agentgo.RoleAssistant {
			return
		}
		managed, err := managedAssistantEvent(session.ResumeRef, event.Message)
		if err != nil {
			emitErr = err
			return
		}
		emitErr = emit(managed)
	})
	r.mu.Lock()
	r.active[session.ID] = agent
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		delete(r.active, session.ID)
		r.mu.Unlock()
	}()

	if action == resumePrompt {
		message := agentgo.UserMsg(input.Text)
		message.Metadata = map[string]any{agentdInputID: input.ID}
		err = agent.PromptMessages(ctx, message)
	} else {
		err = agent.Continue(ctx)
	}
	if err != nil {
		return finish(fmt.Errorf("start AgentGo session: %w", err))
	}
	agent.WaitForIdle()
	emitMu.Lock()
	persistErr := emitErr
	emitMu.Unlock()
	if persistErr != nil {
		return finish(fmt.Errorf("persist managed agent event: %w", persistErr))
	}
	if stateErr := agent.State().Error; stateErr != "" {
		return finish(fmt.Errorf("AgentGo state: %s", stateErr))
	}
	return finish(nil)
}

const agentGoMessageFormat = "application/vnd.compforge.agentgo.message+json;version=1"
const agentdInputID = "agentd.input_id"

type resumeAction int

const (
	resumePrompt resumeAction = iota
	resumeContinue
	resumeCompleted
)

func (r *AgentGoRunner) resumeAction(
	ctx context.Context,
	sessionID string,
	messages []agentgo.AgentMessage,
	inputID string,
) (resumeAction, error) {
	if inputIndex(messages, inputID) < 0 {
		return resumePrompt, nil
	}
	unresolved, err := unresolvedToolAttempts(ctx, r.config.Ledger, sessionID)
	if err != nil {
		return resumePrompt, fmt.Errorf("inspect AgentGo recovery attempts: %w", err)
	}
	if len(unresolved) > 0 {
		return resumePrompt, fmt.Errorf("%w: %d tool attempt(s) have no durable outcome", execution.ErrUnsafeRecovery, len(unresolved))
	}
	if len(messages) == 0 {
		return resumePrompt, fmt.Errorf("%w: committed input is missing from AgentGo state", execution.ErrUnsafeRecovery)
	}
	last := messages[len(messages)-1]
	if last.GetRole() == agentgo.RoleAssistant {
		if last.HasToolCalls() {
			return resumePrompt, fmt.Errorf("%w: AgentGo stopped after committing tool calls without durable results", execution.ErrUnsafeRecovery)
		}
		return resumeCompleted, nil
	}
	if last.GetRole() == agentgo.RoleUser || last.GetRole() == agentgo.RoleTool {
		return resumeContinue, nil
	}
	return resumePrompt, fmt.Errorf("%w: AgentGo cannot continue from role %q", execution.ErrUnsafeRecovery, last.GetRole())
}

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

func inputIndex(messages []agentgo.AgentMessage, inputID string) int {
	for index := len(messages) - 1; index >= 0; index-- {
		item := messages[index]
		var metadata map[string]any
		switch message := item.(type) {
		case agentgo.Message:
			metadata = message.Metadata
		case *agentgo.Message:
			metadata = message.Metadata
		}
		if storedID, _ := metadata[agentdInputID].(string); storedID == inputID {
			return index
		}
	}
	return -1
}

func unresolvedToolAttempts(
	ctx context.Context,
	store agentledger.EventStore,
	sessionID string,
) ([]string, error) {
	pending := make(map[string]struct{})
	for event, err := range store.ScanSession(ctx, sessionID, "") {
		if err != nil {
			return nil, err
		}
		switch event.EventType {
		case "tool.requested":
			if event.AttemptID != "" {
				pending[event.AttemptID] = struct{}{}
			}
		case "tool.completed", "tool.failed":
			delete(pending, event.AttemptID)
		}
	}
	result := make([]string, 0, len(pending))
	for attemptID := range pending {
		result = append(result, attemptID)
	}
	return result, nil
}

func projectAssistantMessages(
	resumeRef string,
	messages []agentgo.AgentMessage,
	inputID string,
	emit func(execution.ManagedEvent) error,
) error {
	index := inputIndex(messages, inputID)
	if index < 0 {
		return nil
	}
	for _, message := range messages[index+1:] {
		if message.GetRole() != agentgo.RoleAssistant {
			continue
		}
		managed, err := managedAssistantEvent(resumeRef, message)
		if err != nil {
			return err
		}
		if err := emit(managed); err != nil {
			return err
		}
	}
	return nil
}

func managedAssistantEvent(resumeRef string, message agentgo.AgentMessage) (execution.ManagedEvent, error) {
	encoded, err := json.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("encode assistant event identity: %w", err)
	}
	digest := sha256.Sum256(append([]byte(resumeRef+"\x00"), encoded...))
	event := execution.NewManagedEvent("agent.message", map[string]any{
		"content": []map[string]any{{"type": "text", "text": message.TextContent()}},
	})
	event["id"] = fmt.Sprintf("event_%x", digest[:12])
	if timestamp := message.GetTimestamp(); !timestamp.IsZero() {
		event["processed_at"] = timestamp.UTC().Format(time.RFC3339Nano)
	}
	return event, nil
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

func agentUsesToolset(agent execution.Agent) bool {
	for _, tool := range agent.Tools {
		if tool["type"] == "agent_toolset_20260401" {
			return true
		}
	}
	return false
}
