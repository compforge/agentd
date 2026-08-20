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
	Checkpoints      agentledger.CheckpointStore
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
	if config.Ledger == nil || config.Checkpoints == nil || config.Sandbox == nil {
		return nil, fmt.Errorf("create AgentGo runner: ledger, checkpoint store, and sandbox engine are required")
	}
	return &AgentGoRunner{config: config, active: make(map[string]*agentgo.Agent)}, nil
}

func (r *AgentGoRunner) Name() string {
	return "agentgo"
}

func (r *AgentGoRunner) Version() string {
	return "0.0.2"
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
	messages, revision, err := r.loadMessages(
		ctx, session.ResumeRef, r.checkpointKey(session.ID), session.ResumeRevision,
	)
	if err != nil {
		return execution.TurnResult{ResumeRevision: revision}, fmt.Errorf("restore AgentGo session: %w", err)
	}
	if err := projectAssistantMessages(messages, input.ID, emit); err != nil {
		return execution.TurnResult{ResumeRevision: revision}, fmt.Errorf("project restored AgentGo messages: %w", err)
	}
	action, err := r.resumeAction(ctx, session.ID, messages, input.ID)
	if err != nil {
		return execution.TurnResult{ResumeRevision: revision}, err
	}
	if action == resumeCompleted {
		return execution.TurnResult{ResumeRef: session.ResumeRef, ResumeRevision: revision}, nil
	}
	if r.config.APIKey == "" {
		return execution.TurnResult{ResumeRevision: revision}, fmt.Errorf("run AgentGo session: ANTHROPIC_API_KEY is not configured")
	}
	runID := "input/" + input.ID
	actor := agentledger.NewActor("agent", "agentgo")
	if err := r.ensureCheckpointActor(ctx, actor); err != nil {
		return execution.TurnResult{ResumeRevision: revision}, err
	}
	recorder, err := agentledger.OpenRecorder(ctx, agentledger.RecorderOptions{
		Store: r.config.Ledger, SessionID: session.ID, RunID: runID, Actor: actor,
	})
	if err != nil {
		return execution.TurnResult{ResumeRevision: revision}, fmt.Errorf("open AgentGo run recorder: %w", err)
	}
	if recorder.Lane().LastSeq == 0 {
		if _, err := recorder.StartRun(ctx, map[string]any{"agent_version": session.Agent.Version}); err != nil {
			return execution.TurnResult{ResumeRevision: revision}, fmt.Errorf("record AgentGo run start: %w", err)
		}
	}
	checkpointID := session.ResumeRef
	finish := func(runErr error) (execution.TurnResult, error) {
		return execution.TurnResult{ResumeRef: checkpointID, ResumeRevision: revision}, r.finishRun(ctx, recorder, runErr)
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
	options = append(options, agentgo.WithMessageCommitter(r.messageCommitter(
		r.checkpointKey(session.ID), actor.ID, session.ID, runID, messages, &checkpointID, &revision,
	)))
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
		managed, err := managedAssistantEvent(input.ID, event.Message)
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

const agentGoCheckpointFormat = "application/vnd.compforge.agentgo.messages+json;version=1"
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

func (r *AgentGoRunner) loadMessages(
	ctx context.Context,
	resumeRef string,
	checkpointKey string,
	expectedRevision int64,
) ([]agentgo.AgentMessage, int64, error) {
	if resumeRef == checkpointKey && expectedRevision == 0 {
		return nil, 0, nil
	}
	checkpoint, exists, err := r.config.Checkpoints.GetCheckpoint(ctx, resumeRef)
	if err != nil {
		return nil, 0, err
	}
	if !exists {
		return nil, 0, fmt.Errorf("AgentGo checkpoint %q does not exist", resumeRef)
	}
	if checkpoint.CheckpointKey != checkpointKey {
		return nil, checkpoint.Revision, fmt.Errorf("AgentGo checkpoint belongs to %q", checkpoint.CheckpointKey)
	}
	if checkpoint.Revision != expectedRevision {
		return nil, checkpoint.Revision, fmt.Errorf(
			"AgentGo checkpoint revision mismatch: control=%d checkpoint=%d",
			expectedRevision,
			checkpoint.Revision,
		)
	}
	if checkpoint.Format != agentGoCheckpointFormat {
		return nil, checkpoint.Revision, fmt.Errorf("unsupported AgentGo checkpoint format %q", checkpoint.Format)
	}
	encoded, err := json.Marshal(checkpoint.State["messages"])
	if err != nil {
		return nil, checkpoint.Revision, fmt.Errorf("encode AgentGo checkpoint messages: %w", err)
	}
	var concrete []agentgo.Message
	if err := json.Unmarshal(encoded, &concrete); err != nil {
		return nil, checkpoint.Revision, fmt.Errorf("decode AgentGo checkpoint revision %d: %w", checkpoint.Revision, err)
	}
	messages := make([]agentgo.AgentMessage, len(concrete))
	for index := range concrete {
		messages[index] = concrete[index]
	}
	return messages, checkpoint.Revision, nil
}

func (r *AgentGoRunner) messageCommitter(
	checkpointKey string,
	actorID string,
	sessionID string,
	runID string,
	existing []agentgo.AgentMessage,
	checkpointID *string,
	revision *int64,
) func(agentgo.AgentMessage) error {
	var mu sync.Mutex
	messages, initErr := concreteAgentGoMessages(existing)
	return func(message agentgo.AgentMessage) error {
		mu.Lock()
		defer mu.Unlock()
		if initErr != nil {
			return initErr
		}
		concrete, err := concreteAgentGoMessage(message)
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(context.Background(), r.config.OperationTimeout)
		defer cancel()
		anchor, err := r.checkpointAnchor(ctx, sessionID, runID)
		if err != nil {
			return err
		}
		messages = append(messages, concrete)
		checkpoint := agentledger.NewCheckpoint(checkpointKey, actorID, agentGoCheckpointFormat, map[string]any{
			"messages": messages,
		})
		checkpoint.Anchor = anchor
		record, err := r.config.Checkpoints.SaveCheckpoint(ctx, *revision, checkpoint)
		if err != nil {
			messages = messages[:len(messages)-1]
			return fmt.Errorf("persist AgentGo checkpoint: %w", err)
		}
		*revision = record.Revision
		*checkpointID = record.ID
		return nil
	}
}

func (r *AgentGoRunner) ensureCheckpointActor(ctx context.Context, actor agentledger.Actor) error {
	_, exists, err := r.config.Checkpoints.GetActor(ctx, actor.ID)
	if err != nil {
		return fmt.Errorf("get AgentGo checkpoint actor: %w", err)
	}
	if exists {
		return nil
	}
	if err := r.config.Checkpoints.CreateActor(ctx, actor); err != nil {
		return fmt.Errorf("create AgentGo checkpoint actor: %w", err)
	}
	return nil
}

func (r *AgentGoRunner) checkpointKey(sessionID string) string {
	return r.Name() + "/" + sessionID
}

func (r *AgentGoRunner) checkpointAnchor(
	ctx context.Context,
	sessionID string,
	runID string,
) (*agentledger.CheckpointAnchor, error) {
	lane, exists, err := r.config.Ledger.FindLane(ctx, sessionID, runID, "main")
	if err != nil {
		return nil, fmt.Errorf("find AgentGo checkpoint lane: %w", err)
	}
	if !exists || lane.LastSeq == 0 {
		return nil, nil
	}
	for event, err := range r.config.Ledger.LoadLane(ctx, lane.ID, lane.LastSeq-1) {
		if err != nil {
			return nil, fmt.Errorf("load AgentGo checkpoint anchor: %w", err)
		}
		return &agentledger.CheckpointAnchor{
			LaneID: lane.ID, LastAppliedSeq: event.Seq, LastAppliedEventID: event.ID,
		}, nil
	}
	return nil, fmt.Errorf("AgentGo checkpoint lane %s is missing seq %d", lane.ID, lane.LastSeq)
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

func concreteAgentGoMessages(messages []agentgo.AgentMessage) ([]agentgo.Message, error) {
	result := make([]agentgo.Message, 0, len(messages))
	for _, message := range messages {
		concrete, err := concreteAgentGoMessage(message)
		if err != nil {
			return nil, err
		}
		result = append(result, concrete)
	}
	return result, nil
}

func concreteAgentGoMessage(message agentgo.AgentMessage) (agentgo.Message, error) {
	encoded, err := encodeAgentGoMessage(message)
	if err != nil {
		return agentgo.Message{}, err
	}
	var concrete agentgo.Message
	if err := json.Unmarshal(encoded, &concrete); err != nil {
		return agentgo.Message{}, fmt.Errorf("clone AgentGo message: %w", err)
	}
	return concrete, nil
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
	view, err := store.LoadSession(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	actionTypes := make(map[string]string, len(view.Actions))
	for _, action := range view.Actions {
		actionTypes[action.ID] = action.Type
	}
	toolAttempts := make(map[string]struct{}, len(view.Attempts))
	for _, attempt := range view.Attempts {
		if actionTypes[attempt.ActionID] == agentledger.ActionTypeToolCall {
			toolAttempts[attempt.ID] = struct{}{}
		}
	}
	pending := make(map[string]struct{})
	for _, event := range view.Events {
		if _, isTool := toolAttempts[event.SubjectID]; !isTool {
			continue
		}
		switch event.EventType {
		case agentledger.EventTypeAttemptRequested:
			pending[event.SubjectID] = struct{}{}
		case agentledger.EventTypeAttemptCompleted, agentledger.EventTypeAttemptFailed:
			delete(pending, event.SubjectID)
		}
	}
	result := make([]string, 0, len(pending))
	for attemptID := range pending {
		result = append(result, attemptID)
	}
	return result, nil
}

func projectAssistantMessages(
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
		managed, err := managedAssistantEvent(inputID, message)
		if err != nil {
			return err
		}
		if err := emit(managed); err != nil {
			return err
		}
	}
	return nil
}

func managedAssistantEvent(inputID string, message agentgo.AgentMessage) (execution.ManagedEvent, error) {
	encoded, err := json.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("encode assistant event identity: %w", err)
	}
	digest := sha256.Sum256(append([]byte(inputID+"\x00"), encoded...))
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

func (r *AgentGoRunner) finishRun(ctx context.Context, recorder *agentledger.LaneRecorder, runErr error) error {
	var err error
	if runErr == nil {
		_, err = recorder.CompleteRun(ctx, nil)
	} else {
		_, err = recorder.FailRun(ctx, runErr)
	}
	if err != nil {
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
