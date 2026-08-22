package harness

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	agentledger "github.com/compforge/agent-ledger/go"
	agentgoadapter "github.com/compforge/agent-ledger/go/adapters/agentgo"
	"github.com/compforge/agentd/agentlet/internal/sandbox"
	"github.com/compforge/agentd/agentlet/internal/sandbox/engine"
	"github.com/compforge/agentgo"
	"github.com/compforge/agentgo/llm"
)

type AgentGoRunnerConfig struct {
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

var _ Harness = (*AgentGoRunner)(nil)

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

func (r *AgentGoRunner) PrepareSession(ctx context.Context, session Session) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return r.Name() + "/" + session.ID, nil
}

// Run executes one AgentGo turn and projects only durable assistant messages.
//
// +spec=`Each AgentGo turn restores the exact Session checkpoint, uses its configured model and Sandbox, persists only complete assistant messages, and keeps model/tool attempts auditable`
// +case:id=model_question_answer,desc=`answer through the configured streaming model provider`,expect=`the final answer is persisted once and the model attempt completes in the ledger`
// +case:id=model_stream_timeout,desc=`a model stream times out after partial output and a later user input succeeds`,expect=`the timed-out attempt is audited and only the later complete answer is persisted`,forbid=`persisting partial output or losing the failed model attempt`
// +case:id=sandbox_resume,desc=`send two tool-using turns to one managed Session`,input=`each turn requires the isolated bash tool and the same final marker`,expect=`both turns finish, the second restores the first checkpoint, and durable Event history contains both answers`,forbid=`losing Session identity, bypassing the Sandbox, or duplicating a completed input`,group=system
// +case:id=unsafe_tool_resolution_resume,desc=`restore a non-idempotent tool call whose requested Attempt has no terminal outcome`,input=`the user denies the exact required tool use`,expect=`the Session continues and the original Attempt becomes user_denied`,forbid=`automatically replaying the write or creating a second tool Attempt`
// +link=agentd/docs/agentlet.md
// +link=agentlet/tests/e2e/cases/recovery.yaml
// +link=tests/e2e/cases/managed-agent.yaml
func (r *AgentGoRunner) Run(
	ctx context.Context,
	session Session,
	input TurnInput,
	emit func(ManagedEvent) error,
) (TurnResult, error) {
	if input.ID == "" {
		return TurnResult{}, fmt.Errorf("run AgentGo session: input ID is required")
	}
	messages, revision, err := r.loadMessages(
		ctx, session.ResumeRef, r.checkpointKey(session.ID), session.ResumeRevision,
	)
	if err != nil {
		return TurnResult{ResumeRevision: revision}, fmt.Errorf("restore AgentGo session: %w", err)
	}
	if err := projectAssistantMessages(messages, input.ID, emit); err != nil {
		return TurnResult{ResumeRevision: revision}, fmt.Errorf("project restored AgentGo messages: %w", err)
	}
	runID := "input/" + input.ID
	action := resumePrompt
	var resolution toolResolutionPlan
	if input.ToolResolution == nil {
		action, err = r.resumeAction(ctx, session.ID, messages, input.ID)
	} else {
		resolution, err = r.planToolResolution(ctx, session.ID, input)
		runID = resolution.Attempt.RunID
		action = resumeContinue
	}
	if err != nil {
		return TurnResult{ResumeRevision: revision}, err
	}
	if action == resumeCompleted {
		return TurnResult{ResumeRef: session.ResumeRef, ResumeRevision: revision}, nil
	}
	if strings.TrimSpace(session.Agent.Model.APIKey) == "" {
		return TurnResult{ResumeRevision: revision}, fmt.Errorf("run AgentGo session: model API key is not configured")
	}
	provider := strings.ToLower(strings.TrimSpace(session.Agent.Model.Provider))
	if !llm.IsProviderRegistered(provider) {
		return TurnResult{ResumeRevision: revision}, fmt.Errorf(
			"run AgentGo session: model provider %q is not registered", session.Agent.Model.Provider,
		)
	}
	actor, err := r.ensureCheckpointActor(ctx, agentledger.NewActorWithKey(
		fmt.Sprintf("agentd/agents/%s/versions/%d", session.Agent.ID, session.Agent.Version),
		"agent",
		"agentgo",
	))
	if err != nil {
		return TurnResult{ResumeRevision: revision}, err
	}
	recorder, err := agentledger.OpenRecorder(ctx, agentledger.RecorderOptions{
		Store: r.config.Ledger, SessionID: session.ID, RunID: runID, Actor: actor,
	})
	if err != nil {
		return TurnResult{ResumeRevision: revision}, fmt.Errorf("open AgentGo run recorder: %w", err)
	}
	if recorder.Lane().LastSeq == 0 {
		if _, err := recorder.StartRun(ctx, map[string]any{"agent_version": session.Agent.Version}); err != nil {
			return TurnResult{ResumeRevision: revision}, fmt.Errorf("record AgentGo run start: %w", err)
		}
	}
	checkpointID := session.ResumeRef
	finish := func(runErr error) (TurnResult, error) {
		return TurnResult{ResumeRef: checkpointID, ResumeRevision: revision}, r.finishRun(
			ctx, session.ID, runID, actor, runErr,
		)
	}

	commitMessage := r.messageCommitter(
		r.checkpointKey(session.ID), actor.ID, session.ID, runID, messages, &checkpointID, &revision,
	)
	if resolution.ToolResult != nil {
		if !hasToolResult(messages, resolution.Attempt.ToolCallID) {
			if err := commitMessage(*resolution.ToolResult); err != nil {
				return finish(fmt.Errorf("persist user tool resolution: %w", err))
			}
			messages = append(messages, *resolution.ToolResult)
		}
		if err := r.recordToolResolution(ctx, recorder, resolution); err != nil {
			return finish(err)
		}
	}

	retryAuthorization := resolution.RetryAuthorization
	// A supplied or denied tool result advances the shared run lane. Open the
	// AgentGo adapter afterwards so its recorder starts from that durable seq.
	adapter, err := agentgoadapter.New(ctx, agentgoadapter.Config{
		Store:            r.config.Ledger,
		SessionID:        session.ID,
		RunID:            runID,
		Actor:            actor,
		NativeSessionID:  session.ID,
		OperationTimeout: r.config.OperationTimeout,
		ToolEffect:       agentGoToolEffect,
		CanRetryTool: func(action agentledger.Action, attempt agentledger.Attempt, _ agentgo.ToolCall) agentgoadapter.ToolRetryDecision {
			if canRetryToolEffect(action.Effect) {
				return agentgoadapter.ToolRetryDecision{Approved: true}
			}
			if retryAuthorization.ActionID == action.ID && retryAuthorization.AttemptID == attempt.ID {
				return agentgoadapter.ToolRetryDecision{
					Approved: true,
					Metadata: map[string]any{"recovery_decision_id": retryAuthorization.DecisionID},
				}
			}
			return agentgoadapter.ToolRetryDecision{}
		},
	})
	if err != nil {
		return finish(fmt.Errorf("create AgentGo ledger adapter: %w", err))
	}
	modelOptions := []llm.ModelOption{
		llm.WithAPIKey(session.Agent.Model.APIKey),
		llm.WithRequestTimeout(r.config.RequestTimeout),
	}
	if session.Agent.Model.BaseURL != "" {
		modelOptions = append(modelOptions, llm.WithBaseURL(session.Agent.Model.BaseURL))
	}
	model, err := llm.NewModel(provider, session.Agent.Model.UpstreamID, modelOptions...)
	if err != nil {
		return finish(fmt.Errorf("create AgentGo model: %w", err))
	}

	options := []agentgo.AgentOption{
		agentgo.WithModel(adapter.WrapModel(model)),
		agentgo.WithSystemPrompt(session.Agent.System),
		agentgo.WithMaxTurns(100),
	}
	if agentUsesToolset(session.Agent) {
		toolset, err := sandbox.PrepareAgentGoToolset(ctx, r.config.Sandbox, session.ID, r.config.ToolTimeout)
		if err != nil {
			return finish(err)
		}
		options = append(options, agentgo.WithTools(toolset...))
	}
	// The ledger adapter still owns strict model/tool audit hooks. The final
	// committer is intentionally replaced so recovery state has its own store.
	options = append(options, adapter.Options()...)
	options = append(options, agentgo.WithMessageCommitter(commitMessage))
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
	unresolved, err := unresolvedToolAttempts(ctx, r.config.Ledger, sessionID, "input/"+inputID)
	if err != nil {
		return resumePrompt, fmt.Errorf("inspect AgentGo recovery attempts: %w", err)
	}
	var blockers []BlockingToolUse
	for _, attempt := range unresolved {
		if !canRetryToolEffect(attempt.Action.Effect) {
			blockers = append(blockers, attempt.blockingToolUse())
		}
	}
	if len(blockers) > 0 {
		return resumePrompt, &RequiresActionError{ToolUses: blockers}
	}
	if len(messages) == 0 {
		return resumePrompt, fmt.Errorf("%w: committed input is missing from AgentGo state", ErrUnsafeRecovery)
	}
	last := messages[len(messages)-1]
	if last.GetRole() == agentgo.RoleAssistant {
		if last.HasToolCalls() {
			if len(unresolved) > 0 {
				return resumeContinue, nil
			}
			return resumePrompt, fmt.Errorf("%w: AgentGo stopped after committing tool calls without durable results", ErrUnsafeRecovery)
		}
		return resumeCompleted, nil
	}
	if last.GetRole() == agentgo.RoleUser || last.GetRole() == agentgo.RoleTool {
		return resumeContinue, nil
	}
	return resumePrompt, fmt.Errorf("%w: AgentGo cannot continue from role %q", ErrUnsafeRecovery, last.GetRole())
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
	if checkpoint.Key != checkpointKey {
		return nil, checkpoint.Revision, fmt.Errorf("AgentGo checkpoint belongs to %q", checkpoint.Key)
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

func (r *AgentGoRunner) ensureCheckpointActor(
	ctx context.Context,
	actor agentledger.Actor,
) (agentledger.Actor, error) {
	stored, err := r.config.Checkpoints.EnsureActor(ctx, actor)
	if err != nil {
		return agentledger.Actor{}, fmt.Errorf("ensure AgentGo checkpoint actor: %w", err)
	}
	return stored, nil
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

func projectAssistantMessages(
	messages []agentgo.AgentMessage,
	inputID string,
	emit func(ManagedEvent) error,
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

func managedAssistantEvent(inputID string, message agentgo.AgentMessage) (ManagedEvent, error) {
	encoded, err := json.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("encode assistant event identity: %w", err)
	}
	digest := sha256.Sum256(append([]byte(inputID+"\x00"), encoded...))
	event := NewManagedEvent("agent.message", map[string]any{
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

func (r *AgentGoRunner) finishRun(
	ctx context.Context,
	sessionID string,
	runID string,
	actor agentledger.Actor,
	runErr error,
) error {
	// The AgentGo adapter records model/tool facts on the same lane. Reopen the
	// recorder after the loop settles so the run terminal uses the latest seq.
	recorder, err := agentledger.OpenRecorder(ctx, agentledger.RecorderOptions{
		Store: r.config.Ledger, SessionID: sessionID, RunID: runID, Actor: actor,
	})
	if err != nil {
		if runErr != nil {
			return fmt.Errorf("%v; open run outcome recorder: %w", runErr, err)
		}
		return fmt.Errorf("open run outcome recorder: %w", err)
	}
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

func agentUsesToolset(agent Agent) bool {
	for _, tool := range agent.Tools {
		if tool["type"] == "agent_toolset_20260401" {
			return true
		}
	}
	return false
}
