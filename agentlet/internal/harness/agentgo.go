package harness

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
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
	return "0.0.3"
}

func (r *AgentGoRunner) PrepareSession(ctx context.Context, session Session) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return r.Name() + "/" + session.ID, nil
}

// Run executes one AgentGo turn and projects only user-visible assistant text.
//
// +spec=`Each AgentGo turn restores the latest validated Session checkpoint, uses its configured model and Sandbox, projects only complete user-visible assistant text as agent.message, and keeps model/tool attempts auditable`
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
	checkpointKey := r.checkpointKey(session.ID)
	native, checkpoint, checkpointExists, err := r.loadAgentGoCheckpoint(
		ctx, session.ResumeRef, checkpointKey, session.ResumeRevision,
	)
	if err != nil {
		return TurnResult{}, fmt.Errorf("restore AgentGo session: %w", err)
	}
	currentResumePoint := TurnResult{ResumeRef: session.ResumeRef, ResumeRevision: session.ResumeRevision}
	if checkpointExists {
		currentResumePoint = TurnResult{ResumeRef: checkpoint.ID, ResumeRevision: checkpoint.Revision}
	}

	recoveryInput := RecoveryInput{ID: input.ID, Text: input.Text}
	runID := "input/" + input.ID
	var resolution toolResolutionPlan
	if input.ToolResolution != nil {
		resolution, err = r.planToolResolution(ctx, session.ID, input)
		if err != nil {
			return currentResumePoint, err
		}
		if input.RecoveryInput == nil || input.RecoveryInput.ID == "" {
			return currentResumePoint, fmt.Errorf("%w: tool resolution has no durable recovery input", ErrUnsafeRecovery)
		}
		recoveryInput = *input.RecoveryInput
		runID = resolution.Attempt.RunID
		if runID != "input/"+recoveryInput.ID {
			return currentResumePoint, fmt.Errorf(
				"%w: tool resolution input %q does not own run %q",
				ErrUnsafeRecovery,
				recoveryInput.ID,
				runID,
			)
		}
	}

	messages := native.Snapshot.State.Messages
	if err := projectAssistantMessages(messages, recoveryInput.ID, input.ID, emit); err != nil {
		return currentResumePoint, fmt.Errorf("project restored AgentGo messages: %w", err)
	}
	if strings.TrimSpace(session.Agent.Model.APIKey) == "" {
		return currentResumePoint, fmt.Errorf("run AgentGo session: model API key is not configured")
	}
	provider := strings.ToLower(strings.TrimSpace(session.Agent.Model.Provider))
	if !llm.IsProviderRegistered(provider) {
		return currentResumePoint, fmt.Errorf(
			"run AgentGo session: model provider %q is not registered", session.Agent.Model.Provider,
		)
	}
	actor, err := r.ensureCheckpointActor(ctx, agentledger.NewActorWithKey(
		fmt.Sprintf("agentd/agents/%s/versions/%d", session.Agent.ID, session.Agent.Version),
		"agent",
		"agentgo",
	))
	if err != nil {
		return currentResumePoint, err
	}
	finish := func(runErr error) (TurnResult, error) {
		outcomeErr := r.finishRun(ctx, session.ID, runID, actor, runErr)
		resumePoint, resumeErr := r.latestResumePoint(ctx, checkpointKey, currentResumePoint)
		return resumePoint, errors.Join(outcomeErr, resumeErr)
	}
	if inputIndex(messages, recoveryInput.ID) >= 0 {
		return finish(nil)
	}

	recorder, err := agentledger.OpenRecorder(ctx, agentledger.RecorderOptions{
		Store: r.config.Ledger, SessionID: session.ID, RunID: runID, Actor: actor,
	})
	if err != nil {
		return currentResumePoint, fmt.Errorf("open AgentGo run recorder: %w", err)
	}
	if recorder.Lane().LastSeq == 0 {
		if _, err := recorder.StartRun(ctx, map[string]any{"agent_version": session.Agent.Version}); err != nil {
			return currentResumePoint, fmt.Errorf("record AgentGo run start: %w", err)
		}
	}
	if resolution.hasTerminalOutcome() {
		if err := r.recordToolResolution(ctx, recorder, resolution); err != nil {
			return finish(err)
		}
	}

	retryAuthorization := resolution.RetryAuthorization
	// A supplied or denied tool result advances the shared run lane. Open the
	// AgentGo adapter afterwards so its recorder starts from that durable seq.
	adapter, err := agentgoadapter.New(ctx, agentgoadapter.Config{
		Store:            r.config.Ledger,
		CheckpointStore:  r.config.Checkpoints,
		CheckpointKey:    checkpointKey,
		SessionID:        session.ID,
		RunID:            runID,
		Actor:            actor,
		OperationTimeout: r.config.OperationTimeout,
		ModelIdentity: func(agentgo.ModelExecution) agentgoadapter.ModelIdentity {
			return agentgoadapter.ModelIdentity{ID: session.Agent.Model.ID, Provider: provider}
		},
		ToolSemantics: func(call agentgo.ToolCall) agentgoadapter.ToolSemantics {
			return agentgoadapter.ToolSemantics{Effect: agentGoToolEffect(call)}
		},
		CanRetryTool: func(action agentledger.Action, attempt agentledger.Attempt, _ agentgo.ToolCall) agentgoadapter.ToolRetryDecision {
			if canRetryToolEffect(action.Effect) {
				return agentgoadapter.ToolRetryDecision{
					Approved: true, RecoveryDecisionID: "agentd:auto-retry/" + attempt.ID,
				}
			}
			if retryAuthorization.ActionID == action.ID && retryAuthorization.AttemptID == attempt.ID {
				return agentgoadapter.ToolRetryDecision{
					Approved:           true,
					RecoveryDecisionID: retryAuthorization.DecisionID,
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
		agentgo.WithModel(model),
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
	// Install the complete Adapter profile last so its checkpoint and execution
	// middleware remain the outer durable boundary around application behavior.
	options = append(options, adapter.Options()...)
	agent := agentgo.NewAgent(options...)

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
		managed, ok, err := managedAssistantEvent(input.ID, event.Message)
		if err != nil {
			emitErr = err
			return
		}
		if !ok {
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

	message := agentgo.UserMsg(recoveryInput.Text)
	message.Metadata = map[string]any{agentdInputID: recoveryInput.ID}
	err = agent.PromptMessages(ctx, message)
	if err != nil {
		var blocked *agentgoadapter.RecoveryBlockedError
		if errors.As(err, &blocked) {
			return finish(requiresActionFromAgentGo(runID, blocked))
		}
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

const agentdInputID = "agentd.input_id"

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
	messageInputID string,
	eventInputID string,
	emit func(ManagedEvent) error,
) error {
	index := inputIndex(messages, messageInputID)
	if index < 0 {
		return nil
	}
	for _, message := range messages[index+1:] {
		if message.GetRole() != agentgo.RoleAssistant {
			continue
		}
		managed, ok, err := managedAssistantEvent(eventInputID, message)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		if err := emit(managed); err != nil {
			return err
		}
	}
	return nil
}

func managedAssistantEvent(inputID string, message agentgo.AgentMessage) (ManagedEvent, bool, error) {
	text := message.TextContent()
	// AgentGo keeps tool-call assistant messages in its checkpoint so the loop can
	// recover, but the public agent.message event carries user-visible content.
	// Tool actions use their own Managed Agent event types instead of empty text.
	if text == "" {
		return nil, false, nil
	}
	encoded, err := json.Marshal(message)
	if err != nil {
		return nil, false, fmt.Errorf("encode assistant event identity: %w", err)
	}
	digest := sha256.Sum256(append([]byte(inputID+"\x00"), encoded...))
	event := NewManagedEvent("agent.message", map[string]any{
		"content": []map[string]any{{"type": "text", "text": text}},
	})
	event["id"] = fmt.Sprintf("event_%x", digest[:12])
	if timestamp := message.GetTimestamp(); !timestamp.IsZero() {
		event["processed_at"] = timestamp.UTC().Format(time.RFC3339Nano)
	}
	return event, true, nil
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
