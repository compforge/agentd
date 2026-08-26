package harness

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	agentledger "github.com/compforge/agent-ledger/go"
	agentgoadapter "github.com/compforge/agent-ledger/go/adapters/agentgo"
	"github.com/compforge/agentgo"
)

type toolAttemptRecord struct {
	RunID              string
	Action             agentledger.Action
	Attempt            agentledger.Attempt
	RequestedEventID   string
	ToolCallID         string
	ToolName           string
	Arguments          json.RawMessage
	RecoveryDecisionID string
	Terminal           bool
	TerminalType       string
	TerminalPayload    map[string]any
}

func (r toolAttemptRecord) blockingToolUse() BlockingToolUse {
	input := make(map[string]any)
	if len(r.Arguments) > 0 {
		_ = json.Unmarshal(r.Arguments, &input)
	}
	return BlockingToolUse{
		ID: "event_" + r.Attempt.ID, Name: r.ToolName, Input: input,
		InputID: strings.TrimPrefix(r.RunID, "input/"),
	}
}

type retryAuthorization struct {
	ActionID   string
	AttemptID  string
	DecisionID string
}

type toolResolutionPlan struct {
	Attempt            toolAttemptRecord
	RetryAuthorization retryAuthorization
	TerminalResult     *agentgo.ToolResult
	TerminalError      map[string]any
	DecisionID         string
}

func (p toolResolutionPlan) hasTerminalOutcome() bool {
	return p.TerminalResult != nil
}

func toolAttemptRecords(
	ctx context.Context,
	store agentledger.EventStore,
	sessionID string,
) ([]toolAttemptRecord, error) {
	view, err := store.LoadSession(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	laneRuns := make(map[string]string, len(view.Lanes))
	for _, lane := range view.Lanes {
		laneRuns[lane.ID] = lane.RunID
	}
	turnRuns := make(map[string]string, len(view.Turns))
	for _, turn := range view.Turns {
		turnRuns[turn.ID] = laneRuns[turn.LaneID]
	}
	actions := make(map[string]agentledger.Action, len(view.Actions))
	for _, action := range view.Actions {
		actions[action.ID] = action
	}
	records := make(map[string]toolAttemptRecord, len(view.Attempts))
	for _, attempt := range view.Attempts {
		action := actions[attempt.ActionID]
		if action.Type == agentledger.ActionTypeToolCall {
			records[attempt.ID] = toolAttemptRecord{
				RunID: turnRuns[action.TurnID], Action: action, Attempt: attempt,
			}
		}
	}
	for _, event := range view.Events {
		record, isTool := records[event.SubjectID]
		if !isTool {
			continue
		}
		switch event.EventType {
		case agentledger.EventTypeAttemptRequested:
			record.RequestedEventID = event.ID
			record.ToolCallID, _ = event.Payload["tool_call_id"].(string)
			record.ToolName, _ = event.Payload["tool_name"].(string)
			switch arguments := event.Payload["input"].(type) {
			case string:
				record.Arguments = json.RawMessage(arguments)
			case map[string]any:
				record.Arguments, _ = json.Marshal(arguments)
			}
			record.RecoveryDecisionID, _ = event.Payload["recovery_decision_id"].(string)
		case agentledger.EventTypeAttemptCompleted, agentledger.EventTypeAttemptFailed,
			agentledger.EventTypeAttemptCancelled, agentledger.EventTypeAttemptOutcomeUnknown:
			record.Terminal = true
			record.TerminalType = event.EventType
			record.TerminalPayload = event.Payload
		}
		records[event.SubjectID] = record
	}
	result := make([]toolAttemptRecord, 0, len(records))
	for _, record := range records {
		if record.RequestedEventID != "" {
			result = append(result, record)
		}
	}
	return result, nil
}

func (r *AgentGoRunner) planToolResolution(
	ctx context.Context,
	sessionID string,
	input TurnInput,
) (toolResolutionPlan, error) {
	resolution := input.ToolResolution
	if resolution == nil || resolution.ToolUseID == "" {
		return toolResolutionPlan{}, fmt.Errorf("resolve AgentGo tool: tool_use_id is required")
	}
	records, err := toolAttemptRecords(ctx, r.config.Ledger, sessionID)
	if err != nil {
		return toolResolutionPlan{}, fmt.Errorf("resolve AgentGo tool attempt: %w", err)
	}
	var target toolAttemptRecord
	for _, record := range records {
		if "event_"+record.Attempt.ID == resolution.ToolUseID {
			target = record
			break
		}
	}
	if target.Attempt.ID == "" {
		return toolResolutionPlan{}, fmt.Errorf("%w: tool use %q is not present in the Ledger", ErrUnsafeRecovery, resolution.ToolUseID)
	}
	latest := target
	for _, record := range records {
		if record.Action.ID == target.Action.ID && record.Attempt.AttemptNo > latest.Attempt.AttemptNo {
			latest = record
		}
	}
	if latest.Attempt.ID != target.Attempt.ID {
		// A retry carrying this exact decision already crossed the execution
		// boundary. Never spend the same user authorization a second time.
		if latest.RecoveryDecisionID == input.ID && !latest.Terminal {
			return toolResolutionPlan{}, &RequiresActionError{ToolUses: []BlockingToolUse{latest.blockingToolUse()}}
		}
		if latest.RecoveryDecisionID == input.ID && latest.Terminal {
			return toolResolutionPlan{Attempt: latest}, nil
		}
		return toolResolutionPlan{}, fmt.Errorf("%w: tool use %q was superseded by attempt %s", ErrUnsafeRecovery, resolution.ToolUseID, latest.Attempt.ID)
	}
	plan := toolResolutionPlan{Attempt: target}
	if target.Terminal {
		if operationID, _ := target.TerminalPayload["external_operation_id"].(string); operationID == input.ID {
			return plan, nil
		}
		return toolResolutionPlan{}, fmt.Errorf(
			"%w: tool use %q is already resolved", ErrUnsafeRecovery, resolution.ToolUseID,
		)
	}
	switch resolution.Decision {
	case "allow":
		plan.RetryAuthorization = retryAuthorization{
			ActionID: target.Action.ID, AttemptID: target.Attempt.ID, DecisionID: input.ID,
		}
	case "deny":
		message := resolution.DenyMessage
		if message == "" {
			message = "tool execution denied by user"
		}
		encoded, _ := json.Marshal(map[string]any{"error": message})
		plan.TerminalResult = &agentgo.ToolResult{
			ToolCallID: target.ToolCallID, ToolName: target.ToolName, Content: encoded, IsError: true,
		}
		plan.TerminalError = map[string]any{"type": "user_denied", "message": message}
		plan.DecisionID = input.ID
	case "result":
		encoded, err := json.Marshal(resolution.Content)
		if err != nil {
			return toolResolutionPlan{}, fmt.Errorf("encode supplied tool result: %w", err)
		}
		plan.TerminalResult = &agentgo.ToolResult{
			ToolCallID: target.ToolCallID, ToolName: target.ToolName,
			Content: encoded, IsError: resolution.IsError,
		}
		if resolution.IsError {
			plan.TerminalError = map[string]any{
				"type": "user_supplied_error", "message": string(encoded),
			}
		}
		plan.DecisionID = input.ID
	default:
		return toolResolutionPlan{}, fmt.Errorf("resolve AgentGo tool: unsupported decision %q", resolution.Decision)
	}
	return plan, nil
}

func (r *AgentGoRunner) recordToolResolution(
	ctx context.Context,
	recorder *agentledger.LaneRecorder,
	plan toolResolutionPlan,
) error {
	if plan.Attempt.Terminal {
		return nil
	}
	if plan.TerminalResult == nil {
		return nil
	}
	eventType := agentledger.EventTypeAttemptCompleted
	payload := map[string]any{
		"output": map[string]any{
			"tool_call_id": plan.TerminalResult.ToolCallID,
			"tool_name":    plan.TerminalResult.ToolName,
			"content":      plan.TerminalResult.Content,
			"is_error":     plan.TerminalResult.IsError,
			"details":      plan.TerminalResult.Details,
		},
		"external_operation_id": plan.DecisionID,
	}
	if plan.TerminalError != nil {
		eventType = agentledger.EventTypeAttemptFailed
		payload["error"] = plan.TerminalError
	}
	if _, err := recorder.Record(
		ctx, eventType, plan.Attempt.Attempt.ID, payload, plan.Attempt.RequestedEventID,
	); err != nil {
		return fmt.Errorf("record user tool resolution: %w", err)
	}
	return nil
}

func requiresActionFromAgentGo(runID string, blocked *agentgoadapter.RecoveryBlockedError) *RequiresActionError {
	inputID := strings.TrimPrefix(runID, "input/")
	uses := make([]BlockingToolUse, 0, len(blocked.Tools))
	for _, pending := range blocked.Tools {
		input := make(map[string]any)
		_ = json.Unmarshal(pending.Call.Args, &input)
		uses = append(uses, BlockingToolUse{
			ID: "event_" + pending.Attempt.ID, Name: pending.Call.Name,
			Input: input, InputID: inputID,
		})
	}
	return &RequiresActionError{ToolUses: uses}
}

func agentGoToolEffect(call agentgo.ToolCall) agentledger.Effect {
	switch call.Name {
	case "read", "glob", "grep":
		return agentledger.Effect{Kind: agentledger.EffectKindRead, Idempotency: agentledger.IdempotencyNotApplicable}
	case "write", "edit":
		return agentledger.Effect{Kind: agentledger.EffectKindWrite, Idempotency: agentledger.IdempotencyNone}
	default:
		return agentledger.UnknownEffect()
	}
}

func canRetryToolEffect(effect agentledger.Effect) bool {
	effect = agentledger.NormalizeEffect(effect)
	if effect.Kind == agentledger.EffectKindNone || effect.Kind == agentledger.EffectKindRead {
		return true
	}
	return effect.Kind == agentledger.EffectKindWrite &&
		(effect.Idempotency == agentledger.IdempotencyInherent || effect.Idempotency == agentledger.IdempotencyKeyed)
}
