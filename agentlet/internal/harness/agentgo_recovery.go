package harness

import (
	"context"
	"encoding/json"
	"fmt"

	agentledger "github.com/compforge/agent-ledger/go"
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
	return BlockingToolUse{ID: "event_" + r.Attempt.ID, Name: r.ToolName, Input: input}
}

type retryAuthorization struct {
	ActionID   string
	AttemptID  string
	DecisionID string
}

type toolResolutionPlan struct {
	Attempt            toolAttemptRecord
	RetryAuthorization retryAuthorization
	ToolResult         *agentgo.Message
	FailurePayload     map[string]any
	CompletionPayload  map[string]any
}

func unresolvedToolAttempts(
	ctx context.Context,
	store agentledger.EventStore,
	sessionID string,
	runID string,
) ([]toolAttemptRecord, error) {
	records, err := toolAttemptRecords(ctx, store, sessionID)
	if err != nil {
		return nil, err
	}
	latest := make(map[string]toolAttemptRecord)
	for _, record := range records {
		if record.RunID != runID || record.Terminal {
			continue
		}
		if current, ok := latest[record.Action.ID]; !ok || record.Attempt.AttemptNo > current.Attempt.AttemptNo {
			latest[record.Action.ID] = record
		}
	}
	result := make([]toolAttemptRecord, 0, len(latest))
	for _, record := range latest {
		result = append(result, record)
	}
	return result, nil
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
			switch arguments := event.Payload["arguments"].(type) {
			case string:
				record.Arguments = json.RawMessage(arguments)
			case map[string]any:
				record.Arguments, _ = json.Marshal(arguments)
			}
			record.RecoveryDecisionID, _ = event.Payload["recovery_decision_id"].(string)
		case agentledger.EventTypeAttemptCompleted, agentledger.EventTypeAttemptFailed:
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
			plan := toolResolutionPlan{Attempt: latest}
			plan.ToolResult = toolResultFromTerminal(latest)
			return plan, nil
		}
		return toolResolutionPlan{}, fmt.Errorf("%w: tool use %q was superseded by attempt %s", ErrUnsafeRecovery, resolution.ToolUseID, latest.Attempt.ID)
	}
	plan := toolResolutionPlan{Attempt: target}
	switch resolution.Decision {
	case "allow":
		if target.Terminal {
			return toolResolutionPlan{}, fmt.Errorf("%w: tool use %q is already resolved", ErrUnsafeRecovery, resolution.ToolUseID)
		}
		plan.RetryAuthorization = retryAuthorization{
			ActionID: target.Action.ID, AttemptID: target.Attempt.ID, DecisionID: input.ID,
		}
	case "deny":
		message := resolution.DenyMessage
		if message == "" {
			message = "tool execution denied by user"
		}
		encoded, _ := json.Marshal(map[string]any{"error": message})
		toolResult := agentgo.ToolResultMsg(target.ToolCallID, encoded, true)
		plan.ToolResult = &toolResult
		plan.FailurePayload = map[string]any{
			"error":               map[string]any{"type": "user_denied", "message": message},
			"resolution_event_id": input.ID,
		}
	case "result":
		encoded, err := json.Marshal(resolution.Content)
		if err != nil {
			return toolResolutionPlan{}, fmt.Errorf("encode supplied tool result: %w", err)
		}
		toolResult := agentgo.ToolResultMsg(target.ToolCallID, encoded, resolution.IsError)
		plan.ToolResult = &toolResult
		if resolution.IsError {
			plan.FailurePayload = map[string]any{
				"error":               map[string]any{"type": "user_supplied_error", "message": string(encoded)},
				"resolution_event_id": input.ID,
			}
		} else {
			plan.CompletionPayload = map[string]any{
				"result": resolution.Content, "source": "user.tool_result", "resolution_event_id": input.ID,
			}
		}
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
	eventType := agentledger.EventTypeAttemptCompleted
	payload := plan.CompletionPayload
	if plan.FailurePayload != nil {
		eventType = agentledger.EventTypeAttemptFailed
		payload = plan.FailurePayload
	}
	if _, err := recorder.Record(
		ctx, eventType, plan.Attempt.Attempt.ID, payload, plan.Attempt.RequestedEventID,
	); err != nil {
		return fmt.Errorf("record user tool resolution: %w", err)
	}
	return nil
}

func hasToolResult(messages []agentgo.AgentMessage, toolCallID string) bool {
	for _, item := range messages {
		if item.GetRole() != agentgo.RoleTool {
			continue
		}
		var metadata map[string]any
		switch message := item.(type) {
		case agentgo.Message:
			metadata = message.Metadata
		case *agentgo.Message:
			metadata = message.Metadata
		}
		if storedID, _ := metadata["tool_call_id"].(string); storedID == toolCallID {
			return true
		}
	}
	return false
}

func toolResultFromTerminal(record toolAttemptRecord) *agentgo.Message {
	isError := record.TerminalType == agentledger.EventTypeAttemptFailed
	content := record.TerminalPayload["result"]
	if isError {
		content = record.TerminalPayload["error"]
	}
	encoded, err := json.Marshal(content)
	if err != nil {
		encoded = json.RawMessage(`{"error":"unable to encode recorded tool outcome"}`)
	}
	message := agentgo.ToolResultMsg(record.ToolCallID, encoded, isError)
	return &message
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
