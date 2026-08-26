package harness

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	agentledger "github.com/compforge/agent-ledger/go"
	agentgoadapter "github.com/compforge/agent-ledger/go/adapters/agentgo"
	"github.com/compforge/agentgo"
)

func TestAgentGoLoadsNativeSnapshotAheadOfControlState(t *testing.T) {
	ctx := context.Background()
	store := agentledger.NewMemoryEventStore()
	actor := agentledger.NewActor("agent", "agentgo")
	if err := store.CreateActor(ctx, actor); err != nil {
		t.Fatal(err)
	}
	runner := &AgentGoRunner{config: AgentGoRunnerConfig{Checkpoints: store}}
	key := "agentgo/session-1"
	first := saveAgentGoCheckpoint(t, ctx, store, actor.ID, key, 0, "scope-1", []agentgo.AgentMessage{
		agentgo.UserMsg("first"),
	})
	second := saveAgentGoCheckpoint(t, ctx, store, actor.ID, key, first.Revision, "scope-2", []agentgo.AgentMessage{
		agentgo.UserMsg("first"),
		agentgo.Message{Role: agentgo.RoleAssistant, Content: []agentgo.ContentBlock{agentgo.TextBlock("done")}},
	})

	native, checkpoint, exists, err := runner.loadAgentGoCheckpoint(ctx, first.ID, key, first.Revision)
	if err != nil {
		t.Fatal(err)
	}
	if !exists || checkpoint.ID != second.ID || checkpoint.Revision != 2 {
		t.Fatalf("checkpoint = %#v exists=%t", checkpoint, exists)
	}
	if len(native.Snapshot.State.Messages) != 2 || native.Snapshot.State.Messages[1].TextContent() != "done" {
		t.Fatalf("snapshot messages = %#v", native.Snapshot.State.Messages)
	}
	if native.ExecutionScope != "scope-2" || !native.ScopeComplete {
		t.Fatalf("native checkpoint = %#v", native)
	}
}

func TestAgentGoToolAuthorizationIsScopedToOneAttempt(t *testing.T) {
	ctx := context.Background()
	ledger := agentledger.NewMemoryEventStore()
	recorder, err := agentledger.OpenRecorder(ctx, agentledger.RecorderOptions{
		Store: ledger, SessionID: "session-1", RunID: "input/original", Actor: agentledger.NewActor("agent", "agentgo"),
	})
	if err != nil {
		t.Fatal(err)
	}
	turn, err := recorder.StartTurn(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	first, err := recorder.BeforeToolCallWithEffect(ctx, turn.ID, "scope-1/call-1", map[string]any{
		"tool_call_id": "call-1", "tool_name": "write", "input": map[string]any{"path": "README.md"},
	}, agentledger.Effect{Kind: agentledger.EffectKindWrite, Idempotency: agentledger.IdempotencyNone})
	if err != nil {
		t.Fatal(err)
	}
	runner := &AgentGoRunner{config: AgentGoRunnerConfig{Ledger: ledger}}
	input := TurnInput{ID: "confirmation-1", ToolResolution: &ToolResolution{
		ToolUseID: "event_" + first.AttemptID, Decision: "allow",
	}}
	plan, err := runner.planToolResolution(ctx, "session-1", input)
	if err != nil {
		t.Fatal(err)
	}
	if plan.RetryAuthorization.AttemptID != first.AttemptID || plan.RetryAuthorization.DecisionID != input.ID {
		t.Fatalf("retry authorization = %#v", plan.RetryAuthorization)
	}

	retry, err := recorder.Retry(ctx, first.ActionID, 2, map[string]any{
		"tool_call_id": "call-1", "tool_name": "write", "input": map[string]any{"path": "README.md"},
		"recovery_decision_id": input.ID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := recorder.MarkAttemptOutcomeUnknown(
		ctx, first, "superseded by an authorized retry", retry.AttemptID,
	); err != nil {
		t.Fatal(err)
	}
	_, err = runner.planToolResolution(ctx, "session-1", input)
	var required *RequiresActionError
	if !errors.As(err, &required) || len(required.ToolUses) != 1 ||
		required.ToolUses[0].ID != "event_"+retry.AttemptID || required.ToolUses[0].InputID != "original" {
		t.Fatalf("reused authorization error = %v, required = %#v", err, required)
	}
}

func TestAgentGoUserToolResultUsesReplayablePayload(t *testing.T) {
	ctx := context.Background()
	ledger := agentledger.NewMemoryEventStore()
	recorder, err := agentledger.OpenRecorder(ctx, agentledger.RecorderOptions{
		Store: ledger, SessionID: "session-1", RunID: "input/original", Actor: agentledger.NewActor("agent", "agentgo"),
	})
	if err != nil {
		t.Fatal(err)
	}
	turn, err := recorder.StartTurn(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	attempt, err := recorder.BeforeToolCallWithEffect(ctx, turn.ID, "scope-1/call-1", map[string]any{
		"tool_call_id": "call-1", "tool_name": "write", "input": map[string]any{"path": "README.md"},
	}, agentledger.Effect{Kind: agentledger.EffectKindWrite, Idempotency: agentledger.IdempotencyNone})
	if err != nil {
		t.Fatal(err)
	}
	runner := &AgentGoRunner{config: AgentGoRunnerConfig{Ledger: ledger}}
	input := TurnInput{ID: "result-1", ToolResolution: &ToolResolution{
		ToolUseID: "event_" + attempt.AttemptID, Decision: "result", Content: map[string]any{"ok": true},
	}}
	plan, err := runner.planToolResolution(ctx, "session-1", input)
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.recordToolResolution(ctx, recorder, plan); err != nil {
		t.Fatal(err)
	}

	records, err := toolAttemptRecords(ctx, ledger, "session-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].TerminalType != agentledger.EventTypeAttemptCompleted {
		t.Fatalf("tool records = %#v", records)
	}
	output, ok := records[0].TerminalPayload["output"].(map[string]any)
	if !ok || output["tool_call_id"] != "call-1" || records[0].TerminalPayload["external_operation_id"] != input.ID {
		t.Fatalf("terminal payload = %#v", records[0].TerminalPayload)
	}
}

func TestAgentGoRecoveryBlockCarriesDurableInput(t *testing.T) {
	blocked := &agentgoadapter.RecoveryBlockedError{Tools: []agentgoadapter.PendingToolRecovery{{
		Attempt: agentledger.Attempt{ID: "attempt-1"},
		Call: agentgo.ToolCall{
			ID: "call-1", Name: "write", Args: json.RawMessage(`{"path":"README.md"}`),
		},
	}}}
	required := requiresActionFromAgentGo("input/input-1", blocked)
	if len(required.ToolUses) != 1 || required.ToolUses[0].ID != "event_attempt-1" ||
		required.ToolUses[0].InputID != "input-1" || required.ToolUses[0].Input["path"] != "README.md" {
		t.Fatalf("required action = %#v", required)
	}
}

func TestAgentGoToolEffectsAreConservative(t *testing.T) {
	tests := []struct {
		name      string
		want      agentledger.Effect
		retryable bool
	}{
		{name: "read", want: agentledger.Effect{Kind: agentledger.EffectKindRead, Idempotency: agentledger.IdempotencyNotApplicable}, retryable: true},
		{name: "write", want: agentledger.Effect{Kind: agentledger.EffectKindWrite, Idempotency: agentledger.IdempotencyNone}},
		{name: "bash", want: agentledger.UnknownEffect()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			effect := agentGoToolEffect(agentgo.ToolCall{Name: test.name})
			if effect != test.want || canRetryToolEffect(effect) != test.retryable {
				t.Fatalf("effect=%#v retryable=%t", effect, canRetryToolEffect(effect))
			}
		})
	}
}

func TestAgentGoFinishRunUsesLatestLaneSequence(t *testing.T) {
	ctx := context.Background()
	ledger := agentledger.NewMemoryEventStore()
	actor := agentledger.NewActor("agent", "agentgo")
	first, err := agentledger.OpenRecorder(ctx, agentledger.RecorderOptions{
		Store: ledger, SessionID: "session-1", RunID: "input/input-1", Actor: actor,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.StartRun(ctx, nil); err != nil {
		t.Fatal(err)
	}
	adapterRecorder, err := agentledger.OpenRecorder(ctx, agentledger.RecorderOptions{
		Store: ledger, SessionID: "session-1", RunID: "input/input-1", Actor: actor,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := adapterRecorder.StartTurn(ctx, nil); err != nil {
		t.Fatal(err)
	}

	runner := &AgentGoRunner{config: AgentGoRunnerConfig{Ledger: ledger}}
	if err := runner.finishRun(ctx, "session-1", "input/input-1", actor, nil); err != nil {
		t.Fatalf("finish run after adapter append: %v", err)
	}
	lane, exists, err := ledger.FindLane(ctx, "session-1", "input/input-1", "main")
	if err != nil || !exists {
		t.Fatalf("find run lane: exists=%t err=%v", exists, err)
	}
	var eventTypes []string
	for event, err := range ledger.LoadLane(ctx, lane.ID, 0) {
		if err != nil {
			t.Fatal(err)
		}
		eventTypes = append(eventTypes, event.EventType)
	}
	if len(eventTypes) != 3 || eventTypes[2] != agentledger.EventTypeRunCompleted {
		t.Fatalf("run events = %v, want run.completed after adapter append", eventTypes)
	}
}

func TestProjectAssistantMessagesScopesCurrentInput(t *testing.T) {
	oldInput := agentgo.UserMsg("old")
	oldInput.Metadata = map[string]any{agentdInputID: "input-1"}
	oldOutput := agentgo.Message{
		Role: agentgo.RoleAssistant, Content: []agentgo.ContentBlock{agentgo.TextBlock("old output")},
	}
	currentInput := agentgo.UserMsg("current")
	currentInput.Metadata = map[string]any{agentdInputID: "input-2"}
	currentToolCall := agentgo.Message{
		Role: agentgo.RoleAssistant,
		Content: []agentgo.ContentBlock{agentgo.ToolCallBlock(agentgo.ToolCall{
			ID: "call-2", Name: "bash", Args: json.RawMessage(`{"command":"true"}`),
		})},
	}
	currentOutput := agentgo.Message{
		Role: agentgo.RoleAssistant, Content: []agentgo.ContentBlock{agentgo.TextBlock("current output")},
	}
	var projected []ManagedEvent
	err := projectAssistantMessages(
		[]agentgo.AgentMessage{oldInput, oldOutput, currentInput, currentToolCall, currentOutput},
		"input-2",
		"confirmation-1",
		func(event ManagedEvent) error {
			projected = append(projected, event)
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(projected) != 1 || projected[0]["content"].([]map[string]any)[0]["text"] != "current output" {
		t.Fatalf("projected events = %#v", projected)
	}
	want, _, err := managedAssistantEvent("confirmation-1", currentOutput)
	if err != nil || projected[0]["id"] != want["id"] {
		t.Fatalf("projected event identity = %#v, want %#v", projected[0]["id"], want["id"])
	}
}

func TestManagedAssistantEventProjectsOnlyUserVisibleText(t *testing.T) {
	toolCall := agentgo.Message{
		Role: agentgo.RoleAssistant,
		Content: []agentgo.ContentBlock{agentgo.ToolCallBlock(agentgo.ToolCall{
			ID: "call-1", Name: "bash", Args: json.RawMessage(`{"command":"true"}`),
		})},
	}
	if event, ok, err := managedAssistantEvent("input-1", toolCall); err != nil || ok || event != nil {
		t.Fatalf("tool-call projection = event:%#v ok:%t err:%v, want skipped", event, ok, err)
	}

	mixed := toolCall
	mixed.Content = append([]agentgo.ContentBlock{agentgo.TextBlock("working")}, mixed.Content...)
	event, ok, err := managedAssistantEvent("input-1", mixed)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || event["content"].([]map[string]any)[0]["text"] != "working" {
		t.Fatalf("mixed projection = event:%#v ok:%t", event, ok)
	}
}

func saveAgentGoCheckpoint(
	t *testing.T,
	ctx context.Context,
	store agentledger.CheckpointStore,
	actorID string,
	key string,
	expectedRevision int64,
	scope string,
	messages []agentgo.AgentMessage,
) agentledger.Checkpoint {
	t.Helper()
	codec, err := agentgo.NewCodec()
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := codec.Marshal(agentGoCheckpoint{
		Snapshot:       agentgo.AgentSnapshot{State: agentgo.AgentState{Messages: messages}},
		ExecutionScope: scope,
		ScopeComplete:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	var state map[string]any
	if err := json.Unmarshal(encoded, &state); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := store.SaveCheckpoint(ctx, expectedRevision, agentledger.NewCheckpoint(
		key, actorID, agentgoadapter.CheckpointFormat, state,
	))
	if err != nil {
		t.Fatal(err)
	}
	return checkpoint
}
