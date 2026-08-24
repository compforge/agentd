package harness

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	agentledger "github.com/compforge/agent-ledger/go"
	"github.com/compforge/agentgo"
)

func TestAgentGoMessagesUseCheckpointStore(t *testing.T) {
	state := agentledger.NewMemoryEventStore()
	actor := agentledger.NewActor("agent", "agentgo")
	if err := state.CreateActor(context.Background(), actor); err != nil {
		t.Fatal(err)
	}
	recorder, err := agentledger.OpenRecorder(context.Background(), agentledger.RecorderOptions{
		Store: state, SessionID: "session-1", RunID: "run-1", Actor: actor,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := recorder.StartRun(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	runner := &AgentGoRunner{config: AgentGoRunnerConfig{
		OperationTimeout: time.Second,
		Ledger:           state,
		Checkpoints:      state,
	}}
	revision := int64(0)
	checkpointID := "agentgo/session-1"
	commit := runner.messageCommitter(
		"agentgo/session-1", actor.ID, "session-1", "run-1", nil, &checkpointID, &revision,
	)
	want := agentgo.UserMsg("hello")
	if err := commit(want); err != nil {
		t.Fatal(err)
	}
	firstCheckpointID := checkpointID

	messages, loadedRef, loadedRevision, err := runner.loadMessages(
		context.Background(), firstCheckpointID, "agentgo/session-1", revision,
	)
	if err != nil {
		t.Fatal(err)
	}
	if loadedRef != firstCheckpointID || loadedRevision != 1 ||
		len(messages) != 1 || messages[0].TextContent() != "hello" {
		t.Fatalf("messages=%#v ref=%q revision=%d", messages, loadedRef, loadedRevision)
	}
	checkpoint, exists, err := state.GetCheckpoint(context.Background(), firstCheckpointID)
	if err != nil || !exists {
		t.Fatalf("checkpoint exists=%v err=%v", exists, err)
	}
	if checkpoint.Anchor == nil || checkpoint.Anchor.LastAppliedSeq != 1 {
		t.Fatalf("checkpoint anchor = %#v", checkpoint.Anchor)
	}
	if err := commit(agentgo.Message{
		Role: agentgo.RoleAssistant, Content: []agentgo.ContentBlock{agentgo.TextBlock("done")},
	}); err != nil {
		t.Fatal(err)
	}
	latestCheckpointID := checkpointID
	staleControlMessages, staleControlRef, staleControlRevision, err := runner.loadMessages(
		context.Background(), firstCheckpointID, "agentgo/session-1", 1,
	)
	if err != nil {
		t.Fatal(err)
	}
	if staleControlRef != latestCheckpointID || staleControlRevision != 2 || len(staleControlMessages) != 2 {
		t.Fatalf(
			"stale control loaded messages=%d ref=%q revision=%d, want latest %q/2",
			len(staleControlMessages), staleControlRef, staleControlRevision, latestCheckpointID,
		)
	}
	initialControlMessages, initialControlRef, initialControlRevision, err := runner.loadMessages(
		context.Background(), "agentgo/session-1", "agentgo/session-1", 0,
	)
	if err != nil {
		t.Fatal(err)
	}
	if initialControlRef != latestCheckpointID || initialControlRevision != 2 || len(initialControlMessages) != 2 {
		t.Fatalf(
			"initial control loaded messages=%d ref=%q revision=%d, want latest %q/2",
			len(initialControlMessages), initialControlRef, initialControlRevision, latestCheckpointID,
		)
	}
	latestMessages, latestRef, latestRevision, err := runner.loadMessages(
		context.Background(), checkpointID, "agentgo/session-1", revision,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(latestMessages) != 2 || latestRef != latestCheckpointID || latestRevision != 2 {
		t.Fatalf("latest=%d ref=%q revision=%d", len(latestMessages), latestRef, latestRevision)
	}
}

func TestAgentGoRunAdoptsCheckpointAheadOfControlState(t *testing.T) {
	ctx := context.Background()
	state := agentledger.NewMemoryEventStore()
	actor := agentledger.NewActor("agent", "agentgo")
	if err := state.CreateActor(ctx, actor); err != nil {
		t.Fatal(err)
	}
	recorder, err := agentledger.OpenRecorder(ctx, agentledger.RecorderOptions{
		Store: state, SessionID: "session-1", RunID: "input/input-1", Actor: actor,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := recorder.StartRun(ctx, nil); err != nil {
		t.Fatal(err)
	}
	runner := &AgentGoRunner{config: AgentGoRunnerConfig{
		OperationTimeout: time.Second,
		Ledger:           state,
		Checkpoints:      state,
	}}
	revision := int64(0)
	checkpointID := "agentgo/session-1"
	commit := runner.messageCommitter(
		"agentgo/session-1", actor.ID, "session-1", "input/input-1", nil, &checkpointID, &revision,
	)
	input := agentgo.UserMsg("hello")
	input.Metadata = map[string]any{agentdInputID: "input-1"}
	if err := commit(input); err != nil {
		t.Fatal(err)
	}
	if err := commit(agentgo.Message{
		Role: agentgo.RoleAssistant, Content: []agentgo.ContentBlock{agentgo.TextBlock("done")},
	}); err != nil {
		t.Fatal(err)
	}
	latestCheckpointID := checkpointID

	var emitted []ManagedEvent
	result, err := runner.Run(ctx, Session{
		ID: "session-1", ResumeRef: "agentgo/session-1", ResumeRevision: 0,
	}, TurnInput{ID: "input-1", Text: "hello"}, func(event ManagedEvent) error {
		emitted = append(emitted, event)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.ResumeRef != latestCheckpointID || result.ResumeRevision != 2 {
		t.Fatalf("resume point = %q/%d, want %q/2", result.ResumeRef, result.ResumeRevision, latestCheckpointID)
	}
	if len(emitted) != 1 || emitted[0]["type"] != "agent.message" {
		t.Fatalf("emitted events = %#v", emitted)
	}
	if latest, exists, loadErr := state.LoadLatestCheckpoint(ctx, "agentgo/session-1"); loadErr != nil || !exists || latest.Revision != 2 {
		t.Fatalf("latest checkpoint exists=%v revision=%d err=%v", exists, latest.Revision, loadErr)
	}
}

func TestAgentGoResumeActionDoesNotDuplicateCommittedInput(t *testing.T) {
	input := agentgo.UserMsg("hello")
	input.Metadata = map[string]any{agentdInputID: "input-1"}
	runner := &AgentGoRunner{config: AgentGoRunnerConfig{Ledger: agentledger.NewMemoryEventStore()}}

	action, err := runner.resumeAction(context.Background(), "session-1", []agentgo.AgentMessage{input}, "input-1")
	if err != nil {
		t.Fatal(err)
	}
	if action != resumeContinue {
		t.Fatalf("action = %v, want resumeContinue", action)
	}

	final := agentgo.Message{
		Role: agentgo.RoleAssistant, Content: []agentgo.ContentBlock{agentgo.TextBlock("done")}, Timestamp: time.Now(),
	}
	action, err = runner.resumeAction(
		context.Background(), "session-1", []agentgo.AgentMessage{input, final}, "input-1",
	)
	if err != nil {
		t.Fatal(err)
	}
	if action != resumeCompleted {
		t.Fatalf("action = %v, want resumeCompleted", action)
	}
}

func TestAgentGoResumeBlocksUncertainToolBoundary(t *testing.T) {
	input := agentgo.UserMsg("change something")
	input.Metadata = map[string]any{agentdInputID: "input-1"}
	toolCall := agentgo.Message{
		Role: agentgo.RoleAssistant,
		Content: []agentgo.ContentBlock{agentgo.ToolCallBlock(agentgo.ToolCall{
			ID: "call-1", Name: "write", Args: json.RawMessage(`{}`),
		})},
		Timestamp: time.Now(),
	}
	ledger := agentledger.NewMemoryEventStore()
	recorder, err := agentledger.OpenRecorder(context.Background(), agentledger.RecorderOptions{
		Store: ledger, SessionID: "session-1", RunID: "input/input-1", Actor: agentledger.NewActor("agent", "agentgo"),
	})
	if err != nil {
		t.Fatal(err)
	}
	turn, err := recorder.StartTurn(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := recorder.BeforeToolCall(context.Background(), turn.ID, "call-1", map[string]any{
		"tool_name": "write", "input": map[string]any{},
	}); err != nil {
		t.Fatal(err)
	}
	runner := &AgentGoRunner{config: AgentGoRunnerConfig{Ledger: ledger}}

	_, err = runner.resumeAction(
		context.Background(), "session-1", []agentgo.AgentMessage{input, toolCall}, "input-1",
	)
	if errors.Is(err, ErrUnsafeRecovery) {
		t.Fatalf("required user action was classified as terminal unsafe recovery: %v", err)
	}
	var required *RequiresActionError
	if !errors.As(err, &required) || len(required.ToolUses) != 1 || required.ToolUses[0].ID == "" {
		t.Fatalf("required action = %#v", required)
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
	first, err := recorder.BeforeToolCallWithEffect(ctx, turn.ID, "call-1", map[string]any{
		"tool_name": "write", "input": map[string]any{"path": "README.md"},
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
		"tool_name": "write", "input": map[string]any{"path": "README.md"},
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
	if !errors.As(err, &required) || len(required.ToolUses) != 1 || required.ToolUses[0].ID != "event_"+retry.AttemptID {
		t.Fatalf("reused authorization error = %v, required = %#v", err, required)
	}
}

func TestAgentGoResumeRetriesReadToolBoundary(t *testing.T) {
	input := agentgo.UserMsg("inspect something")
	input.Metadata = map[string]any{agentdInputID: "input-1"}
	toolCall := agentgo.Message{
		Role: agentgo.RoleAssistant,
		Content: []agentgo.ContentBlock{agentgo.ToolCallBlock(agentgo.ToolCall{
			ID: "call-1", Name: "read", Args: json.RawMessage(`{}`),
		})},
		Timestamp: time.Now(),
	}
	ledger := agentledger.NewMemoryEventStore()
	recorder, err := agentledger.OpenRecorder(context.Background(), agentledger.RecorderOptions{
		Store: ledger, SessionID: "session-1", RunID: "input/input-1", Actor: agentledger.NewActor("agent", "agentgo"),
	})
	if err != nil {
		t.Fatal(err)
	}
	turn, err := recorder.StartTurn(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	readEffect := agentledger.Effect{Kind: agentledger.EffectKindRead, Idempotency: agentledger.IdempotencyNotApplicable}
	if _, err := recorder.BeforeToolCallWithEffect(context.Background(), turn.ID, "call-1", map[string]any{
		"tool_name": "read", "input": map[string]any{},
	}, readEffect); err != nil {
		t.Fatal(err)
	}
	runner := &AgentGoRunner{config: AgentGoRunnerConfig{Ledger: ledger}}

	action, err := runner.resumeAction(
		context.Background(), "session-1", []agentgo.AgentMessage{input, toolCall}, "input-1",
	)
	if err != nil {
		t.Fatal(err)
	}
	if action != resumeContinue {
		t.Fatalf("action = %v, want resumeContinue", action)
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
	currentOutput := agentgo.Message{
		Role: agentgo.RoleAssistant, Content: []agentgo.ContentBlock{agentgo.TextBlock("current output")},
	}
	var projected []ManagedEvent
	err := projectAssistantMessages(
		[]agentgo.AgentMessage{oldInput, oldOutput, currentInput, currentOutput},
		"input-2",
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
}
