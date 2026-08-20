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

	messages, loadedRevision, err := runner.loadMessages(
		context.Background(), firstCheckpointID, "agentgo/session-1", revision,
	)
	if err != nil {
		t.Fatal(err)
	}
	if loadedRevision != 1 || len(messages) != 1 || messages[0].TextContent() != "hello" {
		t.Fatalf("messages=%#v revision=%d", messages, loadedRevision)
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
	firstMessages, _, err := runner.loadMessages(
		context.Background(), firstCheckpointID, "agentgo/session-1", 1,
	)
	if err != nil {
		t.Fatal(err)
	}
	latestMessages, latestRevision, err := runner.loadMessages(
		context.Background(), checkpointID, "agentgo/session-1", revision,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(firstMessages) != 1 || len(latestMessages) != 2 || latestRevision != 2 {
		t.Fatalf("first=%d latest=%d revision=%d", len(firstMessages), len(latestMessages), latestRevision)
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
		Store: ledger, SessionID: "session-1", RunID: "run-1", Actor: agentledger.NewActor("agent", "agentgo"),
	})
	if err != nil {
		t.Fatal(err)
	}
	turn, err := recorder.StartTurn(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := recorder.BeforeToolCall(context.Background(), turn.ID, map[string]any{"tool_call_id": "call-1"}); err != nil {
		t.Fatal(err)
	}
	runner := &AgentGoRunner{config: AgentGoRunnerConfig{Ledger: ledger}}

	_, err = runner.resumeAction(
		context.Background(), "session-1", []agentgo.AgentMessage{input, toolCall}, "input-1",
	)
	if !errors.Is(err, ErrUnsafeRecovery) {
		t.Fatalf("error = %v, want ErrUnsafeRecovery", err)
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
