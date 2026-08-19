package harness

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	agentledger "github.com/compforge/agent-ledger/go"
	"github.com/compforge/agentd/server/internal/app"
	harnessstate "github.com/compforge/agentd/server/internal/harness/state"
	"github.com/compforge/agentgo"
)

func TestAgentGoMessagesUseHarnessState(t *testing.T) {
	state := &memoryHarnessState{}
	runner := &AgentGoRunner{config: AgentGoRunnerConfig{
		OperationTimeout: time.Second,
		State:            state,
	}}
	revision := int64(-1)
	commit := runner.messageCommitter("agentgo/session-1", &revision)
	want := agentgo.UserMsg("hello")
	if err := commit(want); err != nil {
		t.Fatal(err)
	}

	messages, loadedRevision, err := runner.loadMessages(context.Background(), "agentgo/session-1")
	if err != nil {
		t.Fatal(err)
	}
	if loadedRevision != 0 || len(messages) != 1 || messages[0].TextContent() != "hello" {
		t.Fatalf("messages=%#v revision=%d", messages, loadedRevision)
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
	recorder := agentledger.NewSessionRecorder(agentledger.RecorderOptions{
		Store: ledger, SessionID: "session-1", RunID: "run-1", Actor: agentledger.Actor{Type: "agent", ID: "test"},
	})
	if _, err := recorder.BeforeToolCall(context.Background(), "step-1", map[string]any{"tool_call_id": "call-1"}); err != nil {
		t.Fatal(err)
	}
	runner := &AgentGoRunner{config: AgentGoRunnerConfig{Ledger: ledger}}

	_, err := runner.resumeAction(
		context.Background(), "session-1", []agentgo.AgentMessage{input, toolCall}, "input-1",
	)
	if !errors.Is(err, app.ErrUnsafeRecovery) {
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
	var projected []app.ManagedEvent
	err := projectAssistantMessages(
		"agentgo/session-1",
		[]agentgo.AgentMessage{oldInput, oldOutput, currentInput, currentOutput},
		"input-2",
		func(event app.ManagedEvent) error {
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

type memoryHarnessState struct {
	records []harnessstate.Record
}

func (s *memoryHarnessState) Append(
	_ context.Context,
	_ string,
	expectedRevision int64,
	format string,
	data json.RawMessage,
) (harnessstate.Record, error) {
	record := harnessstate.Record{
		Revision: int64(len(s.records)), Format: format, Data: data, CommittedAt: time.Now().UTC(),
	}
	if expectedRevision != record.Revision-1 {
		return harnessstate.Record{}, harnessstate.ErrConflict
	}
	s.records = append(s.records, record)
	return record, nil
}

func (s *memoryHarnessState) Load(context.Context, string) ([]harnessstate.Record, error) {
	return append([]harnessstate.Record(nil), s.records...), nil
}
