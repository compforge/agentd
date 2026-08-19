package harness

import (
	"context"
	"encoding/json"
	"testing"
	"time"

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
