package sandbox

import (
	"context"
	"encoding/json"
	"io/fs"
	"testing"
	"time"
)

func TestAgentGoToolsetUsesStableSandboxIdentity(t *testing.T) {
	t.Parallel()
	engine := &recordingEngine{}
	toolset := NewAgentGoToolset(engine, "session_123", time.Second)
	var bashIndex = -1
	for index, tool := range toolset {
		if tool.Name() == "bash" {
			bashIndex = index
			break
		}
	}
	if bashIndex < 0 {
		t.Fatal("bash tool is missing")
	}
	result, err := toolset[bashIndex].Execute(context.Background(), json.RawMessage(`{"command":"pwd","timeout_ms":42}`))
	if err != nil {
		t.Fatal(err)
	}
	if engine.sandboxID != "session_123" {
		t.Fatalf("sandbox id = %q, want session_123", engine.sandboxID)
	}
	if engine.command.Command != "pwd" || engine.command.Cwd != "/workspace" || engine.command.Timeout != 42*time.Millisecond {
		t.Fatalf("unexpected command: %#v", engine.command)
	}
	var decoded CommandResult
	if err := json.Unmarshal(result, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Output != "ok" {
		t.Fatalf("output = %q, want ok", decoded.Output)
	}
}

type recordingEngine struct {
	sandboxID string
	command   Command
}

func (*recordingEngine) Name() string                         { return "recording" }
func (*recordingEngine) Start(context.Context) error          { return nil }
func (*recordingEngine) Ensure(context.Context, string) error { return nil }

func (*recordingEngine) Stat(context.Context, string, string) (FileInfo, error) {
	return FileInfo{}, fs.ErrNotExist
}

func (*recordingEngine) ReadFile(context.Context, string, string) ([]byte, error) {
	return nil, fs.ErrNotExist
}

func (*recordingEngine) ReadDir(context.Context, string, string) ([]DirEntry, error) {
	return nil, fs.ErrNotExist
}

func (*recordingEngine) WriteFile(context.Context, string, string, []byte, fs.FileMode) error {
	return nil
}

func (*recordingEngine) MkdirAll(context.Context, string, string, fs.FileMode) error {
	return nil
}

func (e *recordingEngine) Execute(_ context.Context, sandboxID string, command Command) (CommandResult, error) {
	e.sandboxID = sandboxID
	e.command = command
	return CommandResult{Output: "ok"}, nil
}
