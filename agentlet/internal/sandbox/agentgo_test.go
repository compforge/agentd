package sandbox

import (
	"context"
	"encoding/json"
	"io/fs"
	"testing"
	"time"

	"github.com/compforge/agentd/agentlet/internal/sandbox/engine"
)

func TestAgentGoToolsetUsesStableSandboxIdentity(t *testing.T) {
	t.Parallel()
	sandboxEngine := &recordingEngine{}
	toolset := NewAgentGoToolset(sandboxEngine, engine.SandboxKey{Value: "session_123"}, time.Second)
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
	if sandboxEngine.sandboxKey.Value != "session_123" {
		t.Fatalf("sandbox key = %#v, want session_123", sandboxEngine.sandboxKey)
	}
	if sandboxEngine.command.Command != "pwd" || sandboxEngine.command.Cwd != "/workspace" || sandboxEngine.command.Timeout != 42*time.Millisecond {
		t.Fatalf("unexpected command: %#v", sandboxEngine.command)
	}
	var decoded engine.CommandResult
	if err := json.Unmarshal(result, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Output != "ok" {
		t.Fatalf("output = %q, want ok", decoded.Output)
	}
}

type recordingEngine struct {
	sandboxKey engine.SandboxKey
	command    engine.Command
}

func (*recordingEngine) Name() string                                    { return "recording" }
func (*recordingEngine) Start(context.Context) error                     { return nil }
func (*recordingEngine) Ensure(context.Context, engine.SandboxKey) error { return nil }

func (*recordingEngine) Stat(context.Context, engine.SandboxKey, string) (engine.FileInfo, error) {
	return engine.FileInfo{}, fs.ErrNotExist
}

func (*recordingEngine) ReadFile(context.Context, engine.SandboxKey, string) ([]byte, error) {
	return nil, fs.ErrNotExist
}

func (*recordingEngine) ReadDir(context.Context, engine.SandboxKey, string) ([]engine.DirEntry, error) {
	return nil, fs.ErrNotExist
}

func (*recordingEngine) WriteFile(context.Context, engine.SandboxKey, string, []byte, fs.FileMode) error {
	return nil
}

func (*recordingEngine) MkdirAll(context.Context, engine.SandboxKey, string, fs.FileMode) error {
	return nil
}

func (e *recordingEngine) Execute(
	_ context.Context,
	sandboxKey engine.SandboxKey,
	command engine.Command,
) (engine.CommandResult, error) {
	e.sandboxKey = sandboxKey
	e.command = command
	return engine.CommandResult{Output: "ok"}, nil
}
