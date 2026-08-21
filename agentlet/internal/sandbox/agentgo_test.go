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
	toolset, err := PrepareAgentGoToolset(context.Background(), sandboxEngine, "session_123", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if sandboxEngine.ensuredKey.Value != "session_123" {
		t.Fatalf("ensured sandbox key = %#v, want session_123", sandboxEngine.ensuredKey)
	}
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
	if sandboxEngine.executedKey.Value != "session_123" {
		t.Fatalf("execution sandbox key = %#v, want session_123", sandboxEngine.executedKey)
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
	ensuredKey  engine.SandboxKey
	executedKey engine.SandboxKey
	command     engine.Command
}

func (*recordingEngine) Name() string { return "recording" }
func (e *recordingEngine) Ensure(_ context.Context, sandboxKey engine.SandboxKey) error {
	e.ensuredKey = sandboxKey
	return nil
}

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
	e.executedKey = sandboxKey
	e.command = command
	return engine.CommandResult{Output: "ok"}, nil
}
