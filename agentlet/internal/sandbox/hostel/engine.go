package hostel

import (
	"context"
	"os"
	"time"

	"github.com/compforge/agentd/agentlet/internal/sandbox/engine"
)

type EngineConfig struct {
	URL            string
	RequestTimeout time.Duration
}

type Engine struct {
	client *Client
}

var _ engine.Engine = (*Engine)(nil)

func NewEngine(config EngineConfig) (*Engine, error) {
	client, err := NewClient(config.URL, config.RequestTimeout)
	if err != nil {
		return nil, err
	}
	return &Engine{client: client}, nil
}

func (e *Engine) Name() string {
	return "hostel"
}

func (e *Engine) Ensure(ctx context.Context, sandboxKey engine.SandboxKey) error {
	return e.client.EnsureBed(ctx, sandboxKey.Value)
}

func (e *Engine) Stat(ctx context.Context, sandboxKey engine.SandboxKey, path string) (engine.FileInfo, error) {
	return e.client.Stat(ctx, sandboxKey.Value, path)
}

func (e *Engine) ReadFile(ctx context.Context, sandboxKey engine.SandboxKey, path string) ([]byte, error) {
	return e.client.ReadFile(ctx, sandboxKey.Value, path)
}

func (e *Engine) ReadDir(ctx context.Context, sandboxKey engine.SandboxKey, path string) ([]engine.DirEntry, error) {
	return e.client.ReadDir(ctx, sandboxKey.Value, path)
}

func (e *Engine) WriteFile(
	ctx context.Context,
	sandboxKey engine.SandboxKey,
	path string,
	data []byte,
	mode os.FileMode,
) error {
	return e.client.WriteFile(ctx, sandboxKey.Value, path, data, mode)
}

func (e *Engine) MkdirAll(ctx context.Context, sandboxKey engine.SandboxKey, path string, mode os.FileMode) error {
	return e.client.MkdirAll(ctx, sandboxKey.Value, path, mode)
}

func (e *Engine) Execute(
	ctx context.Context,
	sandboxKey engine.SandboxKey,
	command engine.Command,
) (engine.CommandResult, error) {
	return e.client.Run(ctx, sandboxKey.Value, command)
}
