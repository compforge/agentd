package hostel

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/compforge/agentd/agentlet/internal/sandbox/engine"
)

type EngineConfig struct {
	URL            string
	RequestTimeout time.Duration
	StartupTimeout time.Duration
}

type Engine struct {
	client  *Client
	starter *Starter
}

var _ engine.Engine = (*Engine)(nil)

func NewEngine(config EngineConfig) (*Engine, error) {
	client, err := NewClient(config.URL, config.RequestTimeout)
	if err != nil {
		return nil, err
	}
	return &Engine{
		client:  client,
		starter: &Starter{Client: client, StartupTimeout: config.StartupTimeout},
	}, nil
}

func (e *Engine) Name() string {
	return "hostel"
}

func (e *Engine) Start(ctx context.Context) error {
	return e.starter.Start(ctx)
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

// Starter only waits for the Sandbox Engine endpoint to become ready. The
// Worker Pod, not Agentlet, owns the Engine process lifecycle.
type Starter struct {
	Client         *Client
	StartupTimeout time.Duration
}

func (s *Starter) Start(ctx context.Context) error {
	if s.StartupTimeout <= 0 {
		return fmt.Errorf("start Hostel: startup timeout must be positive")
	}
	deadlineCtx, cancel := context.WithTimeout(ctx, s.StartupTimeout)
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := s.Client.Health(deadlineCtx); err == nil {
			return nil
		}
		select {
		case <-deadlineCtx.Done():
			return fmt.Errorf("wait for Hostel health: %w", deadlineCtx.Err())
		case <-ticker.C:
		}
	}
}
