package hostel

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"time"

	"github.com/compforge/agentd/server/internal/sandbox"
)

type EngineConfig struct {
	URL            string
	Command        string
	RequestTimeout time.Duration
	StartupTimeout time.Duration
}

type Engine struct {
	client     *Client
	supervisor *Supervisor
}

var _ sandbox.Engine = (*Engine)(nil)

func NewEngine(config EngineConfig) (*Engine, error) {
	client, err := NewClient(config.URL, config.RequestTimeout)
	if err != nil {
		return nil, err
	}
	return &Engine{
		client: client,
		supervisor: &Supervisor{
			Command: config.Command, Client: client, StartupTimeout: config.StartupTimeout,
		},
	}, nil
}

func (e *Engine) Name() string {
	return "hostel"
}

func (e *Engine) Start(ctx context.Context) error {
	return e.supervisor.Start(ctx)
}

func (e *Engine) Ensure(ctx context.Context, sandboxID string) error {
	return e.client.EnsureBed(ctx, sandboxID)
}

func (e *Engine) Stat(ctx context.Context, sandboxID, path string) (sandbox.FileInfo, error) {
	return e.client.Stat(ctx, sandboxID, path)
}

func (e *Engine) ReadFile(ctx context.Context, sandboxID, path string) ([]byte, error) {
	return e.client.ReadFile(ctx, sandboxID, path)
}

func (e *Engine) ReadDir(ctx context.Context, sandboxID, path string) ([]sandbox.DirEntry, error) {
	return e.client.ReadDir(ctx, sandboxID, path)
}

func (e *Engine) WriteFile(ctx context.Context, sandboxID, path string, data []byte, mode os.FileMode) error {
	return e.client.WriteFile(ctx, sandboxID, path, data, mode)
}

func (e *Engine) MkdirAll(ctx context.Context, sandboxID, path string, mode os.FileMode) error {
	return e.client.MkdirAll(ctx, sandboxID, path, mode)
}

func (e *Engine) Execute(ctx context.Context, sandboxID string, command sandbox.Command) (sandbox.CommandResult, error) {
	return e.client.Run(ctx, sandboxID, command)
}

type Supervisor struct {
	Command        string
	Client         *Client
	StartupTimeout time.Duration

	process *exec.Cmd
}

func (s *Supervisor) Start(ctx context.Context) error {
	if s.StartupTimeout <= 0 {
		return fmt.Errorf("start Hostel: startup timeout must be positive")
	}
	if s.Command != "" {
		// The command is operator-owned configuration, not request input. A shell
		// keeps local development practical without coupling agentd to Hostel flags.
		s.process = exec.CommandContext(ctx, "/bin/sh", "-c", s.Command)
		s.process.Stdout = os.Stdout
		s.process.Stderr = os.Stderr
		if err := s.process.Start(); err != nil {
			return fmt.Errorf("start Hostel child process: %w", err)
		}
		go func() { _ = s.process.Wait() }()
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
