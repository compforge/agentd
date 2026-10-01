package engine

import (
	"context"
	"errors"
	"io/fs"
	"time"
)

// SandboxKey is the caller-defined logical identity used to prepare and access
// one sandbox. It is a model so the Engine contract can add grouping or scope
// without changing every operation signature.
type SandboxKey struct {
	Value string
}

// Engine is the boundary between Agentlet and an isolated workspace.
// SandboxKey values are supplied and interpreted by the caller; implementations
// must treat them as opaque lookup keys for local or remote sandbox resources.
type Engine interface {
	Name() string
	Ensure(ctx context.Context, sandboxKey SandboxKey) error
	Stat(ctx context.Context, sandboxKey SandboxKey, path string) (FileInfo, error)
	ReadFile(ctx context.Context, sandboxKey SandboxKey, path string) ([]byte, error)
	ReadDir(ctx context.Context, sandboxKey SandboxKey, path string) ([]DirEntry, error)
	WriteFile(ctx context.Context, sandboxKey SandboxKey, path string, data []byte, mode fs.FileMode) error
	MkdirAll(ctx context.Context, sandboxKey SandboxKey, path string, mode fs.FileMode) error
	Execute(ctx context.Context, sandboxKey SandboxKey, command Command) (CommandResult, error)
}

type Command struct {
	Command string
	Cwd     string
	Timeout time.Duration
}

// ErrExecutionUnknown means the command may have run, but its terminal result
// could not be established. Callers must not assume it is safe to replay.
var ErrExecutionUnknown = errors.New("command execution result unknown")

// CommandResult separates a process outcome from the reason execution stopped.
// ExitCode is present only for an exited process; a signal or preparation
// failure must never acquire a synthetic successful exit code. A nil Go error
// means a terminal result was received, not that the command succeeded.
type CommandResult struct {
	Output      string `json:"output"`
	ProcessKind string `json:"process_kind,omitempty"`
	ExitCode    *int   `json:"exit_code,omitempty"`
	Signal      *int   `json:"signal,omitempty"`
	CoreDumped  bool   `json:"core_dumped,omitempty"`
	Cause       string `json:"termination_cause,omitempty"`
	Error       string `json:"error,omitempty"`
}

type FileInfo struct {
	Name       string
	Size       int64
	Mode       fs.FileMode
	ModifiedAt time.Time
	IsDir      bool
	Version    string
}

type DirEntry struct {
	Name  string
	IsDir bool
}
