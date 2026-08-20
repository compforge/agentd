package engine

import (
	"context"
	"io/fs"
	"time"
)

// Engine is the runtime boundary between Agentlet and an isolated workspace.
// Implementations may manage a local process or call a remote sandbox service.
type Engine interface {
	Name() string
	Start(context.Context) error
	Ensure(context.Context, string) error
	Stat(context.Context, string, string) (FileInfo, error)
	ReadFile(context.Context, string, string) ([]byte, error)
	ReadDir(context.Context, string, string) ([]DirEntry, error)
	WriteFile(context.Context, string, string, []byte, fs.FileMode) error
	MkdirAll(context.Context, string, string, fs.FileMode) error
	Execute(context.Context, string, Command) (CommandResult, error)
}

type Command struct {
	Command string
	Cwd     string
	Timeout time.Duration
}

type CommandResult struct {
	Output   string `json:"output"`
	ExitCode int    `json:"exit_code"`
	Cause    string `json:"termination_cause,omitempty"`
	Error    string `json:"error,omitempty"`
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
