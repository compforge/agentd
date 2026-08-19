package sandbox

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"strings"
	"time"

	"github.com/compforge/agentgo"
	"github.com/compforge/agentgo/tools"
)

func NewAgentGoToolset(engine Engine, sandboxID string, defaultTimeout time.Duration) []agentgo.Tool {
	workspace := &agentGoWorkspace{engine: engine, sandboxID: sandboxID}
	readState := tools.NewFileReadState()
	return []agentgo.Tool{
		newBashTool(engine, sandboxID, defaultTimeout),
		tools.NewRead("/workspace", readState, tools.WithFS(workspace)),
		tools.NewWrite("/workspace", readState, tools.WithFS(workspace)),
		tools.NewEdit("/workspace", readState, tools.WithFS(workspace)),
		newGlobTool(engine, sandboxID, defaultTimeout),
		newGrepTool(engine, sandboxID, defaultTimeout),
	}
}

type agentGoWorkspace struct {
	engine    Engine
	sandboxID string
}

var _ tools.WorkspaceFS = (*agentGoWorkspace)(nil)

func (w *agentGoWorkspace) Stat(ctx context.Context, filePath string) (tools.FileInfo, error) {
	value, err := w.engine.Stat(ctx, w.sandboxID, filePath)
	if err != nil {
		return tools.FileInfo{}, err
	}
	return tools.FileInfo{
		Name: value.Name, Size: value.Size, Mode: value.Mode, ModTime: value.ModifiedAt,
		IsDir: value.IsDir, Version: value.Version,
	}, nil
}

func (w *agentGoWorkspace) Open(ctx context.Context, filePath string) (io.ReadCloser, error) {
	data, err := w.ReadFile(ctx, filePath)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (w *agentGoWorkspace) ReadFile(ctx context.Context, filePath string) ([]byte, error) {
	return w.engine.ReadFile(ctx, w.sandboxID, filePath)
}

func (w *agentGoWorkspace) ReadDir(ctx context.Context, directory string) ([]tools.DirEntry, error) {
	values, err := w.engine.ReadDir(ctx, w.sandboxID, directory)
	if err != nil {
		return nil, err
	}
	entries := make([]tools.DirEntry, len(values))
	for index, value := range values {
		entries[index] = tools.DirEntry{Name: value.Name, IsDir: value.IsDir}
	}
	return entries, nil
}

func (w *agentGoWorkspace) WriteFile(ctx context.Context, filePath string, data []byte, mode fs.FileMode) error {
	return w.engine.WriteFile(ctx, w.sandboxID, filePath, data, mode)
}

func (w *agentGoWorkspace) MkdirAll(ctx context.Context, directory string, mode fs.FileMode) error {
	return w.engine.MkdirAll(ctx, w.sandboxID, directory, mode)
}

func newBashTool(engine Engine, sandboxID string, defaultTimeout time.Duration) agentgo.Tool {
	return agentgo.NewFuncTool("bash", "Execute a shell command in the isolated session workspace.", map[string]any{
		"type": "object",
		"properties": map[string]any{
			"command":    map[string]any{"type": "string"},
			"restart":    map[string]any{"type": "boolean"},
			"timeout_ms": map[string]any{"type": "integer"},
		},
	}, func(ctx context.Context, raw json.RawMessage) (json.RawMessage, error) {
		var input struct {
			Command   string `json:"command"`
			Restart   bool   `json:"restart"`
			TimeoutMS int64  `json:"timeout_ms"`
		}
		if err := json.Unmarshal(raw, &input); err != nil {
			return nil, fmt.Errorf("decode bash input: %w", err)
		}
		if input.Restart {
			return nil, fmt.Errorf("bash restart is not supported by sandbox engine %q", engine.Name())
		}
		if strings.TrimSpace(input.Command) == "" {
			return nil, fmt.Errorf("bash command is required")
		}
		timeout := defaultTimeout
		if input.TimeoutMS > 0 {
			timeout = time.Duration(input.TimeoutMS) * time.Millisecond
		}
		result, err := engine.Execute(ctx, sandboxID, Command{Command: input.Command, Cwd: "/workspace", Timeout: timeout})
		if err != nil {
			return nil, err
		}
		return json.Marshal(result)
	})
}

func newGlobTool(engine Engine, sandboxID string, timeout time.Duration) agentgo.Tool {
	return agentgo.NewFuncTool("glob", "Find files in the isolated session workspace by glob pattern.", map[string]any{
		"type": "object",
		"properties": map[string]any{
			"pattern": map[string]any{"type": "string"},
			"path":    map[string]any{"type": "string"},
		},
		"required": []string{"pattern"},
	}, func(ctx context.Context, raw json.RawMessage) (json.RawMessage, error) {
		var input struct {
			Pattern string `json:"pattern"`
			Path    string `json:"path"`
		}
		if err := json.Unmarshal(raw, &input); err != nil {
			return nil, fmt.Errorf("decode glob input: %w", err)
		}
		root := input.Path
		if root == "" {
			root = "/workspace"
		}
		command := "rg --files --hidden --no-require-git --glob " + shellQuote(input.Pattern) + " " + shellQuote(root)
		result, err := engine.Execute(ctx, sandboxID, Command{Command: command, Cwd: "/workspace", Timeout: timeout})
		if err != nil {
			return nil, err
		}
		return json.Marshal(map[string]any{"files": nonEmptyLines(result.Output), "exit_code": result.ExitCode})
	})
}

func newGrepTool(engine Engine, sandboxID string, timeout time.Duration) agentgo.Tool {
	return agentgo.NewFuncTool("grep", "Search file contents in the isolated session workspace.", map[string]any{
		"type": "object",
		"properties": map[string]any{
			"pattern":    map[string]any{"type": "string"},
			"path":       map[string]any{"type": "string"},
			"glob":       map[string]any{"type": "string"},
			"ignoreCase": map[string]any{"type": "boolean"},
			"literal":    map[string]any{"type": "boolean"},
		},
		"required": []string{"pattern"},
	}, func(ctx context.Context, raw json.RawMessage) (json.RawMessage, error) {
		var input struct {
			Pattern    string `json:"pattern"`
			Path       string `json:"path"`
			Glob       string `json:"glob"`
			IgnoreCase bool   `json:"ignoreCase"`
			Literal    bool   `json:"literal"`
		}
		if err := json.Unmarshal(raw, &input); err != nil {
			return nil, fmt.Errorf("decode grep input: %w", err)
		}
		root := input.Path
		if root == "" {
			root = "/workspace"
		}
		args := []string{"rg", "--line-number", "--no-heading", "--color=never"}
		if input.IgnoreCase {
			args = append(args, "--ignore-case")
		}
		if input.Literal {
			args = append(args, "--fixed-strings")
		}
		if input.Glob != "" {
			args = append(args, "--glob", shellQuote(input.Glob))
		}
		args = append(args, shellQuote(input.Pattern), shellQuote(root))
		result, err := engine.Execute(ctx, sandboxID, Command{Command: strings.Join(args, " "), Cwd: "/workspace", Timeout: timeout})
		if err != nil {
			return nil, err
		}
		return json.Marshal(map[string]any{"matches": nonEmptyLines(result.Output), "exit_code": result.ExitCode})
	})
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}

func nonEmptyLines(value string) []string {
	var lines []string
	for _, line := range strings.Split(value, "\n") {
		if line = strings.TrimSpace(line); line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}
