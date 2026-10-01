package hostel

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/compforge/agentd/agentlet/internal/sandbox/engine"
)

type commandEvent struct {
	Type     string           `json:"type"`
	Text     string           `json:"text"`
	ExitCode *int             `json:"exit_code"`
	Error    string           `json:"error"`
	Result   *commandTerminal `json:"result"`
}

type commandTerminal struct {
	Process *struct {
		Kind       string `json:"kind"`
		ExitCode   *int   `json:"exit_code"`
		Signal     *int   `json:"signal"`
		CoreDumped bool   `json:"core_dumped"`
		Error      string `json:"error"`
	} `json:"process"`
	Cause string `json:"termination_cause"`
	Error string `json:"error"`
}

// readCommandResult trusts an explicit terminal event, never transport EOF.
// Once the result is known, a later connection failure cannot undo that fact.
// +spec=`Command streams require a terminal result; missing or malformed terminal events are unknown outcomes and never cause command replay`
func readCommandResult(reader io.Reader) (engine.CommandResult, error) {
	var result engine.CommandResult
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(nil, 4<<20)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var event commandEvent
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			return result, fmt.Errorf("%w: decode hostel command event: %w", engine.ErrExecutionUnknown, err)
		}
		switch event.Type {
		case "stdout", "stderr":
			result.Output += event.Text
		case "execution_end":
			terminal, err := event.Result.commandResult()
			if err != nil {
				return result, fmt.Errorf("%w: invalid hostel terminal event: %w", engine.ErrExecutionUnknown, err)
			}
			terminal.Output = result.Output
			return terminal, nil
		case "execution_complete":
			// Older Hostel reports a flat terminal event. An omitted exit code is
			// not zero, even if the stream closes cleanly.
			if event.ExitCode == nil {
				return result, fmt.Errorf("%w: legacy hostel terminal event has no exit code: %s", engine.ErrExecutionUnknown, event.Error)
			}
			result.ProcessKind, result.ExitCode, result.Cause, result.Error = "exited", event.ExitCode, "exited", event.Error
			return result, nil
		}
	}
	if err := scanner.Err(); err != nil {
		return result, fmt.Errorf("%w: read hostel command stream: %w", engine.ErrExecutionUnknown, err)
	}
	return result, fmt.Errorf("%w: hostel command stream ended without a terminal event: %w", engine.ErrExecutionUnknown, io.ErrUnexpectedEOF)
}

func (t *commandTerminal) commandResult() (engine.CommandResult, error) {
	if t == nil || t.Cause == "" {
		return engine.CommandResult{}, fmt.Errorf("missing result or termination cause")
	}
	result := engine.CommandResult{Cause: t.Cause, Error: t.Error}
	if t.Process == nil {
		if t.Cause != "preparation_failed" {
			return engine.CommandResult{}, fmt.Errorf("missing process outcome")
		}
		return result, nil
	}
	if t.Cause == "preparation_failed" {
		return engine.CommandResult{}, fmt.Errorf("preparation failure includes a process outcome")
	}
	result.ProcessKind = t.Process.Kind
	switch t.Process.Kind {
	case "exited":
		if t.Process.ExitCode == nil {
			return engine.CommandResult{}, fmt.Errorf("exited process has no exit code")
		}
		result.ExitCode = t.Process.ExitCode
	case "signaled":
		if t.Process.Signal == nil || *t.Process.Signal <= 0 {
			return engine.CommandResult{}, fmt.Errorf("signaled process has no valid signal")
		}
		result.Signal, result.CoreDumped = t.Process.Signal, t.Process.CoreDumped
	case "lost":
	default:
		return engine.CommandResult{}, fmt.Errorf("unknown process outcome %q", t.Process.Kind)
	}
	if result.Error == "" {
		result.Error = t.Process.Error
	}
	return result, nil
}
