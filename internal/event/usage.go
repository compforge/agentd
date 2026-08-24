package event

import (
	"context"
	"encoding/json"
	"fmt"

	agentledger "github.com/compforge/agent-ledger/go"
)

// SessionUsage is the cumulative model token usage recorded for one Session.
type SessionUsage struct {
	InputTokens           int64
	OutputTokens          int64
	CacheReadInputTokens  int64
	CacheWriteInputTokens int64
}

// SessionUsage projects terminal model Attempts instead of interpreting all
// usage-shaped Events. Failed calls may report consumed tokens; tool payloads
// with similar fields do not contribute to model token accounting.
//
// +spec=`Session usage is derived from terminal model Attempts with observed usage in Ledger without adding mutable usage columns to Control State`
// +link=repo://agentd/docs/kernel.md
func (l *Log) SessionUsage(ctx context.Context, sessionID string) (SessionUsage, error) {
	view, err := l.store.LoadSession(ctx, sessionID)
	if err != nil {
		return SessionUsage{}, fmt.Errorf("load Session usage: %w", err)
	}

	modelActions := make(map[string]bool)
	for _, action := range view.Actions {
		if action.Type == agentledger.ActionTypeModelCall {
			modelActions[action.ID] = true
		}
	}
	modelAttempts := make(map[string]bool)
	for _, attempt := range view.Attempts {
		if modelActions[attempt.ActionID] {
			modelAttempts[attempt.ID] = true
		}
	}

	var result SessionUsage
	for _, event := range view.Events {
		if !modelAttempts[event.SubjectID] ||
			(event.EventType != agentledger.EventTypeAttemptCompleted &&
				event.EventType != agentledger.EventTypeAttemptFailed) {
			continue
		}
		usage, ok, decodeErr := decodeModelUsage(event.Payload["usage"])
		if decodeErr != nil {
			return SessionUsage{}, fmt.Errorf("decode model usage for Attempt %s: %w", event.SubjectID, decodeErr)
		}
		if !ok {
			continue
		}
		result.InputTokens += usage.InputTokens
		result.OutputTokens += usage.OutputTokens
		result.CacheReadInputTokens += usage.CacheReadInputTokens
		result.CacheWriteInputTokens += usage.CacheWriteInputTokens
	}
	return result, nil
}

func decodeModelUsage(value any) (SessionUsage, bool, error) {
	if value == nil {
		return SessionUsage{}, false, nil
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return SessionUsage{}, false, err
	}
	var usage struct {
		InputTokens           int64 `json:"input_tokens"`
		OutputTokens          int64 `json:"output_tokens"`
		CacheReadInputTokens  int64 `json:"cache_read_input_tokens"`
		CacheWriteInputTokens int64 `json:"cache_write_input_tokens"`
	}
	if err := json.Unmarshal(encoded, &usage); err != nil {
		return SessionUsage{}, false, err
	}
	return SessionUsage{
		InputTokens: usage.InputTokens, OutputTokens: usage.OutputTokens,
		CacheReadInputTokens:  usage.CacheReadInputTokens,
		CacheWriteInputTokens: usage.CacheWriteInputTokens,
	}, true, nil
}
