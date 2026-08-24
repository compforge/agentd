package view

import (
	"encoding/json"
	"time"

	"github.com/compforge/agentd/agentd/internal/model"
)

type CreateSessionRequest struct {
	Agent         json.RawMessage   `json:"agent"`
	EnvironmentID string            `json:"environment_id"`
	Title         string            `json:"title"`
	Metadata      map[string]string `json:"metadata"`
	InitialEvents []json.RawMessage `json:"initial_events"`
	Budget        json.RawMessage   `json:"budget"`
	Resources     []json.RawMessage `json:"resources"`
	VaultIDs      []string          `json:"vault_ids"`
}

type UpdateSessionRequest struct {
	SessionID string          `path:"session_id"`
	Title     json.RawMessage `json:"title"`
	Metadata  json.RawMessage `json:"metadata"`
	Agent     json.RawMessage `json:"agent"`
	Budget    json.RawMessage `json:"budget"`
	VaultIDs  []string        `json:"vault_ids"`
}

type SessionPathRequest struct {
	SessionID string `path:"session_id"`
}

type ListSessionsRequest struct {
	PageRequest
	IncludeArchived bool   `query:"include_archived"`
	Order           string `query:"order"`
}

type SessionStatsResponse struct {
	ActiveSeconds   float64 `json:"active_seconds"`
	DurationSeconds float64 `json:"duration_seconds"`
}

type CacheCreationResponse struct {
	Ephemeral1HInputTokens int64 `json:"ephemeral_1h_input_tokens"`
	Ephemeral5MInputTokens int64 `json:"ephemeral_5m_input_tokens"`
}

type SessionUsageResponse struct {
	ActiveSeconds        float64               `json:"active_seconds"`
	CacheCreation        CacheCreationResponse `json:"cache_creation"`
	CacheReadInputTokens int64                 `json:"cache_read_input_tokens"`
	InputTokens          int64                 `json:"input_tokens"`
	ListCost             any                   `json:"list_cost"`
	OutputTokens         int64                 `json:"output_tokens"`
	ServerToolUse        any                   `json:"server_tool_use"`
}

type SessionResponse struct {
	ID                 string                `json:"id"`
	Type               string                `json:"type"`
	Agent              AgentSnapshotResponse `json:"agent"`
	EnvironmentID      string                `json:"environment_id"`
	Title              string                `json:"title"`
	Metadata           map[string]string     `json:"metadata"`
	Status             string                `json:"status"`
	CreatedAt          time.Time             `json:"created_at"`
	UpdatedAt          time.Time             `json:"updated_at"`
	ArchivedAt         *time.Time            `json:"archived_at"`
	Budget             any                   `json:"budget"`
	OutcomeEvaluations []any                 `json:"outcome_evaluations"`
	Resources          []any                 `json:"resources"`
	VaultIDs           []any                 `json:"vault_ids"`
	DeploymentID       any                   `json:"deployment_id"`
	Stats              SessionStatsResponse  `json:"stats"`
	Usage              SessionUsageResponse  `json:"usage"`
}

func NewSessionResponse(
	value model.Session,
	agent model.Agent,
	durationSeconds float64,
	usage SessionUsageResponse,
) SessionResponse {
	return SessionResponse{
		ID: value.ID, Type: "session", Agent: NewAgentSnapshotResponse(agent),
		EnvironmentID: value.EnvironmentID, Title: value.Title, Metadata: value.Metadata,
		Status: string(value.Status), CreatedAt: value.CreatedAt, UpdatedAt: value.UpdatedAt, ArchivedAt: value.ArchivedAt,
		OutcomeEvaluations: []any{}, Resources: []any{}, VaultIDs: []any{},
		Stats: SessionStatsResponse{DurationSeconds: durationSeconds}, Usage: usage,
	}
}
