package view

import (
	"encoding/json"
	"time"

	"github.com/compforge/agentd/agentd/internal/model"
)

type CreateAgentRequest struct {
	Name        string            `json:"name"`
	Description string            `json:"description"`
	Model       json.RawMessage   `json:"model"`
	System      string            `json:"system"`
	Tools       []map[string]any  `json:"tools"`
	Metadata    map[string]string `json:"metadata"`
	MCPServers  []json.RawMessage `json:"mcp_servers"`
	Skills      []json.RawMessage `json:"skills"`
	Multiagent  json.RawMessage   `json:"multiagent"`
}

type UpdateAgentRequest struct {
	AgentID     string            `path:"agent_id"`
	Version     *int64            `json:"version"`
	Name        json.RawMessage   `json:"name"`
	Description json.RawMessage   `json:"description"`
	Model       json.RawMessage   `json:"model"`
	System      json.RawMessage   `json:"system"`
	Tools       json.RawMessage   `json:"tools"`
	Metadata    json.RawMessage   `json:"metadata"`
	MCPServers  []json.RawMessage `json:"mcp_servers"`
	Skills      []json.RawMessage `json:"skills"`
	Multiagent  json.RawMessage   `json:"multiagent"`
}

type GetAgentRequest struct {
	AgentID string `path:"agent_id"`
	Version *int64 `query:"version"`
}

type AgentPathRequest struct {
	AgentID string `path:"agent_id"`
}

type ListAgentsRequest struct {
	PageRequest
	IncludeArchived bool `query:"include_archived"`
}

type ListAgentVersionsRequest struct {
	AgentID string `path:"agent_id"`
	PageRequest
}

type AgentModelResponse struct {
	ID    string `json:"id"`
	Speed string `json:"speed"`
}

type AgentResponse struct {
	ID          string             `json:"id"`
	Type        string             `json:"type"`
	Name        string             `json:"name"`
	Description string             `json:"description"`
	Model       AgentModelResponse `json:"model"`
	System      string             `json:"system"`
	Tools       []map[string]any   `json:"tools"`
	MCPServers  []any              `json:"mcp_servers"`
	Skills      []any              `json:"skills"`
	Multiagent  any                `json:"multiagent"`
	Metadata    map[string]string  `json:"metadata"`
	Version     int64              `json:"version"`
	ArchivedAt  *time.Time         `json:"archived_at"`
	CreatedAt   time.Time          `json:"created_at"`
	UpdatedAt   time.Time          `json:"updated_at"`
}

// AgentSnapshotResponse is the immutable Agent configuration pinned by a Session.
type AgentSnapshotResponse struct {
	ID          string             `json:"id"`
	Type        string             `json:"type"`
	Name        string             `json:"name"`
	Description string             `json:"description"`
	Model       AgentModelResponse `json:"model"`
	System      string             `json:"system"`
	Tools       []map[string]any   `json:"tools"`
	MCPServers  []any              `json:"mcp_servers"`
	Skills      []any              `json:"skills"`
	Multiagent  any                `json:"multiagent"`
	Version     int64              `json:"version"`
}

func NewAgentResponse(value model.Agent) AgentResponse {
	return AgentResponse{
		ID: value.ID, Type: "agent", Name: value.Name, Description: value.Description,
		Model: AgentModelResponse{ID: value.ModelID, Speed: "standard"}, System: value.System,
		Tools: agentTools(value.Tools), MCPServers: []any{}, Skills: []any{}, Metadata: value.Metadata,
		Version: value.Version, ArchivedAt: value.ArchivedAt,
		CreatedAt: value.CreatedAt, UpdatedAt: value.UpdatedAt,
	}
}

func NewAgentSnapshotResponse(value model.Agent) AgentSnapshotResponse {
	return AgentSnapshotResponse{
		ID: value.ID, Type: "agent", Name: value.Name, Description: value.Description,
		Model: AgentModelResponse{ID: value.ModelID, Speed: "standard"}, System: value.System,
		Tools: agentTools(value.Tools), MCPServers: []any{}, Skills: []any{}, Version: value.Version,
	}
}

func agentTools(values []map[string]any) []map[string]any {
	tools := make([]map[string]any, 0, len(values))
	for _, tool := range values {
		projected := make(map[string]any, len(tool)+2)
		for key, item := range tool {
			projected[key] = item
		}
		if projected["configs"] == nil {
			projected["configs"] = []any{}
		}
		if projected["default_config"] == nil {
			projected["default_config"] = map[string]any{
				"permission_policy": map[string]any{"type": "always_allow"},
			}
		}
		tools = append(tools, projected)
	}
	return tools
}
