package view

import (
	"encoding/json"
	"time"

	"github.com/compforge/agentd/agentd/internal/model"
)

type CreateEnvironmentRequest struct {
	Name        string            `json:"name"`
	Description string            `json:"description"`
	Config      map[string]any    `json:"config"`
	Scope       string            `json:"scope"`
	Metadata    map[string]string `json:"metadata"`
}

type UpdateEnvironmentRequest struct {
	EnvironmentID string          `path:"environment_id"`
	Name          json.RawMessage `json:"name"`
	Description   json.RawMessage `json:"description"`
	Config        json.RawMessage `json:"config"`
	Scope         json.RawMessage `json:"scope"`
	Metadata      json.RawMessage `json:"metadata"`
}

type GetEnvironmentRequest struct {
	EnvironmentID string `path:"environment_id"`
}

type EnvironmentPathRequest struct {
	EnvironmentID string `path:"environment_id"`
}

type ListEnvironmentsRequest struct {
	PageRequest
	IncludeArchived bool `query:"include_archived"`
}

type EnvironmentResponse struct {
	ID          string            `json:"id"`
	Type        string            `json:"type"`
	Name        string            `json:"name"`
	Description string            `json:"description"`
	Config      map[string]any    `json:"config"`
	Metadata    map[string]string `json:"metadata"`
	Scope       string            `json:"scope"`
	ArchivedAt  *time.Time        `json:"archived_at"`
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
}

func NewEnvironmentResponse(value model.Environment) EnvironmentResponse {
	config := make(map[string]any, len(value.Config)+2)
	for key, item := range value.Config {
		config[key] = item
	}
	if config["networking"] == nil {
		config["networking"] = map[string]any{"type": "unrestricted"}
	}
	if config["packages"] == nil {
		config["packages"] = map[string]any{}
	}
	return EnvironmentResponse{
		ID: value.ID, Type: "environment", Name: value.Name, Description: value.Description,
		Config: config, Metadata: value.Metadata, Scope: "account",
		ArchivedAt: value.ArchivedAt, CreatedAt: value.CreatedAt, UpdatedAt: value.UpdatedAt,
	}
}
