package view

import (
	"time"

	"github.com/compforge/agentd/agentd/internal/model"
)

type CreateModelRequest struct {
	ID       string `json:"id"`
	Provider string `json:"provider"`
	Model    string `json:"model"`
	BaseURL  string `json:"base_url"`
	APIKey   string `json:"api_key"`
}

type GetModelRequest struct {
	ModelID string `path:"model_id"`
}

type ListModelsRequest struct {
	PageRequest
}

type ModelResponse struct {
	ID               string    `json:"id"`
	Type             string    `json:"type"`
	Provider         string    `json:"provider"`
	Model            string    `json:"model"`
	BaseURL          string    `json:"base_url"`
	APIKeyConfigured bool      `json:"api_key_configured"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

func NewModelResponse(value model.Model) ModelResponse {
	return ModelResponse{
		ID: value.ID, Type: "model", Provider: value.Provider, Model: value.UpstreamID,
		BaseURL: value.BaseURL, APIKeyConfigured: value.APIKey != "",
		CreatedAt: value.CreatedAt, UpdatedAt: value.UpdatedAt,
	}
}
