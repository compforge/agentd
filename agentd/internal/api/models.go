package api

import (
	"context"

	hertzapp "github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/compforge/agentd/agentd/internal/model"
)

func (s *Server) createModel(ctx context.Context, request *hertzapp.RequestContext) {
	var input struct {
		ID       string `json:"id"`
		Provider string `json:"provider"`
		Model    string `json:"model"`
		BaseURL  string `json:"base_url"`
		APIKey   string `json:"api_key"`
	}
	if !decodeBody(request, &input) {
		return
	}
	created, err := s.service.CreateModel(ctx, model.Model{
		ID: input.ID, Provider: input.Provider, UpstreamID: input.Model,
		BaseURL: input.BaseURL, APIKey: input.APIKey,
	})
	if err != nil {
		s.writeError(request, err)
		return
	}
	s.logger.InfoContext(ctx, "created Model", "model_id", created.ID, "provider", created.Provider)
	writeJSON(request, consts.StatusOK, modelResponse(created))
}

func (s *Server) getModel(ctx context.Context, request *hertzapp.RequestContext) {
	value, err := s.service.GetModel(ctx, request.Param("model_id"))
	if err != nil {
		s.writeError(request, err)
		return
	}
	writeJSON(request, consts.StatusOK, modelResponse(value))
}

func (s *Server) listModels(ctx context.Context, request *hertzapp.RequestContext) {
	values, err := s.service.ListModels(ctx)
	if err != nil {
		s.writeError(request, err)
		return
	}
	data := make([]map[string]any, 0, len(values))
	for _, value := range values {
		data = append(data, modelResponse(value))
	}
	writeJSON(request, consts.StatusOK, map[string]any{"data": data, "next_page": nil})
}

// modelResponse is the only public projection of a registered Model.
//
// +spec=`Model credentials are write-only: callers may observe whether a credential is configured, but create, get, and list responses never expose its value`
// +case:id=model_secret_redaction,desc=`register a Model with a credential, then read it through create, get, and list`,input=`one unique external model connection and API key`,expect=`all responses identify the Model and report that a key is configured`,forbid=`returning the API key value or an api_key field`,group=system
// +link=agentd/docs/kernel.md
// +link=tests/e2e/cases/managed-agent.yaml
func modelResponse(value model.Model) map[string]any {
	return map[string]any{
		"id": value.ID, "type": "model", "provider": value.Provider, "model": value.UpstreamID,
		"base_url":           value.BaseURL,
		"api_key_configured": value.APIKey != "", "created_at": value.CreatedAt, "updated_at": value.UpdatedAt,
	}
}
