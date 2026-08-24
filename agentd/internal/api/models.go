package api

import (
	"context"

	hertzapp "github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/compforge/agentd/agentd/internal/api/view"
	"github.com/compforge/agentd/agentd/internal/model"
)

func (s *Server) createModel(ctx context.Context, request *hertzapp.RequestContext) {
	var input view.CreateModelRequest
	if !bindRequest(request, &input) {
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
	request.JSON(consts.StatusOK, view.NewModelResponse(created))
}

func (s *Server) getModel(ctx context.Context, request *hertzapp.RequestContext) {
	var input view.GetModelRequest
	if !bindRequest(request, &input) {
		return
	}
	value, err := s.service.GetModel(ctx, input.ModelID)
	if err != nil {
		s.writeError(request, err)
		return
	}
	request.JSON(consts.StatusOK, view.NewModelResponse(value))
}

func (s *Server) listModels(ctx context.Context, request *hertzapp.RequestContext) {
	var input view.ListModelsRequest
	if !bindRequest(request, &input) {
		return
	}
	query, err := parsePage(input.PageRequest)
	if err != nil {
		s.writeError(request, err)
		return
	}
	page, err := s.service.PageModels(ctx, query)
	if err != nil {
		s.writeError(request, err)
		return
	}
	data := make([]view.ModelResponse, 0, len(page.Items))
	for _, value := range page.Items {
		data = append(data, view.NewModelResponse(value))
	}
	next, _ := pageLinks(query, page.HasMore)
	request.JSON(consts.StatusOK, view.Page[view.ModelResponse]{Data: data, NextPage: next})
}

// modelResponse is the only public projection of a registered Model.
//
// +spec=`Model credentials are write-only: callers may observe whether a credential is configured, but create, get, and list responses never expose its value`
// +case:id=model_secret_redaction,desc=`register a Model with a credential, then read it through create, get, and list`,input=`one unique external model connection and API key`,expect=`all responses identify the Model and report that a key is configured`,forbid=`returning the API key value or an api_key field`,group=system
// +link=agentd/docs/kernel.md
// +link=tests/e2e/cases/managed-agent.yaml
