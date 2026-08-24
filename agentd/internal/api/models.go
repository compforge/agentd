package api

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	hertzapp "github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/compforge/agentd/agentd/internal/api/view"
	"github.com/compforge/agentd/agentd/internal/model"
	"github.com/compforge/agentd/agentd/internal/service"
)

func (s *Server) createModel(ctx context.Context, request *hertzapp.RequestContext) error {
	var input view.CreateModelRequest
	if err := bindRequest(request, &input); err != nil {
		return err
	}
	created, err := s.service.CreateModel(ctx, model.Model{
		ID: input.ID, Provider: input.Provider, UpstreamID: input.Model,
		BaseURL: input.BaseURL, APIKey: input.APIKey,
	})
	if err != nil {
		return err
	}
	s.logger.InfoContext(ctx, "created Model", "model_id", created.ID, "provider", created.Provider)
	request.JSON(consts.StatusOK, view.NewModelResponse(created))
	return nil
}

func (s *Server) getModel(ctx context.Context, request *hertzapp.RequestContext) error {
	var input view.GetModelRequest
	if err := bindRequest(request, &input); err != nil {
		return err
	}
	value, err := s.service.GetModel(ctx, input.ModelID)
	if err != nil {
		return err
	}
	request.JSON(consts.StatusOK, view.NewModelResponse(value))
	return nil
}

func (s *Server) updateModel(ctx context.Context, request *hertzapp.RequestContext) error {
	var input view.UpdateModelRequest
	if err := bindRequest(request, &input); err != nil {
		return err
	}
	provider, err := parseModelString(input.Provider, false, "provider")
	if err != nil {
		return err
	}
	upstreamID, err := parseModelString(input.Model, false, "model")
	if err != nil {
		return err
	}
	baseURL, err := parseModelString(input.BaseURL, true, "base_url")
	if err != nil {
		return err
	}
	apiKey, err := parseModelString(input.APIKey, false, "api_key")
	if err != nil {
		return err
	}
	updated, err := s.service.UpdateModel(ctx, input.ModelID, service.ModelUpdate{
		Provider: provider, UpstreamID: upstreamID, BaseURL: baseURL, APIKey: apiKey,
	})
	if err != nil {
		return err
	}
	s.logger.InfoContext(ctx, "updated Model connection", "model_id", updated.ID, "provider", updated.Provider)
	request.JSON(consts.StatusOK, view.NewModelResponse(updated))
	return nil
}

func (s *Server) listModels(ctx context.Context, request *hertzapp.RequestContext) error {
	var input view.ListModelsRequest
	if err := bindRequest(request, &input); err != nil {
		return err
	}
	query, err := parsePage(input.PageRequest, modelCursor, false)
	if err != nil {
		return err
	}
	page, err := s.service.PageModels(ctx, query)
	if err != nil {
		return err
	}
	data := make([]view.ModelResponse, 0, len(page.Items))
	for _, value := range page.Items {
		data = append(data, view.NewModelResponse(value))
	}
	var first, last service.PageAnchor
	if len(page.Items) > 0 {
		first = service.PageAnchor{CreatedAt: page.Items[0].CreatedAt, ID: page.Items[0].ID}
		value := page.Items[len(page.Items)-1]
		last = service.PageAnchor{CreatedAt: value.CreatedAt, ID: value.ID}
	}
	next, _ := pageLinks(modelCursor, query, page.HasMore, first, last)
	request.JSON(consts.StatusOK, view.Page[view.ModelResponse]{Data: data, NextPage: next})
	return nil
}

func parseModelString(raw json.RawMessage, clearable bool, field string) (*string, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	if strings.TrimSpace(string(raw)) == "null" {
		if !clearable {
			return nil, fmt.Errorf("%w: model %s cannot be cleared", service.ErrInvalid, field)
		}
		value := ""
		return &value, nil
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, fmt.Errorf("%w: model %s must be a string", service.ErrInvalid, field)
	}
	return &value, nil
}

// modelResponse is the only public projection of a registered Model.
//
// +spec=`Model credentials are write-only: callers may observe whether a credential is configured, but no Model response exposes its value`
// +case:id=model_secret_redaction,desc=`register a Model with a credential, then read it through create, get, and list`,input=`one unique external model connection and API key`,expect=`all responses identify the Model and report that a key is configured`,forbid=`returning the API key value or an api_key field`,group=system
// +link=agentd/docs/kernel.md
// +link=tests/e2e/cases/managed-agent.yaml
