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

func (s *Server) createAgent(ctx context.Context, request *hertzapp.RequestContext) error {
	var input view.CreateAgentRequest
	if err := bindRequest(request, &input); err != nil {
		return err
	}
	if len(input.MCPServers) > 0 || len(input.Skills) > 0 || present(input.Multiagent) {
		return fmt.Errorf("%w: MCP, skills, and multi-agent agents", service.ErrUnsupported)
	}
	modelID, err := parseModel(input.Model)
	if err != nil {
		return err
	}
	created, err := s.service.CreateAgent(ctx, model.Agent{
		Name: input.Name, Description: input.Description, ModelID: modelID, System: input.System,
		Tools: input.Tools, Metadata: input.Metadata,
	})
	if err != nil {
		return err
	}
	request.JSON(consts.StatusOK, view.NewAgentResponse(created))
	return nil
}

func (s *Server) getAgent(ctx context.Context, request *hertzapp.RequestContext) error {
	var input view.GetAgentRequest
	if err := bindRequest(request, &input); err != nil {
		return err
	}
	var value model.Agent
	var err error
	if input.Version != nil {
		value, err = s.service.FindAgentVersion(ctx, input.AgentID, *input.Version)
	} else {
		value, err = s.service.GetAgent(ctx, input.AgentID)
	}
	if err != nil {
		return err
	}
	request.JSON(consts.StatusOK, view.NewAgentResponse(value))
	return nil
}

func (s *Server) updateAgent(ctx context.Context, request *hertzapp.RequestContext) error {
	var input view.UpdateAgentRequest
	if err := bindRequest(request, &input); err != nil {
		return err
	}
	if len(input.MCPServers) > 0 || len(input.Skills) > 0 || present(input.Multiagent) {
		return fmt.Errorf("%w: MCP, skills, and multi-agent agents", service.ErrUnsupported)
	}
	name, err := parseOptionalString(input.Name, false, "name")
	if err != nil {
		return err
	}
	description, err := parseOptionalString(input.Description, true, "description")
	if err != nil {
		return err
	}
	system, err := parseOptionalString(input.System, true, "system")
	if err != nil {
		return err
	}
	var modelID *string
	if len(input.Model) > 0 {
		value, parseErr := parseModel(input.Model)
		if parseErr != nil {
			return parseErr
		}
		modelID = &value
	}
	tools, err := parseOptionalTools(input.Tools)
	if err != nil {
		return err
	}
	metadata, err := parseMetadataPatch(input.Metadata)
	if err != nil {
		return err
	}
	updated, err := s.service.UpdateAgent(ctx, input.AgentID, service.AgentUpdate{
		Version: input.Version, Name: name, Description: description, ModelID: modelID,
		System: system, Tools: tools, Metadata: metadata,
	})
	if err != nil {
		return err
	}
	request.JSON(consts.StatusOK, view.NewAgentResponse(updated))
	return nil
}

func (s *Server) archiveAgent(ctx context.Context, request *hertzapp.RequestContext) error {
	var input view.AgentPathRequest
	if err := bindRequest(request, &input); err != nil {
		return err
	}
	archived, err := s.service.ArchiveAgent(ctx, input.AgentID)
	if err != nil {
		return err
	}
	request.JSON(consts.StatusOK, view.NewAgentResponse(archived))
	return nil
}

func (s *Server) listAgentVersions(ctx context.Context, request *hertzapp.RequestContext) error {
	var input view.ListAgentVersionsRequest
	if err := bindRequest(request, &input); err != nil {
		return err
	}
	query, err := parsePage(input.PageRequest, agentVersionCursor, true)
	if err != nil {
		return err
	}
	page, err := s.service.PageAgentVersions(ctx, input.AgentID, query)
	if err != nil {
		return err
	}
	data := make([]view.AgentResponse, 0, len(page.Items))
	for _, value := range page.Items {
		data = append(data, view.NewAgentResponse(value))
	}
	var first, last service.PageAnchor
	if len(page.Items) > 0 {
		first.Version = page.Items[0].Version
		last.Version = page.Items[len(page.Items)-1].Version
	}
	next, _ := pageLinks(agentVersionCursor, query, page.HasMore, first, last)
	request.JSON(consts.StatusOK, view.Page[view.AgentResponse]{Data: data, NextPage: next})
	return nil
}

func (s *Server) listAgents(ctx context.Context, request *hertzapp.RequestContext) error {
	var input view.ListAgentsRequest
	if err := bindRequest(request, &input); err != nil {
		return err
	}
	query, err := parsePage(input.PageRequest, agentCursor, false)
	if err != nil {
		return err
	}
	page, err := s.service.PageAgents(ctx, query, input.IncludeArchived)
	if err != nil {
		return err
	}
	data := make([]view.AgentResponse, 0, len(page.Items))
	for _, value := range page.Items {
		data = append(data, view.NewAgentResponse(value))
	}
	var first, last service.PageAnchor
	if len(page.Items) > 0 {
		first = service.PageAnchor{CreatedAt: page.Items[0].CreatedAt, ID: page.Items[0].ID}
		value := page.Items[len(page.Items)-1]
		last = service.PageAnchor{CreatedAt: value.CreatedAt, ID: value.ID}
	}
	next, _ := pageLinks(agentCursor, query, page.HasMore, first, last)
	request.JSON(consts.StatusOK, view.Page[view.AgentResponse]{Data: data, NextPage: next})
	return nil
}

func parseModel(raw json.RawMessage) (string, error) {
	var modelID string
	if err := json.Unmarshal(raw, &modelID); err == nil && modelID != "" {
		return modelID, nil
	}
	var value struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(raw, &value); err != nil || value.ID == "" {
		return "", fmt.Errorf("%w: model must be an ID string or object", service.ErrInvalid)
	}
	return value.ID, nil
}

func parseOptionalString(raw json.RawMessage, clearable bool, field string) (*string, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	if strings.TrimSpace(string(raw)) == "null" {
		if !clearable {
			return nil, fmt.Errorf("%w: agent %s cannot be cleared", service.ErrInvalid, field)
		}
		value := ""
		return &value, nil
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, fmt.Errorf("%w: agent %s must be a string", service.ErrInvalid, field)
	}
	return &value, nil
}

func parseOptionalTools(raw json.RawMessage) (*[]map[string]any, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	if strings.TrimSpace(string(raw)) == "null" {
		value := []map[string]any{}
		return &value, nil
	}
	var value []map[string]any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, fmt.Errorf("%w: agent tools must be an array", service.ErrInvalid)
	}
	return &value, nil
}

func parseMetadataPatch(raw json.RawMessage) (map[string]*string, error) {
	if len(raw) == 0 || strings.TrimSpace(string(raw)) == "null" {
		return nil, nil
	}
	var value map[string]*string
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, fmt.Errorf("%w: agent metadata must map keys to strings or null", service.ErrInvalid)
	}
	return value, nil
}
