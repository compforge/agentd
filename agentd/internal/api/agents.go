package api

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	hertzapp "github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/compforge/agentd/agentd/internal/model"
	"github.com/compforge/agentd/agentd/internal/service"
)

func (s *Server) createAgent(ctx context.Context, request *hertzapp.RequestContext) {
	var input struct {
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
	if !decodeBody(request, &input) {
		return
	}
	if len(input.MCPServers) > 0 || len(input.Skills) > 0 || present(input.Multiagent) {
		s.writeError(request, fmt.Errorf("%w: MCP, skills, and multi-agent agents", service.ErrUnsupported))
		return
	}
	modelID, err := parseModel(input.Model)
	if err != nil {
		s.writeError(request, err)
		return
	}
	created, err := s.service.CreateAgent(ctx, model.Agent{
		Name: input.Name, Description: input.Description, ModelID: modelID, System: input.System,
		Tools: input.Tools, Metadata: input.Metadata,
	})
	if err != nil {
		s.writeError(request, err)
		return
	}
	writeJSON(request, consts.StatusOK, agentResponse(created))
}

func (s *Server) getAgent(ctx context.Context, request *hertzapp.RequestContext) {
	var value model.Agent
	var err error
	if raw := string(request.QueryArgs().Peek("version")); raw != "" {
		version, parseErr := strconv.ParseInt(raw, 10, 64)
		if parseErr != nil {
			s.writeError(request, fmt.Errorf("%w: agent version must be an integer", service.ErrInvalid))
			return
		}
		value, err = s.service.FindAgentVersion(ctx, request.Param("agent_id"), version)
	} else {
		value, err = s.service.GetAgent(ctx, request.Param("agent_id"))
	}
	if err != nil {
		s.writeError(request, err)
		return
	}
	writeJSON(request, consts.StatusOK, agentResponse(value))
}

func (s *Server) updateAgent(ctx context.Context, request *hertzapp.RequestContext) {
	var input struct {
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
	if !decodeBody(request, &input) {
		return
	}
	if len(input.MCPServers) > 0 || len(input.Skills) > 0 || present(input.Multiagent) {
		s.writeError(request, fmt.Errorf("%w: MCP, skills, and multi-agent agents", service.ErrUnsupported))
		return
	}
	name, err := parseOptionalString(input.Name, false, "name")
	if err != nil {
		s.writeError(request, err)
		return
	}
	description, err := parseOptionalString(input.Description, true, "description")
	if err != nil {
		s.writeError(request, err)
		return
	}
	system, err := parseOptionalString(input.System, true, "system")
	if err != nil {
		s.writeError(request, err)
		return
	}
	var modelID *string
	if len(input.Model) > 0 {
		value, parseErr := parseModel(input.Model)
		if parseErr != nil {
			s.writeError(request, parseErr)
			return
		}
		modelID = &value
	}
	tools, err := parseOptionalTools(input.Tools)
	if err != nil {
		s.writeError(request, err)
		return
	}
	metadata, err := parseMetadataPatch(input.Metadata)
	if err != nil {
		s.writeError(request, err)
		return
	}
	updated, err := s.service.UpdateAgent(ctx, request.Param("agent_id"), service.AgentUpdate{
		Version: input.Version, Name: name, Description: description, ModelID: modelID,
		System: system, Tools: tools, Metadata: metadata,
	})
	if err != nil {
		s.writeError(request, err)
		return
	}
	writeJSON(request, consts.StatusOK, agentResponse(updated))
}

func (s *Server) archiveAgent(ctx context.Context, request *hertzapp.RequestContext) {
	archived, err := s.service.ArchiveAgent(ctx, request.Param("agent_id"))
	if err != nil {
		s.writeError(request, err)
		return
	}
	writeJSON(request, consts.StatusOK, agentResponse(archived))
}

func (s *Server) listAgentVersions(ctx context.Context, request *hertzapp.RequestContext) {
	values, err := s.service.ListAgentVersions(ctx, request.Param("agent_id"))
	if err != nil {
		s.writeError(request, err)
		return
	}
	data := make([]map[string]any, 0, len(values))
	for _, value := range values {
		data = append(data, agentResponse(value))
	}
	writeJSON(request, consts.StatusOK, map[string]any{"data": data, "next_page": nil})
}

func (s *Server) listAgents(ctx context.Context, request *hertzapp.RequestContext) {
	values, err := s.service.ListAgents(ctx)
	if err != nil {
		s.writeError(request, err)
		return
	}
	data := make([]map[string]any, 0, len(values))
	includeArchived := string(request.QueryArgs().Peek("include_archived")) == "true"
	for _, value := range values {
		if value.ArchivedAt != nil && !includeArchived {
			continue
		}
		data = append(data, agentResponse(value))
	}
	writeJSON(request, consts.StatusOK, map[string]any{"data": data, "next_page": nil})
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

func agentResponse(value model.Agent) map[string]any {
	tools := make([]map[string]any, 0, len(value.Tools))
	for _, tool := range value.Tools {
		copy := make(map[string]any, len(tool)+2)
		for key, item := range tool {
			copy[key] = item
		}
		if copy["configs"] == nil {
			copy["configs"] = []any{}
		}
		if copy["default_config"] == nil {
			copy["default_config"] = map[string]any{"permission_policy": map[string]any{"type": "always_allow"}}
		}
		tools = append(tools, copy)
	}
	return map[string]any{
		"id": value.ID, "type": "agent", "name": value.Name, "description": value.Description,
		"model": map[string]any{"id": value.ModelID, "speed": "standard"}, "system": value.System,
		"tools": tools, "mcp_servers": []any{}, "skills": []any{}, "multiagent": nil,
		"metadata": value.Metadata, "version": value.Version, "archived_at": value.ArchivedAt,
		"created_at": value.CreatedAt, "updated_at": value.UpdatedAt,
	}
}
