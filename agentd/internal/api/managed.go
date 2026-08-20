package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	hertzapp "github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/compforge/agentd/agentd/internal/model"
	"github.com/compforge/agentd/agentd/internal/repo"
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
	value, err := s.service.GetAgent(ctx, request.Param("agent_id"))
	if err != nil {
		s.writeError(request, err)
		return
	}
	writeJSON(request, consts.StatusOK, agentResponse(value))
}

func (s *Server) listAgents(ctx context.Context, request *hertzapp.RequestContext) {
	values, err := s.service.ListAgents(ctx)
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

func (s *Server) createEnvironment(ctx context.Context, request *hertzapp.RequestContext) {
	var input struct {
		Name        string            `json:"name"`
		Description string            `json:"description"`
		Config      map[string]any    `json:"config"`
		Metadata    map[string]string `json:"metadata"`
	}
	if !decodeBody(request, &input) {
		return
	}
	if input.Config["type"] != "cloud" {
		s.writeError(request, fmt.Errorf("%w: environment type %q", service.ErrUnsupported, input.Config["type"]))
		return
	}
	created, err := s.service.CreateEnvironment(ctx, model.Environment{
		Name: input.Name, Description: input.Description, Config: input.Config, Metadata: input.Metadata,
	})
	if err != nil {
		s.writeError(request, err)
		return
	}
	writeJSON(request, consts.StatusOK, environmentResponse(created))
}

func (s *Server) getEnvironment(ctx context.Context, request *hertzapp.RequestContext) {
	value, err := s.service.GetEnvironment(ctx, request.Param("environment_id"))
	if err != nil {
		s.writeError(request, err)
		return
	}
	writeJSON(request, consts.StatusOK, environmentResponse(value))
}

func (s *Server) listEnvironments(ctx context.Context, request *hertzapp.RequestContext) {
	values, err := s.service.ListEnvironments(ctx)
	if err != nil {
		s.writeError(request, err)
		return
	}
	data := make([]map[string]any, 0, len(values))
	for _, value := range values {
		data = append(data, environmentResponse(value))
	}
	writeJSON(request, consts.StatusOK, map[string]any{"data": data, "next_page": nil})
}

func (s *Server) createSession(ctx context.Context, request *hertzapp.RequestContext) {
	var input struct {
		Agent         json.RawMessage   `json:"agent"`
		EnvironmentID string            `json:"environment_id"`
		Title         string            `json:"title"`
		Metadata      map[string]string `json:"metadata"`
		InitialEvents []json.RawMessage `json:"initial_events"`
		Budget        json.RawMessage   `json:"budget"`
		Resources     []json.RawMessage `json:"resources"`
		VaultIDs      []string          `json:"vault_ids"`
	}
	if !decodeBody(request, &input) {
		return
	}
	if len(input.InitialEvents) > 0 || present(input.Budget) || len(input.Resources) > 0 || len(input.VaultIDs) > 0 {
		s.writeError(request, fmt.Errorf("%w: initial events, budgets, resources, or vaults", service.ErrUnsupported))
		return
	}
	agentID, version, err := parseAgentReference(input.Agent)
	if err != nil {
		s.writeError(request, err)
		return
	}
	created, err := s.service.CreateSession(ctx, agentID, version, input.EnvironmentID, input.Title, input.Metadata)
	if err != nil {
		s.writeError(request, err)
		return
	}
	agent, err := s.service.GetAgent(ctx, created.AgentID)
	if err != nil {
		s.writeError(request, err)
		return
	}
	writeJSON(request, consts.StatusOK, sessionResponse(created, agent))
}

func (s *Server) getSession(ctx context.Context, request *hertzapp.RequestContext) {
	value, err := s.service.GetSession(ctx, request.Param("session_id"))
	if err != nil {
		s.writeError(request, err)
		return
	}
	agent, err := s.service.GetAgent(ctx, value.AgentID)
	if err != nil {
		s.writeError(request, err)
		return
	}
	writeJSON(request, consts.StatusOK, sessionResponse(value, agent))
}

func (s *Server) listSessions(ctx context.Context, request *hertzapp.RequestContext) {
	values, err := s.service.ListSessions(ctx)
	if err != nil {
		s.writeError(request, err)
		return
	}
	data := make([]map[string]any, 0, len(values))
	for _, value := range values {
		agent, err := s.service.GetAgent(ctx, value.AgentID)
		if err != nil {
			s.writeError(request, err)
			return
		}
		data = append(data, sessionResponse(value, agent))
	}
	writeJSON(request, consts.StatusOK, map[string]any{"data": data, "next_page": nil, "prev_page": nil})
}

func (s *Server) writeError(request *hertzapp.RequestContext, err error) {
	status := consts.StatusInternalServerError
	errorType := "api_error"
	switch {
	case errors.Is(err, repo.ErrNotFound):
		status, errorType = consts.StatusNotFound, "not_found_error"
	case errors.Is(err, service.ErrUnsupported):
		status, errorType = consts.StatusBadRequest, "unsupported_feature"
	case errors.Is(err, service.ErrInvalid), errors.Is(err, service.ErrConflict):
		status, errorType = consts.StatusBadRequest, "invalid_request_error"
	}
	if status >= 500 {
		s.logger.Error("request failed", "method", string(request.Request.Method()), "path", string(request.Request.URI().Path()), "error", err)
	}
	writeJSON(request, status, map[string]any{
		"type": "error", "error": map[string]any{"type": errorType, "message": err.Error()},
	})
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
		return "", fmt.Errorf("%w: model must be an ID string or object", service.ErrConflict)
	}
	return value.ID, nil
}

func parseAgentReference(raw json.RawMessage) (string, int64, error) {
	var id string
	if err := json.Unmarshal(raw, &id); err == nil && id != "" {
		return id, 0, nil
	}
	var reference map[string]any
	if err := json.Unmarshal(raw, &reference); err != nil {
		return "", 0, fmt.Errorf("%w: invalid agent reference", service.ErrConflict)
	}
	id, _ = reference["id"].(string)
	if id == "" {
		return "", 0, fmt.Errorf("%w: agent reference id is required", service.ErrConflict)
	}
	for key := range reference {
		if key != "id" && key != "type" && key != "version" {
			return "", 0, fmt.Errorf("%w: per-session agent overrides", service.ErrUnsupported)
		}
	}
	version, _ := reference["version"].(float64)
	return id, int64(version), nil
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
		"metadata": value.Metadata, "version": value.Version, "archived_at": nil,
		"created_at": value.CreatedAt, "updated_at": value.UpdatedAt,
	}
}

func environmentResponse(value model.Environment) map[string]any {
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
	return map[string]any{
		"id": value.ID, "type": "environment", "name": value.Name, "description": value.Description,
		"config": config, "metadata": value.Metadata, "scope": "account", "archived_at": nil,
		"created_at": value.CreatedAt.Format(time.RFC3339Nano), "updated_at": value.UpdatedAt.Format(time.RFC3339Nano),
	}
}

func sessionResponse(value model.Session, agent model.Agent) map[string]any {
	agentValue := agentResponse(agent)
	delete(agentValue, "metadata")
	delete(agentValue, "created_at")
	delete(agentValue, "updated_at")
	delete(agentValue, "archived_at")
	return map[string]any{
		"id": value.ID, "type": "session", "agent": agentValue, "environment_id": value.EnvironmentID,
		"title": value.Title, "metadata": value.Metadata, "status": value.Status,
		"created_at": value.CreatedAt, "updated_at": value.UpdatedAt, "archived_at": nil,
		"budget": nil, "outcome_evaluations": []any{}, "resources": []any{}, "vault_ids": []any{},
		"deployment_id": nil,
		"stats":         map[string]any{"active_seconds": 0, "duration_seconds": time.Since(value.CreatedAt).Seconds()},
		"usage":         map[string]any{"active_seconds": 0, "cache_creation": map[string]any{}, "cache_read_input_tokens": 0, "input_tokens": 0, "list_cost": nil, "output_tokens": 0, "server_tool_use": nil},
	}
}

func decodeBody(request *hertzapp.RequestContext, target any) bool {
	body, err := request.Body()
	if err == nil {
		err = json.NewDecoder(bytes.NewReader(body)).Decode(target)
	}
	if err != nil {
		writeJSON(request, consts.StatusBadRequest, map[string]any{
			"type": "error", "error": map[string]any{"type": "invalid_request_error", "message": err.Error()},
		})
		return false
	}
	return true
}

func writeJSON(request *hertzapp.RequestContext, status int, value any) {
	request.JSON(status, value)
}

func present(raw json.RawMessage) bool {
	value := strings.TrimSpace(string(raw))
	return value != "" && value != "null" && value != "{}" && value != "[]"
}
