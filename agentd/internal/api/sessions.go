package api

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	hertzapp "github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/compforge/agentd/agentd/internal/model"
	"github.com/compforge/agentd/agentd/internal/service"
)

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
	s.logger.InfoContext(ctx, "created Session", "session_id", created.ID,
		"agent_id", created.AgentID, "environment_id", created.EnvironmentID)
	agent, err := s.service.GetAgentVersion(ctx, created.AgentVersionID)
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
	agent, err := s.service.GetAgentVersion(ctx, value.AgentVersionID)
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
		agent, err := s.service.GetAgentVersion(ctx, value.AgentVersionID)
		if err != nil {
			s.writeError(request, err)
			return
		}
		data = append(data, sessionResponse(value, agent))
	}
	writeJSON(request, consts.StatusOK, map[string]any{"data": data, "next_page": nil, "prev_page": nil})
}

func parseAgentReference(raw json.RawMessage) (string, int64, error) {
	var id string
	if err := json.Unmarshal(raw, &id); err == nil && id != "" {
		return id, 0, nil
	}
	var reference map[string]any
	if err := json.Unmarshal(raw, &reference); err != nil {
		return "", 0, fmt.Errorf("%w: invalid agent reference", service.ErrInvalid)
	}
	id, _ = reference["id"].(string)
	if id == "" {
		return "", 0, fmt.Errorf("%w: agent reference id is required", service.ErrInvalid)
	}
	for key := range reference {
		if key != "id" && key != "type" && key != "version" {
			return "", 0, fmt.Errorf("%w: per-session agent overrides", service.ErrUnsupported)
		}
	}
	version, _ := reference["version"].(float64)
	return id, int64(version), nil
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
