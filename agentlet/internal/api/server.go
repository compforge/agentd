package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	hertzapp "github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/cloudwego/hertz/pkg/protocol/sse"
	"github.com/cloudwego/hertz/pkg/route"
	"github.com/compforge/agentd/agentlet/internal/app"
)

type Server struct {
	app    *app.App
	logger *slog.Logger
}

func New(application *app.App, logger *slog.Logger) *Server {
	return &Server{app: application, logger: logger}
}

func (s *Server) Register(engine *route.Engine) {
	engine.Use(s.requestMetadata())
	engine.GET("/healthz", func(_ context.Context, request *hertzapp.RequestContext) {
		writeJSON(request, consts.StatusOK, map[string]any{"ok": true})
	})
	engine.POST("/v1/agents", s.createAgent)
	engine.GET("/v1/agents", s.listAgents)
	engine.GET("/v1/agents/:agent_id", s.getAgent)
	engine.POST("/v1/environments", s.createEnvironment)
	engine.GET("/v1/environments", s.listEnvironments)
	engine.GET("/v1/environments/:environment_id", s.getEnvironment)
	engine.POST("/v1/sessions", s.createSession)
	engine.GET("/v1/sessions", s.listSessions)
	engine.GET("/v1/sessions/:session_id", s.getSession)
	engine.POST("/v1/sessions/:session_id/events", s.sendEvents)
	engine.GET("/v1/sessions/:session_id/events", s.listEvents)
	engine.GET("/v1/sessions/:session_id/events/stream", s.streamEvents)
}

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
		s.writeError(ctx, request, fmt.Errorf("%w: MCP, skills, and multi-agent agents", app.ErrUnsupported))
		return
	}
	modelID, err := parseModel(input.Model)
	if err != nil {
		s.writeError(ctx, request, err)
		return
	}
	for _, tool := range input.Tools {
		if tool["type"] != "agent_toolset_20260401" {
			s.writeError(ctx, request, fmt.Errorf("%w: agent tool type %q", app.ErrUnsupported, tool["type"]))
			return
		}
		if defaultConfig, ok := tool["default_config"].(map[string]any); ok {
			if policy, ok := defaultConfig["permission_policy"].(map[string]any); ok && policy["type"] == "always_ask" {
				s.writeError(ctx, request, fmt.Errorf("%w: always_ask tool confirmation", app.ErrUnsupported))
				return
			}
		}
	}
	created, err := s.app.CreateAgent(ctx, app.Agent{
		Name: input.Name, Description: input.Description, ModelID: modelID, System: input.System,
		Tools: input.Tools, Metadata: input.Metadata,
	})
	if err != nil {
		s.writeError(ctx, request, err)
		return
	}
	writeJSON(request, consts.StatusOK, agentResponse(created))
}

func (s *Server) getAgent(ctx context.Context, request *hertzapp.RequestContext) {
	value, err := s.app.GetAgent(ctx, request.Param("agent_id"))
	if err != nil {
		s.writeError(ctx, request, err)
		return
	}
	writeJSON(request, consts.StatusOK, agentResponse(value))
}

func (s *Server) listAgents(ctx context.Context, request *hertzapp.RequestContext) {
	values, err := s.app.ListAgents(ctx)
	if err != nil {
		s.writeError(ctx, request, err)
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
		s.writeError(ctx, request, fmt.Errorf("%w: environment type %q", app.ErrUnsupported, input.Config["type"]))
		return
	}
	if networking, ok := input.Config["networking"].(map[string]any); ok && networking["type"] != nil && networking["type"] != "unrestricted" {
		s.writeError(ctx, request, fmt.Errorf("%w: Hostel network policy %q", app.ErrUnsupported, networking["type"]))
		return
	}
	created, err := s.app.CreateEnvironment(ctx, app.Environment{
		Name: input.Name, Description: input.Description, Config: input.Config, Metadata: input.Metadata,
	})
	if err != nil {
		s.writeError(ctx, request, err)
		return
	}
	writeJSON(request, consts.StatusOK, environmentResponse(created))
}

func (s *Server) getEnvironment(ctx context.Context, request *hertzapp.RequestContext) {
	value, err := s.app.GetEnvironment(ctx, request.Param("environment_id"))
	if err != nil {
		s.writeError(ctx, request, err)
		return
	}
	writeJSON(request, consts.StatusOK, environmentResponse(value))
}

func (s *Server) listEnvironments(ctx context.Context, request *hertzapp.RequestContext) {
	values, err := s.app.ListEnvironments(ctx)
	if err != nil {
		s.writeError(ctx, request, err)
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
	if present(input.Budget) || len(input.Resources) > 0 || len(input.VaultIDs) > 0 {
		s.writeError(ctx, request, fmt.Errorf("%w: session budgets, resources, or vaults", app.ErrUnsupported))
		return
	}
	agentID, version, err := parseAgentReference(input.Agent)
	if err != nil {
		s.writeError(ctx, request, err)
		return
	}
	initial, err := parseEvents(input.InitialEvents)
	if err != nil {
		s.writeError(ctx, request, err)
		return
	}
	created, err := s.app.CreateSession(ctx, agentID, version, input.EnvironmentID, input.Title, input.Metadata)
	if err != nil {
		s.writeError(ctx, request, err)
		return
	}
	if len(initial) > 0 {
		if _, err := s.app.SendEvents(ctx, created.ID, initial); err != nil {
			s.writeError(ctx, request, err)
			return
		}
		created, _ = s.app.GetSession(ctx, created.ID)
	}
	writeJSON(request, consts.StatusOK, sessionResponse(created))
}

func (s *Server) getSession(ctx context.Context, request *hertzapp.RequestContext) {
	value, err := s.app.GetSession(ctx, request.Param("session_id"))
	if err != nil {
		s.writeError(ctx, request, err)
		return
	}
	writeJSON(request, consts.StatusOK, sessionResponse(value))
}

func (s *Server) listSessions(ctx context.Context, request *hertzapp.RequestContext) {
	values, err := s.app.ListSessions(ctx)
	if err != nil {
		s.writeError(ctx, request, err)
		return
	}
	data := make([]map[string]any, 0, len(values))
	for _, value := range values {
		data = append(data, sessionResponse(value))
	}
	writeJSON(request, consts.StatusOK, map[string]any{"data": data, "next_page": nil, "prev_page": nil})
}

func (s *Server) sendEvents(ctx context.Context, request *hertzapp.RequestContext) {
	var input struct {
		Events []json.RawMessage `json:"events"`
	}
	if !decodeBody(request, &input) {
		return
	}
	events, err := parseEvents(input.Events)
	if err != nil {
		s.writeError(ctx, request, err)
		return
	}
	accepted, err := s.app.SendEvents(ctx, request.Param("session_id"), events)
	if err != nil {
		s.writeError(ctx, request, err)
		return
	}
	writeJSON(request, consts.StatusOK, map[string]any{"data": accepted})
}

func (s *Server) listEvents(ctx context.Context, request *hertzapp.RequestContext) {
	events, err := s.app.ListEvents(ctx, request.Param("session_id"))
	if err != nil {
		s.writeError(ctx, request, err)
		return
	}
	writeJSON(request, consts.StatusOK, map[string]any{"data": events, "next_page": nil})
}

func (s *Server) streamEvents(ctx context.Context, request *hertzapp.RequestContext) {
	if len(request.QueryArgs().PeekAll("event_deltas[]")) > 0 || len(request.QueryArgs().PeekAll("event_deltas")) > 0 {
		s.writeError(ctx, request, fmt.Errorf("%w: streaming event deltas", app.ErrUnsupported))
		return
	}
	sessionID := request.Param("session_id")
	if _, err := s.app.GetSession(ctx, sessionID); err != nil {
		s.writeError(ctx, request, err)
		return
	}
	channel, cancel := s.app.Subscribe(sessionID)
	defer cancel()
	history, err := s.app.ListEvents(ctx, sessionID)
	if err != nil {
		s.writeError(ctx, request, err)
		return
	}
	request.Header("X-Accel-Buffering", "no")
	writer := sse.NewWriter(request)
	defer writer.Close()
	seen := make(map[string]struct{}, len(history))
	for _, event := range history {
		if err := writeSSE(writer, event); err != nil {
			return
		}
		if id, ok := event["id"].(string); ok {
			seen[id] = struct{}{}
		}
	}
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-channel:
			if id, ok := event["id"].(string); ok {
				if _, duplicate := seen[id]; duplicate {
					continue
				}
				seen[id] = struct{}{}
			}
			if err := writeSSE(writer, event); err != nil {
				return
			}
		}
	}
}

func (s *Server) requestMetadata() hertzapp.HandlerFunc {
	return func(ctx context.Context, request *hertzapp.RequestContext) {
		request.Header("anthropic-beta", "managed-agents-2026-04-01")
		request.Header("request-id", "req-"+fmt.Sprint(time.Now().UnixNano()))
		request.Next(ctx)
	}
}

func (s *Server) writeError(_ context.Context, request *hertzapp.RequestContext, err error) {
	status := consts.StatusInternalServerError
	errorType := "api_error"
	switch {
	case errors.Is(err, app.ErrNotFound):
		status, errorType = consts.StatusNotFound, "not_found_error"
	case errors.Is(err, app.ErrUnsupported):
		status, errorType = consts.StatusBadRequest, "unsupported_feature"
	case errors.Is(err, app.ErrConflict):
		status, errorType = consts.StatusBadRequest, "invalid_request_error"
	}
	if status >= 500 {
		s.logger.Error("request failed", "method", string(request.Request.Method()), "path", string(request.Request.URI().Path()), "error", err)
	}
	writeJSON(request, status, map[string]any{
		"type":  "error",
		"error": map[string]any{"type": errorType, "message": err.Error()},
	})
}

func parseModel(raw json.RawMessage) (string, error) {
	var modelID string
	if err := json.Unmarshal(raw, &modelID); err == nil && modelID != "" {
		return modelID, nil
	}
	var model struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(raw, &model); err != nil || model.ID == "" {
		return "", fmt.Errorf("%w: model must be an ID string or object", app.ErrConflict)
	}
	return model.ID, nil
}

func parseAgentReference(raw json.RawMessage) (string, int64, error) {
	var id string
	if err := json.Unmarshal(raw, &id); err == nil && id != "" {
		return id, 0, nil
	}
	var reference map[string]any
	if err := json.Unmarshal(raw, &reference); err != nil {
		return "", 0, fmt.Errorf("%w: invalid agent reference", app.ErrConflict)
	}
	id, _ = reference["id"].(string)
	if id == "" {
		return "", 0, fmt.Errorf("%w: agent reference id is required", app.ErrConflict)
	}
	for key := range reference {
		if key != "id" && key != "type" && key != "version" {
			return "", 0, fmt.Errorf("%w: per-session agent overrides", app.ErrUnsupported)
		}
	}
	version, _ := reference["version"].(float64)
	return id, int64(version), nil
}

func parseEvents(rawEvents []json.RawMessage) ([]app.IncomingEvent, error) {
	if len(rawEvents) > 50 {
		return nil, fmt.Errorf("%w: at most 50 events may be sent at once", app.ErrConflict)
	}
	events := make([]app.IncomingEvent, 0, len(rawEvents))
	for _, raw := range rawEvents {
		var event struct {
			Type    string           `json:"type"`
			Content []map[string]any `json:"content"`
		}
		if err := json.Unmarshal(raw, &event); err != nil {
			return nil, fmt.Errorf("%w: decode session event: %v", app.ErrConflict, err)
		}
		events = append(events, app.IncomingEvent{Type: event.Type, Content: event.Content})
	}
	return events, nil
}

func agentResponse(value app.Agent) map[string]any {
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

func environmentResponse(value app.Environment) map[string]any {
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

func sessionResponse(value app.Session) map[string]any {
	agent := agentResponse(value.Agent)
	delete(agent, "metadata")
	delete(agent, "created_at")
	delete(agent, "updated_at")
	delete(agent, "archived_at")
	return map[string]any{
		"id": value.ID, "type": "session", "agent": agent, "environment_id": value.EnvironmentID,
		"title": value.Title, "metadata": value.Metadata, "status": value.Control.Status,
		"created_at": value.CreatedAt, "updated_at": value.UpdatedAt, "archived_at": nil,
		"budget": nil, "outcome_evaluations": []any{}, "resources": []any{}, "vault_ids": []any{},
		"deployment_id": nil,
		"stats":         map[string]any{"active_seconds": 0, "duration_seconds": time.Since(value.CreatedAt).Seconds()},
		"usage":         map[string]any{"active_seconds": 0, "cache_creation": map[string]any{}, "cache_read_input_tokens": 0, "input_tokens": 0, "list_cost": nil, "output_tokens": 0, "server_tool_use": nil},
	}
}

func decodeBody(request *hertzapp.RequestContext, target any) bool {
	body, err := request.Body()
	if err == nil && len(body) > 2<<20 {
		err = errors.New("request body exceeds 2 MiB")
	}
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

func writeSSE(writer *sse.Writer, event app.ManagedEvent) error {
	eventType, _ := event["type"].(string)
	encoded, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encode SSE event: %w", err)
	}
	return writer.WriteEvent("", eventType, encoded)
}

func present(raw json.RawMessage) bool {
	value := strings.TrimSpace(string(raw))
	return value != "" && value != "null" && value != "{}" && value != "[]"
}
