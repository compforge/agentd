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
	"github.com/compforge/agentd/agentlet/internal/service"
	"github.com/compforge/agentd/internal/executionapi"
)

type Server struct {
	service  *service.Service
	logger   *slog.Logger
	workerID string
}

type Option func(*Server)

func WithWorkerID(workerID string) Option {
	return func(server *Server) { server.workerID = workerID }
}

func New(executionService *service.Service, logger *slog.Logger, options ...Option) *Server {
	server := &Server{service: executionService, logger: logger}
	for _, option := range options {
		option(server)
	}
	return server
}

func (s *Server) Register(engine *route.Engine) {
	engine.GET("/healthz", func(_ context.Context, request *hertzapp.RequestContext) {
		writeJSON(request, consts.StatusOK, map[string]any{"ok": true})
	})
	engine.POST("/internal/v1/agents", s.createAgent)
	engine.GET("/internal/v1/agents", s.listAgents)
	engine.GET("/internal/v1/agents/:agent_id", s.getAgent)
	engine.POST("/internal/v1/environments", s.createEnvironment)
	engine.GET("/internal/v1/environments", s.listEnvironments)
	engine.GET("/internal/v1/environments/:environment_id", s.getEnvironment)
	engine.POST("/internal/v1/sessions", s.createSession)
	engine.GET("/internal/v1/sessions", s.listSessions)
	engine.PUT("/internal/v1/sessions/:session_id", s.applyWorkSpec)
	engine.GET("/internal/v1/sessions/:session_id", s.getSession)
	engine.GET("/internal/v1/sessions/:session_id/state", s.getExecutionState)
	engine.POST("/internal/v1/sessions/:session_id/events", s.sendEvents)
	engine.GET("/internal/v1/sessions/:session_id/events", s.listEvents)
	engine.GET("/internal/v1/sessions/:session_id/events/stream", s.streamEvents)
}

func (s *Server) applyWorkSpec(ctx context.Context, request *hertzapp.RequestContext) {
	var spec executionapi.WorkSpec
	if !decodeBody(request, &spec) {
		return
	}
	sessionID := request.Param("session_id")
	if spec.Session.ID != sessionID || spec.AssignmentID != string(request.GetHeader(executionapi.AssignmentHeader)) ||
		spec.WorkerID != string(request.GetHeader(executionapi.WorkerHeader)) {
		s.writeError(ctx, request, fmt.Errorf("%w: WorkSpec path or Assignment headers do not match", service.ErrConflict))
		return
	}
	if s.workerID != "" && spec.WorkerID != s.workerID {
		s.writeError(ctx, request, fmt.Errorf(
			"%w: Work targets Worker %q, Agentlet is %q", service.ErrConflict, spec.WorkerID, s.workerID,
		))
		return
	}
	if _, err := s.service.ApplyWorkSpec(ctx, spec); err != nil {
		s.writeError(ctx, request, err)
		return
	}
	writeJSON(request, consts.StatusOK, map[string]any{"ok": true})
}

func (s *Server) getExecutionState(ctx context.Context, request *hertzapp.RequestContext) {
	if !s.requireAssignment(ctx, request) {
		return
	}
	state, err := s.service.ExecutionState(ctx, request.Param("session_id"))
	if err != nil {
		s.writeError(ctx, request, err)
		return
	}
	writeJSON(request, consts.StatusOK, state)
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
		s.writeError(ctx, request, fmt.Errorf("%w: MCP, skills, and multi-agent agents", service.ErrUnsupported))
		return
	}
	modelID, err := parseModel(input.Model)
	if err != nil {
		s.writeError(ctx, request, err)
		return
	}
	for _, tool := range input.Tools {
		if tool["type"] != "agent_toolset_20260401" {
			s.writeError(ctx, request, fmt.Errorf("%w: agent tool type %q", service.ErrUnsupported, tool["type"]))
			return
		}
		if defaultConfig, ok := tool["default_config"].(map[string]any); ok {
			if policy, ok := defaultConfig["permission_policy"].(map[string]any); ok && policy["type"] == "always_ask" {
				s.writeError(ctx, request, fmt.Errorf("%w: always_ask tool confirmation", service.ErrUnsupported))
				return
			}
		}
	}
	created, err := s.service.CreateAgent(ctx, service.Agent{
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
	value, err := s.service.GetAgent(ctx, request.Param("agent_id"))
	if err != nil {
		s.writeError(ctx, request, err)
		return
	}
	writeJSON(request, consts.StatusOK, agentResponse(value))
}

func (s *Server) listAgents(ctx context.Context, request *hertzapp.RequestContext) {
	values, err := s.service.ListAgents(ctx)
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
		s.writeError(ctx, request, fmt.Errorf("%w: environment type %q", service.ErrUnsupported, input.Config["type"]))
		return
	}
	if networking, ok := input.Config["networking"].(map[string]any); ok && networking["type"] != nil && networking["type"] != "unrestricted" {
		s.writeError(ctx, request, fmt.Errorf("%w: sandbox network policy %q", service.ErrUnsupported, networking["type"]))
		return
	}
	created, err := s.service.CreateEnvironment(ctx, service.Environment{
		Name: input.Name, Description: input.Description, Config: input.Config, Metadata: input.Metadata,
	})
	if err != nil {
		s.writeError(ctx, request, err)
		return
	}
	writeJSON(request, consts.StatusOK, environmentResponse(created))
}

func (s *Server) getEnvironment(ctx context.Context, request *hertzapp.RequestContext) {
	value, err := s.service.GetEnvironment(ctx, request.Param("environment_id"))
	if err != nil {
		s.writeError(ctx, request, err)
		return
	}
	writeJSON(request, consts.StatusOK, environmentResponse(value))
}

func (s *Server) listEnvironments(ctx context.Context, request *hertzapp.RequestContext) {
	values, err := s.service.ListEnvironments(ctx)
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
		s.writeError(ctx, request, fmt.Errorf("%w: session budgets, resources, or vaults", service.ErrUnsupported))
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
	created, err := s.service.CreateSession(ctx, agentID, version, input.EnvironmentID, input.Title, input.Metadata)
	if err != nil {
		s.writeError(ctx, request, err)
		return
	}
	if len(initial) > 0 {
		if _, err := s.service.SendEvents(ctx, created.ID, initial); err != nil {
			s.writeError(ctx, request, err)
			return
		}
		created, _ = s.service.GetSession(ctx, created.ID)
	}
	writeJSON(request, consts.StatusOK, sessionResponse(created))
}

func (s *Server) getSession(ctx context.Context, request *hertzapp.RequestContext) {
	if !s.requireAssignment(ctx, request) {
		return
	}
	value, err := s.service.GetSession(ctx, request.Param("session_id"))
	if err != nil {
		s.writeError(ctx, request, err)
		return
	}
	writeJSON(request, consts.StatusOK, sessionResponse(value))
}

func (s *Server) listSessions(ctx context.Context, request *hertzapp.RequestContext) {
	values, err := s.service.ListSessions(ctx)
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
	if !s.requireAssignment(ctx, request) {
		return
	}
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
	accepted, err := s.service.SendEvents(ctx, request.Param("session_id"), events)
	if err != nil {
		s.writeError(ctx, request, err)
		return
	}
	writeJSON(request, consts.StatusOK, map[string]any{"data": accepted})
}

func (s *Server) listEvents(ctx context.Context, request *hertzapp.RequestContext) {
	if !s.requireAssignment(ctx, request) {
		return
	}
	events, err := s.service.ListEvents(ctx, request.Param("session_id"))
	if err != nil {
		s.writeError(ctx, request, err)
		return
	}
	writeJSON(request, consts.StatusOK, map[string]any{"data": events, "next_page": nil})
}

func (s *Server) streamEvents(ctx context.Context, request *hertzapp.RequestContext) {
	if !s.requireAssignment(ctx, request) {
		return
	}
	if len(request.QueryArgs().PeekAll("event_deltas[]")) > 0 || len(request.QueryArgs().PeekAll("event_deltas")) > 0 {
		s.writeError(ctx, request, fmt.Errorf("%w: streaming event deltas", service.ErrUnsupported))
		return
	}
	sessionID := request.Param("session_id")
	if _, err := s.service.GetSession(ctx, sessionID); err != nil {
		s.writeError(ctx, request, err)
		return
	}
	channel, cancel := s.service.Subscribe(sessionID)
	defer cancel()
	history, err := s.service.ListEvents(ctx, sessionID)
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

func (s *Server) requireAssignment(ctx context.Context, request *hertzapp.RequestContext) bool {
	err := s.service.ValidateAssignment(
		ctx,
		request.Param("session_id"),
		string(request.GetHeader(executionapi.WorkerHeader)),
		string(request.GetHeader(executionapi.AssignmentHeader)),
	)
	if err != nil {
		s.writeError(ctx, request, err)
		return false
	}
	return true
}

func (s *Server) writeError(_ context.Context, request *hertzapp.RequestContext, err error) {
	status := consts.StatusInternalServerError
	errorType := "api_error"
	switch {
	case errors.Is(err, service.ErrNotFound):
		status, errorType = consts.StatusNotFound, "not_found_error"
	case errors.Is(err, service.ErrUnsupported):
		status, errorType = consts.StatusBadRequest, "unsupported_feature"
	case errors.Is(err, service.ErrConflict):
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
		return "", fmt.Errorf("%w: model must be an ID string or object", service.ErrConflict)
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

func parseEvents(rawEvents []json.RawMessage) ([]service.IncomingEvent, error) {
	if len(rawEvents) > 50 {
		return nil, fmt.Errorf("%w: at most 50 events may be sent at once", service.ErrConflict)
	}
	events := make([]service.IncomingEvent, 0, len(rawEvents))
	for _, raw := range rawEvents {
		var event struct {
			Type    string           `json:"type"`
			Content []map[string]any `json:"content"`
		}
		if err := json.Unmarshal(raw, &event); err != nil {
			return nil, fmt.Errorf("%w: decode session event: %v", service.ErrConflict, err)
		}
		events = append(events, service.IncomingEvent{Type: event.Type, Content: event.Content})
	}
	return events, nil
}

func agentResponse(value service.Agent) map[string]any {
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

func environmentResponse(value service.Environment) map[string]any {
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

func sessionResponse(value service.Session) map[string]any {
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

func writeSSE(writer *sse.Writer, event service.ManagedEvent) error {
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
