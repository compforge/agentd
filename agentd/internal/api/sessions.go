package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	hertzapp "github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/compforge/agentd/agentd/internal/api/view"
	"github.com/compforge/agentd/agentd/internal/model"
	"github.com/compforge/agentd/agentd/internal/service"
)

// createSession validates every initial Event before creating the Session, then
// persists accepted input through the same ingress boundary as sendEvents.
//
// +case:id=session_initial_events,desc=`create a Session with an initial user.message`,expect=`the Event is durable before the create response and wakes normal reconciliation`,forbid=`a second ingress implementation or an acknowledged but unpersisted Event`,group=system
// +link=agentd/docs/kernel.md
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
		s.writeError(request, fmt.Errorf("%w: budgets, resources, or vaults", service.ErrUnsupported))
		return
	}
	initialEvents, err := decodeInitialEvents(input.InitialEvents)
	if err != nil {
		s.writeError(request, err)
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
	if len(initialEvents) > 0 {
		if _, err := s.appendIngressEvents(ctx, created.ID, initialEvents); err != nil {
			s.writeError(request, fmt.Errorf("persist initial Session Events: %w", err))
			return
		}
		s.logger.InfoContext(ctx, "accepted initial Session Events",
			"session_id", created.ID, "event_count", len(initialEvents))
		s.executionNotifier.Notify()
	}
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
	includeArchived := string(request.QueryArgs().Peek("include_archived")) == "true"
	for _, value := range values {
		if value.ArchivedAt != nil && !includeArchived {
			continue
		}
		agent, err := s.service.GetAgentVersion(ctx, value.AgentVersionID)
		if err != nil {
			s.writeError(request, err)
			return
		}
		data = append(data, sessionResponse(value, agent))
	}
	writeJSON(request, consts.StatusOK, map[string]any{"data": data, "next_page": nil, "prev_page": nil})
}

func (s *Server) updateSession(ctx context.Context, request *hertzapp.RequestContext) {
	var input struct {
		Title    json.RawMessage `json:"title"`
		Metadata json.RawMessage `json:"metadata"`
		Agent    json.RawMessage `json:"agent"`
		Budget   json.RawMessage `json:"budget"`
		VaultIDs []string        `json:"vault_ids"`
	}
	if !decodeBody(request, &input) {
		return
	}
	if present(input.Agent) || present(input.Budget) || len(input.VaultIDs) > 0 {
		s.writeError(request, fmt.Errorf("%w: Session agent overrides, budgets, or vaults", service.ErrUnsupported))
		return
	}
	title, err := parseSessionTitle(input.Title)
	if err != nil {
		s.writeError(request, err)
		return
	}
	metadata, err := parseSessionMetadataPatch(input.Metadata)
	if err != nil {
		s.writeError(request, err)
		return
	}
	updated, err := s.service.UpdateSession(ctx, request.Param("session_id"), service.SessionUpdate{
		Title: title, Metadata: metadata,
	})
	if err != nil {
		s.writeError(request, err)
		return
	}
	agent, err := s.service.GetAgentVersion(ctx, updated.AgentVersionID)
	if err != nil {
		s.writeError(request, err)
		return
	}
	writeJSON(request, consts.StatusOK, sessionResponse(updated, agent))
}

// +case:id=session_archive_preserves_history,desc=`archive an idle Session after it has executed Events`,expect=`terminated Session remains readable with its Event history`,forbid=`accepting new ingress or deleting Ledger history`,group=system
// +link=agentd/docs/kernel.md
func (s *Server) archiveSession(ctx context.Context, request *hertzapp.RequestContext) {
	archived, err := s.service.ArchiveSession(ctx, request.Param("session_id"))
	if err != nil {
		s.writeError(request, err)
		return
	}
	s.logger.InfoContext(ctx, "archived Session", "session_id", archived.ID)
	s.executionNotifier.Notify()
	agent, err := s.service.GetAgentVersion(ctx, archived.AgentVersionID)
	if err != nil {
		s.writeError(request, err)
		return
	}
	writeJSON(request, consts.StatusOK, sessionResponse(archived, agent))
}

func decodeInitialEvents(rawEvents []json.RawMessage) ([]view.IngressEvent, error) {
	if len(rawEvents) == 0 {
		return nil, nil
	}
	ingress, err := view.DecodeIngressEvents(rawEvents)
	if err != nil {
		if errors.Is(err, view.ErrUnsupported) {
			return nil, fmt.Errorf("%w: %v", service.ErrUnsupported, err)
		}
		return nil, fmt.Errorf("%w: %v", service.ErrInvalid, err)
	}
	for _, event := range ingress {
		if event.Type != "user.message" {
			return nil, fmt.Errorf("%w: initial Event type %q", service.ErrUnsupported, event.Type)
		}
	}
	return ingress, nil
}

func parseSessionTitle(raw json.RawMessage) (*string, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	if strings.TrimSpace(string(raw)) == "null" {
		value := ""
		return &value, nil
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, fmt.Errorf("%w: Session title must be a string", service.ErrInvalid)
	}
	return &value, nil
}

func parseSessionMetadataPatch(raw json.RawMessage) (map[string]*string, error) {
	if len(raw) == 0 || strings.TrimSpace(string(raw)) == "null" {
		return nil, nil
	}
	var value map[string]*string
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, fmt.Errorf("%w: Session metadata must map keys to strings or null", service.ErrInvalid)
	}
	return value, nil
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
		"created_at": value.CreatedAt, "updated_at": value.UpdatedAt, "archived_at": value.ArchivedAt,
		"budget": nil, "outcome_evaluations": []any{}, "resources": []any{}, "vault_ids": []any{},
		"deployment_id": nil,
		"stats":         map[string]any{"active_seconds": 0, "duration_seconds": time.Since(value.CreatedAt).Seconds()},
		"usage":         map[string]any{"active_seconds": 0, "cache_creation": map[string]any{}, "cache_read_input_tokens": 0, "input_tokens": 0, "list_cost": nil, "output_tokens": 0, "server_tool_use": nil},
	}
}
