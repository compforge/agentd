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
func (s *Server) createSession(ctx context.Context, request *hertzapp.RequestContext) error {
	var input view.CreateSessionRequest
	if err := bindRequest(request, &input); err != nil {
		return err
	}
	if present(input.Budget) || len(input.Resources) > 0 || len(input.VaultIDs) > 0 {
		return fmt.Errorf("%w: budgets, resources, or vaults", service.ErrUnsupported)
	}
	initialEvents, err := decodeInitialEvents(input.InitialEvents)
	if err != nil {
		return err
	}
	agentID, version, err := parseAgentReference(input.Agent)
	if err != nil {
		return err
	}
	created, err := s.service.CreateSession(ctx, agentID, version, input.EnvironmentID, input.Title, input.Metadata)
	if err != nil {
		return err
	}
	s.logger.InfoContext(ctx, "created Session", "session_id", created.ID,
		"agent_id", created.AgentID, "environment_id", created.EnvironmentID)
	if len(initialEvents) > 0 {
		if _, err := s.appendIngressEvents(ctx, created.ID, initialEvents); err != nil {
			return fmt.Errorf("persist initial Session Events: %w", err)
		}
		s.logger.InfoContext(ctx, "accepted initial Session Events",
			"session_id", created.ID, "event_count", len(initialEvents))
		s.executionNotifier.Notify()
	}
	agent, err := s.service.GetAgentVersion(ctx, created.AgentVersionID)
	if err != nil {
		return err
	}
	response, err := s.sessionResponse(ctx, created, agent)
	if err != nil {
		return err
	}
	request.JSON(consts.StatusOK, response)
	return nil
}

func (s *Server) getSession(ctx context.Context, request *hertzapp.RequestContext) error {
	var input view.SessionPathRequest
	if err := bindRequest(request, &input); err != nil {
		return err
	}
	value, err := s.service.GetSession(ctx, input.SessionID)
	if err != nil {
		return err
	}
	agent, err := s.service.GetAgentVersion(ctx, value.AgentVersionID)
	if err != nil {
		return err
	}
	response, err := s.sessionResponse(ctx, value, agent)
	if err != nil {
		return err
	}
	request.JSON(consts.StatusOK, response)
	return nil
}

func (s *Server) listSessions(ctx context.Context, request *hertzapp.RequestContext) error {
	var input view.ListSessionsRequest
	if err := bindRequest(request, &input); err != nil {
		return err
	}
	descending := false
	switch input.Order {
	case "", "desc":
		descending = true
	case "asc":
	default:
		return fmt.Errorf(
			"%w: order must be asc or desc", service.ErrInvalid,
		)
	}
	query, err := parsePage(input.PageRequest, sessionCursor, descending)
	if err != nil {
		return err
	}
	page, err := s.service.PageSessions(ctx, query, input.IncludeArchived)
	if err != nil {
		return err
	}
	data := make([]view.SessionResponse, 0, len(page.Items))
	for _, value := range page.Items {
		agent, err := s.service.GetAgentVersion(ctx, value.AgentVersionID)
		if err != nil {
			return err
		}
		response, err := s.sessionResponse(ctx, value, agent)
		if err != nil {
			return err
		}
		data = append(data, response)
	}
	var first, last service.PageAnchor
	if len(page.Items) > 0 {
		first = service.PageAnchor{CreatedAt: page.Items[0].CreatedAt, ID: page.Items[0].ID}
		value := page.Items[len(page.Items)-1]
		last = service.PageAnchor{CreatedAt: value.CreatedAt, ID: value.ID}
	}
	next, previous := pageLinks(sessionCursor, query, page.HasMore, first, last)
	request.JSON(consts.StatusOK, view.BidirectionalPage[view.SessionResponse]{
		Data: data, NextPage: next, PrevPage: previous,
	})
	return nil
}

func (s *Server) updateSession(ctx context.Context, request *hertzapp.RequestContext) error {
	var input view.UpdateSessionRequest
	if err := bindRequest(request, &input); err != nil {
		return err
	}
	if present(input.Agent) || present(input.Budget) || len(input.VaultIDs) > 0 {
		return fmt.Errorf(
			"%w: Session agent overrides, budgets, or vaults", service.ErrUnsupported,
		)
	}
	title, err := parseSessionTitle(input.Title)
	if err != nil {
		return err
	}
	metadata, err := parseSessionMetadataPatch(input.Metadata)
	if err != nil {
		return err
	}
	updated, err := s.service.UpdateSession(ctx, input.SessionID, service.SessionUpdate{
		Title: title, Metadata: metadata,
	})
	if err != nil {
		return err
	}
	agent, err := s.service.GetAgentVersion(ctx, updated.AgentVersionID)
	if err != nil {
		return err
	}
	response, err := s.sessionResponse(ctx, updated, agent)
	if err != nil {
		return err
	}
	request.JSON(consts.StatusOK, response)
	return nil
}

// +case:id=session_archive_preserves_history,desc=`archive an idle Session after it has executed Events`,expect=`terminated Session remains readable with its Event history`,forbid=`accepting new ingress or deleting Ledger history`,group=system
// +link=agentd/docs/kernel.md
func (s *Server) archiveSession(ctx context.Context, request *hertzapp.RequestContext) error {
	var input view.SessionPathRequest
	if err := bindRequest(request, &input); err != nil {
		return err
	}
	archived, err := s.service.ArchiveSession(ctx, input.SessionID)
	if err != nil {
		return err
	}
	s.logger.InfoContext(ctx, "archived Session", "session_id", archived.ID)
	s.executionNotifier.Notify()
	agent, err := s.service.GetAgentVersion(ctx, archived.AgentVersionID)
	if err != nil {
		return err
	}
	response, err := s.sessionResponse(ctx, archived, agent)
	if err != nil {
		return err
	}
	request.JSON(consts.StatusOK, response)
	return nil
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

func (s *Server) sessionResponse(ctx context.Context, value model.Session, agent model.Agent) (view.SessionResponse, error) {
	usage, err := s.events.SessionUsage(ctx, value.ID)
	if err != nil {
		return view.SessionResponse{}, err
	}
	durationEnd := time.Now()
	if value.Status == "terminated" {
		durationEnd = value.UpdatedAt
	}
	durationSeconds := max(durationEnd.Sub(value.CreatedAt).Seconds(), 0)
	return view.NewSessionResponse(value, agent, durationSeconds, view.SessionUsageResponse{
		// Ledger currently records cache creation as one combined value. The public
		// contract splits it by TTL, so do not invent a 5m/1h attribution here.
		CacheCreation:        view.CacheCreationResponse{},
		CacheReadInputTokens: usage.CacheReadInputTokens,
		InputTokens:          usage.InputTokens, OutputTokens: usage.OutputTokens,
	}), nil
}
