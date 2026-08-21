package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	hertzapp "github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/cloudwego/hertz/pkg/protocol/sse"
	"github.com/compforge/agentd/agentd/internal/api/view"
	"github.com/compforge/agentd/agentd/internal/model"
	"github.com/compforge/agentd/agentd/internal/service"
	"github.com/compforge/agentd/agentd/internal/session/connector"
	managedevent "github.com/compforge/agentd/internal/event"
)

func (s *Server) sendEvents(ctx context.Context, request *hertzapp.RequestContext) {
	var input view.SendEventsRequest
	if !decodeBody(request, &input) {
		return
	}
	ingress, err := view.DecodeIngressEvents(input.Events)
	if err != nil {
		if errors.Is(err, view.ErrUnsupported) {
			err = fmt.Errorf("%w: %v", service.ErrUnsupported, err)
		} else {
			err = fmt.Errorf("%w: %v", service.ErrInvalid, err)
		}
		s.writeError(request, err)
		return
	}

	sessionID := request.Param("session_id")
	session, err := s.service.GetSession(ctx, sessionID)
	if err != nil {
		s.writeError(request, err)
		return
	}
	if session.Status == model.SessionStatusTerminated {
		s.writeError(request, fmt.Errorf("%w: Session %q is terminated", service.ErrConflict, sessionID))
		return
	}

	var hasMessage, hasInterrupt bool
	for _, item := range ingress {
		hasMessage = hasMessage || item.Type == "user.message"
		hasInterrupt = hasInterrupt || item.Type == "user.interrupt"
	}
	accepted := make([]managedevent.ManagedEvent, 0, len(ingress))
	for _, item := range ingress {
		value := managedevent.New(item.Type, nil)
		if item.Type == "user.message" {
			value["content"] = item.Content
			value["processed_at"] = nil
		}
		if err := s.events.AppendIngress(ctx, sessionID, value); err != nil {
			s.writeError(request, fmt.Errorf("persist Session ingress Event: %w", err))
			return
		}
		accepted = append(accepted, value)
	}

	if hasMessage {
		s.executionNotifier.Notify()
	}
	if hasInterrupt {
		execution, err := s.service.CurrentExecution(ctx, sessionID)
		if errors.Is(err, service.ErrNoAssignment) {
			writeJSON(request, consts.StatusOK, view.Page[managedevent.ManagedEvent]{Data: accepted})
			return
		}
		if err != nil {
			s.writeError(request, err)
			return
		}
		target := connector.Target{Endpoint: execution.Endpoint, Work: execution.Work}
		if err := s.connector.Ensure(ctx, target); err != nil {
			s.writeError(request, fmt.Errorf("%w: prepare Agentlet interrupt: %v", service.ErrUnavailable, err))
			return
		}
		if err := s.connector.Interrupt(ctx, target); err != nil {
			s.writeError(request, fmt.Errorf("%w: interrupt Agentlet Session: %v", service.ErrUnavailable, err))
			return
		}
	}
	writeJSON(request, consts.StatusOK, view.Page[managedevent.ManagedEvent]{Data: accepted})
}

func (s *Server) listEvents(ctx context.Context, request *hertzapp.RequestContext) {
	sessionID := request.Param("session_id")
	if _, err := s.service.GetSession(ctx, sessionID); err != nil {
		s.writeError(request, err)
		return
	}
	events, err := s.events.List(ctx, sessionID)
	if err != nil {
		s.writeError(request, err)
		return
	}
	writeJSON(request, consts.StatusOK, view.Page[managedevent.ManagedEvent]{Data: events})
}

func (s *Server) streamEvents(ctx context.Context, request *hertzapp.RequestContext) {
	if len(request.QueryArgs().PeekAll("event_deltas[]")) > 0 || len(request.QueryArgs().PeekAll("event_deltas")) > 0 {
		s.writeError(request, fmt.Errorf("%w: streaming event deltas", service.ErrUnsupported))
		return
	}
	sessionID := request.Param("session_id")
	if _, err := s.service.GetSession(ctx, sessionID); err != nil {
		s.writeError(request, err)
		return
	}
	history, cursor, err := s.events.Load(ctx, sessionID, 0)
	if err != nil {
		s.writeError(request, err)
		return
	}
	request.Header("X-Accel-Buffering", "no")
	writer := sse.NewWriter(request)
	defer writer.Close()
	for _, event := range history {
		if err := writeEventSSE(writer, event); err != nil {
			return
		}
	}
	ticker := time.NewTicker(s.eventPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			events, nextCursor, err := s.events.Load(ctx, sessionID, cursor)
			if err != nil {
				s.logger.Error("poll persisted Session Events", "session_id", sessionID, "error", err)
				return
			}
			for _, event := range events {
				if err := writeEventSSE(writer, event); err != nil {
					return
				}
			}
			cursor = nextCursor
		}
	}
}

func writeEventSSE(writer *sse.Writer, event managedevent.ManagedEvent) error {
	eventType, _ := event["type"].(string)
	encoded, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encode SSE event: %w", err)
	}
	return writer.WriteEvent("", eventType, encoded)
}
