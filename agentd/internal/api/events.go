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

func (s *Server) sendEvents(ctx context.Context, request *hertzapp.RequestContext) error {
	var input view.SendEventsRequest
	if err := bindRequest(request, &input); err != nil {
		return err
	}
	ingress, err := decodeIngressEvents(input.Events)
	if err != nil {
		return err
	}
	retryCount, err := parseStainlessRetryCount(
		request.Request.Header.Peek(stainlessRetryCountHeader),
	)
	if err != nil {
		return fmt.Errorf("%w: %v", service.ErrInvalid, err)
	}
	flags := summarizeIngress(ingress)

	sessionID := input.SessionID
	session, err := s.service.GetSession(ctx, sessionID)
	if err != nil {
		return err
	}
	if retryCount > 0 {
		reused, ok, err := s.latestRetriedUserMessage(ctx, sessionID, ingress)
		if err != nil {
			return err
		}
		if ok {
			s.logger.InfoContext(ctx, "reused Session ingress Event for SDK retry",
				"session_id", sessionID, "event_id", reused["id"],
				"stainless_retry_count", retryCount)
			if flags.hasInput {
				s.executionNotifier.Notify()
			}
			request.JSON(consts.StatusOK, view.Page[managedevent.ManagedEvent]{
				Data: []managedevent.ManagedEvent{reused},
			})
			return nil
		}
	}
	if session.ArchivedAt != nil {
		return fmt.Errorf(
			"%w: Session %q is archived", service.ErrConflict, sessionID,
		)
	}
	if session.Status == model.SessionStatusTerminated {
		return fmt.Errorf(
			"%w: Session %q is terminated", service.ErrConflict, sessionID,
		)
	}

	blocking, err := s.events.UnresolvedToolUses(ctx, sessionID)
	if err != nil {
		return fmt.Errorf("read Session required actions: %w", err)
	}
	blockingIDs := make(map[string]bool, len(blocking))
	for _, value := range blocking {
		id, _ := value["id"].(string)
		blockingIDs[id] = true
	}
	if err := validateIngressSequence(ingress, blockingIDs); err != nil {
		return err
	}
	if flags.hasToolResult {
		environment, err := s.service.GetEnvironment(ctx, session.EnvironmentID)
		if err != nil {
			return err
		}
		if environment.Config["type"] != "self_hosted" {
			return fmt.Errorf(
				"%w: user.tool_result requires a self_hosted Environment", service.ErrUnsupported,
			)
		}
	}
	accepted, err := s.appendIngressEvents(ctx, sessionID, ingress)
	if err != nil {
		return err
	}
	s.logger.InfoContext(ctx, "accepted Session ingress Events",
		"session_id", sessionID, "event_count", len(accepted),
		"has_message", flags.hasMessage, "has_interrupt", flags.hasInterrupt,
		"stainless_retry_count", retryCount)

	if flags.hasInput {
		s.executionNotifier.Notify()
	}
	if flags.hasInterrupt {
		execution, err := s.service.CurrentExecution(ctx, sessionID)
		if errors.Is(err, service.ErrNoAssignment) {
			request.JSON(consts.StatusOK, view.Page[managedevent.ManagedEvent]{Data: accepted})
			return nil
		}
		if err != nil {
			return err
		}
		target := connector.Target{Endpoint: execution.Endpoint, Work: execution.Work}
		if err := s.connector.Ensure(ctx, target); err != nil {
			return fmt.Errorf(
				"%w: prepare Agentlet interrupt: %v", service.ErrUnavailable, err,
			)
		}
		if err := s.connector.Interrupt(ctx, target); err != nil {
			return fmt.Errorf(
				"%w: interrupt Agentlet Session: %v", service.ErrUnavailable, err,
			)
		}
	}
	request.JSON(consts.StatusOK, view.Page[managedevent.ManagedEvent]{Data: accepted})
	return nil
}

// latestRetriedUserMessage is a transitional retry heuristic. A positive
// X-Stainless-Retry-Count says that the official Anthropic SDK retried a request,
// but it does not identify which accepted request is being retried. For now,
// agentd treats a matching most-recent single user.message as that request. Keep
// this policy local to ingress and refine it when real traffic provides stronger
// correlation requirements.
//
// +spec=`A positive X-Stainless-Retry-Count reuses the latest identical single user.message Event instead of appending duplicate durable input`
// +ideal=`Replace latest-message matching with a stable client request identity when the public protocol supports one`
func (s *Server) latestRetriedUserMessage(
	ctx context.Context,
	sessionID string,
	ingress []view.IngressEvent,
) (managedevent.ManagedEvent, bool, error) {
	if len(ingress) != 1 || ingress[0].Type != "user.message" {
		return nil, false, nil
	}
	events, err := s.events.List(ctx, sessionID)
	if err != nil {
		return nil, false, fmt.Errorf("read recent Session ingress Event: %w", err)
	}
	for index := len(events) - 1; index >= 0; index-- {
		if events[index]["type"] != "user.message" {
			continue
		}
		storedContent, err := json.Marshal(events[index]["content"])
		if err != nil {
			return nil, false, fmt.Errorf("encode recent user.message content: %w", err)
		}
		incomingContent, err := json.Marshal(ingress[0].Content)
		if err != nil {
			return nil, false, fmt.Errorf("encode retried user.message content: %w", err)
		}
		if string(storedContent) == string(incomingContent) {
			return events[index], true, nil
		}
		return nil, false, nil
	}
	return nil, false, nil
}

type ingressFlags struct {
	hasInput      bool
	hasMessage    bool
	hasInterrupt  bool
	hasToolResult bool
}

func decodeIngressEvents(rawEvents []json.RawMessage) ([]view.IngressEvent, error) {
	ingress, err := view.DecodeIngressEvents(rawEvents)
	if err == nil {
		return ingress, nil
	}
	if errors.Is(err, view.ErrUnsupported) {
		return nil, fmt.Errorf("%w: %v", service.ErrUnsupported, err)
	}
	return nil, fmt.Errorf("%w: %v", service.ErrInvalid, err)
}

func summarizeIngress(ingress []view.IngressEvent) ingressFlags {
	var flags ingressFlags
	for _, item := range ingress {
		flags.hasMessage = flags.hasMessage || item.Type == "user.message"
		flags.hasInterrupt = flags.hasInterrupt || item.Type == "user.interrupt"
		flags.hasInput = flags.hasInput || item.Type != "user.interrupt"
		flags.hasToolResult = flags.hasToolResult || item.Type == "user.tool_result"
	}
	return flags
}

func (s *Server) appendIngressEvents(
	ctx context.Context,
	sessionID string,
	ingress []view.IngressEvent,
) ([]managedevent.ManagedEvent, error) {
	accepted := make([]managedevent.ManagedEvent, 0, len(ingress))
	for _, item := range ingress {
		value := managedevent.New(item.Type, nil)
		switch item.Type {
		case "user.message":
			value["content"] = item.Content
			value["processed_at"] = nil
		case "user.tool_confirmation":
			value["tool_use_id"] = item.ToolUseID
			value["result"] = item.Result
			if item.DenyMessage != "" {
				value["deny_message"] = item.DenyMessage
			}
		case "user.tool_result":
			value["tool_use_id"] = item.ToolUseID
			value["content"] = item.Content
			value["is_error"] = item.IsError
		}
		accepted = append(accepted, value)
	}
	if err := s.events.AppendIngressBatch(ctx, sessionID, accepted); err != nil {
		return nil, fmt.Errorf("persist Session ingress Events: %w", err)
	}
	return accepted, nil
}

func validateIngressSequence(ingress []view.IngressEvent, blockingIDs map[string]bool) error {
	remaining := make(map[string]bool, len(blockingIDs))
	for id := range blockingIDs {
		remaining[id] = true
	}
	for _, item := range ingress {
		switch item.Type {
		case "user.tool_confirmation", "user.tool_result":
			if !remaining[item.ToolUseID] {
				return fmt.Errorf("%w: tool_use_id %q is not awaiting user action", service.ErrConflict, item.ToolUseID)
			}
			delete(remaining, item.ToolUseID)
		case "user.message":
			if len(remaining) > 0 {
				return fmt.Errorf("%w: Session requires tool action before accepting a new message", service.ErrConflict)
			}
		}
	}
	return nil
}

func (s *Server) listEvents(ctx context.Context, request *hertzapp.RequestContext) error {
	var input view.ListEventsRequest
	if err := bindRequest(request, &input); err != nil {
		return err
	}
	limit, afterSeq, err := parseEventPage(input.PageRequest)
	if err != nil {
		return err
	}
	sessionID := input.SessionID
	if _, err := s.service.GetSession(ctx, sessionID); err != nil {
		return err
	}
	events, nextSeq, hasMore, err := s.events.Page(ctx, sessionID, afterSeq, limit)
	if err != nil {
		return err
	}
	var next *string
	if hasMore {
		next = encodeEventCursor(nextSeq)
	}
	request.JSON(consts.StatusOK, view.Page[managedevent.ManagedEvent]{
		Data: events, NextPage: next,
	})
	return nil
}

func (s *Server) streamEvents(ctx context.Context, request *hertzapp.RequestContext) error {
	var input view.StreamEventsRequest
	if err := bindRequest(request, &input); err != nil {
		return err
	}
	if len(input.EventDeltas) > 0 || len(input.LegacyEventDelta) > 0 {
		return fmt.Errorf("%w: streaming event deltas", service.ErrUnsupported)
	}
	sessionID := input.SessionID
	if _, err := s.service.GetSession(ctx, sessionID); err != nil {
		return err
	}
	history, cursor, err := s.events.Load(ctx, sessionID, 0)
	if err != nil {
		return err
	}
	request.Header("X-Accel-Buffering", "no")
	writer := sse.NewWriter(request)
	defer writer.Close()
	for _, event := range history {
		if err := writeEventSSE(writer, event); err != nil {
			return nil
		}
	}
	ticker := time.NewTicker(s.eventPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			events, nextCursor, err := s.events.Load(ctx, sessionID, cursor)
			if err != nil {
				s.logger.Error("poll persisted Session Events", "session_id", sessionID, "error", err)
				return nil
			}
			for _, event := range events {
				if err := writeEventSSE(writer, event); err != nil {
					return nil
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
