package api

import (
	"context"
	"log/slog"
	"time"

	hertzapp "github.com/cloudwego/hertz/pkg/app"
	agentledger "github.com/compforge/agent-ledger/go"
)

const (
	requestIDHeader        = "request-id"
	requestErrorContextKey = "agentd.api.request_error"
	streamEventsRoute      = "/v1/sessions/:session_id/events/stream"
)

func (s *Server) observeHTTP(ctx context.Context, request *hertzapp.RequestContext) {
	started := time.Now()
	requestID := "req_" + agentledger.NewID()
	request.Header(requestIDHeader, requestID)

	request.Next(ctx)

	// Set the correlation header again in case a handler replaced the response.
	request.Header(requestIDHeader, requestID)
	duration := time.Since(started)
	status := request.Response.StatusCode()
	requestCanceled := ctx.Err() != nil
	routePath := request.FullPath()
	if routePath == "" {
		routePath = string(request.Request.URI().Path())
	}
	// A stream's lifetime is client-controlled, so its duration is not server latency.
	isSlow := duration >= s.slowRequestThreshold && routePath != streamEventsRoute
	if status < 500 && !requestCanceled && !isSlow {
		return
	}

	attributes := []slog.Attr{
		slog.String("request_id", requestID),
		slog.String("method", string(request.Request.Method())),
		slog.String("route", routePath),
		slog.Int("status", status),
		slog.Int64("duration_ms", duration.Milliseconds()),
		slog.Bool("client_disconnected", requestCanceled),
	}
	if value, exists := request.Get(requestErrorContextKey); exists {
		if err, ok := value.(error); ok {
			attributes = append(attributes, slog.Any("error", err))
		}
	}
	level := slog.LevelWarn
	if status >= 500 {
		level = slog.LevelError
	}
	s.logger.LogAttrs(ctx, level, "HTTP request completed", attributes...)
}
