package api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	hertzapp "github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/compforge/agentd/agentd/internal/service"
	"github.com/compforge/agentd/agentd/internal/session/connector"
)

const maxConnectorResponseBody = 2 << 20

func (s *Server) sendEvents(ctx context.Context, request *hertzapp.RequestContext) {
	target, err := s.service.PrepareExecution(ctx, request.Param("session_id"))
	if err != nil {
		s.writeError(request, err)
		return
	}
	s.proxyEvents(ctx, request, target, false)
}

func (s *Server) listEvents(ctx context.Context, request *hertzapp.RequestContext) {
	target, err := s.service.CurrentExecution(ctx, request.Param("session_id"))
	if errors.Is(err, service.ErrNoAssignment) {
		writeJSON(request, consts.StatusOK, map[string]any{"data": []any{}, "next_page": nil})
		return
	}
	if err != nil {
		s.writeError(request, err)
		return
	}
	s.proxyEvents(ctx, request, target, false)
}

func (s *Server) streamEvents(ctx context.Context, request *hertzapp.RequestContext) {
	target, err := s.service.CurrentExecution(ctx, request.Param("session_id"))
	if err != nil {
		s.writeError(request, err)
		return
	}
	s.proxyEvents(ctx, request, target, true)
}

func (s *Server) proxyEvents(
	ctx context.Context,
	request *hertzapp.RequestContext,
	target service.ExecutionTarget,
	stream bool,
) {
	connectorTarget := connector.Target{Endpoint: target.Endpoint, Work: target.Work}
	if err := s.connector.Ensure(ctx, connectorTarget); err != nil {
		s.writeError(request, fmt.Errorf("%w: prepare Agentlet execution: %v", service.ErrUnavailable, err))
		return
	}
	body, err := request.Body()
	if err != nil {
		s.writeError(request, fmt.Errorf("read Event request: %w", err))
		return
	}
	path := "/internal/v1/sessions/" + url.PathEscape(target.Work.Session.ID) + "/events"
	if stream {
		path += "/stream"
	}
	response, err := s.connector.Forward(
		ctx,
		connectorTarget,
		string(request.Method()),
		path,
		string(request.Request.URI().QueryString()),
		body,
		connectorHeaders(request),
		stream,
	)
	if err != nil {
		s.writeError(request, fmt.Errorf(
			"%w: forward Session %q Events: %v", service.ErrUnavailable, target.Work.Session.ID, err,
		))
		return
	}
	copyConnectorHeaders(request, response.Header)
	request.Response.SetStatusCode(response.StatusCode)
	if stream {
		request.Response.SetBodyStream(response.Body, -1)
		return
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, maxConnectorResponseBody+1))
	if err != nil {
		s.writeError(request, fmt.Errorf("read Agentlet response: %w", err))
		return
	}
	if len(responseBody) > maxConnectorResponseBody {
		s.writeError(request, fmt.Errorf("Agentlet response exceeds %d bytes", maxConnectorResponseBody))
		return
	}
	request.Response.SetBody(responseBody)
}

func connectorHeaders(request *hertzapp.RequestContext) http.Header {
	headers := make(http.Header)
	for _, name := range []string{
		"Accept", "Content-Type", "Anthropic-Beta", "Traceparent", "Tracestate", "Baggage", "X-Request-ID",
	} {
		if value := request.GetHeader(name); len(value) > 0 {
			headers.Set(name, string(value))
		}
	}
	return headers
}

func copyConnectorHeaders(request *hertzapp.RequestContext, headers http.Header) {
	for name, values := range headers {
		switch strings.ToLower(name) {
		case "connection", "content-length", "keep-alive", "proxy-authenticate", "proxy-authorization",
			"te", "trailer", "transfer-encoding", "upgrade":
			continue
		}
		for _, value := range values {
			request.Response.Header.Add(name, value)
		}
	}
}
