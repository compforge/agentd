package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	hertzapp "github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
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
	engine.PUT("/internal/v1/sessions/:session_id", s.applyWorkSpec)
	engine.GET("/internal/v1/sessions/:session_id/state", s.getExecutionState)
	engine.POST("/internal/v1/sessions/:session_id/wake", s.wakeSession)
	engine.POST("/internal/v1/sessions/:session_id/interrupt", s.interruptSession)
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

func (s *Server) wakeSession(ctx context.Context, request *hertzapp.RequestContext) {
	if !s.requireAssignment(ctx, request) {
		return
	}
	if err := s.service.Wake(ctx, request.Param("session_id")); err != nil {
		s.writeError(ctx, request, err)
		return
	}
	writeJSON(request, consts.StatusOK, map[string]any{"ok": true})
}

func (s *Server) interruptSession(ctx context.Context, request *hertzapp.RequestContext) {
	if !s.requireAssignment(ctx, request) {
		return
	}
	if err := s.service.Interrupt(ctx, request.Param("session_id")); err != nil {
		s.writeError(ctx, request, err)
		return
	}
	writeJSON(request, consts.StatusOK, map[string]any{"ok": true})
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
	case errors.Is(err, service.ErrCapacity):
		status, errorType = consts.StatusServiceUnavailable, "overloaded_error"
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
