package api

import (
	"context"
	"log/slog"
	"time"

	hertzapp "github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/cloudwego/hertz/pkg/route"
	"github.com/compforge/agentd/agentd/internal/service"
	"github.com/compforge/agentd/agentd/internal/session/connector"
	managedevent "github.com/compforge/agentd/internal/event"
)

type Server struct {
	service              *service.Service
	events               *managedevent.Log
	connector            *connector.Client
	executionNotifier    ExecutionNotifier
	logger               *slog.Logger
	eventPollInterval    time.Duration
	slowRequestThreshold time.Duration
}

type ExecutionNotifier interface {
	Notify()
}

type Option func(*Server)

func WithEventPollInterval(interval time.Duration) Option {
	return func(server *Server) { server.eventPollInterval = interval }
}

func New(
	controlService *service.Service,
	events *managedevent.Log,
	agentletConnector *connector.Client,
	executionNotifier ExecutionNotifier,
	logger *slog.Logger,
	options ...Option,
) *Server {
	server := &Server{
		service: controlService, events: events, connector: agentletConnector,
		executionNotifier: executionNotifier, logger: logger,
		eventPollInterval:    500 * time.Millisecond,
		slowRequestThreshold: time.Second,
	}
	for _, option := range options {
		option(server)
	}
	return server
}

func (s *Server) Register(engine *route.Engine) {
	engine.Use(s.observeHTTP)
	engine.GET("/healthz", func(_ context.Context, request *hertzapp.RequestContext) {
		request.JSON(consts.StatusOK, map[string]any{"ok": true})
	})
	engine.POST("/v1/models", s.createModel)
	engine.GET("/v1/models", s.listModels)
	engine.GET("/v1/models/:model_id", s.getModel)
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
