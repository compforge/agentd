package api

import (
	"context"
	"log/slog"
	"time"

	hertzapp "github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/cloudwego/hertz/pkg/route"
	"github.com/compforge/agentd/agentd/internal/api/middleware"
	"github.com/compforge/agentd/agentd/internal/api/view"
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
	apiKey               middleware.APIKey
}

type ExecutionNotifier interface {
	Notify()
}

type Option func(*Server)

func WithEventPollInterval(interval time.Duration) Option {
	return func(server *Server) { server.eventPollInterval = interval }
}

func WithAPIKey(apiKey string) Option {
	return func(server *Server) {
		server.apiKey = middleware.NewAPIKey(apiKey)
	}
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
	// Hertz middleware unwinds in reverse order: errors must be rendered before
	// observation records the final status and correlated error.
	engine.Use(s.observeHTTP)
	engine.Use(middleware.HandleErrors)
	engine.Use(s.apiKey.Handle)
	engine.GET("/healthz", func(_ context.Context, request *hertzapp.RequestContext) {
		request.JSON(consts.StatusOK, view.HealthResponse{OK: true})
	})
	engine.POST("/v1/models", adaptHandler(s.createModel))
	engine.GET("/v1/models", adaptHandler(s.listModels))
	engine.GET("/v1/models/:model_id", adaptHandler(s.getModel))
	engine.POST("/v1/models/:model_id", adaptHandler(s.updateModel))
	engine.POST("/v1/agents", adaptHandler(s.createAgent))
	engine.GET("/v1/agents", adaptHandler(s.listAgents))
	engine.GET("/v1/agents/:agent_id", adaptHandler(s.getAgent))
	engine.POST("/v1/agents/:agent_id", adaptHandler(s.updateAgent))
	engine.POST("/v1/agents/:agent_id/archive", adaptHandler(s.archiveAgent))
	engine.GET("/v1/agents/:agent_id/versions", adaptHandler(s.listAgentVersions))
	engine.POST("/v1/environments", adaptHandler(s.createEnvironment))
	engine.GET("/v1/environments", adaptHandler(s.listEnvironments))
	engine.GET("/v1/environments/:environment_id", adaptHandler(s.getEnvironment))
	engine.POST("/v1/environments/:environment_id", adaptHandler(s.updateEnvironment))
	engine.POST("/v1/environments/:environment_id/archive", adaptHandler(s.archiveEnvironment))
	engine.POST("/v1/sessions", adaptHandler(s.createSession))
	engine.GET("/v1/sessions", adaptHandler(s.listSessions))
	engine.GET("/v1/sessions/:session_id", adaptHandler(s.getSession))
	engine.POST("/v1/sessions/:session_id", adaptHandler(s.updateSession))
	engine.POST("/v1/sessions/:session_id/archive", adaptHandler(s.archiveSession))
	engine.POST("/v1/sessions/:session_id/events", adaptHandler(s.sendEvents))
	engine.GET("/v1/sessions/:session_id/events", adaptHandler(s.listEvents))
	engine.GET("/v1/sessions/:session_id/events/stream", adaptHandler(s.streamEvents))
}
