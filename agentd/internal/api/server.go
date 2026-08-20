package api

import (
	"context"
	"log/slog"

	hertzapp "github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/cloudwego/hertz/pkg/route"
	"github.com/compforge/agentd/agentd/internal/service"
	"github.com/compforge/agentd/agentd/internal/session/connector"
)

type Server struct {
	service   *service.Service
	connector *connector.Client
	logger    *slog.Logger
}

func New(controlService *service.Service, agentletConnector *connector.Client, logger *slog.Logger) *Server {
	return &Server{service: controlService, connector: agentletConnector, logger: logger}
}

func (s *Server) Register(engine *route.Engine) {
	engine.GET("/healthz", func(_ context.Context, request *hertzapp.RequestContext) {
		request.JSON(consts.StatusOK, map[string]any{"ok": true})
	})
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
