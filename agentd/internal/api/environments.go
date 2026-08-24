package api

import (
	"context"
	"fmt"
	"time"

	hertzapp "github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/compforge/agentd/agentd/internal/model"
	"github.com/compforge/agentd/agentd/internal/service"
)

func (s *Server) createEnvironment(ctx context.Context, request *hertzapp.RequestContext) {
	var input struct {
		Name        string            `json:"name"`
		Description string            `json:"description"`
		Config      map[string]any    `json:"config"`
		Metadata    map[string]string `json:"metadata"`
	}
	if !decodeBody(request, &input) {
		return
	}
	if input.Config["type"] != "cloud" {
		s.writeError(request, fmt.Errorf("%w: environment type %q", service.ErrUnsupported, input.Config["type"]))
		return
	}
	created, err := s.service.CreateEnvironment(ctx, model.Environment{
		Name: input.Name, Description: input.Description, Config: input.Config, Metadata: input.Metadata,
	})
	if err != nil {
		s.writeError(request, err)
		return
	}
	writeJSON(request, consts.StatusOK, environmentResponse(created))
}

func (s *Server) getEnvironment(ctx context.Context, request *hertzapp.RequestContext) {
	value, err := s.service.GetEnvironment(ctx, request.Param("environment_id"))
	if err != nil {
		s.writeError(request, err)
		return
	}
	writeJSON(request, consts.StatusOK, environmentResponse(value))
}

func (s *Server) listEnvironments(ctx context.Context, request *hertzapp.RequestContext) {
	values, err := s.service.ListEnvironments(ctx)
	if err != nil {
		s.writeError(request, err)
		return
	}
	data := make([]map[string]any, 0, len(values))
	for _, value := range values {
		data = append(data, environmentResponse(value))
	}
	writeJSON(request, consts.StatusOK, map[string]any{"data": data, "next_page": nil})
}

func environmentResponse(value model.Environment) map[string]any {
	config := make(map[string]any, len(value.Config)+2)
	for key, item := range value.Config {
		config[key] = item
	}
	if config["networking"] == nil {
		config["networking"] = map[string]any{"type": "unrestricted"}
	}
	if config["packages"] == nil {
		config["packages"] = map[string]any{}
	}
	return map[string]any{
		"id": value.ID, "type": "environment", "name": value.Name, "description": value.Description,
		"config": config, "metadata": value.Metadata, "scope": "account", "archived_at": nil,
		"created_at": value.CreatedAt.Format(time.RFC3339Nano), "updated_at": value.UpdatedAt.Format(time.RFC3339Nano),
	}
}
