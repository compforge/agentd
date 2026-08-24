package api

import (
	"context"
	"fmt"

	hertzapp "github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/compforge/agentd/agentd/internal/api/view"
	"github.com/compforge/agentd/agentd/internal/model"
	"github.com/compforge/agentd/agentd/internal/service"
)

func (s *Server) createEnvironment(ctx context.Context, request *hertzapp.RequestContext) {
	var input view.CreateEnvironmentRequest
	if !bindRequest(request, &input) {
		return
	}
	if input.Config["type"] != "cloud" {
		s.writeError(request, fmt.Errorf(
			"%w: environment type %q", service.ErrUnsupported, input.Config["type"],
		))
		return
	}
	created, err := s.service.CreateEnvironment(ctx, model.Environment{
		Name: input.Name, Description: input.Description, Config: input.Config, Metadata: input.Metadata,
	})
	if err != nil {
		s.writeError(request, err)
		return
	}
	request.JSON(consts.StatusOK, view.NewEnvironmentResponse(created))
}

func (s *Server) getEnvironment(ctx context.Context, request *hertzapp.RequestContext) {
	var input view.GetEnvironmentRequest
	if !bindRequest(request, &input) {
		return
	}
	value, err := s.service.GetEnvironment(ctx, input.EnvironmentID)
	if err != nil {
		s.writeError(request, err)
		return
	}
	request.JSON(consts.StatusOK, view.NewEnvironmentResponse(value))
}

func (s *Server) listEnvironments(ctx context.Context, request *hertzapp.RequestContext) {
	var input view.ListEnvironmentsRequest
	if !bindRequest(request, &input) {
		return
	}
	query, err := parsePage(input.PageRequest, environmentCursor, false)
	if err != nil {
		s.writeError(request, err)
		return
	}
	page, err := s.service.PageEnvironments(ctx, query)
	if err != nil {
		s.writeError(request, err)
		return
	}
	data := make([]view.EnvironmentResponse, 0, len(page.Items))
	for _, value := range page.Items {
		data = append(data, view.NewEnvironmentResponse(value))
	}
	var first, last service.PageAnchor
	if len(page.Items) > 0 {
		first = service.PageAnchor{CreatedAt: page.Items[0].CreatedAt, ID: page.Items[0].ID}
		value := page.Items[len(page.Items)-1]
		last = service.PageAnchor{CreatedAt: value.CreatedAt, ID: value.ID}
	}
	next, _ := pageLinks(environmentCursor, query, page.HasMore, first, last)
	request.JSON(consts.StatusOK, view.Page[view.EnvironmentResponse]{Data: data, NextPage: next})
}
