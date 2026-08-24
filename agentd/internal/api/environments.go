package api

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	hertzapp "github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/compforge/agentd/agentd/internal/api/view"
	"github.com/compforge/agentd/agentd/internal/model"
	"github.com/compforge/agentd/agentd/internal/service"
)

func (s *Server) createEnvironment(ctx context.Context, request *hertzapp.RequestContext) error {
	var input view.CreateEnvironmentRequest
	if err := bindRequest(request, &input); err != nil {
		return err
	}
	if input.Scope != "" && input.Scope != "account" {
		return fmt.Errorf(
			"%w: environment scope %q", service.ErrUnsupported, input.Scope,
		)
	}
	if err := validateEnvironmentConfig(input.Config); err != nil {
		return err
	}
	created, err := s.service.CreateEnvironment(ctx, model.Environment{
		Name: input.Name, Description: input.Description, Config: input.Config, Metadata: input.Metadata,
	})
	if err != nil {
		return err
	}
	request.JSON(consts.StatusOK, view.NewEnvironmentResponse(created))
	return nil
}

// +case:id=environment_lifecycle,desc=`update and archive an Environment used by an existing Session`,expect=`updates are visible, archive blocks new Sessions, and the existing Session remains readable`,forbid=`silently accepting unsupported scope or deleting the Environment`,group=system
// +link=agentd/docs/kernel.md
func (s *Server) updateEnvironment(ctx context.Context, request *hertzapp.RequestContext) error {
	var input view.UpdateEnvironmentRequest
	if err := bindRequest(request, &input); err != nil {
		return err
	}
	name, err := parseEnvironmentString(input.Name, false, "name")
	if err != nil {
		return err
	}
	description, err := parseEnvironmentString(input.Description, true, "description")
	if err != nil {
		return err
	}
	config, err := parseEnvironmentConfig(input.Config)
	if err != nil {
		return err
	}
	if err := validateEnvironmentScope(input.Scope); err != nil {
		return err
	}
	metadata, err := parseEnvironmentMetadataPatch(input.Metadata)
	if err != nil {
		return err
	}
	updated, err := s.service.UpdateEnvironment(ctx, input.EnvironmentID, service.EnvironmentUpdate{
		Name: name, Description: description, Config: config, Metadata: metadata,
	})
	if err != nil {
		return err
	}
	request.JSON(consts.StatusOK, view.NewEnvironmentResponse(updated))
	return nil
}

func (s *Server) archiveEnvironment(ctx context.Context, request *hertzapp.RequestContext) error {
	var input view.EnvironmentPathRequest
	if err := bindRequest(request, &input); err != nil {
		return err
	}
	archived, err := s.service.ArchiveEnvironment(ctx, input.EnvironmentID)
	if err != nil {
		return err
	}
	s.logger.InfoContext(ctx, "archived Environment", "environment_id", archived.ID)
	request.JSON(consts.StatusOK, view.NewEnvironmentResponse(archived))
	return nil
}

func (s *Server) getEnvironment(ctx context.Context, request *hertzapp.RequestContext) error {
	var input view.GetEnvironmentRequest
	if err := bindRequest(request, &input); err != nil {
		return err
	}
	value, err := s.service.GetEnvironment(ctx, input.EnvironmentID)
	if err != nil {
		return err
	}
	request.JSON(consts.StatusOK, view.NewEnvironmentResponse(value))
	return nil
}

func (s *Server) listEnvironments(ctx context.Context, request *hertzapp.RequestContext) error {
	var input view.ListEnvironmentsRequest
	if err := bindRequest(request, &input); err != nil {
		return err
	}
	query, err := parsePage(input.PageRequest, environmentCursor, false)
	if err != nil {
		return err
	}
	page, err := s.service.PageEnvironments(ctx, query, input.IncludeArchived)
	if err != nil {
		return err
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
	return nil
}

func parseEnvironmentString(raw json.RawMessage, clearable bool, field string) (*string, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	if strings.TrimSpace(string(raw)) == "null" {
		if !clearable {
			return nil, fmt.Errorf("%w: environment %s cannot be cleared", service.ErrInvalid, field)
		}
		value := ""
		return &value, nil
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, fmt.Errorf("%w: environment %s must be a string", service.ErrInvalid, field)
	}
	return &value, nil
}

func parseEnvironmentConfig(raw json.RawMessage) (*map[string]any, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	if strings.TrimSpace(string(raw)) == "null" {
		return nil, fmt.Errorf("%w: environment config cannot be cleared", service.ErrInvalid)
	}
	var value map[string]any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, fmt.Errorf("%w: environment config must be an object", service.ErrInvalid)
	}
	if err := validateEnvironmentConfig(value); err != nil {
		return nil, err
	}
	return &value, nil
}

func validateEnvironmentConfig(config map[string]any) error {
	environmentType, ok := config["type"].(string)
	if !ok || environmentType == "" {
		return fmt.Errorf("%w: environment config type is required", service.ErrInvalid)
	}
	if environmentType != "cloud" {
		return fmt.Errorf("%w: environment type %q", service.ErrUnsupported, environmentType)
	}
	return nil
}

func validateEnvironmentScope(raw json.RawMessage) error {
	if len(raw) == 0 {
		return nil
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return fmt.Errorf("%w: environment scope must be a string", service.ErrInvalid)
	}
	if value != "account" {
		return fmt.Errorf("%w: environment scope %q", service.ErrUnsupported, value)
	}
	return nil
}

func parseEnvironmentMetadataPatch(raw json.RawMessage) (map[string]*string, error) {
	if len(raw) == 0 || strings.TrimSpace(string(raw)) == "null" {
		return nil, nil
	}
	var value map[string]*string
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, fmt.Errorf("%w: environment metadata must map keys to strings or null", service.ErrInvalid)
	}
	for key, item := range value {
		if item != nil && *item == "" {
			value[key] = nil
		}
	}
	return value, nil
}
