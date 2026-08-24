package api

import (
	"encoding/json"
	"errors"
	"strings"

	hertzapp "github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/compforge/agentd/agentd/internal/api/view"
	"github.com/compforge/agentd/agentd/internal/repo"
	"github.com/compforge/agentd/agentd/internal/service"
)

func (s *Server) writeError(request *hertzapp.RequestContext, err error) {
	status := consts.StatusInternalServerError
	errorType := "api_error"
	switch {
	case errors.Is(err, repo.ErrNotFound):
		status, errorType = consts.StatusNotFound, "not_found_error"
	case errors.Is(err, service.ErrNoCapacity), errors.Is(err, service.ErrUnavailable):
		status, errorType = consts.StatusServiceUnavailable, "overloaded_error"
	case errors.Is(err, service.ErrNoAssignment):
		status, errorType = consts.StatusConflict, "invalid_request_error"
	case errors.Is(err, service.ErrUnsupported):
		status, errorType = consts.StatusBadRequest, "unsupported_feature"
	case errors.Is(err, service.ErrConflict):
		status, errorType = consts.StatusConflict, "conflict_error"
	case errors.Is(err, service.ErrInvalid):
		status, errorType = consts.StatusBadRequest, "invalid_request_error"
	}
	request.Set(requestErrorContextKey, err)
	request.JSON(status, view.ErrorResponse{
		Type: "error", Error: view.Error{Type: errorType, Message: err.Error()},
	})
}

func bindRequest(request *hertzapp.RequestContext, target any) bool {
	if err := request.BindAndValidate(target); err != nil {
		request.JSON(consts.StatusBadRequest, view.ErrorResponse{
			Type: "error", Error: view.Error{Type: "invalid_request_error", Message: err.Error()},
		})
		return false
	}
	return true
}

func present(raw json.RawMessage) bool {
	value := strings.TrimSpace(string(raw))
	return value != "" && value != "null" && value != "{}" && value != "[]"
}
