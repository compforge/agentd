package middleware

import (
	"context"
	"errors"

	hertzapp "github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/compforge/agentd/agentd/internal/api/view"
	"github.com/compforge/agentd/agentd/internal/repo"
	"github.com/compforge/agentd/agentd/internal/service"
)

// HandleErrors renders the last error reported through Hertz's request error
// chain after the route handler returns.
func HandleErrors(ctx context.Context, request *hertzapp.RequestContext) {
	request.Next(ctx)

	last := request.Errors.Last()
	if last == nil {
		return
	}
	status, errorType := classifyError(last.Err)
	request.JSON(status, view.ErrorResponse{
		Type: "error", Error: view.Error{Type: errorType, Message: last.Err.Error()},
	})
}

func classifyError(err error) (int, string) {
	switch {
	case errors.Is(err, errAuthentication):
		return consts.StatusUnauthorized, "authentication_error"
	case errors.Is(err, repo.ErrNotFound):
		return consts.StatusNotFound, "not_found_error"
	case errors.Is(err, service.ErrNoCapacity), errors.Is(err, service.ErrUnavailable):
		return consts.StatusServiceUnavailable, "overloaded_error"
	case errors.Is(err, service.ErrNoAssignment):
		return consts.StatusConflict, "invalid_request_error"
	case errors.Is(err, service.ErrUnsupported):
		return consts.StatusBadRequest, "unsupported_feature"
	case errors.Is(err, service.ErrConflict):
		return consts.StatusConflict, "conflict_error"
	case errors.Is(err, service.ErrInvalid):
		return consts.StatusBadRequest, "invalid_request_error"
	default:
		return consts.StatusInternalServerError, "api_error"
	}
}
