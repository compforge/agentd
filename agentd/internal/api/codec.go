package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"

	hertzapp "github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
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
	writeJSON(request, status, map[string]any{
		"type": "error", "error": map[string]any{"type": errorType, "message": err.Error()},
	})
}

func decodeBody(request *hertzapp.RequestContext, target any) bool {
	body, err := request.Body()
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

func present(raw json.RawMessage) bool {
	value := strings.TrimSpace(string(raw))
	return value != "" && value != "null" && value != "{}" && value != "[]"
}
