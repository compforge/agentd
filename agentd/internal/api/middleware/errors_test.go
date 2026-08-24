package middleware

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	hertzapp "github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/common/config"
	"github.com/cloudwego/hertz/pkg/common/ut"
	"github.com/cloudwego/hertz/pkg/route"
	"github.com/compforge/agentd/agentd/internal/api/view"
	"github.com/compforge/agentd/agentd/internal/repo"
	"github.com/compforge/agentd/agentd/internal/service"
)

func TestHandleErrorsMapsErrorChainToPublicEnvelope(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		err       error
		status    int
		errorType string
	}{
		{name: "authentication", err: errAuthentication, status: 401, errorType: "authentication_error"},
		{name: "not found", err: repo.ErrNotFound, status: 404, errorType: "not_found_error"},
		{name: "unavailable", err: service.ErrUnavailable, status: 503, errorType: "overloaded_error"},
		{name: "no assignment", err: service.ErrNoAssignment, status: 409, errorType: "invalid_request_error"},
		{name: "unsupported", err: service.ErrUnsupported, status: 400, errorType: "unsupported_feature"},
		{name: "conflict", err: service.ErrConflict, status: 409, errorType: "conflict_error"},
		{name: "invalid", err: service.ErrInvalid, status: 400, errorType: "invalid_request_error"},
		{name: "internal", err: errors.New("database unavailable"), status: 500, errorType: "api_error"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			engine := route.NewEngine(config.NewOptions(nil))
			engine.Use(HandleErrors)
			engine.GET("/", func(_ context.Context, request *hertzapp.RequestContext) {
				_ = request.Error(fmt.Errorf("operation failed: %w", test.err))
			})

			response := ut.PerformRequest(engine, "GET", "/", nil).Result()
			if response.StatusCode() != test.status {
				t.Fatalf("status = %d, want %d", response.StatusCode(), test.status)
			}
			var body view.ErrorResponse
			if err := json.Unmarshal(response.Body(), &body); err != nil {
				t.Fatal(err)
			}
			if body.Type != "error" || body.Error.Type != test.errorType {
				t.Fatalf("error response = %#v", body)
			}
		})
	}
}
