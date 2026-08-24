package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	hertzapp "github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/common/config"
	"github.com/cloudwego/hertz/pkg/common/ut"
	"github.com/cloudwego/hertz/pkg/route"
	"github.com/compforge/agentd/agentd/internal/api/middleware"
	"github.com/compforge/agentd/agentd/internal/api/view"
)

func TestHTTPObservationAddsRequestIDWithoutLoggingHealthyRequest(t *testing.T) {
	var output bytes.Buffer
	server := &Server{
		logger:               slog.New(slog.NewJSONHandler(&output, nil)),
		slowRequestThreshold: time.Hour,
	}
	engine := route.NewEngine(config.NewOptions(nil))
	server.Register(engine)

	response := ut.PerformRequest(engine, "GET", "/healthz", nil).Result()

	if requestID := response.Header.Get(requestIDHeader); !strings.HasPrefix(requestID, "req_") {
		t.Fatalf("request-id = %q", requestID)
	}
	if output.Len() != 0 {
		t.Fatalf("healthy request log = %s", output.String())
	}
}

func TestAPIKeyAuthenticationProtectsPublicAPIAndLeavesHealthAnonymous(t *testing.T) {
	server := &Server{
		logger:               slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)),
		slowRequestThreshold: time.Hour,
	}
	WithAPIKey("correct-secret")(server)
	engine := route.NewEngine(config.NewOptions(nil))
	engine.Use(server.observeHTTP)
	engine.Use(middleware.HandleErrors)
	engine.Use(server.apiKey.Handle)
	engine.GET("/healthz", func(_ context.Context, request *hertzapp.RequestContext) {
		request.JSON(200, map[string]bool{"ok": true})
	})
	engine.GET("/v1/protected", func(_ context.Context, request *hertzapp.RequestContext) {
		request.JSON(200, map[string]bool{"ok": true})
	})

	health := ut.PerformRequest(engine, "GET", "/healthz", nil).Result()
	if health.StatusCode() != 200 {
		t.Fatalf("anonymous health status = %d", health.StatusCode())
	}
	for _, request := range []struct {
		name   string
		header []ut.Header
	}{
		{name: "missing"},
		{name: "wrong", header: []ut.Header{{Key: middleware.APIKeyHeader, Value: "wrong-secret"}}},
	} {
		t.Run(request.name, func(t *testing.T) {
			response := ut.PerformRequest(engine, "GET", "/v1/protected", nil, request.header...).Result()
			if response.StatusCode() != 401 {
				t.Fatalf("status = %d, want 401", response.StatusCode())
			}
			var body view.ErrorResponse
			if err := json.Unmarshal(response.Body(), &body); err != nil {
				t.Fatal(err)
			}
			requestID := response.Header.Get(requestIDHeader)
			if body.Type != "error" || body.Error.Type != "authentication_error" ||
				!strings.HasPrefix(requestID, "req_") {
				t.Fatalf("authentication response = %#v request-id=%q", body, requestID)
			}
		})
	}
	authorized := ut.PerformRequest(engine, "GET", "/v1/protected", nil,
		ut.Header{Key: middleware.APIKeyHeader, Value: "correct-secret"},
	).Result()
	if authorized.StatusCode() != 200 {
		t.Fatalf("authorized status = %d body=%s", authorized.StatusCode(), authorized.Body())
	}
}

func TestHTTPObservationLogsServerErrorWithRouteAndRequestID(t *testing.T) {
	var output bytes.Buffer
	server := &Server{
		logger:               slog.New(slog.NewJSONHandler(&output, nil)),
		slowRequestThreshold: time.Hour,
	}
	engine := route.NewEngine(config.NewOptions(nil))
	engine.Use(server.observeHTTP)
	engine.Use(middleware.HandleErrors)
	engine.GET("/v1/sessions/:session_id", adaptHandler(func(_ context.Context, _ *hertzapp.RequestContext) error {
		return errors.New("database unavailable")
	}))

	response := ut.PerformRequest(engine, "GET", "/v1/sessions/session-1", nil).Result()
	requestID := response.Header.Get(requestIDHeader)
	var entry map[string]any
	if err := json.Unmarshal(output.Bytes(), &entry); err != nil {
		t.Fatalf("decode log %q: %v", output.String(), err)
	}

	if response.StatusCode() != 500 {
		t.Fatalf("status = %d", response.StatusCode())
	}
	if entry["level"] != "ERROR" || entry["request_id"] != requestID {
		t.Fatalf("log correlation = %#v", entry)
	}
	if entry["route"] != "/v1/sessions/:session_id" || entry["status"] != float64(500) {
		t.Fatalf("log request fields = %#v", entry)
	}
	if entry["error"] != "database unavailable" || entry["client_disconnected"] != false {
		t.Fatalf("log failure fields = %#v", entry)
	}
}

func TestHTTPObservationLogsSlowRequestAtWarnLevel(t *testing.T) {
	var output bytes.Buffer
	server := &Server{
		logger:               slog.New(slog.NewJSONHandler(&output, nil)),
		slowRequestThreshold: 0,
	}
	engine := route.NewEngine(config.NewOptions(nil))
	engine.Use(server.observeHTTP)
	engine.GET("/v1/models", func(_ context.Context, request *hertzapp.RequestContext) {
		request.JSON(200, map[string]any{"data": []any{}})
	})

	ut.PerformRequest(engine, "GET", "/v1/models", nil)
	var entry map[string]any
	if err := json.Unmarshal(output.Bytes(), &entry); err != nil {
		t.Fatalf("decode log %q: %v", output.String(), err)
	}

	if entry["level"] != "WARN" || entry["route"] != "/v1/models" {
		t.Fatalf("slow request log = %#v", entry)
	}
}
