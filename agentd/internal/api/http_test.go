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

func TestHTTPObservationLogsServerErrorWithRouteAndRequestID(t *testing.T) {
	var output bytes.Buffer
	server := &Server{
		logger:               slog.New(slog.NewJSONHandler(&output, nil)),
		slowRequestThreshold: time.Hour,
	}
	engine := route.NewEngine(config.NewOptions(nil))
	engine.Use(server.observeHTTP)
	engine.GET("/v1/sessions/:session_id", func(_ context.Context, request *hertzapp.RequestContext) {
		server.writeError(request, errors.New("database unavailable"))
	})

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
