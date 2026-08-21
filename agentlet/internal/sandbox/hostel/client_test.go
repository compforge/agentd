package hostel

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	sandboxengine "github.com/compforge/agentd/agentlet/internal/sandbox/engine"
)

func TestRemoteEngineExecutesInSessionBed(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/healthz":
			writer.WriteHeader(http.StatusOK)
		case "/v1/beds":
			_ = json.NewEncoder(writer).Encode(map[string]any{"state": "active"})
		case "/command":
			if value := request.Header.Get(bedHeader); value != "session_123" {
				t.Errorf("bed header = %q", value)
			}
			_, _ = writer.Write([]byte("{\"type\":\"stdout\",\"text\":\"hello\"}\n"))
			_, _ = writer.Write([]byte("{\"type\":\"execution_complete\",\"exit_code\":0}\n"))
		default:
			http.NotFound(writer, request)
		}
	}))
	t.Cleanup(server.Close)

	engine, err := NewEngine(EngineConfig{
		URL: server.URL, RequestTimeout: time.Second, StartupTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	result, err := engine.Execute(context.Background(), sandboxengine.SandboxKey{Value: "session_123"}, sandboxengine.Command{
		Command: "printf hello", Cwd: "/workspace", Timeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Output != "hello" || result.ExitCode != 0 || result.Cause != "exited" {
		t.Fatalf("unexpected command result: %#v", result)
	}
}

func TestBedReadyRequiresReadyField(t *testing.T) {
	t.Parallel()
	response := &http.Response{
		StatusCode: http.StatusOK,
		Status:     http.StatusText(http.StatusOK),
		Body:       http.NoBody,
	}
	response.Body = io.NopCloser(strings.NewReader(`{"ready":false}`))
	ready, err := bedReady(response)
	if err != nil {
		t.Fatal(err)
	}
	if ready {
		t.Fatal("HTTP 200 without ready=true must not mark a bed ready")
	}
}
