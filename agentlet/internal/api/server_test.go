package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"

	hertzserver "github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/network/standard"
	agentledger "github.com/compforge/agent-ledger/go"
	"github.com/compforge/agentd/agentlet/internal/api"
	"github.com/compforge/agentd/agentlet/internal/harness"
	"github.com/compforge/agentd/agentlet/internal/service"
	"github.com/compforge/agentd/internal/executionapi"
)

func TestAgentdInternalExecutionAPI(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	resources := service.NewMemoryRepository()
	events := service.NewEventLog(agentledger.NewMemoryEventStore())
	executionService := service.New(
		resources, events, fakeHarness{},
		service.WithWorkCapacity(2),
	)
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = executionService.Shutdown(shutdownCtx)
	})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := hertzserver.Default(
		hertzserver.WithListener(listener),
		hertzserver.WithTransport(standard.NewTransporter),
		hertzserver.WithReadTimeout(time.Second),
		hertzserver.WithWriteTimeout(0),
		hertzserver.WithIdleTimeout(time.Second),
		hertzserver.WithMaxRequestBodySize(2<<20),
		hertzserver.WithSenseClientDisconnection(true),
	)
	api.New(
		executionService, slog.New(slog.NewTextHandler(io.Discard, nil)), api.WithWorkerID("worker-1"),
	).Register(server.Engine)
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Run() }()
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
		<-serveErr
	})
	publicResponse, err := http.Get("http://" + listener.Addr().String() + "/v1/agents")
	if err != nil {
		t.Fatal(err)
	}
	defer publicResponse.Body.Close()
	if publicResponse.StatusCode != http.StatusNotFound {
		t.Fatalf("public Agentlet API status = %d, want 404", publicResponse.StatusCode)
	}
	internalResourceResponse, err := http.Get("http://" + listener.Addr().String() + "/internal/v1/agents")
	if err != nil {
		t.Fatal(err)
	}
	defer internalResourceResponse.Body.Close()
	if internalResourceResponse.StatusCode != http.StatusNotFound {
		t.Fatalf("Agentlet resource API status = %d, want 404", internalResourceResponse.StatusCode)
	}

	workSpec := executionapi.WorkSpec{
		AssignmentID: "assignment-1", WorkerID: "worker-1",
		Session: executionapi.SessionSnapshot{
			ID: "assigned-session", EnvironmentID: "assigned-environment", Status: "idle",
			Harness: "fake", HarnessVersion: "test", CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
		},
		Agent: executionapi.AgentSnapshot{
			ID: "assigned-agent", Name: "assigned",
			Model: executionapi.ModelSnapshot{
				ID: "test-model", Provider: "anthropic", UpstreamID: "test-model", APIKey: "secret",
			}, Version: 1,
		},
		Environment: executionapi.EnvironmentSnapshot{
			ID: "assigned-environment", Config: map[string]any{"type": "cloud"},
		},
	}
	body, err := json.Marshal(workSpec)
	if err != nil {
		t.Fatal(err)
	}
	workRequest, err := http.NewRequest(
		http.MethodPut,
		"http://"+listener.Addr().String()+"/internal/v1/sessions/assigned-session",
		bytes.NewReader(body),
	)
	if err != nil {
		t.Fatal(err)
	}
	workRequest.Header.Set("Content-Type", "application/json")
	workRequest.Header.Set(executionapi.AssignmentHeader, "assignment-1")
	workRequest.Header.Set(executionapi.WorkerHeader, "worker-1")
	workResponse, err := http.DefaultClient.Do(workRequest)
	if err != nil {
		t.Fatal(err)
	}
	workResponse.Body.Close()
	if workResponse.StatusCode != http.StatusOK {
		t.Fatalf("install assigned Work status = %d", workResponse.StatusCode)
	}
	eventResponse, err := http.Get(
		"http://" + listener.Addr().String() + "/internal/v1/sessions/assigned-session/events",
	)
	if err != nil {
		t.Fatal(err)
	}
	defer eventResponse.Body.Close()
	if eventResponse.StatusCode != http.StatusNotFound {
		t.Fatalf("Agentlet Event API status = %d, want 404", eventResponse.StatusCode)
	}
	input := service.NewManagedEvent("user.message", map[string]any{
		"content": []map[string]any{{"type": "text", "text": "hello"}},
	})
	input["processed_at"] = nil
	if err := events.AppendIngress(ctx, workSpec.Session.ID, input); err != nil {
		t.Fatal(err)
	}
	wakeRequest, err := http.NewRequest(
		http.MethodPost,
		"http://"+listener.Addr().String()+"/internal/v1/sessions/assigned-session/wake",
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	wakeRequest.Header.Set(executionapi.AssignmentHeader, "assignment-1")
	wakeRequest.Header.Set(executionapi.WorkerHeader, "worker-1")
	wakeResponse, err := http.DefaultClient.Do(wakeRequest)
	if err != nil {
		t.Fatal(err)
	}
	wakeResponse.Body.Close()
	if wakeResponse.StatusCode != http.StatusOK {
		t.Fatalf("wake assigned Work status = %d", wakeResponse.StatusCode)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		stored, err := events.List(ctx, workSpec.Session.ID)
		if err != nil {
			t.Fatal(err)
		}
		foundOutput := false
		for _, event := range stored {
			if event["type"] == "agent.message" {
				foundOutput = true
				break
			}
		}
		if foundOutput {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for harness output; got %d events", len(stored))
		}
		time.Sleep(10 * time.Millisecond)
	}

	stateRequest, err := http.NewRequest(
		http.MethodGet,
		"http://"+listener.Addr().String()+"/internal/v1/sessions/assigned-session/state",
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	stateRequest.Header.Set(executionapi.AssignmentHeader, "stale-assignment")
	stateRequest.Header.Set(executionapi.WorkerHeader, "worker-1")
	stateResponse, err := http.DefaultClient.Do(stateRequest)
	if err != nil {
		t.Fatal(err)
	}
	stateResponse.Body.Close()
	if stateResponse.StatusCode != http.StatusBadRequest {
		t.Fatalf("stale Assignment status = %d, want 400", stateResponse.StatusCode)
	}
}

type fakeHarness struct{}

func (fakeHarness) Name() string { return "fake" }

func (fakeHarness) Version() string { return "test" }

func (fakeHarness) PrepareSession(_ context.Context, session harness.Session) (string, error) {
	return "fake/" + session.ID, nil
}

func (fakeHarness) Run(
	_ context.Context,
	_ harness.Session,
	input service.TurnInput,
	emit func(service.ManagedEvent) error,
) (service.TurnResult, error) {
	err := emit(service.NewTurnEvent(input.ID, "agent.message", map[string]any{
		"content": []map[string]any{{"type": "text", "text": "echo: " + input.Text}},
	}))
	return service.TurnResult{}, err
}

func (fakeHarness) Interrupt(string) {}
