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

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"
	"github.com/anthropics/anthropic-sdk-go/packages/param"
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
	executionService := service.New(resources, service.NewEventLog(agentledger.NewMemoryEventStore()), fakeHarness{})
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
	client := anthropic.NewClient(option.WithAPIKey("test"), option.WithBaseURL("http://"+listener.Addr().String()+"/internal"))

	agent, err := client.Beta.Agents.New(ctx, anthropic.BetaAgentNewParams{
		Name:  "contract-test",
		Model: anthropic.BetaManagedAgentsModelConfigParams{ID: anthropic.BetaManagedAgentsModelClaudeSonnet4_6},
	})
	if err != nil {
		t.Fatalf("create agent through official SDK: %v", err)
	}
	if agent.ID == "" || agent.Name != "contract-test" {
		t.Fatalf("unexpected agent: %#v", agent)
	}
	if _, err := client.Beta.Agents.Get(ctx, agent.ID, anthropic.BetaAgentGetParams{}); err != nil {
		t.Fatalf("get agent through official SDK: %v", err)
	}
	agents, err := client.Beta.Agents.List(ctx, anthropic.BetaAgentListParams{})
	if err != nil {
		t.Fatalf("list agents through official SDK: %v", err)
	}
	if len(agents.Data) != 1 {
		t.Fatalf("list agents through official SDK: data=%d", len(agents.Data))
	}

	unrestricted := anthropic.NewBetaUnrestrictedNetworkParam()
	environment, err := client.Beta.Environments.New(ctx, anthropic.BetaEnvironmentNewParams{
		Name: "contract-test",
		Config: anthropic.BetaEnvironmentNewParamsConfigUnion{OfCloud: &anthropic.BetaCloudConfigParams{
			Networking: anthropic.BetaCloudConfigParamsNetworkingUnion{OfUnrestricted: &unrestricted},
		}},
	})
	if err != nil {
		t.Fatalf("create environment through official SDK: %v", err)
	}
	if environment.ID == "" {
		t.Fatalf("environment id is empty: %#v", environment)
	}
	if _, err := client.Beta.Environments.Get(ctx, environment.ID, anthropic.BetaEnvironmentGetParams{}); err != nil {
		t.Fatalf("get environment through official SDK: %v", err)
	}
	environments, err := client.Beta.Environments.List(ctx, anthropic.BetaEnvironmentListParams{})
	if err != nil {
		t.Fatalf("list environments through official SDK: %v", err)
	}
	if len(environments.Data) != 1 {
		t.Fatalf("list environments through official SDK: data=%d", len(environments.Data))
	}

	session, err := client.Beta.Sessions.New(ctx, anthropic.BetaSessionNewParams{
		Agent:         anthropic.BetaSessionNewParamsAgentUnion{OfString: param.NewOpt(agent.ID)},
		EnvironmentID: environment.ID,
	})
	if err != nil {
		t.Fatalf("create session through official SDK: %v", err)
	}
	if session.ID == "" || session.Status != anthropic.BetaManagedAgentsSessionStatusIdle {
		t.Fatalf("unexpected session: %#v", session)
	}
	if _, err := client.Beta.Sessions.Get(ctx, session.ID, anthropic.BetaSessionGetParams{}); err != nil {
		t.Fatalf("get session through official SDK: %v", err)
	}
	sessions, err := client.Beta.Sessions.List(ctx, anthropic.BetaSessionListParams{})
	if err != nil {
		t.Fatalf("list sessions through official SDK: %v", err)
	}
	if len(sessions.Data) != 1 {
		t.Fatalf("list sessions through official SDK: data=%d", len(sessions.Data))
	}

	_, err = client.Beta.Sessions.Events.Send(ctx, session.ID, anthropic.BetaSessionEventSendParams{
		Events: []anthropic.BetaManagedAgentsEventParamsUnion{{
			OfUserMessage: &anthropic.BetaManagedAgentsUserMessageEventParams{
				Type: anthropic.BetaManagedAgentsUserMessageEventParamsTypeUserMessage,
				Content: []anthropic.BetaManagedAgentsUserMessageEventParamsContentUnion{{
					OfText: &anthropic.BetaManagedAgentsTextBlockParam{Type: anthropic.BetaManagedAgentsTextBlockTypeText, Text: "hello"},
				}},
			},
		}},
	})
	if err != nil {
		t.Fatalf("send event through official SDK: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		page, listErr := client.Beta.Sessions.Events.List(ctx, session.ID, anthropic.BetaSessionEventListParams{})
		if listErr != nil {
			t.Fatalf("list events through official SDK: %v", listErr)
		}
		foundAgentMessage := false
		for _, event := range page.Data {
			foundAgentMessage = foundAgentMessage || event.Type == "agent.message"
		}
		if foundAgentMessage {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for harness output; got %d events", len(page.Data))
		}
		time.Sleep(10 * time.Millisecond)
	}

	streamCtx, cancelStream := context.WithTimeout(ctx, 2*time.Second)
	stream := client.Beta.Sessions.Events.StreamEvents(streamCtx, session.ID, anthropic.BetaSessionEventStreamParams{})
	if !stream.Next() {
		cancelStream()
		t.Fatalf("stream events through official SDK: %v", stream.Err())
	}
	if stream.Current().Type == "" {
		cancelStream()
		t.Fatal("streamed event type is empty")
	}
	if err := stream.Close(); err != nil {
		cancelStream()
		t.Fatalf("close event stream: %v", err)
	}
	cancelStream()

	workSpec := executionapi.WorkSpec{
		AssignmentID: "assignment-1", WorkerID: "worker-1",
		Session: executionapi.SessionSnapshot{
			ID: "assigned-session", EnvironmentID: "assigned-environment", Status: "idle",
			Harness: "fake", HarnessVersion: "test", CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
		},
		Agent: executionapi.AgentSnapshot{
			ID: "assigned-agent", Name: "assigned", ModelID: "test-model", Version: 1,
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
