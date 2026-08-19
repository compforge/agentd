package api_test

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"
	"github.com/anthropics/anthropic-sdk-go/packages/param"
	hertzserver "github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/network/standard"
	agentledger "github.com/compforge/agent-ledger/go"
	"github.com/compforge/agentd/server/internal/api"
	"github.com/compforge/agentd/server/internal/app"
	"github.com/compforge/agentd/server/internal/store"
)

func TestClaudeManagedAgentsSDKContract(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	resources := store.NewMemory()
	application := app.New(resources, app.NewEventLog(agentledger.NewMemoryEventStore()), fakeHarness{})
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = application.Shutdown(shutdownCtx)
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
	api.New(application, slog.New(slog.NewTextHandler(io.Discard, nil))).Register(server.Engine)
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Run() }()
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
		<-serveErr
	})
	client := anthropic.NewClient(option.WithAPIKey("test"), option.WithBaseURL("http://"+listener.Addr().String()))

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
}

type fakeHarness struct{}

func (fakeHarness) Name() string { return "fake" }

func (fakeHarness) Version() string { return "test" }

func (fakeHarness) PrepareSession(_ context.Context, session app.Session) (string, error) {
	return "fake/" + session.ID, nil
}

func (fakeHarness) Run(
	_ context.Context,
	_ app.Session,
	input app.TurnInput,
	emit func(app.ManagedEvent) error,
) (app.TurnResult, error) {
	err := emit(app.NewTurnEvent(input.ID, "agent.message", map[string]any{
		"content": []map[string]any{{"type": "text", "text": "echo: " + input.Text}},
	}))
	return app.TurnResult{}, err
}

func (fakeHarness) Interrupt(string) {}
