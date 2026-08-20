package api_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"
	"github.com/anthropics/anthropic-sdk-go/packages/param"
	hertzserver "github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/network/standard"
	"github.com/compforge/agentd/agentd/internal/api"
	"github.com/compforge/agentd/agentd/internal/connector"
	"github.com/compforge/agentd/agentd/internal/model"
	gormrepo "github.com/compforge/agentd/agentd/internal/repo/gorm"
	"github.com/compforge/agentd/agentd/internal/service"
	"github.com/compforge/agentd/internal/executionapi"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestManagedAgentSDKRunsThroughControlPlaneAndAssignedAgentlet(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	worker := &fakeAgentlet{workerID: "worker-1"}
	workerServer := httptest.NewServer(worker)
	defer workerServer.Close()

	database, err := gorm.Open(sqlite.Open(fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name())), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatal(err)
	}
	repository, err := gormrepo.NewGORM(database)
	if err != nil {
		t.Fatal(err)
	}
	controlService, err := service.New(repository, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	observedAt := time.Now().UTC()
	observerStatus, err := json.Marshal(model.WorkerObserverStatus{
		ObservedAt: observedAt, Exists: true, Ready: true, Endpoint: workerServer.URL,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controlService.ObserveWorker(ctx, model.Worker{
		ID: "worker-1", Name: "worker-1", Capacity: 1, ObserverStatus: observerStatus,
	}); err != nil {
		t.Fatal(err)
	}
	agentletConnector, err := connector.New(connector.Config{
		RequestTimeout: time.Second, DialTimeout: time.Second, ResponseHeaderTimeout: time.Second,
		IdleConnTimeout: time.Second, MaxIdleConns: 4, MaxIdleConnsPerHost: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer agentletConnector.CloseIdleConnections()

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
		controlService, agentletConnector, slog.New(slog.NewTextHandler(io.Discard, nil)),
	).Register(server.Engine)
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Run() }()
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
		<-serveErr
	})

	client := anthropic.NewClient(
		option.WithAPIKey("test"), option.WithBaseURL("http://"+listener.Addr().String()),
	)
	agent, err := client.Beta.Agents.New(ctx, anthropic.BetaAgentNewParams{
		Name: "contract-test",
		Model: anthropic.BetaManagedAgentsModelConfigParams{
			ID: anthropic.BetaManagedAgentsModelClaudeSonnet4_6,
		},
	})
	if err != nil {
		t.Fatalf("create Agent through public control plane: %v", err)
	}
	unrestricted := anthropic.NewBetaUnrestrictedNetworkParam()
	environment, err := client.Beta.Environments.New(ctx, anthropic.BetaEnvironmentNewParams{
		Name: "contract-test",
		Config: anthropic.BetaEnvironmentNewParamsConfigUnion{OfCloud: &anthropic.BetaCloudConfigParams{
			Networking: anthropic.BetaCloudConfigParamsNetworkingUnion{OfUnrestricted: &unrestricted},
		}},
	})
	if err != nil {
		t.Fatalf("create Environment through public control plane: %v", err)
	}
	session, err := client.Beta.Sessions.New(ctx, anthropic.BetaSessionNewParams{
		Agent:         anthropic.BetaSessionNewParamsAgentUnion{OfString: param.NewOpt(agent.ID)},
		EnvironmentID: environment.ID,
	})
	if err != nil {
		t.Fatalf("create Session through public control plane: %v", err)
	}
	page, err := client.Beta.Sessions.Events.List(ctx, session.ID, anthropic.BetaSessionEventListParams{})
	if err != nil {
		t.Fatalf("list unassigned Session Events: %v", err)
	}
	if len(page.Data) != 0 {
		t.Fatalf("unassigned Session Events = %d", len(page.Data))
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
		t.Fatalf("send Event through assigned Agentlet: %v", err)
	}
	installed := worker.work()
	if installed.Session.ID != session.ID || installed.Agent.ID != agent.ID ||
		installed.Environment.ID != environment.ID || installed.AssignmentID == "" {
		t.Fatalf("installed WorkSpec = %#v", installed)
	}

	page, err = client.Beta.Sessions.Events.List(ctx, session.ID, anthropic.BetaSessionEventListParams{})
	if err != nil {
		t.Fatalf("list Event through assigned Agentlet: %v", err)
	}
	if len(page.Data) != 1 || page.Data[0].Type != "agent.message" {
		t.Fatalf("listed Events = %#v", page.Data)
	}

	current, err := client.Beta.Sessions.Get(ctx, session.ID, anthropic.BetaSessionGetParams{})
	if err != nil {
		t.Fatalf("sync Session state from assigned Agentlet: %v", err)
	}
	if current.Status != anthropic.BetaManagedAgentsSessionStatusIdle {
		t.Fatalf("Session status = %q, want idle", current.Status)
	}
	stored, err := repository.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.ResumeRef != "checkpoint-7" || stored.ResumeRevision != 7 {
		t.Fatalf("persisted observed state = %#v", stored)
	}

	streamCtx, cancelStream := context.WithTimeout(ctx, 2*time.Second)
	stream := client.Beta.Sessions.Events.StreamEvents(
		streamCtx, session.ID, anthropic.BetaSessionEventStreamParams{},
	)
	if !stream.Next() {
		cancelStream()
		t.Fatalf("stream Event through assigned Agentlet: %v", stream.Err())
	}
	if stream.Current().Type != "agent.message" {
		cancelStream()
		t.Fatalf("streamed Event = %#v", stream.Current())
	}
	if err := stream.Close(); err != nil {
		cancelStream()
		t.Fatal(err)
	}
	cancelStream()
}

type fakeAgentlet struct {
	mu       sync.Mutex
	workerID string
	workSpec executionapi.WorkSpec
	state    executionapi.SessionState
}

func (a *fakeAgentlet) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if request.Header.Get(executionapi.WorkerHeader) != a.workerID {
		http.Error(response, "wrong Worker", http.StatusBadRequest)
		return
	}
	switch {
	case request.Method == http.MethodPut && strings.HasPrefix(request.URL.Path, "/internal/v1/sessions/"):
		var spec executionapi.WorkSpec
		if err := json.NewDecoder(request.Body).Decode(&spec); err != nil {
			http.Error(response, err.Error(), http.StatusBadRequest)
			return
		}
		if request.Header.Get(executionapi.AssignmentHeader) != spec.AssignmentID || spec.WorkerID != a.workerID {
			http.Error(response, "wrong Assignment", http.StatusBadRequest)
			return
		}
		if a.workSpec.AssignmentID != spec.AssignmentID {
			a.state = executionapi.SessionState{
				AssignmentID: spec.AssignmentID, Status: "idle", ResumeRef: "fake/" + spec.Session.ID,
			}
		}
		a.workSpec = spec
		writeFakeJSON(response, map[string]any{"ok": true})
	case request.Method == http.MethodGet && strings.HasSuffix(request.URL.Path, "/state"):
		if !a.currentAssignment(request) {
			http.Error(response, "stale Assignment", http.StatusBadRequest)
			return
		}
		writeFakeJSON(response, a.state)
	case request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/events"):
		if !a.currentAssignment(request) {
			http.Error(response, "stale Assignment", http.StatusBadRequest)
			return
		}
		a.state.Status = "idle"
		a.state.ResumeRef = "checkpoint-7"
		a.state.ResumeRevision = 7
		writeFakeJSON(response, map[string]any{"data": []any{map[string]any{
			"id": "event-user-1", "type": "user.message", "processed_at": time.Now().UTC(),
			"content": []any{map[string]any{"type": "text", "text": "hello"}},
		}}})
	case request.Method == http.MethodGet && strings.HasSuffix(request.URL.Path, "/events/stream"):
		if !a.currentAssignment(request) {
			http.Error(response, "stale Assignment", http.StatusBadRequest)
			return
		}
		response.Header().Set("Content-Type", "text/event-stream")
		encoded, _ := json.Marshal(fakeAgentMessage())
		_, _ = fmt.Fprintf(response, "event: agent.message\ndata: %s\n\n", encoded)
	case request.Method == http.MethodGet && strings.HasSuffix(request.URL.Path, "/events"):
		if !a.currentAssignment(request) {
			http.Error(response, "stale Assignment", http.StatusBadRequest)
			return
		}
		writeFakeJSON(response, map[string]any{"data": []any{fakeAgentMessage()}, "next_page": nil})
	default:
		http.NotFound(response, request)
	}
}

func (a *fakeAgentlet) currentAssignment(request *http.Request) bool {
	return a.workSpec.AssignmentID != "" &&
		request.Header.Get(executionapi.AssignmentHeader) == a.workSpec.AssignmentID
}

func (a *fakeAgentlet) work() executionapi.WorkSpec {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.workSpec
}

func fakeAgentMessage() map[string]any {
	return map[string]any{
		"id": "event-agent-1", "type": "agent.message", "processed_at": time.Now().UTC(),
		"content": []any{map[string]any{"type": "text", "text": "done"}},
	}
}

func writeFakeJSON(response http.ResponseWriter, value any) {
	response.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(response).Encode(value)
}
