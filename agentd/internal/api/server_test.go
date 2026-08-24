package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
	agentledger "github.com/compforge/agent-ledger/go"
	"github.com/compforge/agentd/agentd/internal/api"
	"github.com/compforge/agentd/agentd/internal/model"
	gormrepo "github.com/compforge/agentd/agentd/internal/repo/gorm"
	"github.com/compforge/agentd/agentd/internal/service"
	"github.com/compforge/agentd/agentd/internal/session/connector"
	sessionobserver "github.com/compforge/agentd/agentd/internal/session/observer"
	sessionreconciler "github.com/compforge/agentd/agentd/internal/session/reconciler"
	managedevent "github.com/compforge/agentd/internal/event"
	"github.com/compforge/agentd/internal/executionapi"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestManagedAgentSDKRunsThroughControlPlaneAndAssignedAgentlet(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ledgerStore := agentledger.NewMemoryEventStore()
	events := managedevent.NewLog(ledgerStore)
	worker := &fakeAgentlet{workerID: "worker-1", events: events}
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
	controlService, err := service.New(repository, time.Minute, 0)
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
	executionReconciler, err := sessionreconciler.New(controlService, events, agentletConnector, nil, sessionreconciler.Config{
		Interval: time.Second, RequestTimeout: time.Second, Concurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

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
		controlService, events, agentletConnector, executionReconciler,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		api.WithAPIKey("test"),
	).Register(server.Engine)
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Run() }()
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
		<-serveErr
	})
	modelSecret := "model-secret-that-must-not-be-returned"
	modelBody, err := json.Marshal(map[string]any{
		"id": "claude-sonnet-4-6", "provider": "anthropic", "api_key": modelSecret,
	})
	if err != nil {
		t.Fatal(err)
	}
	modelRequest, err := http.NewRequestWithContext(
		ctx, http.MethodPost, "http://"+listener.Addr().String()+"/v1/models", bytes.NewReader(modelBody),
	)
	if err != nil {
		t.Fatal(err)
	}
	modelRequest.Header.Set("Content-Type", "application/json")
	modelRequest.Header.Set("x-api-key", "test")
	modelResponse, err := http.DefaultClient.Do(modelRequest)
	if err != nil {
		t.Fatalf("create Model through control plane: %v", err)
	}
	createdModelBody, err := io.ReadAll(modelResponse.Body)
	modelResponse.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if modelResponse.StatusCode != http.StatusOK || bytes.Contains(createdModelBody, []byte(modelSecret)) {
		t.Fatalf("create Model response status=%d body=%s", modelResponse.StatusCode, createdModelBody)
	}
	for _, path := range []string{"/v1/models/claude-sonnet-4-6", "/v1/models"} {
		modelReadRequest, err := http.NewRequestWithContext(
			ctx, http.MethodGet, "http://"+listener.Addr().String()+path, nil,
		)
		if err != nil {
			t.Fatal(err)
		}
		modelReadRequest.Header.Set("x-api-key", "test")
		response, err := http.DefaultClient.Do(modelReadRequest)
		if err != nil {
			t.Fatalf("read Model resource %s: %v", path, err)
		}
		body, readErr := io.ReadAll(response.Body)
		response.Body.Close()
		if readErr != nil {
			t.Fatal(readErr)
		}
		if response.StatusCode != http.StatusOK || bytes.Contains(body, []byte(modelSecret)) {
			t.Fatalf("read Model %s status=%d body=%s", path, response.StatusCode, body)
		}
	}
	rotatedModelSecret := "rotated-model-secret-that-must-not-be-returned"
	modelUpdateBody, err := json.Marshal(map[string]any{
		"provider": "anthropic", "model": "claude-sonnet-4-6",
		"base_url": "https://model.example.test", "api_key": rotatedModelSecret,
	})
	if err != nil {
		t.Fatal(err)
	}
	modelUpdateRequest, err := http.NewRequestWithContext(
		ctx, http.MethodPost, "http://"+listener.Addr().String()+"/v1/models/claude-sonnet-4-6",
		bytes.NewReader(modelUpdateBody),
	)
	if err != nil {
		t.Fatal(err)
	}
	modelUpdateRequest.Header.Set("Content-Type", "application/json")
	modelUpdateRequest.Header.Set("x-api-key", "test")
	modelUpdateResponse, err := http.DefaultClient.Do(modelUpdateRequest)
	if err != nil {
		t.Fatalf("update Model through control plane: %v", err)
	}
	updatedModelBody, err := io.ReadAll(modelUpdateResponse.Body)
	modelUpdateResponse.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if modelUpdateResponse.StatusCode != http.StatusOK ||
		bytes.Contains(updatedModelBody, []byte(modelSecret)) ||
		bytes.Contains(updatedModelBody, []byte(rotatedModelSecret)) {
		t.Fatalf("update Model response status=%d body=%s", modelUpdateResponse.StatusCode, updatedModelBody)
	}

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
	updatedDescription := "contract-test-v2"
	agent, err = client.Beta.Agents.Update(ctx, agent.ID, anthropic.BetaAgentUpdateParams{
		Version: param.NewOpt(agent.Version), Description: param.NewOpt(updatedDescription),
	})
	if err != nil {
		t.Fatalf("update Agent through public control plane: %v", err)
	}
	if agent.Version != 2 || agent.Description != updatedDescription {
		t.Fatalf("updated Agent = %#v", agent)
	}
	versions, err := client.Beta.Agents.Versions.List(ctx, agent.ID, anthropic.BetaAgentVersionListParams{
		Limit: param.NewOpt(int64(1)),
	})
	if err != nil {
		t.Fatalf("list Agent versions through public control plane: %v", err)
	}
	if len(versions.Data) != 1 || versions.Data[0].Version != 2 || versions.NextPage == "" {
		t.Fatalf("Agent versions = %#v", versions.Data)
	}
	versions, err = versions.GetNextPage()
	if err != nil {
		t.Fatalf("list next Agent version page: %v", err)
	}
	if versions == nil || len(versions.Data) != 1 || versions.Data[0].Version != 1 || versions.NextPage != "" {
		t.Fatalf("next Agent versions = %#v", versions)
	}
	original, err := client.Beta.Agents.Get(ctx, agent.ID, anthropic.BetaAgentGetParams{Version: param.NewOpt(int64(1))})
	if err != nil {
		t.Fatalf("get pinned Agent version through public control plane: %v", err)
	}
	if original.Version != 1 || original.Description != "" {
		t.Fatalf("pinned Agent version = %#v", original)
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
	updatedEnvironmentName := "contract-test-v2"
	environment, err = client.Beta.Environments.Update(ctx, environment.ID, anthropic.BetaEnvironmentUpdateParams{
		Name: param.NewOpt(updatedEnvironmentName), Metadata: map[string]string{"team": "quality"},
	})
	if err != nil {
		t.Fatalf("update Environment through public control plane: %v", err)
	}
	if environment.Name != updatedEnvironmentName || environment.Metadata["team"] != "quality" {
		t.Fatalf("updated Environment = %#v", environment)
	}
	session, err := client.Beta.Sessions.New(ctx, anthropic.BetaSessionNewParams{
		Agent:         anthropic.BetaSessionNewParamsAgentUnion{OfString: param.NewOpt(agent.ID)},
		EnvironmentID: environment.ID,
		InitialEvents: []anthropic.BetaSessionNewParamsInitialEventUnion{{
			OfUserMessage: &anthropic.BetaManagedAgentsUserMessageEventParams{
				Type: anthropic.BetaManagedAgentsUserMessageEventParamsTypeUserMessage,
				Content: []anthropic.BetaManagedAgentsUserMessageEventParamsContentUnion{{
					OfText: &anthropic.BetaManagedAgentsTextBlockParam{Type: anthropic.BetaManagedAgentsTextBlockTypeText, Text: "hello"},
				}},
			},
		}},
	})
	if err != nil {
		t.Fatalf("create Session through public control plane: %v", err)
	}
	page, err := client.Beta.Sessions.Events.List(ctx, session.ID, anthropic.BetaSessionEventListParams{})
	if err != nil {
		t.Fatalf("list unassigned Session Events: %v", err)
	}
	if len(page.Data) != 1 || page.Data[0].Type != "user.message" {
		t.Fatalf("initial Session Events = %#v", page.Data)
	}
	if err := executionReconciler.Reconcile(ctx); err != nil {
		t.Fatalf("reconcile durable Event into Agentlet execution: %v", err)
	}
	installed := worker.work()
	if installed.Session.ID != session.ID || installed.Agent.ID != agent.ID ||
		installed.Environment.ID != environment.ID || installed.AssignmentID == "" {
		t.Fatalf("installed WorkSpec = %#v", installed)
	}
	if installed.Agent.Model.ID != "claude-sonnet-4-6" || installed.Agent.Model.Provider != "anthropic" ||
		installed.Agent.Model.BaseURL != "https://model.example.test" ||
		installed.Agent.Model.APIKey != rotatedModelSecret {
		t.Fatalf("installed Model snapshot = %#v", installed.Agent.Model)
	}

	page, err = client.Beta.Sessions.Events.List(ctx, session.ID, anthropic.BetaSessionEventListParams{
		Limit: param.NewOpt(int64(1)),
	})
	if err != nil {
		t.Fatalf("list Event from shared Ledger: %v", err)
	}
	if len(page.Data) != 1 || page.Data[0].Type != "user.message" || page.NextPage == "" {
		t.Fatalf("listed Events = %#v", page.Data)
	}
	page, err = page.GetNextPage()
	if err != nil {
		t.Fatalf("list next Event page: %v", err)
	}
	if page == nil || len(page.Data) != 1 || page.Data[0].Type != "agent.message" || page.NextPage != "" {
		t.Fatalf("next Events = %#v", page)
	}

	streamCtx, cancelStream := context.WithTimeout(ctx, 2*time.Second)
	stream := client.Beta.Sessions.Events.StreamEvents(
		streamCtx, session.ID, anthropic.BetaSessionEventStreamParams{},
	)
	if !stream.Next() {
		cancelStream()
		t.Fatalf("stream Event through assigned Agentlet: %v", stream.Err())
	}
	if stream.Current().Type != "user.message" {
		cancelStream()
		t.Fatalf("streamed Event = %#v", stream.Current())
	}
	if err := stream.Close(); err != nil {
		cancelStream()
		t.Fatal(err)
	}
	cancelStream()

	usageRecorder, err := agentledger.OpenRecorder(ctx, agentledger.RecorderOptions{
		Store: ledgerStore, SessionID: session.ID, RunID: "usage-test", LaneName: "main",
		Actor: agentledger.NewActor("agent", "contract-test"),
	})
	if err != nil {
		t.Fatal(err)
	}
	usageTurn, err := usageRecorder.StartTurn(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	completedCall, err := usageRecorder.BeforeModelCall(ctx, usageTurn.ID, map[string]any{"input": []any{}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := usageRecorder.ModelCompleted(ctx, completedCall, map[string]any{
		"output": "done", "usage": map[string]any{
			"input_tokens": 10, "output_tokens": 4,
			"cache_read_input_tokens": 6, "cache_write_input_tokens": 2,
		},
	}); err != nil {
		t.Fatal(err)
	}
	failedCall, err := usageRecorder.BeforeModelCall(ctx, usageTurn.ID, map[string]any{"input": []any{}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := usageRecorder.ModelFailed(ctx, failedCall, errors.New("provider timeout"), map[string]any{
		"usage": map[string]any{"input_tokens": 3, "cache_read_input_tokens": 1},
	}); err != nil {
		t.Fatal(err)
	}

	source, err := sessionobserver.NewAgentletSource(controlService, agentletConnector)
	if err != nil {
		t.Fatal(err)
	}
	sessionObserver, err := sessionobserver.New(source, controlService, nil, sessionobserver.Config{
		Interval: time.Second, RequestTimeout: time.Second, Concurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := sessionObserver.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := controlService.ReconcilePlacement(ctx, session.ID, false); err != nil {
		t.Fatal(err)
	}
	current, err := client.Beta.Sessions.Get(ctx, session.ID, anthropic.BetaSessionGetParams{})
	if err != nil {
		t.Fatalf("read Session after background observation: %v", err)
	}
	if current.Status != anthropic.BetaManagedAgentsSessionStatusIdle {
		t.Fatalf("Session status = %q, want idle", current.Status)
	}
	if current.Usage.InputTokens != 13 || current.Usage.OutputTokens != 4 ||
		current.Usage.CacheReadInputTokens != 7 {
		t.Fatalf("Session usage = %#v", current.Usage)
	}
	stored, err := repository.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.ResumeRef != "checkpoint-7" || stored.ResumeRevision != 7 ||
		stored.Placement.Bound() || len(stored.ObserverStatus) != 0 {
		t.Fatalf("persisted observed state = %#v", stored)
	}
	page, err = client.Beta.Sessions.Events.List(ctx, session.ID, anthropic.BetaSessionEventListParams{})
	if err != nil {
		t.Fatalf("list Events after Assignment release: %v", err)
	}
	if len(page.Data) != 2 || page.Data[1].Type != "agent.message" {
		t.Fatalf("Events after Assignment release = %#v", page.Data)
	}
	updatedSession, err := client.Beta.Sessions.Update(ctx, session.ID, anthropic.BetaSessionUpdateParams{
		Title: param.NewOpt("renamed"), Metadata: map[string]string{"suite": "managed-api"},
	})
	if err != nil {
		t.Fatalf("update Session through public control plane: %v", err)
	}
	if updatedSession.Title != "renamed" || updatedSession.Metadata["suite"] != "managed-api" {
		t.Fatalf("updated Session = %#v", updatedSession)
	}
	archivedSession, err := client.Beta.Sessions.Archive(ctx, session.ID, anthropic.BetaSessionArchiveParams{})
	if err != nil {
		t.Fatalf("archive Session through public control plane: %v", err)
	}
	if archivedSession.ArchivedAt.IsZero() || archivedSession.Status != anthropic.BetaManagedAgentsSessionStatusTerminated {
		t.Fatalf("archived Session = %#v", archivedSession)
	}
	page, err = client.Beta.Sessions.Events.List(ctx, session.ID, anthropic.BetaSessionEventListParams{})
	if err != nil {
		t.Fatalf("list archived Session Events: %v", err)
	}
	if len(page.Data) != 2 || page.Data[0].Type != "user.message" || page.Data[1].Type != "agent.message" {
		t.Fatalf("archived Session Events = %#v", page.Data)
	}
	_, err = client.Beta.Sessions.Events.Send(ctx, session.ID, anthropic.BetaSessionEventSendParams{
		Events: []anthropic.BetaManagedAgentsEventParamsUnion{{
			OfUserMessage: &anthropic.BetaManagedAgentsUserMessageEventParams{
				Type: anthropic.BetaManagedAgentsUserMessageEventParamsTypeUserMessage,
				Content: []anthropic.BetaManagedAgentsUserMessageEventParamsContentUnion{{
					OfText: &anthropic.BetaManagedAgentsTextBlockParam{Type: anthropic.BetaManagedAgentsTextBlockTypeText, Text: "too late"},
				}},
			},
		}},
	})
	if err == nil {
		t.Fatal("archived Session accepted a new Event")
	}
	sessions, err := client.Beta.Sessions.List(ctx, anthropic.BetaSessionListParams{})
	if err != nil {
		t.Fatalf("list active Sessions through public control plane: %v", err)
	}
	if len(sessions.Data) != 0 {
		t.Fatalf("active Sessions after archive = %#v", sessions.Data)
	}
	sessions, err = client.Beta.Sessions.List(ctx, anthropic.BetaSessionListParams{
		IncludeArchived: param.NewOpt(true),
	})
	if err != nil {
		t.Fatalf("list archived Sessions through public control plane: %v", err)
	}
	if len(sessions.Data) != 1 || sessions.Data[0].ArchivedAt.IsZero() {
		t.Fatalf("archived Sessions = %#v", sessions.Data)
	}
	archivedEnvironment, err := client.Beta.Environments.Archive(
		ctx, environment.ID, anthropic.BetaEnvironmentArchiveParams{},
	)
	if err != nil {
		t.Fatalf("archive Environment through public control plane: %v", err)
	}
	if archivedEnvironment.ArchivedAt == "" {
		t.Fatalf("archived Environment = %#v", archivedEnvironment)
	}
	_, err = client.Beta.Sessions.New(ctx, anthropic.BetaSessionNewParams{
		Agent:         anthropic.BetaSessionNewParamsAgentUnion{OfString: param.NewOpt(agent.ID)},
		EnvironmentID: environment.ID,
	})
	if err == nil {
		t.Fatal("archived Environment accepted a new Session")
	}
	environments, err := client.Beta.Environments.List(ctx, anthropic.BetaEnvironmentListParams{})
	if err != nil || len(environments.Data) != 0 {
		t.Fatalf("active Environments after archive = %#v, %v", environments, err)
	}
	environments, err = client.Beta.Environments.List(ctx, anthropic.BetaEnvironmentListParams{
		IncludeArchived: param.NewOpt(true),
	})
	if err != nil || len(environments.Data) != 1 || environments.Data[0].ID != environment.ID {
		t.Fatalf("archived Environments = %#v, %v", environments, err)
	}
	archived, err := client.Beta.Agents.Archive(ctx, agent.ID, anthropic.BetaAgentArchiveParams{})
	if err != nil {
		t.Fatalf("archive Agent through public control plane: %v", err)
	}
	if archived.ArchivedAt.IsZero() || archived.Version != agent.Version {
		t.Fatalf("archived Agent = %#v", archived)
	}
	agents, err := client.Beta.Agents.List(ctx, anthropic.BetaAgentListParams{})
	if err != nil {
		t.Fatalf("list active Agents through public control plane: %v", err)
	}
	if len(agents.Data) != 0 {
		t.Fatalf("active Agents after archive = %#v", agents.Data)
	}
	agents, err = client.Beta.Agents.List(ctx, anthropic.BetaAgentListParams{
		IncludeArchived: param.NewOpt(true),
	})
	if err != nil {
		t.Fatalf("list archived Agents through public control plane: %v", err)
	}
	if len(agents.Data) != 1 || agents.Data[0].ArchivedAt.IsZero() {
		t.Fatalf("archived Agents = %#v", agents.Data)
	}
}

type fakeAgentlet struct {
	mu       sync.Mutex
	workerID string
	workSpec executionapi.WorkSpec
	state    executionapi.SessionState
	events   *managedevent.Log
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
	case request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/wake"):
		if !a.currentAssignment(request) {
			http.Error(response, "stale Assignment", http.StatusBadRequest)
			return
		}
		a.state.Status = "idle"
		a.state.ResumeRef = "checkpoint-7"
		a.state.ResumeRevision = 7
		pending, err := a.events.UnprocessedUserMessages(request.Context(), a.workSpec.Session.ID)
		if err != nil || len(pending) == 0 {
			http.Error(response, "missing durable input", http.StatusInternalServerError)
			return
		}
		inputID, _ := pending[0]["id"].(string)
		if err := a.events.AppendExecution(
			request.Context(), a.workSpec.Session.ID,
			managedevent.NewTurn(inputID, "agent.message", map[string]any{
				"content": []any{map[string]any{"type": "text", "text": "done"}},
			}),
		); err != nil {
			http.Error(response, err.Error(), http.StatusInternalServerError)
			return
		}
		if err := a.events.MarkProcessed(request.Context(), a.workSpec.Session.ID, inputID); err != nil {
			http.Error(response, err.Error(), http.StatusInternalServerError)
			return
		}
		writeFakeJSON(response, map[string]any{"ok": true})
	case request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/interrupt"):
		writeFakeJSON(response, map[string]any{"ok": true})
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

func writeFakeJSON(response http.ResponseWriter, value any) {
	response.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(response).Encode(value)
}
