//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	ledgergorm "github.com/compforge/agent-ledger/go/stores/gorm"
	"github.com/compforge/agentd/agentd/internal/model"
	control "github.com/compforge/agentd/agentd/internal/service"
	"github.com/compforge/agentd/agentd/internal/session/connector"
	sessionobserver "github.com/compforge/agentd/agentd/internal/session/observer"
	sessionreconciler "github.com/compforge/agentd/agentd/internal/session/reconciler"
	managedevent "github.com/compforge/agentd/internal/event"
	"github.com/compforge/agentd/internal/executionapi"
)

func TestSessionReconcilerReleasesObservedIdlePlacement(t *testing.T) {
	ctx := context.Background()
	repository, database := openRepositoryDatabase(t)
	ledger, err := ledgergorm.New(database, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := ledger.Initialize(ctx); err != nil {
		t.Fatal(err)
	}
	var assignmentID string
	agentlet := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/internal/v1/sessions/session-e2e/state":
			if request.Header.Get(executionapi.AssignmentHeader) != assignmentID ||
				request.Header.Get(executionapi.WorkerHeader) != "worker-e2e" {
				http.Error(response, "stale Assignment", http.StatusConflict)
				return
			}
			response.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(response).Encode(executionapi.SessionState{
				AssignmentID: assignmentID, Status: "idle",
				ResumeRef: "checkpoint-e2e", ResumeRevision: 3,
			})
		default:
			http.Error(response, "unexpected Agentlet request", http.StatusMethodNotAllowed)
		}
	}))
	defer agentlet.Close()

	application, err := control.New(repository, time.Minute, 0)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := repository.PutModel(ctx, model.Model{
		ID: "model-e2e", Provider: "anthropic", UpstreamID: "claude-sonnet-4-6",
		APIKey: "test", CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	agent := model.Agent{
		ID: "agent-e2e", VersionID: "agent-version-e2e", Name: "e2e", ModelID: "model-e2e", Version: 1,
		Tools: []map[string]any{}, Metadata: map[string]string{}, CreatedAt: now, UpdatedAt: now,
	}
	if err := repository.CreateAgentVersion(ctx, agent); err != nil {
		t.Fatal(err)
	}
	if err := repository.PutAgent(ctx, agent); err != nil {
		t.Fatal(err)
	}
	if err := repository.PutEnvironment(ctx, model.Environment{
		ID: "environment-e2e", Name: "e2e", Config: map[string]any{},
		Metadata: map[string]string{}, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := repository.PutSession(ctx, model.Session{
		ID: "session-e2e", AgentID: "agent-e2e", AgentVersionID: "agent-version-e2e",
		EnvironmentID: "environment-e2e", Metadata: map[string]string{},
		Status: model.SessionStatusIdle, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	workerStatus, err := json.Marshal(model.WorkerObserverStatus{
		ObservedAt: now, Exists: true, Ready: true, Endpoint: agentlet.URL,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := application.ObserveWorker(ctx, model.Worker{
		ID: "worker-e2e", Name: "worker-e2e", Capacity: 1, ObserverStatus: workerStatus,
	}); err != nil {
		t.Fatal(err)
	}
	placed, err := application.ReconcilePlacement(ctx, "session-e2e", true)
	if err != nil {
		t.Fatal(err)
	}
	assignmentID = placed.Placement.Fence
	events := managedevent.NewLog(ledger)
	input := managedevent.New("user.message", map[string]any{
		"content": []map[string]any{{"type": "text", "text": "hello"}},
	})
	input["processed_at"] = nil
	if err := events.AppendIngress(ctx, "session-e2e", input); err != nil {
		t.Fatal(err)
	}
	if err := events.AppendExecution(ctx, "session-e2e", managedevent.NewTurn(
		input["id"].(string), "agent.message",
		map[string]any{"content": []map[string]any{{"type": "text", "text": "done"}}},
	)); err != nil {
		t.Fatal(err)
	}

	client, err := connector.New(connector.Config{
		RequestTimeout: time.Second, DialTimeout: time.Second, ResponseHeaderTimeout: time.Second,
		IdleConnTimeout: time.Second, MaxIdleConns: 4, MaxIdleConnsPerHost: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	source, err := sessionobserver.NewAgentletSource(application, client)
	if err != nil {
		t.Fatal(err)
	}
	observer, err := sessionobserver.New(source, application, nil, sessionobserver.Config{
		Interval: time.Second, RequestTimeout: time.Second, Concurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := observer.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if err := events.MarkProcessed(ctx, "session-e2e", input["id"].(string)); err != nil {
		t.Fatal(err)
	}
	executionReconciler, err := sessionreconciler.New(application, events, client, nil, sessionreconciler.Config{
		Interval: time.Second, RequestTimeout: time.Second, Concurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := executionReconciler.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}

	session, err := repository.GetSession(ctx, "session-e2e")
	if err != nil {
		t.Fatal(err)
	}
	if session.Status != model.SessionStatusIdle || session.Placement.Bound() ||
		session.ResumeRef != "checkpoint-e2e" || session.ResumeRevision != 3 || len(session.ObserverStatus) != 0 {
		t.Fatalf("observed Session = %#v", session)
	}
	worker, err := repository.GetWorker(ctx, "worker-e2e")
	if err != nil {
		t.Fatal(err)
	}
	if worker.IdleSince == nil {
		t.Fatalf("released Worker = %#v", worker)
	}
	projected, err := events.List(ctx, "session-e2e")
	if err != nil {
		t.Fatal(err)
	}
	if len(projected) != 2 || projected[0]["type"] != "user.message" || projected[1]["type"] != "agent.message" {
		t.Fatalf("Events after Assignment release = %#v", projected)
	}
}
