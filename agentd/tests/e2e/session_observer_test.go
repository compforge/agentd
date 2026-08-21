//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/compforge/agentd/agentd/internal/model"
	control "github.com/compforge/agentd/agentd/internal/service"
	"github.com/compforge/agentd/agentd/internal/session/connector"
	sessionobserver "github.com/compforge/agentd/agentd/internal/session/observer"
	"github.com/compforge/agentd/internal/executionapi"
)

func TestSessionObservationReleasesIdleAssignment(t *testing.T) {
	ctx := context.Background()
	repository := openRepository(t)
	var assignmentID string
	eventReadWithoutAssignment := false
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
		case request.Method == http.MethodGet && request.URL.Path == "/internal/v1/sessions/session-e2e/events":
			if request.Header.Get(executionapi.AssignmentHeader) != "" {
				http.Error(response, "Event read carried Assignment", http.StatusBadRequest)
				return
			}
			eventReadWithoutAssignment = true
			response.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(response).Encode(map[string]any{"data": []any{}, "next_page": nil})
		default:
			http.Error(response, "unexpected Agentlet request", http.StatusMethodNotAllowed)
		}
	}))
	defer agentlet.Close()

	application, err := control.New(repository, time.Minute, nil)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := repository.PutAgent(ctx, model.Agent{
		ID: "agent-e2e", Name: "e2e", ModelID: "model-e2e", Version: 1,
		Tools: []map[string]any{}, Metadata: map[string]string{}, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := repository.PutEnvironment(ctx, model.Environment{
		ID: "environment-e2e", Name: "e2e", Config: map[string]any{},
		Metadata: map[string]string{}, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := repository.PutSession(ctx, model.Session{
		ID: "session-e2e", AgentID: "agent-e2e", AgentVersion: 1,
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
	target, err := application.PrepareExecution(ctx, "session-e2e")
	if err != nil {
		t.Fatal(err)
	}
	assignmentID = target.Work.AssignmentID

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
	observer, err := sessionobserver.New(source, application, sessionobserver.Config{
		Interval: time.Second, RequestTimeout: time.Second, Concurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := observer.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}

	session, err := repository.GetSession(ctx, "session-e2e")
	if err != nil {
		t.Fatal(err)
	}
	if session.Status != model.SessionStatusIdle || session.AssignmentID != "" || session.WorkerID != "" ||
		session.ResumeRef != "checkpoint-e2e" || session.ResumeRevision != 3 || len(session.ObserverStatus) == 0 {
		t.Fatalf("observed Session = %#v", session)
	}
	worker, err := repository.GetWorker(ctx, "worker-e2e")
	if err != nil {
		t.Fatal(err)
	}
	if worker.IdleSince == nil {
		t.Fatalf("released Worker = %#v", worker)
	}
	eventTarget, err := application.ResolveEventTarget(ctx, "session-e2e")
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.ForwardEventRead(
		ctx,
		connector.EventTarget{Endpoint: eventTarget.Endpoint, WorkerID: eventTarget.WorkerID},
		http.MethodGet,
		"/internal/v1/sessions/session-e2e/events",
		"",
		nil,
		false,
	)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK || !eventReadWithoutAssignment {
		t.Fatalf("Event read after release = status %d, unassigned %t", response.StatusCode, eventReadWithoutAssignment)
	}
}
