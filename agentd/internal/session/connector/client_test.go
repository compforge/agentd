package connector

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/compforge/agentd/internal/executionapi"
)

func TestClientInstallsWorkAndCallsAssignedSessionActions(t *testing.T) {
	t.Parallel()
	var installed executionapi.WorkSpec
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Header.Get(executionapi.AssignmentHeader) != "assignment-1" ||
			request.Header.Get(executionapi.WorkerHeader) != "worker-1" {
			http.Error(response, "missing Assignment fence", http.StatusBadRequest)
			return
		}
		switch request.URL.Path {
		case "/internal/v1/sessions/session-1":
			if request.Method != http.MethodPut {
				http.Error(response, "method", http.StatusMethodNotAllowed)
				return
			}
			if err := json.NewDecoder(request.Body).Decode(&installed); err != nil {
				http.Error(response, err.Error(), http.StatusBadRequest)
				return
			}
			response.Header().Set("Content-Type", "application/json")
			_, _ = response.Write([]byte(`{"ok":true}`))
		case "/internal/v1/sessions/session-1/state":
			response.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(response).Encode(executionapi.SessionState{
				AssignmentID: "assignment-1", Status: "idle", ResumeRef: "checkpoint-7", ResumeRevision: 7,
			})
		case "/internal/v1/sessions/session-1/wake", "/internal/v1/sessions/session-1/interrupt":
			if request.Method != http.MethodPost {
				http.Error(response, "method", http.StatusMethodNotAllowed)
				return
			}
			_, _ = response.Write([]byte(`{"ok":true}`))
		default:
			http.NotFound(response, request)
		}
	}))
	defer server.Close()

	client, err := New(Config{
		RequestTimeout: time.Second, DialTimeout: time.Second, ResponseHeaderTimeout: time.Second,
		IdleConnTimeout: time.Second, MaxIdleConns: 4, MaxIdleConnsPerHost: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	target := Target{
		Endpoint: server.URL,
		Work: executionapi.WorkSpec{
			AssignmentID: "assignment-1", WorkerID: "worker-1",
			Session: executionapi.SessionSnapshot{ID: "session-1"},
		},
	}

	if err := client.Ensure(context.Background(), target); err != nil {
		t.Fatal(err)
	}
	if installed.AssignmentID != "assignment-1" || installed.Session.ID != "session-1" {
		t.Fatalf("installed WorkSpec = %#v", installed)
	}
	state, err := client.SessionState(context.Background(), target)
	if err != nil {
		t.Fatal(err)
	}
	if state.ResumeRef != "checkpoint-7" || state.ResumeRevision != 7 {
		t.Fatalf("Session state = %#v", state)
	}

	if err := client.Wake(context.Background(), target); err != nil {
		t.Fatal(err)
	}
	if err := client.Interrupt(context.Background(), target); err != nil {
		t.Fatal(err)
	}
}
