package connector

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/compforge/agentd/internal/executionapi"
)

func TestClientInstallsWorkAndForwardsAssignedRequests(t *testing.T) {
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
		case "/internal/v1/sessions/session-1/events":
			if request.URL.Query().Get("limit") != "10" || request.Header.Get("X-Request-ID") != "request-1" {
				http.Error(response, "forwarded request metadata", http.StatusBadRequest)
				return
			}
			body, _ := io.ReadAll(request.Body)
			response.Header().Set("Content-Type", "application/json")
			_, _ = response.Write(body)
		case "/internal/v1/sessions/session-1/events/stream":
			response.Header().Set("Content-Type", "text/event-stream")
			_, _ = response.Write([]byte("event: message\ndata: {\"type\":\"agent.message\"}\n\n"))
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

	response, err := client.Forward(
		context.Background(), target, http.MethodPost,
		"/internal/v1/sessions/session-1/events", "limit=10", []byte(`{"events":[]}`),
		http.Header{"X-Request-ID": []string{"request-1"}}, false,
	)
	if err != nil {
		t.Fatal(err)
	}
	body, readErr := io.ReadAll(response.Body)
	response.Body.Close()
	if readErr != nil || string(body) != `{"events":[]}` {
		t.Fatalf("forwarded response = %q, error = %v", body, readErr)
	}

	response, err = client.Forward(
		context.Background(), target, http.MethodGet,
		"/internal/v1/sessions/session-1/events/stream", "", nil, nil, true,
	)
	if err != nil {
		t.Fatal(err)
	}
	body, readErr = io.ReadAll(response.Body)
	response.Body.Close()
	if readErr != nil || !strings.Contains(string(body), `"type":"agent.message"`) {
		t.Fatalf("streamed response = %q, error = %v", body, readErr)
	}
}

func TestClientForwardsEventReadsWithoutAssignment(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Header.Get(executionapi.WorkerHeader) != "worker-2" {
			http.Error(response, "wrong Worker", http.StatusBadRequest)
			return
		}
		if request.Header.Get(executionapi.AssignmentHeader) != "" {
			http.Error(response, "unexpected Assignment", http.StatusBadRequest)
			return
		}
		_, _ = response.Write([]byte(`{"data":[],"next_page":null}`))
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
	response, err := client.ForwardEventRead(
		context.Background(), EventTarget{Endpoint: server.URL, WorkerID: "worker-2"},
		http.MethodGet, "/internal/v1/sessions/session-1/events", "", nil, false,
	)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("Event read status = %d", response.StatusCode)
	}
}
