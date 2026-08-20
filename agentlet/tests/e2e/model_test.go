//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/compforge/agentd/agentlet/internal/app"
	"github.com/compforge/agentd/agentlet/internal/harness"
	"github.com/compforge/agentd/agentlet/internal/sandbox/engine"
)

func TestManagedAgentAnswersThroughModel(t *testing.T) {
	model := newAnthropicModelStub(t, func(_ int, writer http.ResponseWriter, _ *http.Request) {
		writeAnthropicAnswer(writer, "42")
	})
	backend, client := startAgentGoModelE2E(t, model.URL(), time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, _, session := createSQLiteE2EConfiguredSession(
		t, ctx, client, "model-answer", anthropic.BetaManagedAgentsModelClaudeSonnet4_6,
		"Answer arithmetic questions with only the number.",
	)
	sendSQLiteE2EMessage(t, ctx, client, session.ID, "What is 6 * 7?")
	waitForSQLiteE2EIdle(t, ctx, client, session.ID)

	model.assertRequests(t, 1, "Answer arithmetic questions with only the number.", "What is 6 * 7?")
	assertSQLiteE2EAgentMessages(t, ctx, client, session.ID, []string{"42"})
	assertSQLiteE2EModelLedger(t, ctx, backend, session.ID, []string{"model.requested", "model.completed"})
}

func TestManagedAgentContinuesAfterMidStreamModelTimeout(t *testing.T) {
	model := newAnthropicModelStub(t, func(attempt int, writer http.ResponseWriter, request *http.Request) {
		if attempt == 1 {
			writeAnthropicPartial(writer, "PARTIAL")
			<-request.Context().Done()
			return
		}
		writeAnthropicAnswer(writer, "RECOVERED")
	})
	backend, client := startAgentGoModelE2E(t, model.URL(), 100*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, _, session := createSQLiteE2EConfiguredSession(
		t, ctx, client, "model-timeout", anthropic.BetaManagedAgentsModelClaudeSonnet4_6,
		"Return one complete answer and never expose partial output.",
	)
	sendSQLiteE2EMessage(t, ctx, client, session.ID, "This turn will time out.")
	waitForSQLiteE2EIdle(t, ctx, client, session.ID)
	model.assertRequests(t, 1, "This turn will time out.")
	assertSQLiteE2EAgentMessages(t, ctx, client, session.ID, nil)
	assertSQLiteE2EEventContains(t, ctx, client, session.ID, "session.error", "runtime_error")
	assertSQLiteE2EEventContains(t, ctx, client, session.ID, "session.status_idle", "retries_exhausted")
	assertSQLiteE2EModelLedger(t, ctx, backend, session.ID, []string{"model.requested", "model.failed"})

	sendSQLiteE2EMessage(t, ctx, client, session.ID, "Try again.")
	waitForSQLiteE2EIdle(t, ctx, client, session.ID)
	model.assertRequests(t, 2)
	assertSQLiteE2EAgentMessages(t, ctx, client, session.ID, []string{"RECOVERED"})
	assertSQLiteE2EModelLedger(t, ctx, backend, session.ID, []string{
		"model.requested", "model.failed", "model.requested", "model.completed",
	})
}

func TestManagedAgentAnswersThroughRealModel(t *testing.T) {
	apiKey := integrationEnv(t, "ANTHROPIC_API_KEY")
	modelID := integrationEnv(t, "AGENTD_TEST_MODEL")
	backend, client := startAgentGoModelE2EWithKey(
		t, apiKey, os.Getenv("ANTHROPIC_BASE_URL"), 3*time.Minute,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	_, _, session := createSQLiteE2EConfiguredSession(
		t, ctx, client, "real-model", anthropic.BetaManagedAgentsModel(modelID),
		"Follow the user's response-format instruction exactly.",
	)
	sendSQLiteE2EMessage(t, ctx, client, session.ID, "Reply with exactly: AGENTD_REAL_MODEL_E2E_OK")
	waitForSQLiteE2EIdle(t, ctx, client, session.ID)

	assertSQLiteE2EAgentMessageContains(t, ctx, client, session.ID, "AGENTD_REAL_MODEL_E2E_OK")
	assertSQLiteE2EModelLedger(t, ctx, backend, session.ID, []string{"model.requested", "model.completed"})
}

func startAgentGoModelE2E(
	t *testing.T,
	modelURL string,
	requestTimeout time.Duration,
) (*sqliteE2EBackend, anthropic.Client) {
	return startAgentGoModelE2EWithKey(t, "test", modelURL, requestTimeout)
}

func startAgentGoModelE2EWithKey(
	t *testing.T,
	apiKey, modelURL string,
	requestTimeout time.Duration,
) (*sqliteE2EBackend, anthropic.Client) {
	t.Helper()
	backend := openSQLiteE2EBackend(t, filepath.Join(t.TempDir(), "agentd-model-e2e.db"))
	t.Cleanup(func() { backend.close(t) })
	runner, err := harness.NewAgentGoRunner(harness.AgentGoRunnerConfig{
		APIKey: apiKey, BaseURL: modelURL, RequestTimeout: requestTimeout,
		OperationTimeout: 2 * time.Second, ToolTimeout: 2 * time.Second,
		Ledger: backend.ledger, Checkpoints: backend.checkpoints, Sandbox: noopSandbox{},
	})
	if err != nil {
		t.Fatalf("create AgentGo E2E runner: %v", err)
	}
	application := app.New(backend.resources, app.NewEventLog(backend.ledger), runner)
	_, client := startSQLiteE2EServer(t, application)
	return backend, client
}

func assertSQLiteE2EAgentMessageContains(
	t *testing.T,
	ctx context.Context,
	client anthropic.Client,
	sessionID, expected string,
) {
	t.Helper()
	page, err := client.Beta.Sessions.Events.List(ctx, sessionID, anthropic.BetaSessionEventListParams{})
	if err != nil {
		t.Fatalf("list events through official SDK: %v", err)
	}
	for _, event := range page.Data {
		if event.Type != "agent.message" {
			continue
		}
		for _, content := range event.AsAgentMessage().Content {
			if strings.Contains(content.AsText().Text, expected) {
				return
			}
		}
	}
	t.Fatalf("agent message containing %q was not found", expected)
}

type anthropicModelStub struct {
	server *httptest.Server

	mu       sync.Mutex
	requests [][]byte
	problems []string
}

func newAnthropicModelStub(
	t *testing.T,
	respond func(attempt int, writer http.ResponseWriter, request *http.Request),
) *anthropicModelStub {
	t.Helper()
	stub := &anthropicModelStub{}
	stub.server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			stub.recordProblem("read model request: %v", err)
			http.Error(writer, "invalid request", http.StatusBadRequest)
			return
		}
		stub.mu.Lock()
		stub.requests = append(stub.requests, append([]byte(nil), body...))
		attempt := len(stub.requests)
		stub.mu.Unlock()
		if request.URL.Path != "/v1/messages" {
			stub.recordProblem("model request path = %q, want /v1/messages", request.URL.Path)
		}
		respond(attempt, writer, request)
	}))
	t.Cleanup(stub.server.Close)
	return stub
}

func (s *anthropicModelStub) URL() string { return s.server.URL }

func (s *anthropicModelStub) recordProblem(format string, values ...any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.problems = append(s.problems, fmt.Sprintf(format, values...))
}

func (s *anthropicModelStub) assertRequests(t *testing.T, count int, contains ...string) {
	t.Helper()
	s.mu.Lock()
	requests := append([][]byte(nil), s.requests...)
	problems := append([]string(nil), s.problems...)
	s.mu.Unlock()
	if len(problems) > 0 {
		t.Fatalf("model server problems: %s", strings.Join(problems, "; "))
	}
	if len(requests) != count {
		t.Fatalf("model requests = %d, want %d", len(requests), count)
	}
	for _, expected := range contains {
		for _, request := range requests {
			if !strings.Contains(string(request), expected) {
				t.Fatalf("model request does not contain %q: %s", expected, request)
			}
		}
	}
}

func writeAnthropicPartial(writer http.ResponseWriter, text string) {
	writer.Header().Set("Content-Type", "text/event-stream")
	_, _ = fmt.Fprintf(writer, "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_e2e\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-sonnet-4-6\",\"content\":[],\"stop_reason\":null,\"usage\":{\"input_tokens\":1,\"output_tokens\":0}}}\n\n")
	_, _ = fmt.Fprintf(writer, "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n")
	encoded, _ := json.Marshal(text)
	_, _ = fmt.Fprintf(writer, "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":%s}}\n\n", encoded)
	if flusher, ok := writer.(http.Flusher); ok {
		flusher.Flush()
	}
}

func writeAnthropicAnswer(writer http.ResponseWriter, text string) {
	writeAnthropicPartial(writer, text)
	_, _ = fmt.Fprint(writer, "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n")
	_, _ = fmt.Fprint(writer, "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\",\"stop_sequence\":null},\"usage\":{\"output_tokens\":1}}\n\n")
	_, _ = fmt.Fprint(writer, "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n")
}

func assertSQLiteE2EAgentMessages(
	t *testing.T,
	ctx context.Context,
	client anthropic.Client,
	sessionID string,
	want []string,
) {
	t.Helper()
	page, err := client.Beta.Sessions.Events.List(ctx, sessionID, anthropic.BetaSessionEventListParams{})
	if err != nil {
		t.Fatalf("list events through official SDK: %v", err)
	}
	var got []string
	for _, event := range page.Data {
		if event.Type != "agent.message" {
			continue
		}
		message := event.AsAgentMessage()
		for _, content := range message.Content {
			if text := content.AsText().Text; text != "" {
				got = append(got, text)
			}
		}
	}
	if strings.Join(got, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("agent messages = %q, want %q", got, want)
	}
}

func assertSQLiteE2EModelLedger(
	t *testing.T,
	ctx context.Context,
	backend *sqliteE2EBackend,
	sessionID string,
	want []string,
) {
	t.Helper()
	var got []string
	view, err := backend.ledger.LoadSession(ctx, sessionID)
	if err != nil {
		t.Fatalf("load model ledger session: %v", err)
	}
	actions := make(map[string]string, len(view.Actions))
	for _, action := range view.Actions {
		actions[action.ID] = action.Type
	}
	attempts := make(map[string]string, len(view.Attempts))
	for _, attempt := range view.Attempts {
		attempts[attempt.ID] = actions[attempt.ActionID]
	}
	for _, event := range view.Events {
		if attempts[event.SubjectID] == "model_call" && strings.HasPrefix(event.EventType, "attempt.") {
			got = append(got, "model."+strings.TrimPrefix(event.EventType, "attempt."))
		}
	}
	if strings.Join(got, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("model ledger events = %q, want %q", got, want)
	}
}

func assertSQLiteE2EEventContains(
	t *testing.T,
	ctx context.Context,
	client anthropic.Client,
	sessionID, eventType, expected string,
) {
	t.Helper()
	page, err := client.Beta.Sessions.Events.List(ctx, sessionID, anthropic.BetaSessionEventListParams{})
	if err != nil {
		t.Fatalf("list events through official SDK: %v", err)
	}
	for _, event := range page.Data {
		if event.Type == eventType && strings.Contains(event.RawJSON(), expected) {
			return
		}
	}
	t.Fatalf("event %q containing %q was not found", eventType, expected)
}

type noopSandbox struct{}

func (noopSandbox) Name() string                         { return "noop" }
func (noopSandbox) Start(context.Context) error          { return nil }
func (noopSandbox) Ensure(context.Context, string) error { return nil }
func (noopSandbox) Stat(context.Context, string, string) (engine.FileInfo, error) {
	return engine.FileInfo{}, fs.ErrNotExist
}
func (noopSandbox) ReadFile(context.Context, string, string) ([]byte, error) {
	return nil, fs.ErrNotExist
}
func (noopSandbox) ReadDir(context.Context, string, string) ([]engine.DirEntry, error) {
	return nil, fs.ErrNotExist
}
func (noopSandbox) WriteFile(context.Context, string, string, []byte, fs.FileMode) error { return nil }
func (noopSandbox) MkdirAll(context.Context, string, string, fs.FileMode) error          { return nil }
func (noopSandbox) Execute(context.Context, string, engine.Command) (engine.CommandResult, error) {
	return engine.CommandResult{}, fmt.Errorf("noop sandbox does not execute commands")
}
