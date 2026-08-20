//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	agentledger "github.com/compforge/agent-ledger/go"
	"github.com/compforge/agentd/server/internal/app"
	"github.com/compforge/agentd/server/internal/harness"
	"github.com/compforge/agentd/server/internal/hostel"
	"github.com/compforge/agentd/server/internal/persistence"
)

func TestManagedAgentMySQLHostelRoundTripAndRestart(t *testing.T) {
	dsn := integrationEnv(t, "AGENTD_TEST_MYSQL_DSN")
	hostelURL := integrationEnv(t, "AGENTD_TEST_HOSTEL_URL")
	model := newAnthropicModelStub(t, func(attempt int, writer http.ResponseWriter, _ *http.Request) {
		if attempt%2 == 1 {
			writeAnthropicBashCall(writer, attempt)
			return
		}
		answer := "READY"
		if attempt > 2 {
			answer = "RESUMED"
		}
		writeAnthropicAnswer(writer, answer)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	storage, err := persistence.OpenMySQL(ctx, persistence.Config{
		MySQLDSN: dsn, OperationTimeout: 15 * time.Second,
		MaxOpenConns: 4, MaxIdleConns: 2, ConnMaxLifetime: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = storage.Close() })
	sandboxEngine, err := hostel.NewEngine(hostel.EngineConfig{
		URL: hostelURL, RequestTimeout: 30 * time.Second, StartupTimeout: 30 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := sandboxEngine.Start(ctx); err != nil {
		t.Fatal(err)
	}
	agentHarness, err := harness.NewAgentGoRunner(harness.AgentGoRunnerConfig{
		APIKey: "test", BaseURL: model.URL(),
		RequestTimeout: 2 * time.Minute, OperationTimeout: 15 * time.Second, ToolTimeout: time.Minute,
		Ledger: storage.Ledger, State: storage.HarnessStates, Sandbox: sandboxEngine,
	})
	if err != nil {
		t.Fatal(err)
	}

	application := app.New(storage.Resources, app.NewEventLog(storage.Ledger), agentHarness)
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = application.Shutdown(shutdownCtx)
	})
	agent, err := application.CreateAgent(ctx, app.Agent{
		Name: "integration-" + time.Now().UTC().Format("20060102150405.000000000"), ModelID: "claude-sonnet-4-6",
		System: "For every request, call the bash tool with command pwd exactly once, then answer the user.",
		Tools:  []map[string]any{{"type": "agent_toolset_20260401"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	environment, err := application.CreateEnvironment(ctx, app.Environment{Name: "integration"})
	if err != nil {
		t.Fatal(err)
	}
	session, err := application.CreateSession(ctx, agent.ID, agent.Version, environment.ID, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := application.SendEvents(ctx, session.ID, []app.IncomingEvent{{
		Type: "user.message", Content: []map[string]any{{"type": "text", "text": "Reply with READY."}},
	}}); err != nil {
		t.Fatal(err)
	}
	waitForIdle(t, ctx, application, session.ID)
	firstMessages := assistantMessageCount(t, ctx, application, session.ID)
	if firstMessages == 0 {
		t.Fatal("first turn produced no assistant message")
	}
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := application.Shutdown(shutdownCtx); err != nil {
		t.Fatal(err)
	}

	restarted := app.New(storage.Resources, app.NewEventLog(storage.Ledger), agentHarness)
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = restarted.Shutdown(shutdownCtx)
	})
	if _, err := restarted.SendEvents(ctx, session.ID, []app.IncomingEvent{{
		Type: "user.message", Content: []map[string]any{{"type": "text", "text": "Reply with RESUMED."}},
	}}); err != nil {
		t.Fatal(err)
	}
	waitForIdle(t, ctx, restarted, session.ID)
	if count := assistantMessageCount(t, ctx, restarted, session.ID); count <= firstMessages {
		t.Fatalf("assistant messages after restart = %d, want more than %d", count, firstMessages)
	}
	model.assertRequests(t, 4)
	assertLiveToolLedger(t, ctx, storage.Ledger, session.ID)
}

func writeAnthropicBashCall(writer http.ResponseWriter, attempt int) {
	writer.Header().Set("Content-Type", "text/event-stream")
	_, _ = fmt.Fprint(writer, "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_e2e\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-sonnet-4-6\",\"content\":[],\"stop_reason\":null,\"usage\":{\"input_tokens\":1,\"output_tokens\":0}}}\n\n")
	_, _ = fmt.Fprintf(writer, "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_e2e_%d\",\"name\":\"bash\",\"input\":{}}}\n\n", attempt)
	_, _ = fmt.Fprint(writer, "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"command\\\":\\\"pwd\\\"}\"}}\n\n")
	_, _ = fmt.Fprint(writer, "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n")
	_, _ = fmt.Fprint(writer, "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\",\"stop_sequence\":null},\"usage\":{\"output_tokens\":1}}\n\n")
	_, _ = fmt.Fprint(writer, "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n")
}

func assertLiveToolLedger(
	t *testing.T,
	ctx context.Context,
	store agentledger.EventStore,
	sessionID string,
) {
	t.Helper()
	counts := make(map[string]int)
	for event, err := range store.ScanSession(ctx, sessionID, "") {
		if err != nil {
			t.Fatalf("scan live ledger events: %v", err)
		}
		counts[event.EventType]++
	}
	if counts["tool.requested"] != 2 || counts["tool.completed"] != 2 {
		t.Fatalf("tool ledger counts = %#v, want two requested and two completed", counts)
	}
}

func integrationEnv(t *testing.T, name string) string {
	t.Helper()
	value := os.Getenv(name)
	if value != "" {
		return value
	}
	if os.Getenv("AGENTD_REQUIRE_INTEGRATION") == "1" {
		t.Fatalf("%s is required", name)
	}
	t.Skip("managed agent integration environment is not configured")
	return ""
}

func waitForIdle(t *testing.T, ctx context.Context, application *app.App, sessionID string) {
	t.Helper()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		session, err := application.GetSession(ctx, sessionID)
		if err != nil {
			t.Fatal(err)
		}
		if session.Control.Status == "idle" {
			return
		}
		if session.Control.Status == "terminated" {
			t.Fatal("session terminated during integration test")
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-ticker.C:
		}
	}
}

func assistantMessageCount(t *testing.T, ctx context.Context, application *app.App, sessionID string) int {
	t.Helper()
	events, err := application.ListEvents(ctx, sessionID)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, event := range events {
		if event["type"] == "agent.message" {
			count++
		}
	}
	return count
}
