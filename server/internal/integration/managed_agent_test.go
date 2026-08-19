package integration

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/compforge/agentd/server/internal/app"
	"github.com/compforge/agentd/server/internal/harness"
	"github.com/compforge/agentd/server/internal/hostel"
	"github.com/compforge/agentd/server/internal/persistence"
)

func TestManagedAgentMySQLHostelRoundTripAndRestart(t *testing.T) {
	dsn := integrationEnv(t, "AGENTD_TEST_MYSQL_DSN")
	hostelURL := integrationEnv(t, "AGENTD_TEST_HOSTEL_URL")
	apiKey := integrationEnv(t, "ANTHROPIC_API_KEY")
	modelID := integrationEnv(t, "AGENTD_TEST_MODEL")

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
		APIKey: apiKey, BaseURL: os.Getenv("ANTHROPIC_BASE_URL"),
		RequestTimeout: 2 * time.Minute, OperationTimeout: 15 * time.Second, ToolTimeout: time.Minute,
		Ledger: storage.Ledger, State: storage.HarnessStates, Sandbox: sandboxEngine,
	})
	if err != nil {
		t.Fatal(err)
	}

	application := app.New(storage.Resources, app.NewEventLog(storage.Ledger), agentHarness)
	agent, err := application.CreateAgent(ctx, app.Agent{
		Name: "integration-" + time.Now().UTC().Format("20060102150405.000000000"), ModelID: modelID,
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
	if err := restarted.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.SendEvents(ctx, session.ID, []app.IncomingEvent{{
		Type: "user.message", Content: []map[string]any{{"type": "text", "text": "Reply with RESUMED."}},
	}}); err != nil {
		t.Fatal(err)
	}
	waitForIdle(t, ctx, restarted, session.ID)
	if count := assistantMessageCount(t, ctx, restarted, session.ID); count <= firstMessages {
		t.Fatalf("assistant messages after restart = %d, want more than %d", count, firstMessages)
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
