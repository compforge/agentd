//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	agentledger "github.com/compforge/agent-ledger/go"
	"github.com/compforge/agentd/agentlet/internal/harness"
	"github.com/compforge/agentd/agentlet/internal/sandbox/engine"
	"github.com/compforge/agentd/agentlet/internal/service"
	"github.com/compforge/agentgo"
)

func TestUnsafeToolResolutionResumesWithoutReplay(t *testing.T) {
	model := newAnthropicModelStub(t, func(_ int, writer http.ResponseWriter, _ *http.Request) {
		writeAnthropicAnswer(writer, "AGENTD_TOOL_RESOLUTION_OK")
	})
	backend := openSQLiteE2EBackend(
		t, filepath.Join(t.TempDir(), "agentd-tool-resolution-e2e.db"), service.NewMemoryRepository(),
	)
	t.Cleanup(func() { backend.close(t) })
	events := service.NewEventLog(backend.ledger)
	sandbox := &countingWriteSandbox{}
	seedRunner := newToolResolutionRunner(t, backend, model.URL(), sandbox)
	seed := service.New(backend.resources, events, seedRunner)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	agent, err := seed.CreateAgent(ctx, service.Agent{
		Name: "tool-resolution",
		Model: harness.Model{
			ID: "tool-resolution-model", Provider: "anthropic", UpstreamID: "claude-sonnet-4-6",
			BaseURL: model.URL(), APIKey: "test",
		},
		System: "After a denied tool result, answer exactly AGENTD_TOOL_RESOLUTION_OK.",
		Tools:  []map[string]any{{"type": "agent_toolset_20260401"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	environment, err := seed.CreateEnvironment(ctx, service.Environment{
		Name: "tool-resolution", Config: map[string]any{"type": "cloud"},
	})
	if err != nil {
		t.Fatal(err)
	}
	session, err := seed.CreateSession(ctx, agent.ID, agent.Version, environment.ID, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	input := service.NewManagedEvent("user.message", map[string]any{
		"content": []map[string]any{{"type": "text", "text": "Write the release marker."}},
	})
	input["processed_at"] = nil
	if err := events.AppendIngress(ctx, session.ID, input); err != nil {
		t.Fatal(err)
	}
	toolUseID := seedUnresolvedWriteAttempt(t, ctx, backend, agent, &session, input)

	recoveredRunner := newToolResolutionRunner(t, backend, model.URL(), sandbox)
	recovered := service.New(backend.resources, events, recoveredRunner)
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Second)
		defer shutdownCancel()
		_ = recovered.Shutdown(shutdownCtx)
	})
	if err := recovered.Wake(ctx, session.ID); err != nil {
		t.Fatalf("wake replacement Agentlet for unresolved tool attempt: %v", err)
	}
	if got := waitForRequiredToolAction(t, ctx, recovered, events, session.ID); got != toolUseID {
		t.Fatalf("required tool use = %q, want %q", got, toolUseID)
	}
	assertSQLiteE2EEventContains(t, ctx, &sqliteE2EClient{events: events}, session.ID, "session.status_idle", "requires_action")
	model.assertRequests(t, 0)
	if writes := sandbox.writeCount(); writes != 0 {
		t.Fatalf("Sandbox writes before resolution = %d, want 0", writes)
	}

	denyMessage := "operator reconciled the unknown write without replay"
	resolution := service.NewManagedEvent("user.tool_confirmation", map[string]any{
		"tool_use_id": toolUseID, "result": "deny", "deny_message": denyMessage,
	})
	if err := events.AppendIngress(ctx, session.ID, resolution); err != nil {
		t.Fatal(err)
	}
	if err := recovered.Wake(ctx, session.ID); err != nil {
		t.Fatalf("wake Session with tool resolution: %v", err)
	}
	waitForResolvedAgentMessage(t, ctx, recovered, events, session.ID, "AGENTD_TOOL_RESOLUTION_OK")

	if writes := sandbox.writeCount(); writes != 0 {
		t.Fatalf("uncertain write was replayed %d time(s)", writes)
	}
	blocking, err := events.UnresolvedToolUses(ctx, session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(blocking) != 0 {
		t.Fatalf("resolved tool uses = %#v, want none", blocking)
	}
	model.assertRequests(t, 1, denyMessage)
	assertDeniedAttemptWasNotReplayed(t, ctx, backend.ledger, session.ID)
}

func newToolResolutionRunner(
	t *testing.T,
	backend *sqliteE2EBackend,
	modelURL string,
	sandbox engine.Engine,
) *harness.AgentGoRunner {
	t.Helper()
	runner, err := harness.NewAgentGoRunner(harness.AgentGoRunnerConfig{
		RequestTimeout: time.Second, OperationTimeout: 2 * time.Second, ToolTimeout: 2 * time.Second,
		Ledger: backend.ledger, Checkpoints: backend.checkpoints, Sandbox: sandbox,
	})
	if err != nil {
		t.Fatalf("create AgentGo tool-resolution runner: %v", err)
	}
	return runner
}

func seedUnresolvedWriteAttempt(
	t *testing.T,
	ctx context.Context,
	backend *sqliteE2EBackend,
	agent service.Agent,
	session *service.Session,
	input service.ManagedEvent,
) string {
	t.Helper()
	actor, err := backend.checkpoints.EnsureActor(ctx, agentledger.NewActorWithKey(
		fmt.Sprintf("agentd/agents/%s/versions/%d", agent.ID, agent.Version), "agent", "agentgo",
	))
	if err != nil {
		t.Fatalf("seed AgentGo actor: %v", err)
	}
	inputID, _ := input["id"].(string)
	userMessage := agentgo.UserMsg("Write the release marker.")
	userMessage.Metadata = map[string]any{"agentd.input_id": inputID}
	toolCallID := "toolu_uncertain_write"
	assistantMessage := agentgo.Message{
		Role: agentgo.RoleAssistant,
		Content: []agentgo.ContentBlock{agentgo.ToolCallBlock(agentgo.ToolCall{
			ID: toolCallID, Name: "write",
			Args: json.RawMessage(`{"path":"release.txt","content":"released"}`),
		})},
		StopReason: agentgo.StopReasonToolUse,
		Timestamp:  time.Now().UTC(),
	}
	checkpoint := agentledger.NewCheckpoint(
		"agentgo/"+session.ID,
		actor.ID,
		"application/vnd.compforge.agentgo.messages+json;version=1",
		map[string]any{"messages": []agentgo.Message{userMessage, assistantMessage}},
	)
	stored, err := backend.checkpoints.SaveCheckpoint(ctx, 0, checkpoint)
	if err != nil {
		t.Fatalf("seed AgentGo checkpoint: %v", err)
	}
	session.Control.ResumeRef = stored.ID
	session.Control.ResumeRevision = stored.Revision
	if err := backend.resources.PutSession(ctx, *session); err != nil {
		t.Fatalf("seed Session resume point: %v", err)
	}

	recorder, err := agentledger.OpenRecorder(ctx, agentledger.RecorderOptions{
		Store: backend.ledger, SessionID: session.ID, RunID: "input/" + inputID, Actor: actor,
	})
	if err != nil {
		t.Fatalf("open seeded AgentGo recorder: %v", err)
	}
	if _, err := recorder.StartRun(ctx, map[string]any{"agent_version": agent.Version}); err != nil {
		t.Fatalf("seed AgentGo run: %v", err)
	}
	turn, err := recorder.StartTurn(ctx, nil)
	if err != nil {
		t.Fatalf("seed AgentGo turn: %v", err)
	}
	attempt, err := recorder.BeforeToolCallWithEffect(ctx, turn.ID, map[string]any{
		"tool_call_id": toolCallID,
		"tool_name":    "write",
		"arguments":    `{"path":"release.txt","content":"released"}`,
	}, agentledger.Effect{Kind: agentledger.EffectKindWrite, Idempotency: agentledger.IdempotencyNone})
	if err != nil {
		t.Fatalf("seed unresolved write attempt: %v", err)
	}
	return "event_" + attempt.AttemptID
}

func waitForRequiredToolAction(
	t *testing.T,
	ctx context.Context,
	executionService *service.Service,
	events *service.EventLog,
	sessionID string,
) string {
	t.Helper()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		blocking, err := events.UnresolvedToolUses(ctx, sessionID)
		if err != nil {
			t.Fatal(err)
		}
		current, err := executionService.GetSession(ctx, sessionID)
		if err != nil {
			t.Fatal(err)
		}
		if current.Control.Status == "idle" && len(blocking) == 1 {
			toolUseID, _ := blocking[0]["id"].(string)
			return toolUseID
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for required tool action: %v", ctx.Err())
		case <-ticker.C:
		}
	}
}

func waitForResolvedAgentMessage(
	t *testing.T,
	ctx context.Context,
	executionService *service.Service,
	events *service.EventLog,
	sessionID string,
	marker string,
) {
	t.Helper()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		current, err := executionService.GetSession(ctx, sessionID)
		if err != nil {
			t.Fatal(err)
		}
		stored, err := events.List(ctx, sessionID)
		if err != nil {
			t.Fatal(err)
		}
		markerSeen := false
		for _, event := range stored {
			if event["type"] == "session.error" {
				encoded, _ := json.Marshal(event)
				t.Fatalf("Session failed while resolving tool use: %s", encoded)
			}
			markerSeen = markerSeen || event["type"] == "agent.message" &&
				strings.Contains(strings.Join(managedEventText(event), "\n"), marker)
		}
		if markerSeen && current.Control.Status == "idle" {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for resolved agent message: %v", ctx.Err())
		case <-ticker.C:
		}
	}
}

func assertDeniedAttemptWasNotReplayed(
	t *testing.T,
	ctx context.Context,
	store agentledger.EventStore,
	sessionID string,
) {
	t.Helper()
	view, err := store.LoadSession(ctx, sessionID)
	if err != nil {
		t.Fatal(err)
	}
	actionTypes := make(map[string]string, len(view.Actions))
	for _, action := range view.Actions {
		actionTypes[action.ID] = action.Type
	}
	toolAttempts := make(map[string]bool)
	for _, attempt := range view.Attempts {
		if actionTypes[attempt.ActionID] == agentledger.ActionTypeToolCall {
			toolAttempts[attempt.ID] = true
		}
	}
	counts := make(map[string]int)
	denied := false
	for _, event := range view.Events {
		if !toolAttempts[event.SubjectID] {
			continue
		}
		counts[event.EventType]++
		encoded, _ := json.Marshal(event.Payload)
		denied = denied || strings.Contains(string(encoded), "user_denied")
	}
	if len(toolAttempts) != 1 || counts[agentledger.EventTypeAttemptRequested] != 1 ||
		counts[agentledger.EventTypeAttemptFailed] != 1 ||
		counts[agentledger.EventTypeAttemptCompleted] != 0 || !denied {
		t.Fatalf("tool recovery ledger = attempts:%d events:%#v denied:%t", len(toolAttempts), counts, denied)
	}
}

type countingWriteSandbox struct {
	noopSandbox

	mu     sync.Mutex
	writes int
}

func (s *countingWriteSandbox) WriteFile(
	context.Context,
	engine.SandboxKey,
	string,
	[]byte,
	fs.FileMode,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writes++
	return nil
}

func (s *countingWriteSandbox) writeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writes
}
