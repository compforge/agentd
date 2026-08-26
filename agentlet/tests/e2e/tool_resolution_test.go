//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"errors"
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
)

func TestUnsafeToolResolutionResumesWithoutReplay(t *testing.T) {
	model := newAnthropicModelStub(t, func(attempt int, writer http.ResponseWriter, _ *http.Request) {
		if attempt == 1 {
			writeAnthropicWriteCall(writer)
			return
		}
		writeAnthropicAnswer(writer, "AGENTD_TOOL_RESOLUTION_OK")
	})
	backend := openSQLiteE2EBackend(
		t, filepath.Join(t.TempDir(), "agentd-tool-resolution-e2e.db"), service.NewMemoryRepository(),
	)
	t.Cleanup(func() { backend.close(t) })
	events := service.NewEventLog(backend.ledger)
	sandbox := &countingWriteSandbox{}
	seedLedger := &failToolOutcomeStore{EventStore: backend.ledger}
	seedRunner := newToolResolutionRunner(t, backend, seedLedger, model.URL(), sandbox)
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
	inputID, _ := input["id"].(string)
	resume, seedErr := seedRunner.Run(ctx, harness.Session{
		ID: session.ID,
		Agent: harness.Agent{
			ID: agent.ID, Model: agent.Model, System: agent.System, Tools: agent.Tools, Version: agent.Version,
		},
		EnvironmentID:  session.EnvironmentID,
		ResumeRef:      session.Control.ResumeRef,
		ResumeRevision: session.Control.ResumeRevision,
	}, harness.TurnInput{ID: inputID, Text: "Write the release marker."}, func(harness.ManagedEvent) error {
		return nil
	})
	if seedErr == nil {
		t.Fatal("seed execution unexpectedly persisted the uncertain tool outcome")
	}
	t.Logf("seed execution stopped at injected boundary: %v", seedErr)
	session.Control.ResumeRef = resume.ResumeRef
	session.Control.ResumeRevision = resume.ResumeRevision
	if err := backend.resources.PutSession(ctx, session); err != nil {
		t.Fatal(err)
	}
	toolUseID := unresolvedToolUseID(t, ctx, backend.ledger, session.ID)

	recoveredRunner := newToolResolutionRunner(t, backend, backend.ledger, model.URL(), sandbox)
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
	model.assertRequests(t, 1)
	if writes := sandbox.writeCount(); writes != 1 {
		t.Fatalf("Sandbox writes before resolution = %d, want one uncertain execution", writes)
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

	if writes := sandbox.writeCount(); writes != 1 {
		t.Fatalf("uncertain write execution count = %d, want no replay", writes)
	}
	blocking, err := events.UnresolvedToolUses(ctx, session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(blocking) != 0 {
		t.Fatalf("resolved tool uses = %#v, want none", blocking)
	}
	model.assertRequests(t, 2)
	model.assertLastRequestContains(t, denyMessage)
	assertDeniedAttemptWasNotReplayed(t, ctx, backend.ledger, session.ID)
}

func newToolResolutionRunner(
	t *testing.T,
	backend *sqliteE2EBackend,
	ledger agentledger.EventStore,
	modelURL string,
	sandbox engine.Engine,
) *harness.AgentGoRunner {
	t.Helper()
	runner, err := harness.NewAgentGoRunner(harness.AgentGoRunnerConfig{
		RequestTimeout: time.Second, OperationTimeout: 2 * time.Second, ToolTimeout: 2 * time.Second,
		Ledger: ledger, Checkpoints: backend.checkpoints, Sandbox: sandbox,
	})
	if err != nil {
		t.Fatalf("create AgentGo tool-resolution runner: %v", err)
	}
	return runner
}

func writeAnthropicWriteCall(writer http.ResponseWriter) {
	writer.Header().Set("Content-Type", "text/event-stream")
	_, _ = writer.Write([]byte("event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_e2e\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-sonnet-4-6\",\"content\":[],\"stop_reason\":null,\"usage\":{\"input_tokens\":1,\"output_tokens\":0}}}\n\n"))
	_, _ = writer.Write([]byte("event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_uncertain_write\",\"name\":\"write\",\"input\":{}}}\n\n"))
	_, _ = writer.Write([]byte("event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"file_path\\\":\\\"release.txt\\\",\\\"content\\\":\\\"released\\\"}\"}}\n\n"))
	_, _ = writer.Write([]byte("event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n"))
	_, _ = writer.Write([]byte("event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\",\"stop_sequence\":null},\"usage\":{\"output_tokens\":1}}\n\n"))
	_, _ = writer.Write([]byte("event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"))
}

func unresolvedToolUseID(
	t *testing.T,
	ctx context.Context,
	store agentledger.EventStore,
	sessionID string,
) string {
	t.Helper()
	view, err := store.LoadSession(ctx, sessionID)
	if err != nil {
		t.Fatal(err)
	}
	actions := make(map[string]string, len(view.Actions))
	for _, action := range view.Actions {
		actions[action.ID] = action.Type
	}
	toolAttempts := make(map[string]bool)
	for _, attempt := range view.Attempts {
		if actions[attempt.ActionID] == agentledger.ActionTypeToolCall {
			toolAttempts[attempt.ID] = true
		}
	}
	requested := make(map[string]bool)
	terminal := make(map[string]bool)
	for _, event := range view.Events {
		if !toolAttempts[event.SubjectID] {
			continue
		}
		switch event.EventType {
		case agentledger.EventTypeAttemptRequested:
			requested[event.SubjectID] = true
		case agentledger.EventTypeAttemptCompleted, agentledger.EventTypeAttemptFailed,
			agentledger.EventTypeAttemptCancelled, agentledger.EventTypeAttemptOutcomeUnknown:
			terminal[event.SubjectID] = true
		}
	}
	for attemptID := range requested {
		if !terminal[attemptID] {
			return "event_" + attemptID
		}
	}
	t.Fatalf("no unresolved tool attempt was recorded: actions=%#v attempts=%#v events=%#v", view.Actions, view.Attempts, view.Events)
	return ""
}

// failToolOutcomeStore lets the external write finish, then drops its durable
// outcome once. That is the ambiguity recovery must never interpret as safe to
// replay automatically.
type failToolOutcomeStore struct {
	agentledger.EventStore

	mu     sync.Mutex
	failed bool
}

func (s *failToolOutcomeStore) Append(
	ctx context.Context,
	laneID string,
	expectedLastSeq int64,
	appendID string,
	events ...agentledger.ProposedEvent,
) (agentledger.AppendReceipt, error) {
	s.mu.Lock()
	shouldFail := false
	for _, event := range events {
		if event.EventType != agentledger.EventTypeAttemptCompleted || s.failed {
			continue
		}
		encoded, _ := json.Marshal(event.Payload["output"])
		if strings.Contains(string(encoded), `"tool_call_id"`) {
			s.failed = true
			shouldFail = true
		}
	}
	s.mu.Unlock()
	if shouldFail {
		return agentledger.AppendReceipt{}, errors.New("injected tool outcome persistence failure")
	}
	return s.EventStore.Append(ctx, laneID, expectedLastSeq, appendID, events...)
}

func (s *anthropicModelStub) assertLastRequestContains(t *testing.T, expected string) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.requests) == 0 {
		t.Fatal("model received no requests")
	}
	last := s.requests[len(s.requests)-1]
	if !strings.Contains(string(last), expected) {
		t.Fatalf("last model request does not contain %q: %s", expected, last)
	}
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
