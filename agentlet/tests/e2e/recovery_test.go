//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"
	"github.com/anthropics/anthropic-sdk-go/packages/param"
	hertzserver "github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/network/standard"
	agentledger "github.com/compforge/agent-ledger/go"
	ledgergorm "github.com/compforge/agent-ledger/go/stores/gorm"
	"github.com/compforge/agentd/agentlet/internal/api"
	"github.com/compforge/agentd/agentlet/internal/harness"
	"github.com/compforge/agentd/agentlet/internal/service"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

func TestRecoverCommittedInputAfterRestart(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "agentd-e2e.db")
	resources := service.NewMemoryRepository()
	firstBackend := openSQLiteE2EBackend(t, databasePath, resources)
	blockingHarness := newSQLiteRecoveryHarness(firstBackend.checkpoints, true)
	firstService := service.New(firstBackend.resources, service.NewEventLog(firstBackend.ledger), blockingHarness)
	firstServer, firstClient := startSQLiteE2EServer(t, firstService)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	agent, environment, session := createSQLiteE2ESession(t, ctx, firstClient)
	sendSQLiteE2EMessage(t, ctx, firstClient, session.ID, "first")
	select {
	case <-blockingHarness.started:
	case <-ctx.Done():
		t.Fatal("first harness run did not reach the durable boundary")
	}
	firstServer.stop(t)
	firstBackend.close(t)

	// Agentd owns these resource snapshots and supplies them again when assigning
	// a Session. This test keeps that upstream state while replacing the Agentlet.
	restartedBackend := openSQLiteE2EBackend(t, databasePath, resources)
	t.Cleanup(func() { restartedBackend.close(t) })
	restartedHarness := newSQLiteRecoveryHarness(restartedBackend.checkpoints, false)
	restartedService := service.New(restartedBackend.resources, service.NewEventLog(restartedBackend.ledger), restartedHarness)
	if err := restartedService.Recover(ctx); err != nil {
		t.Fatalf("recover service: %v", err)
	}
	_, restartedClient := startSQLiteE2EServer(t, restartedService)

	waitForSQLiteE2EIdle(t, ctx, restartedClient, session.ID)
	assertSQLiteE2EEvents(t, ctx, restartedClient, session.ID, 1, 1)
	assertSQLiteE2EState(t, ctx, restartedBackend, session.ID, 1, 1)

	if _, err := restartedClient.Beta.Agents.Get(ctx, agent.ID, anthropic.BetaAgentGetParams{}); err != nil {
		t.Fatalf("get persisted agent after restart: %v", err)
	}
	if _, err := restartedClient.Beta.Environments.Get(ctx, environment.ID, anthropic.BetaEnvironmentGetParams{}); err != nil {
		t.Fatalf("get persisted environment after restart: %v", err)
	}
	sendSQLiteE2EMessage(t, ctx, restartedClient, session.ID, "second")
	waitForSQLiteE2EIdle(t, ctx, restartedClient, session.ID)
	assertSQLiteE2EEvents(t, ctx, restartedClient, session.ID, 2, 2)
	assertSQLiteE2EState(t, ctx, restartedBackend, session.ID, 2, 2)
}

type sqliteE2EBackend struct {
	resources     service.Repository
	ledger        agentledger.EventStore
	checkpoints   agentledger.CheckpointStore
	closeOnce     sync.Once
	closeErr      error
	closeDatabase func() error
}

func openSQLiteE2EBackend(t *testing.T, path string, resources service.Repository) *sqliteE2EBackend {
	t.Helper()
	database, err := gorm.Open(sqlite.Open("file:"+path+"?_busy_timeout=5000"), &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: true,
		Logger:                                   gormlogger.Default.LogMode(gormlogger.Silent),
	})
	if err != nil {
		t.Fatalf("open SQLite E2E database: %v", err)
	}
	sqlDatabase, err := database.DB()
	if err != nil {
		t.Fatalf("resolve SQLite E2E connection: %v", err)
	}
	sqlDatabase.SetMaxOpenConns(1)
	sqlDatabase.SetMaxIdleConns(1)
	sqlDatabase.SetConnMaxLifetime(time.Minute)
	pingCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := sqlDatabase.PingContext(pingCtx); err != nil {
		_ = sqlDatabase.Close()
		t.Fatalf("ping SQLite E2E database: %v", err)
	}
	ledger, err := ledgergorm.New(database, 2*time.Second)
	if err != nil {
		_ = sqlDatabase.Close()
		t.Fatal(err)
	}
	if err := ledger.Initialize(context.Background()); err != nil {
		_ = sqlDatabase.Close()
		t.Fatal(err)
	}
	return &sqliteE2EBackend{
		resources: resources, ledger: ledger, checkpoints: ledger,
		closeDatabase: sqlDatabase.Close,
	}
}

func (b *sqliteE2EBackend) close(t *testing.T) {
	t.Helper()
	b.closeOnce.Do(func() { b.closeErr = b.closeDatabase() })
	if b.closeErr != nil {
		t.Fatalf("close SQLite E2E database: %v", b.closeErr)
	}
}

type sqliteE2EServer struct {
	service  *service.Service
	server   *hertzserver.Hertz
	serveErr chan error
	stopOnce sync.Once
	stopErr  error
}

func startSQLiteE2EServer(t *testing.T, executionService *service.Service) (*sqliteE2EServer, anthropic.Client) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for SQLite E2E server: %v", err)
	}
	server := hertzserver.Default(
		hertzserver.WithListener(listener),
		hertzserver.WithTransport(standard.NewTransporter),
		hertzserver.WithDisablePrintRoute(true),
		hertzserver.WithReadTimeout(2*time.Second),
		hertzserver.WithWriteTimeout(0),
		hertzserver.WithIdleTimeout(100*time.Millisecond),
		hertzserver.WithMaxRequestBodySize(2<<20),
		hertzserver.WithSenseClientDisconnection(true),
	)
	api.New(executionService, slog.New(slog.NewTextHandler(io.Discard, nil))).Register(server.Engine)
	running := &sqliteE2EServer{service: executionService, server: server, serveErr: make(chan error, 1)}
	go func() { running.serveErr <- server.Run() }()
	t.Cleanup(func() { running.stop(t) })
	client := anthropic.NewClient(
		option.WithAPIKey("test"),
		option.WithBaseURL("http://"+listener.Addr().String()+"/internal"),
	)
	return running, client
}

func (s *sqliteE2EServer) stop(t *testing.T) {
	t.Helper()
	s.stopOnce.Do(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		serviceErr := s.service.Shutdown(shutdownCtx)
		serverErr := s.server.Shutdown(shutdownCtx)
		<-s.serveErr
		s.stopErr = errors.Join(serviceErr, serverErr)
	})
	if s.stopErr != nil {
		t.Fatalf("stop SQLite E2E server: %v", s.stopErr)
	}
}

type sqliteRecoveryInput struct {
	ID   string `json:"id"`
	Text string `json:"text"`
}

type sqliteRecoveryHarness struct {
	state       agentledger.CheckpointStore
	actor       agentledger.Actor
	block       bool
	started     chan struct{}
	startedOnce sync.Once
}

func newSQLiteRecoveryHarness(state agentledger.CheckpointStore, block bool) *sqliteRecoveryHarness {
	return &sqliteRecoveryHarness{
		state: state, actor: agentledger.NewActor("harness", "sqlite-recovery"),
		block: block, started: make(chan struct{}),
	}
}

func (*sqliteRecoveryHarness) Name() string { return "sqlite-recovery" }

func (*sqliteRecoveryHarness) Version() string { return "test" }

func (*sqliteRecoveryHarness) PrepareSession(_ context.Context, session harness.Session) (string, error) {
	return "sqlite-recovery/" + session.ID, nil
}

func (h *sqliteRecoveryHarness) Run(
	ctx context.Context,
	session harness.Session,
	input harness.TurnInput,
	emit func(harness.ManagedEvent) error,
) (harness.TurnResult, error) {
	key := "sqlite-recovery/" + session.ID
	checkpoint, exists, err := h.loadCheckpoint(ctx, session.ResumeRef, key, session.ResumeRevision)
	if err != nil {
		return harness.TurnResult{}, fmt.Errorf("load SQLite E2E harness state: %w", err)
	}
	revision := int64(0)
	checkpointID := session.ResumeRef
	var inputs []sqliteRecoveryInput
	if exists {
		revision = checkpoint.Revision
		checkpointID = checkpoint.ID
		data, err := json.Marshal(checkpoint.State["inputs"])
		if err != nil {
			return harness.TurnResult{ResumeRef: checkpointID, ResumeRevision: revision}, err
		}
		if err := json.Unmarshal(data, &inputs); err != nil {
			return harness.TurnResult{ResumeRef: checkpointID, ResumeRevision: revision}, fmt.Errorf("decode SQLite E2E harness state: %w", err)
		}
	}
	committed := false
	for _, stored := range inputs {
		committed = committed || stored.ID == input.ID
	}
	if !committed {
		if err := h.ensureActor(ctx); err != nil {
			return harness.TurnResult{ResumeRef: checkpointID, ResumeRevision: revision}, err
		}
		inputs = append(inputs, sqliteRecoveryInput{ID: input.ID, Text: input.Text})
		proposed := agentledger.NewCheckpoint(
			key, h.actor.ID, "application/vnd.compforge.e2e.inputs+json", map[string]any{"inputs": inputs},
		)
		record, err := h.state.SaveCheckpoint(ctx, revision, proposed)
		if err != nil {
			return harness.TurnResult{ResumeRef: checkpointID, ResumeRevision: revision}, fmt.Errorf("save SQLite E2E harness checkpoint: %w", err)
		}
		revision = record.Revision
		checkpointID = record.ID
	}
	if h.block {
		h.startedOnce.Do(func() { close(h.started) })
		<-ctx.Done()
		return harness.TurnResult{ResumeRef: checkpointID, ResumeRevision: revision}, ctx.Err()
	}
	err = emit(service.NewTurnEvent(input.ID, "agent.message", map[string]any{
		"content": []map[string]any{{"type": "text", "text": "echo: " + input.Text}},
	}))
	return harness.TurnResult{ResumeRef: checkpointID, ResumeRevision: revision}, err
}

func (h *sqliteRecoveryHarness) loadCheckpoint(
	ctx context.Context,
	resumeRef string,
	key string,
	resumeRevision int64,
) (agentledger.Checkpoint, bool, error) {
	if resumeRef != "" && resumeRef != key {
		checkpoint, exists, err := h.state.GetCheckpoint(ctx, resumeRef)
		if err != nil || !exists {
			return checkpoint, exists, err
		}
		if checkpoint.Revision != resumeRevision {
			return agentledger.Checkpoint{}, false, fmt.Errorf(
				"checkpoint revision %d does not match resume revision %d", checkpoint.Revision, resumeRevision,
			)
		}
		return checkpoint, true, nil
	}
	// A process may die after persisting its deterministic input boundary but
	// before Control State receives the exact checkpoint ID. The recovery test
	// harness can safely adopt that latest checkpoint because inputs are idempotent.
	return h.state.LoadLatestCheckpoint(ctx, key)
}

func (h *sqliteRecoveryHarness) ensureActor(ctx context.Context) error {
	_, exists, err := h.state.GetActor(ctx, h.actor.ID)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	return h.state.CreateActor(ctx, h.actor)
}

func (*sqliteRecoveryHarness) Interrupt(string) {}

func createSQLiteE2ESession(
	t *testing.T,
	ctx context.Context,
	client anthropic.Client,
) (*anthropic.BetaManagedAgentsAgent, *anthropic.BetaEnvironment, *anthropic.BetaManagedAgentsSession) {
	return createSQLiteE2EConfiguredSession(
		t, ctx, client, "sqlite-e2e", anthropic.BetaManagedAgentsModelClaudeSonnet4_6, "",
	)
}

func createSQLiteE2EConfiguredSession(
	t *testing.T,
	ctx context.Context,
	client anthropic.Client,
	name string,
	modelID anthropic.BetaManagedAgentsModel,
	system string,
) (*anthropic.BetaManagedAgentsAgent, *anthropic.BetaEnvironment, *anthropic.BetaManagedAgentsSession) {
	t.Helper()
	agent, err := client.Beta.Agents.New(ctx, anthropic.BetaAgentNewParams{
		Name: name, Model: anthropic.BetaManagedAgentsModelConfigParams{
			ID: modelID,
		}, System: param.NewOpt(system),
	})
	if err != nil {
		t.Fatalf("create agent through official SDK: %v", err)
	}
	unrestricted := anthropic.NewBetaUnrestrictedNetworkParam()
	environment, err := client.Beta.Environments.New(ctx, anthropic.BetaEnvironmentNewParams{
		Name: name, Config: anthropic.BetaEnvironmentNewParamsConfigUnion{
			OfCloud: &anthropic.BetaCloudConfigParams{
				Networking: anthropic.BetaCloudConfigParamsNetworkingUnion{OfUnrestricted: &unrestricted},
			},
		},
	})
	if err != nil {
		t.Fatalf("create environment through official SDK: %v", err)
	}
	session, err := client.Beta.Sessions.New(ctx, anthropic.BetaSessionNewParams{
		Agent:         anthropic.BetaSessionNewParamsAgentUnion{OfString: param.NewOpt(agent.ID)},
		EnvironmentID: environment.ID,
	})
	if err != nil {
		t.Fatalf("create session through official SDK: %v", err)
	}
	return agent, environment, session
}

func sendSQLiteE2EMessage(t *testing.T, ctx context.Context, client anthropic.Client, sessionID, text string) {
	t.Helper()
	_, err := client.Beta.Sessions.Events.Send(ctx, sessionID, anthropic.BetaSessionEventSendParams{
		Events: []anthropic.BetaManagedAgentsEventParamsUnion{{
			OfUserMessage: &anthropic.BetaManagedAgentsUserMessageEventParams{
				Type: anthropic.BetaManagedAgentsUserMessageEventParamsTypeUserMessage,
				Content: []anthropic.BetaManagedAgentsUserMessageEventParamsContentUnion{{
					OfText: &anthropic.BetaManagedAgentsTextBlockParam{
						Type: anthropic.BetaManagedAgentsTextBlockTypeText, Text: text,
					},
				}},
			},
		}},
	})
	if err != nil {
		t.Fatalf("send event through official SDK: %v", err)
	}
}

func waitForSQLiteE2EIdle(t *testing.T, ctx context.Context, client anthropic.Client, sessionID string) {
	t.Helper()
	for {
		session, err := client.Beta.Sessions.Get(ctx, sessionID, anthropic.BetaSessionGetParams{})
		if err != nil {
			t.Fatalf("get session through official SDK: %v", err)
		}
		if session.Status == anthropic.BetaManagedAgentsSessionStatusIdle {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for idle session: %v", ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func assertSQLiteE2EEvents(
	t *testing.T,
	ctx context.Context,
	client anthropic.Client,
	sessionID string,
	wantUserMessages, wantAgentMessages int,
) {
	t.Helper()
	page, err := client.Beta.Sessions.Events.List(ctx, sessionID, anthropic.BetaSessionEventListParams{})
	if err != nil {
		t.Fatalf("list events through official SDK: %v", err)
	}
	userMessages := 0
	agentMessages := 0
	for _, event := range page.Data {
		switch event.Type {
		case "user.message":
			userMessages++
		case "agent.message":
			agentMessages++
		}
	}
	if userMessages != wantUserMessages || agentMessages != wantAgentMessages {
		t.Fatalf(
			"event counts = user:%d agent:%d, want user:%d agent:%d",
			userMessages, agentMessages, wantUserMessages, wantAgentMessages,
		)
	}
}

func assertSQLiteE2EState(
	t *testing.T,
	ctx context.Context,
	backend *sqliteE2EBackend,
	sessionID string,
	wantRecords int,
	wantRevision int64,
) {
	t.Helper()
	session, err := backend.resources.GetSession(ctx, sessionID)
	if err != nil {
		t.Fatalf("get persisted session: %v", err)
	}
	checkpoint, exists, err := backend.checkpoints.GetCheckpoint(ctx, session.Control.ResumeRef)
	if err != nil || !exists {
		t.Fatalf("load persisted harness checkpoint: exists=%t err=%v", exists, err)
	}
	data, err := json.Marshal(checkpoint.State["inputs"])
	if err != nil {
		t.Fatalf("encode persisted harness checkpoint: %v", err)
	}
	var inputs []sqliteRecoveryInput
	if err := json.Unmarshal(data, &inputs); err != nil {
		t.Fatalf("decode persisted harness checkpoint: %v", err)
	}
	if len(inputs) != wantRecords || checkpoint.Revision != wantRevision || session.Control.ResumeRevision != wantRevision {
		t.Fatalf(
			"harness state = records:%d checkpoint revision:%d control revision:%d, want records:%d revision:%d",
			len(inputs), checkpoint.Revision, session.Control.ResumeRevision, wantRecords, wantRevision,
		)
	}
}
