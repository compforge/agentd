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
	"github.com/compforge/agentd/agentlet/internal/api"
	"github.com/compforge/agentd/agentlet/internal/app"
	"github.com/compforge/agentd/agentlet/internal/execution"
	harnessstate "github.com/compforge/agentd/agentlet/internal/harness/state"
	ledgerstore "github.com/compforge/agentd/agentlet/internal/ledger/store"
	"github.com/compforge/agentd/agentlet/internal/store"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

func TestRecoverCommittedInputAfterRestart(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "agentd-e2e.db")
	firstBackend := openSQLiteE2EBackend(t, databasePath)
	blockingHarness := newSQLiteRecoveryHarness(firstBackend.harnessStates, true)
	firstApp := app.New(firstBackend.resources, app.NewEventLog(firstBackend.ledger), blockingHarness)
	firstServer, firstClient := startSQLiteE2EServer(t, firstApp)

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

	restartedBackend := openSQLiteE2EBackend(t, databasePath)
	t.Cleanup(func() { restartedBackend.close(t) })
	restartedHarness := newSQLiteRecoveryHarness(restartedBackend.harnessStates, false)
	restartedApp := app.New(restartedBackend.resources, app.NewEventLog(restartedBackend.ledger), restartedHarness)
	if err := restartedApp.Recover(ctx); err != nil {
		t.Fatalf("recover application: %v", err)
	}
	_, restartedClient := startSQLiteE2EServer(t, restartedApp)

	waitForSQLiteE2EIdle(t, ctx, restartedClient, session.ID)
	assertSQLiteE2EEvents(t, ctx, restartedClient, session.ID, 1, 1)
	assertSQLiteE2EState(t, ctx, restartedBackend, session.ID, 1, 0)

	if _, err := restartedClient.Beta.Agents.Get(ctx, agent.ID, anthropic.BetaAgentGetParams{}); err != nil {
		t.Fatalf("get persisted agent after restart: %v", err)
	}
	if _, err := restartedClient.Beta.Environments.Get(ctx, environment.ID, anthropic.BetaEnvironmentGetParams{}); err != nil {
		t.Fatalf("get persisted environment after restart: %v", err)
	}
	sendSQLiteE2EMessage(t, ctx, restartedClient, session.ID, "second")
	waitForSQLiteE2EIdle(t, ctx, restartedClient, session.ID)
	assertSQLiteE2EEvents(t, ctx, restartedClient, session.ID, 2, 2)
	assertSQLiteE2EState(t, ctx, restartedBackend, session.ID, 2, 1)
}

type sqliteE2EBackend struct {
	resources     app.Repository
	harnessStates harnessstate.Store
	ledger        agentledger.EventStore
	closeOnce     sync.Once
	closeErr      error
	closeDatabase func() error
}

func openSQLiteE2EBackend(t *testing.T, path string) *sqliteE2EBackend {
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
	resources, err := store.NewGORM(database)
	if err != nil {
		_ = sqlDatabase.Close()
		t.Fatal(err)
	}
	harnessStates, err := harnessstate.NewGORM(database)
	if err != nil {
		_ = sqlDatabase.Close()
		t.Fatal(err)
	}
	ledger, err := ledgerstore.NewGORM(database)
	if err != nil {
		_ = sqlDatabase.Close()
		t.Fatal(err)
	}
	return &sqliteE2EBackend{
		resources: resources, harnessStates: harnessStates, ledger: ledger,
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
	application *app.App
	server      *hertzserver.Hertz
	serveErr    chan error
	stopOnce    sync.Once
	stopErr     error
}

func startSQLiteE2EServer(t *testing.T, application *app.App) (*sqliteE2EServer, anthropic.Client) {
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
	api.New(application, slog.New(slog.NewTextHandler(io.Discard, nil))).Register(server.Engine)
	running := &sqliteE2EServer{application: application, server: server, serveErr: make(chan error, 1)}
	go func() { running.serveErr <- server.Run() }()
	t.Cleanup(func() { running.stop(t) })
	client := anthropic.NewClient(
		option.WithAPIKey("test"),
		option.WithBaseURL("http://"+listener.Addr().String()),
	)
	return running, client
}

func (s *sqliteE2EServer) stop(t *testing.T) {
	t.Helper()
	s.stopOnce.Do(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		appErr := s.application.Shutdown(shutdownCtx)
		serverErr := s.server.Shutdown(shutdownCtx)
		<-s.serveErr
		s.stopErr = errors.Join(appErr, serverErr)
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
	state       harnessstate.Store
	block       bool
	started     chan struct{}
	startedOnce sync.Once
}

func newSQLiteRecoveryHarness(state harnessstate.Store, block bool) *sqliteRecoveryHarness {
	return &sqliteRecoveryHarness{state: state, block: block, started: make(chan struct{})}
}

func (*sqliteRecoveryHarness) Name() string { return "sqlite-recovery" }

func (*sqliteRecoveryHarness) Version() string { return "test" }

func (*sqliteRecoveryHarness) PrepareSession(_ context.Context, session execution.Session) (string, error) {
	return "sqlite-recovery/" + session.ID, nil
}

func (h *sqliteRecoveryHarness) Run(
	ctx context.Context,
	session execution.Session,
	input execution.TurnInput,
	emit func(execution.ManagedEvent) error,
) (execution.TurnResult, error) {
	records, err := h.state.Load(ctx, session.ResumeRef)
	if err != nil {
		return execution.TurnResult{}, fmt.Errorf("load SQLite E2E harness state: %w", err)
	}
	revision := int64(-1)
	committed := false
	for _, record := range records {
		var stored sqliteRecoveryInput
		if err := json.Unmarshal(record.Data, &stored); err != nil {
			return execution.TurnResult{ResumeRevision: revision}, fmt.Errorf("decode SQLite E2E harness state: %w", err)
		}
		revision = record.Revision
		committed = committed || stored.ID == input.ID
	}
	if !committed {
		data, err := json.Marshal(sqliteRecoveryInput{ID: input.ID, Text: input.Text})
		if err != nil {
			return execution.TurnResult{ResumeRevision: revision}, err
		}
		record, err := h.state.Append(ctx, session.ResumeRef, revision, "application/vnd.compforge.e2e.input+json", data)
		if err != nil {
			return execution.TurnResult{ResumeRevision: revision}, fmt.Errorf("append SQLite E2E harness state: %w", err)
		}
		revision = record.Revision
	}
	if h.block {
		h.startedOnce.Do(func() { close(h.started) })
		<-ctx.Done()
		return execution.TurnResult{ResumeRevision: revision}, ctx.Err()
	}
	err = emit(app.NewTurnEvent(input.ID, "agent.message", map[string]any{
		"content": []map[string]any{{"type": "text", "text": "echo: " + input.Text}},
	}))
	return execution.TurnResult{ResumeRevision: revision}, err
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
	records, err := backend.harnessStates.Load(ctx, session.Control.ResumeRef)
	if err != nil {
		t.Fatalf("load persisted harness state: %v", err)
	}
	if len(records) != wantRecords || session.Control.ResumeRevision != wantRevision {
		t.Fatalf(
			"harness state = records:%d revision:%d, want records:%d revision:%d",
			len(records), session.Control.ResumeRevision, wantRecords, wantRevision,
		)
	}
}
