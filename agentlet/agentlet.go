package agentlet

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	hertzserver "github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/network/standard"
	"github.com/compforge/agentd/agentlet/internal/api"
	"github.com/compforge/agentd/agentlet/internal/harness"
	"github.com/compforge/agentd/agentlet/internal/persistence"
	"github.com/compforge/agentd/agentlet/internal/sandbox"
	"github.com/compforge/agentd/agentlet/internal/service"
)

// Run starts the Agentlet execution service and blocks until it stops.
func Run(logger *slog.Logger) error {
	config, err := loadConfig()
	if err != nil {
		return err
	}
	storage, err := persistence.Open(context.Background(), persistence.Config{
		MySQLDSN:         config.mysqlDSN,
		SQLitePath:       config.sqlitePath,
		OperationTimeout: config.storageTimeout, MaxOpenConns: config.mysqlMaxOpenConns,
		MaxIdleConns: config.mysqlMaxIdleConns, ConnMaxLifetime: config.mysqlConnMaxLifetime,
	})
	if err != nil {
		return err
	}
	defer storage.Close()
	if storage.Provider == "sqlite" {
		logger.Warn("agentlet uses local SQLite; replacing the Worker Pod loses Ledger and Checkpoints")
	}

	sandboxEngine, err := sandbox.NewEngine(sandbox.Config{
		Endpoint:       config.sandboxEndpoint,
		RequestTimeout: config.sandboxRequestTimeout, StartupTimeout: config.sandboxStartupTimeout,
	})
	if err != nil {
		return err
	}
	processCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := sandboxEngine.Start(processCtx); err != nil {
		return err
	}

	agentHarness, err := harness.NewAgentGoRunner(harness.AgentGoRunnerConfig{
		APIKey: os.Getenv("ANTHROPIC_API_KEY"), BaseURL: os.Getenv("ANTHROPIC_BASE_URL"),
		RequestTimeout: config.modelRequestTimeout, OperationTimeout: config.ledgerOperationTimeout,
		ToolTimeout: config.toolTimeout, Ledger: storage.Ledger, Checkpoints: storage.Checkpoints, Sandbox: sandboxEngine,
	})
	if err != nil {
		return err
	}
	executionService := service.New(
		service.NewMemoryRepository(),
		service.NewEventLog(storage.Ledger),
		agentHarness,
		service.WithLogger(logger),
		service.WithReconcileInterval(config.reconcileInterval),
	)
	if err := executionService.Start(processCtx); err != nil {
		return err
	}

	httpServer := hertzserver.Default(
		hertzserver.WithHostPorts(config.address),
		hertzserver.WithTransport(standard.NewTransporter),
		hertzserver.WithReadTimeout(config.readTimeout),
		// SSE responses are intentionally not bounded by a server-wide write timeout.
		hertzserver.WithWriteTimeout(0),
		hertzserver.WithIdleTimeout(config.idleTimeout),
		hertzserver.WithMaxRequestBodySize(2<<20),
		hertzserver.WithSenseClientDisconnection(true),
	)
	api.New(executionService, logger, api.WithWorkerID(config.workerID)).Register(httpServer.Engine)
	serveErr := make(chan error, 1)
	go func() {
		logger.Info("agentlet listening", "address", config.address, "storage_provider", storage.Provider,
			"sandbox_engine", sandboxEngine.Name(), "harness", agentHarness.Name())
		serveErr <- httpServer.Run()
	}()

	select {
	case err := <-serveErr:
		shutdownCtx, cancel := context.WithTimeout(context.Background(), config.shutdownTimeout)
		defer cancel()
		if shutdownErr := executionService.Shutdown(shutdownCtx); shutdownErr != nil {
			logger.Error("session workers did not stop cleanly", "error", shutdownErr)
		}
		if err == nil {
			return nil
		}
		return fmt.Errorf("serve HTTP: %w", err)
	case <-processCtx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), config.shutdownTimeout)
		defer cancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shut down HTTP server: %w", err)
		}
		if err := executionService.Shutdown(shutdownCtx); err != nil {
			return err
		}
		return nil
	}
}

type config struct {
	address                string
	workerID               string
	mysqlDSN               string
	sqlitePath             string
	mysqlMaxOpenConns      int
	mysqlMaxIdleConns      int
	mysqlConnMaxLifetime   time.Duration
	sandboxEndpoint        string
	storageTimeout         time.Duration
	sandboxRequestTimeout  time.Duration
	sandboxStartupTimeout  time.Duration
	modelRequestTimeout    time.Duration
	ledgerOperationTimeout time.Duration
	toolTimeout            time.Duration
	readTimeout            time.Duration
	idleTimeout            time.Duration
	shutdownTimeout        time.Duration
	reconcileInterval      time.Duration
}

func loadConfig() (config, error) {
	value := config{
		address:              envOr("AGENTD_ADDRESS", "127.0.0.1:8019"),
		workerID:             os.Getenv("AGENTD_WORKER_ID"),
		mysqlDSN:             os.Getenv("AGENTD_MYSQL_DSN"),
		sqlitePath:           envOr("AGENTD_SQLITE_PATH", "agentlet.db"),
		mysqlMaxOpenConns:    32,
		mysqlMaxIdleConns:    8,
		mysqlConnMaxLifetime: 30 * time.Minute,
		sandboxEndpoint:      envOr("AGENTD_SANDBOX_ENDPOINT", "http://127.0.0.1:8080"),
		storageTimeout:       5 * time.Second,
	}
	var err error
	if value.mysqlMaxOpenConns, err = positiveIntEnv("AGENTD_MYSQL_MAX_OPEN_CONNS", value.mysqlMaxOpenConns); err != nil {
		return config{}, err
	}
	if value.mysqlMaxIdleConns, err = positiveIntEnv("AGENTD_MYSQL_MAX_IDLE_CONNS", value.mysqlMaxIdleConns); err != nil {
		return config{}, err
	}
	if value.mysqlMaxIdleConns > value.mysqlMaxOpenConns {
		return config{}, errors.New("AGENTD_MYSQL_MAX_IDLE_CONNS must not exceed AGENTD_MYSQL_MAX_OPEN_CONNS")
	}
	durations := []struct {
		name        string
		fallback    time.Duration
		destination *time.Duration
	}{
		{"AGENTD_SANDBOX_REQUEST_TIMEOUT", 30 * time.Second, &value.sandboxRequestTimeout},
		{"AGENTD_SANDBOX_STARTUP_TIMEOUT", 30 * time.Second, &value.sandboxStartupTimeout},
		{"AGENTD_MODEL_REQUEST_TIMEOUT", 2 * time.Minute, &value.modelRequestTimeout},
		{"AGENTD_LEDGER_OPERATION_TIMEOUT", 30 * time.Second, &value.ledgerOperationTimeout},
		{"AGENTD_TOOL_TIMEOUT", 2 * time.Minute, &value.toolTimeout},
		{"AGENTD_HTTP_READ_TIMEOUT", 30 * time.Second, &value.readTimeout},
		{"AGENTD_HTTP_IDLE_TIMEOUT", 2 * time.Minute, &value.idleTimeout},
		{"AGENTD_SHUTDOWN_TIMEOUT", 15 * time.Second, &value.shutdownTimeout},
		{"AGENTD_RECONCILE_INTERVAL", 5 * time.Second, &value.reconcileInterval},
		{"AGENTD_STORAGE_OPERATION_TIMEOUT", 5 * time.Second, &value.storageTimeout},
		{"AGENTD_MYSQL_CONN_MAX_LIFETIME", 30 * time.Minute, &value.mysqlConnMaxLifetime},
	}
	for _, item := range durations {
		parsed, err := durationEnv(item.name, item.fallback)
		if err != nil {
			return config{}, err
		}
		*item.destination = parsed
	}
	return value, nil
}

func positiveIntEnv(name string, fallback int) (int, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return value, nil
}

func durationEnv(name string, fallback time.Duration) (time.Duration, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback, nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration", name)
	}
	return value, nil
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
