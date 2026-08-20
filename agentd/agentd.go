package agentd

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
	"github.com/compforge/agentd/agentd/internal/api"
	control "github.com/compforge/agentd/agentd/internal/app"
	controlk8s "github.com/compforge/agentd/agentd/internal/k8s"
	"github.com/compforge/agentd/agentd/internal/observer"
	"github.com/compforge/agentd/agentd/internal/store"
	drivermysql "github.com/go-sql-driver/mysql"
	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func Run(logger *slog.Logger) error {
	config, err := loadConfig()
	if err != nil {
		return err
	}
	database, closeDatabase, err := openMySQL(config)
	if err != nil {
		return err
	}
	defer closeDatabase()
	repository, err := store.NewGORM(database)
	if err != nil {
		return err
	}
	application, err := control.New(repository, config.observationTimeout)
	if err != nil {
		return err
	}
	workerObserver, err := buildWorkerObserver(config, application, logger)
	if err != nil {
		return err
	}

	httpServer := hertzserver.Default(
		hertzserver.WithHostPorts(config.address),
		hertzserver.WithTransport(standard.NewTransporter),
		hertzserver.WithReadTimeout(config.readTimeout),
		hertzserver.WithWriteTimeout(config.writeTimeout),
		hertzserver.WithIdleTimeout(config.idleTimeout),
		hertzserver.WithMaxRequestBodySize(1<<20),
	)
	api.New().Register(httpServer.Engine)

	processCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if workerObserver != nil {
		go workerObserver.Run(processCtx)
	}
	serveErr := make(chan error, 1)
	go func() {
		logger.Info("agentd listening", "address", config.address)
		serveErr <- httpServer.Run()
	}()
	select {
	case err := <-serveErr:
		if err == nil {
			return nil
		}
		return fmt.Errorf("serve agentd HTTP: %w", err)
	case <-processCtx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), config.shutdownTimeout)
		defer cancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shut down agentd HTTP: %w", err)
		}
		return nil
	}
}

type config struct {
	address            string
	mysqlDSN           string
	maxOpenConns       int
	maxIdleConns       int
	connMaxLifetime    time.Duration
	storageTimeout     time.Duration
	observationTimeout time.Duration
	workerSource       string
	workerNamespace    string
	workerSelector     string
	workerPort         int
	workerMaxRuns      int
	observerInterval   time.Duration
	observerTimeout    time.Duration
	readTimeout        time.Duration
	writeTimeout       time.Duration
	idleTimeout        time.Duration
	shutdownTimeout    time.Duration
}

func loadConfig() (config, error) {
	value := config{
		address:      envOr("AGENTD_CONTROL_ADDRESS", "127.0.0.1:8082"),
		mysqlDSN:     os.Getenv("AGENTD_MYSQL_DSN"),
		maxOpenConns: 32, maxIdleConns: 8,
		connMaxLifetime: 30 * time.Minute, storageTimeout: 5 * time.Second,
		observationTimeout: 15 * time.Second,
		workerSource:       os.Getenv("AGENTD_WORKER_SOURCE"),
		workerNamespace:    envOr("AGENTD_WORKER_NAMESPACE", envOr("POD_NAMESPACE", "default")),
		workerSelector:     os.Getenv("AGENTD_WORKER_LABEL_SELECTOR"),
		workerPort:         8081,
		workerMaxRuns:      4,
		observerInterval:   5 * time.Second,
		observerTimeout:    5 * time.Second,
		readTimeout:        30 * time.Second, writeTimeout: 30 * time.Second,
		idleTimeout: 2 * time.Minute, shutdownTimeout: 15 * time.Second,
	}
	if value.mysqlDSN == "" {
		return config{}, errors.New("AGENTD_MYSQL_DSN is required")
	}
	var err error
	if value.maxOpenConns, err = positiveIntEnv("AGENTD_MYSQL_MAX_OPEN_CONNS", value.maxOpenConns); err != nil {
		return config{}, err
	}
	if value.maxIdleConns, err = positiveIntEnv("AGENTD_MYSQL_MAX_IDLE_CONNS", value.maxIdleConns); err != nil {
		return config{}, err
	}
	if value.maxIdleConns > value.maxOpenConns {
		return config{}, errors.New("AGENTD_MYSQL_MAX_IDLE_CONNS must not exceed AGENTD_MYSQL_MAX_OPEN_CONNS")
	}
	if value.workerPort, err = positiveIntEnv("AGENTD_WORKER_PORT", value.workerPort); err != nil {
		return config{}, err
	}
	if value.workerPort > 65535 {
		return config{}, errors.New("AGENTD_WORKER_PORT must not exceed 65535")
	}
	if value.workerMaxRuns, err = positiveIntEnv("AGENTD_WORKER_MAX_RUNS", value.workerMaxRuns); err != nil {
		return config{}, err
	}
	durations := []struct {
		name  string
		value *time.Duration
	}{
		{"AGENTD_MYSQL_CONN_MAX_LIFETIME", &value.connMaxLifetime},
		{"AGENTD_STORAGE_OPERATION_TIMEOUT", &value.storageTimeout},
		{"AGENTD_WORKER_OBSERVATION_TIMEOUT", &value.observationTimeout},
		{"AGENTD_WORKER_OBSERVER_INTERVAL", &value.observerInterval},
		{"AGENTD_WORKER_OBSERVER_REQUEST_TIMEOUT", &value.observerTimeout},
		{"AGENTD_HTTP_READ_TIMEOUT", &value.readTimeout},
		{"AGENTD_HTTP_WRITE_TIMEOUT", &value.writeTimeout},
		{"AGENTD_HTTP_IDLE_TIMEOUT", &value.idleTimeout},
		{"AGENTD_SHUTDOWN_TIMEOUT", &value.shutdownTimeout},
	}
	for _, item := range durations {
		parsed, err := durationEnv(item.name, *item.value)
		if err != nil {
			return config{}, err
		}
		*item.value = parsed
	}
	switch value.workerSource {
	case "":
	case "kubernetes":
		if value.workerSelector == "" {
			return config{}, errors.New("AGENTD_WORKER_LABEL_SELECTOR is required for the Kubernetes Worker source")
		}
		if value.observationTimeout <= value.observerInterval {
			return config{}, errors.New("AGENTD_WORKER_OBSERVATION_TIMEOUT must exceed AGENTD_WORKER_OBSERVER_INTERVAL")
		}
	default:
		return config{}, fmt.Errorf("unsupported AGENTD_WORKER_SOURCE %q", value.workerSource)
	}
	return value, nil
}

func buildWorkerObserver(config config, application *control.App, logger *slog.Logger) (*observer.Observer, error) {
	if config.workerSource == "" {
		return nil, nil
	}
	kubernetesClient, err := controlk8s.NewInCluster(controlk8s.Config{
		Namespace: config.workerNamespace, LabelSelector: config.workerSelector,
		RequestTimeout: config.observerTimeout, QPS: 5, Burst: 10,
	})
	if err != nil {
		return nil, err
	}
	source, err := observer.NewKubernetesSource(kubernetesClient, config.workerPort, config.workerMaxRuns)
	if err != nil {
		return nil, err
	}
	return observer.New(source, application, observer.Config{
		Interval: config.observerInterval, RequestTimeout: config.observerTimeout, Logger: logger,
	})
}

func openMySQL(config config) (*gorm.DB, func() error, error) {
	dsn, err := drivermysql.ParseDSN(config.mysqlDSN)
	if err != nil {
		return nil, nil, fmt.Errorf("parse MySQL DSN: %w", err)
	}
	dsn.ParseTime = true
	dsn.Loc = time.UTC
	dsn.Timeout = config.storageTimeout
	dsn.ReadTimeout = config.storageTimeout
	dsn.WriteTimeout = config.storageTimeout
	if dsn.Params == nil {
		dsn.Params = map[string]string{}
	}
	if dsn.Params["charset"] == "" {
		dsn.Params["charset"] = "utf8mb4"
	}
	database, err := gorm.Open(gormmysql.Open(dsn.FormatDSN()), &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: true,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("open MySQL storage: %w", err)
	}
	sqlDatabase, err := database.DB()
	if err != nil {
		return nil, nil, fmt.Errorf("resolve MySQL connection pool: %w", err)
	}
	sqlDatabase.SetMaxOpenConns(config.maxOpenConns)
	sqlDatabase.SetMaxIdleConns(config.maxIdleConns)
	sqlDatabase.SetConnMaxLifetime(config.connMaxLifetime)
	ctx, cancel := context.WithTimeout(context.Background(), config.storageTimeout)
	defer cancel()
	if err := sqlDatabase.PingContext(ctx); err != nil {
		_ = sqlDatabase.Close()
		return nil, nil, fmt.Errorf("ping MySQL storage: %w", err)
	}
	return database, sqlDatabase.Close, nil
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
