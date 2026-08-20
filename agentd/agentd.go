package agentd

import (
	"context"
	"fmt"
	"log/slog"
	"os/signal"
	"syscall"
	"time"

	hertzserver "github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/network/standard"
	"github.com/compforge/agentd/agentd/internal/api"
	"github.com/compforge/agentd/agentd/internal/connector"
	gormrepo "github.com/compforge/agentd/agentd/internal/repo/gorm"
	control "github.com/compforge/agentd/agentd/internal/service"
	drivermysql "github.com/go-sql-driver/mysql"
	gormmysql "gorm.io/driver/mysql"
	gormsqlite "gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func Run(logger *slog.Logger) error {
	config, err := loadConfig()
	if err != nil {
		return err
	}
	database, closeDatabase, err := openDatabase(config)
	if err != nil {
		return err
	}
	defer closeDatabase()
	repository, err := gormrepo.NewGORM(database)
	if err != nil {
		return err
	}
	workerControllers, err := buildWorkerControllers(config, database, repository, logger)
	if err != nil {
		return err
	}
	var demandNotifier control.DemandNotifier
	if workerControllers != nil {
		demandNotifier = workerControllers.lifecycler
	}
	controlService, err := control.New(repository, config.observationTimeout, demandNotifier)
	if err != nil {
		return err
	}
	if workerControllers != nil {
		if err := workerControllers.attachObserver(config, controlService, logger); err != nil {
			return err
		}
	}
	agentletConnector, err := connector.New(connector.Config{
		RequestTimeout:        config.connectorRequestTimeout,
		DialTimeout:           config.connectorDialTimeout,
		ResponseHeaderTimeout: config.connectorHeaderTimeout,
		IdleConnTimeout:       config.connectorIdleConnTimeout,
		MaxIdleConns:          config.connectorMaxIdleConns,
		MaxIdleConnsPerHost:   config.connectorMaxIdleConnsPerHost,
	})
	if err != nil {
		return err
	}
	defer agentletConnector.CloseIdleConnections()

	httpServer := hertzserver.Default(
		hertzserver.WithHostPorts(config.address),
		hertzserver.WithTransport(standard.NewTransporter),
		hertzserver.WithReadTimeout(config.readTimeout),
		// SSE responses inherit their lifetime from the client context.
		hertzserver.WithWriteTimeout(0),
		hertzserver.WithIdleTimeout(config.idleTimeout),
		hertzserver.WithMaxRequestBodySize(1<<20),
		hertzserver.WithSenseClientDisconnection(true),
	)
	api.New(controlService, agentletConnector, logger).Register(httpServer.Engine)

	processCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	serveErr := make(chan error, 1)
	go func() {
		logger.Info("agentd listening", "address", config.address)
		serveErr <- httpServer.Run()
	}()
	if workerControllers != nil {
		workerControllers.Run(processCtx)
	}
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

func openDatabase(config config) (*gorm.DB, func() error, error) {
	if config.mysqlDSN != "" {
		return openMySQL(config)
	}
	database, err := gorm.Open(gormsqlite.Open(config.sqlitePath), &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: true,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("open SQLite storage %q: %w", config.sqlitePath, err)
	}
	sqlDatabase, err := database.DB()
	if err != nil {
		return nil, nil, fmt.Errorf("resolve SQLite connection pool: %w", err)
	}
	// SQLite is the single-replica fallback. One connection keeps transactions
	// serialized instead of surfacing avoidable database-locked failures.
	sqlDatabase.SetMaxOpenConns(1)
	sqlDatabase.SetMaxIdleConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), config.storageTimeout)
	defer cancel()
	if err := sqlDatabase.PingContext(ctx); err != nil {
		_ = sqlDatabase.Close()
		return nil, nil, fmt.Errorf("ping SQLite storage %q: %w", config.sqlitePath, err)
	}
	return database, sqlDatabase.Close, nil
}
