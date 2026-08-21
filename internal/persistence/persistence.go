// Package persistence assembles the database, Agent Ledger, and Checkpoint stores shared by agentd and Agentlet.
package persistence

import (
	"context"
	"fmt"
	"time"

	agentledger "github.com/compforge/agent-ledger/go"
	ledgergorm "github.com/compforge/agent-ledger/go/stores/gorm"
	drivermysql "github.com/go-sql-driver/mysql"
	gormmysql "gorm.io/driver/mysql"
	gormsqlite "gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type Config struct {
	MySQLDSN         string
	SQLitePath       string
	OperationTimeout time.Duration
	MaxOpenConns     int
	MaxIdleConns     int
	ConnMaxLifetime  time.Duration
}

type Backend struct {
	Provider    string
	Database    *gorm.DB
	Ledger      agentledger.EventStore
	Checkpoints agentledger.CheckpointStore
	close       func() error
}

func Open(ctx context.Context, config Config) (*Backend, error) {
	if config.MySQLDSN != "" {
		return OpenMySQL(ctx, config)
	}
	return openSQLite(ctx, config)
}

func OpenMySQL(ctx context.Context, config Config) (*Backend, error) {
	if config.OperationTimeout <= 0 {
		return nil, fmt.Errorf("storage operation timeout must be positive")
	}
	if config.MySQLDSN == "" {
		return nil, fmt.Errorf("AGENTD_MYSQL_DSN is required")
	}
	if config.MaxOpenConns <= 0 || config.MaxIdleConns <= 0 || config.MaxIdleConns > config.MaxOpenConns {
		return nil, fmt.Errorf("MySQL connection pool limits are invalid")
	}
	if config.ConnMaxLifetime <= 0 {
		return nil, fmt.Errorf("MySQL connection max lifetime must be positive")
	}
	dsn, err := drivermysql.ParseDSN(config.MySQLDSN)
	if err != nil {
		return nil, fmt.Errorf("parse MySQL DSN: %w", err)
	}
	dsn.ParseTime = true
	dsn.Loc = time.UTC
	dsn.Timeout = config.OperationTimeout
	dsn.ReadTimeout = config.OperationTimeout
	dsn.WriteTimeout = config.OperationTimeout
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
		return nil, fmt.Errorf("open MySQL storage: %w", err)
	}
	sqlDatabase, err := database.DB()
	if err != nil {
		return nil, fmt.Errorf("resolve MySQL connection pool: %w", err)
	}
	sqlDatabase.SetMaxOpenConns(config.MaxOpenConns)
	sqlDatabase.SetMaxIdleConns(config.MaxIdleConns)
	sqlDatabase.SetConnMaxLifetime(config.ConnMaxLifetime)
	pingCtx, cancel := context.WithTimeout(ctx, config.OperationTimeout)
	defer cancel()
	if err := sqlDatabase.PingContext(pingCtx); err != nil {
		_ = sqlDatabase.Close()
		return nil, fmt.Errorf("ping MySQL storage: %w", err)
	}

	return initializeBackend(ctx, "mysql", database, sqlDatabase.Close, config.OperationTimeout)
}

func openSQLite(ctx context.Context, config Config) (*Backend, error) {
	if config.OperationTimeout <= 0 {
		return nil, fmt.Errorf("storage operation timeout must be positive")
	}
	if config.SQLitePath == "" {
		return nil, fmt.Errorf("SQLite path is required when AGENTD_MYSQL_DSN is not set")
	}
	database, err := gorm.Open(gormsqlite.Open(config.SQLitePath), &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: true,
	})
	if err != nil {
		return nil, fmt.Errorf("open SQLite storage %q: %w", config.SQLitePath, err)
	}
	sqlDatabase, err := database.DB()
	if err != nil {
		return nil, fmt.Errorf("resolve SQLite connection pool: %w", err)
	}
	// SQLite is a single-process fallback. One connection keeps transactions
	// serialized instead of surfacing avoidable database-locked failures.
	sqlDatabase.SetMaxOpenConns(1)
	sqlDatabase.SetMaxIdleConns(1)
	pingCtx, cancel := context.WithTimeout(ctx, config.OperationTimeout)
	defer cancel()
	if err := sqlDatabase.PingContext(pingCtx); err != nil {
		_ = sqlDatabase.Close()
		return nil, fmt.Errorf("ping SQLite storage %q: %w", config.SQLitePath, err)
	}
	return initializeBackend(ctx, "sqlite", database, sqlDatabase.Close, config.OperationTimeout)
}

func initializeBackend(
	ctx context.Context,
	provider string,
	database *gorm.DB,
	closeDatabase func() error,
	operationTimeout time.Duration,
) (*Backend, error) {
	ledger, err := ledgergorm.New(database, operationTimeout)
	if err != nil {
		_ = closeDatabase()
		return nil, err
	}
	if err := ledger.Initialize(ctx); err != nil {
		_ = closeDatabase()
		return nil, fmt.Errorf("initialize Agent Ledger store: %w", err)
	}
	return &Backend{
		Provider: provider, Database: database, Ledger: ledger, Checkpoints: ledger,
		close: closeDatabase,
	}, nil
}

func (b *Backend) Close() error {
	if b == nil || b.close == nil {
		return nil
	}
	return b.close()
}
