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
	db, err := gorm.Open(gormmysql.Open(dsn.FormatDSN()), &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: true,
	})
	if err != nil {
		return nil, fmt.Errorf("open MySQL storage: %w", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("resolve MySQL connection pool: %w", err)
	}
	sqlDB.SetMaxOpenConns(config.MaxOpenConns)
	sqlDB.SetMaxIdleConns(config.MaxIdleConns)
	sqlDB.SetConnMaxLifetime(config.ConnMaxLifetime)
	pingCtx, cancel := context.WithTimeout(ctx, config.OperationTimeout)
	defer cancel()
	if err := sqlDB.PingContext(pingCtx); err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("ping MySQL storage: %w", err)
	}

	return initializeBackend(ctx, "mysql", db, sqlDB.Close, config.OperationTimeout)
}

func openSQLite(ctx context.Context, config Config) (*Backend, error) {
	if config.OperationTimeout <= 0 {
		return nil, fmt.Errorf("storage operation timeout must be positive")
	}
	if config.SQLitePath == "" {
		return nil, fmt.Errorf("SQLite path is required when AGENTD_MYSQL_DSN is not set")
	}
	db, err := gorm.Open(gormsqlite.Open(config.SQLitePath), &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: true,
	})
	if err != nil {
		return nil, fmt.Errorf("open SQLite storage %q: %w", config.SQLitePath, err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("resolve SQLite connection pool: %w", err)
	}
	// The fallback belongs to one Worker Pod. Serializing its DB access avoids
	// lock contention without pretending that local SQLite is shared storage.
	sqlDB.SetMaxOpenConns(1)
	sqlDB.SetMaxIdleConns(1)
	pingCtx, cancel := context.WithTimeout(ctx, config.OperationTimeout)
	defer cancel()
	if err := sqlDB.PingContext(pingCtx); err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("ping SQLite storage %q: %w", config.SQLitePath, err)
	}
	return initializeBackend(ctx, "sqlite", db, sqlDB.Close, config.OperationTimeout)
}

func initializeBackend(
	ctx context.Context,
	provider string,
	db *gorm.DB,
	closeDatabase func() error,
	operationTimeout time.Duration,
) (*Backend, error) {
	ledger, err := ledgergorm.New(db, operationTimeout)
	if err != nil {
		_ = closeDatabase()
		return nil, err
	}
	if err := ledger.Initialize(ctx); err != nil {
		_ = closeDatabase()
		return nil, fmt.Errorf("initialize Agent Ledger store: %w", err)
	}
	return &Backend{
		Provider: provider, Ledger: ledger, Checkpoints: ledger,
		close: closeDatabase,
	}, nil
}

func (b *Backend) Close() error {
	if b == nil || b.close == nil {
		return nil
	}
	return b.close()
}
