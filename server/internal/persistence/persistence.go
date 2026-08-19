package persistence

import (
	"context"
	"fmt"
	"time"

	agentledger "github.com/compforge/agent-ledger/go"
	"github.com/compforge/agentd/server/internal/app"
	"github.com/compforge/agentd/server/internal/harnessstate"
	"github.com/compforge/agentd/server/internal/ledgerstore"
	"github.com/compforge/agentd/server/internal/store"
	drivermysql "github.com/go-sql-driver/mysql"
	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type Config struct {
	MySQLDSN         string
	OperationTimeout time.Duration
	MaxOpenConns     int
	MaxIdleConns     int
	ConnMaxLifetime  time.Duration
}

type Backend struct {
	Resources     app.Repository
	HarnessStates harnessstate.Store
	Ledger        agentledger.EventStore
	close         func() error
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

	resources, err := store.NewGORM(db.WithContext(ctx))
	if err != nil {
		_ = sqlDB.Close()
		return nil, err
	}
	harnessStates, err := harnessstate.NewGORM(db.WithContext(ctx))
	if err != nil {
		_ = sqlDB.Close()
		return nil, err
	}
	ledger, err := ledgerstore.NewGORM(db.WithContext(ctx))
	if err != nil {
		_ = sqlDB.Close()
		return nil, err
	}
	return &Backend{
		Resources: resources, HarnessStates: harnessStates, Ledger: ledger,
		close: func() error { return sqlDB.Close() },
	}, nil
}

func (b *Backend) Close() error {
	if b == nil || b.close == nil {
		return nil
	}
	return b.close()
}
