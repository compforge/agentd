package gorm

import (
	"context"
	"errors"
	"fmt"
	"time"

	ledgergorm "github.com/compforge/agent-ledger/go/stores/gorm"
	"github.com/compforge/agentd/agentd/internal/model"
	"github.com/compforge/agentd/agentd/internal/repo"
	managedevent "github.com/compforge/agentd/internal/event"
	"github.com/go-sql-driver/mysql"
	"github.com/mattn/go-sqlite3"
	"github.com/qiankunli/go-stdx/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type idempotencyRow struct {
	ID string `gorm:"primaryKey;size:36"`
	// Binary comparison preserves opaque key identity on case-insensitive MySQL databases.
	IdempotencyKey string `gorm:"type:varbinary(255);not null;uniqueIndex:idx_idempotency_resource_key"`
	ResourceType   string `gorm:"size:32;not null;uniqueIndex:idx_idempotency_resource_key"`
	RequestDigest  string `gorm:"not null;size:64"`
	ResponseStatus int    `gorm:"not null"`
	// Preserve exact response bytes; MySQL's JSON type can normalize the snapshot.
	ResponseBody []byte    `gorm:"type:longblob"`
	CreatedAt    time.Time `gorm:"not null"`
}

func (idempotencyRow) TableName() string { return "idempotency_keys" }

type IdempotencyStore struct {
	db      *gorm.DB
	timeout time.Duration
}

func NewIdempotencyStore(db *gorm.DB, timeout time.Duration) (*IdempotencyStore, error) {
	if db == nil || timeout <= 0 {
		return nil, errors.New("idempotency store requires database and positive timeout")
	}
	return &IdempotencyStore{db: db, timeout: timeout}, nil
}

// Transaction reuses Ledger's public store with the same transaction handle.
// +spec=`A receipt, its Session and all ingress Events commit together or not at all`
// +rule=`Never initialize Ledger schema or call an Agentlet inside the idempotency transaction`
// +link=agentd/docs/idempotency.md
func (s *IdempotencyStore) Transaction(ctx context.Context, fn func(context.Context, repo.Repository, *managedevent.Log, repo.IdempotencyRepository) error) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		ledger, err := ledgergorm.New(tx, s.timeout)
		if err != nil {
			return err
		}
		return fn(ctx, &GORMRepository{db: tx}, managedevent.NewLog(ledger), newIdempotencyRecords(tx))
	})
}

func (s *IdempotencyStore) GetIdempotencyKey(ctx context.Context, identity model.IdempotencyIdentity) (model.IdempotencyRecord, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	return newIdempotencyRecords(s.db).GetIdempotencyKey(ctx, identity)
}

type idempotencyRecords struct{ db *gorm.DB }

func newIdempotencyRecords(db *gorm.DB) *idempotencyRecords {
	// SQL error traces would include the raw key or cached response. Return
	// contextual errors to the service instead of logging those parameters.
	return &idempotencyRecords{db: db.Session(&gorm.Session{Logger: db.Logger.LogMode(logger.Silent)})}
}

func (s *idempotencyRecords) GetIdempotencyKey(ctx context.Context, identity model.IdempotencyIdentity) (model.IdempotencyRecord, error) {
	var row idempotencyRow
	if err := s.db.WithContext(ctx).First(&row, "resource_type = ? AND idempotency_key = ?", identity.ResourceType, identity.IdempotencyKey).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return model.IdempotencyRecord{}, repo.ErrNotFound
		}
		return model.IdempotencyRecord{}, fmt.Errorf("read idempotency record: %w", err)
	}
	return model.IdempotencyRecord{
		ID:                  row.ID,
		IdempotencyIdentity: model.IdempotencyIdentity{IdempotencyKey: row.IdempotencyKey, ResourceType: row.ResourceType, RequestDigest: row.RequestDigest},
		IdempotencyResponse: model.IdempotencyResponse{StatusCode: row.ResponseStatus, Body: row.ResponseBody},
		CreatedAt:           row.CreatedAt,
	}, nil
}

func (s *idempotencyRecords) CreateIdempotencyKey(ctx context.Context, identity model.IdempotencyIdentity) error {
	row := idempotencyRow{ID: uuid.New(), IdempotencyKey: identity.IdempotencyKey, ResourceType: identity.ResourceType, RequestDigest: identity.RequestDigest, CreatedAt: time.Now().UTC()}
	if err := s.db.WithContext(ctx).Create(&row).Error; err != nil {
		var my *mysql.MySQLError
		var sq sqlite3.Error
		if errors.Is(err, gorm.ErrDuplicatedKey) || (errors.As(err, &my) && my.Number == 1062) ||
			(errors.As(err, &sq) && (sq.ExtendedCode == sqlite3.ErrConstraintPrimaryKey || sq.ExtendedCode == sqlite3.ErrConstraintUnique)) {
			return repo.ErrIdempotencyKeyExists
		}
		return fmt.Errorf("claim idempotency record: %w", err)
	}
	return nil
}

func (s *idempotencyRecords) SaveIdempotencyResponse(ctx context.Context, identity model.IdempotencyIdentity, response model.IdempotencyResponse) error {
	result := s.db.WithContext(ctx).Model(&idempotencyRow{}).Where("resource_type = ? AND idempotency_key = ? AND request_digest = ?", identity.ResourceType, identity.IdempotencyKey, identity.RequestDigest).
		Updates(map[string]any{"response_status": response.StatusCode, "response_body": response.Body})
	if result.Error != nil {
		return fmt.Errorf("save idempotency record: %w", result.Error)
	}
	if result.RowsAffected != 1 {
		return errors.New("save idempotency record: claim missing")
	}
	return nil
}
