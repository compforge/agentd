package gorm

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	agentledger "github.com/compforge/agent-ledger/go"
	controllock "github.com/compforge/agentd/agentd/internal/lock"
	gormio "gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type resourceLockRow struct {
	Resource  string    `gorm:"primaryKey;size:191"`
	LockerID  string    `gorm:"not null;size:191"`
	ExpiresAt time.Time `gorm:"not null;index"`
	CreatedAt time.Time `gorm:"not null"`
	UpdatedAt time.Time `gorm:"not null"`
}

func (resourceLockRow) TableName() string { return "resource_locks" }

type GORMLocker struct {
	db       *gormio.DB
	lockerID string
}

var _ controllock.Locker = (*GORMLocker)(nil)

func NewGORMLocker(db *gormio.DB, lockerID string) (*GORMLocker, error) {
	if db == nil {
		return nil, fmt.Errorf("create resource locker: database is required")
	}
	if strings.TrimSpace(lockerID) == "" {
		return nil, fmt.Errorf("create resource locker: locker ID is required")
	}
	return &GORMLocker{db: db, lockerID: lockerID}, nil
}

func (l *GORMLocker) Lock(ctx context.Context, resource string, ttl time.Duration) (*controllock.Token, error) {
	if strings.TrimSpace(resource) == "" || ttl <= 0 {
		return nil, fmt.Errorf("lock control resource: resource and positive TTL are required")
	}
	now := time.Now().UTC()
	lockerID := l.lockerID + "/" + agentledger.NewID()
	acquired := false
	err := l.db.WithContext(ctx).Transaction(func(tx *gormio.DB) error {
		var row resourceLockRow
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("resource = ?", resource).First(&row).Error
		if errors.Is(err, gormio.ErrRecordNotFound) {
			row = resourceLockRow{
				Resource: resource, LockerID: lockerID, ExpiresAt: now.Add(ttl),
				CreatedAt: now, UpdatedAt: now,
			}
			result := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&row)
			if result.Error != nil {
				return result.Error
			}
			acquired = result.RowsAffected == 1
			return nil
		}
		if err != nil {
			return err
		}
		if row.ExpiresAt.After(now) {
			return nil
		}
		result := tx.Model(&resourceLockRow{}).Where("resource = ?", resource).Updates(map[string]any{
			"locker_id": lockerID, "expires_at": now.Add(ttl), "updated_at": now,
		})
		if result.Error != nil {
			return result.Error
		}
		acquired = result.RowsAffected == 1
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("lock control resource %q: %w", resource, err)
	}
	if !acquired {
		return nil, controllock.ErrLocked
	}
	return &controllock.Token{Resource: resource, LockerID: lockerID}, nil
}

func (l *GORMLocker) Unlock(ctx context.Context, token *controllock.Token) error {
	if token == nil {
		return nil
	}
	result := l.db.WithContext(ctx).Where(
		"resource = ? AND locker_id = ?", token.Resource, token.LockerID,
	).Delete(&resourceLockRow{})
	if result.Error != nil {
		return fmt.Errorf("unlock control resource %q: %w", token.Resource, result.Error)
	}
	return nil
}
