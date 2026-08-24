package gorm

import (
	"context"
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

var _ controllock.LeaseLocker = (*GORMLocker)(nil)

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

	// Expired takeover is one conditional update. Correctness must not depend
	// on SELECT FOR UPDATE, which is not implemented consistently across the
	// SQLite and MySQL deployment modes. The expiration predicate is the CAS
	// fence: after one contender renews it, every other contender must miss.
	result := l.db.WithContext(ctx).Model(&resourceLockRow{}).
		Where("resource = ? AND expires_at <= ?", resource, now).
		Updates(map[string]any{
			"locker_id": lockerID, "expires_at": now.Add(ttl), "updated_at": now,
		})
	if result.Error != nil {
		return nil, fmt.Errorf("take over control resource %q: %w", resource, result.Error)
	}
	if result.RowsAffected == 1 {
		return &controllock.Token{Resource: resource, LockerID: lockerID, LeaseTTL: ttl}, nil
	}

	row := resourceLockRow{
		Resource: resource, LockerID: lockerID, ExpiresAt: now.Add(ttl),
		CreatedAt: now, UpdatedAt: now,
	}
	result = l.db.WithContext(ctx).Clauses(clause.OnConflict{DoNothing: true}).Create(&row)
	if result.Error != nil {
		return nil, fmt.Errorf("create control resource lease %q: %w", resource, result.Error)
	}
	// MySQL can report a duplicate no-op as affected when CLIENT_FOUND_ROWS is
	// enabled. Confirm ownership by the unique acquisition ID instead of using
	// driver-specific RowsAffected semantics.
	var owned int64
	if err := l.db.WithContext(ctx).Model(&resourceLockRow{}).
		Where("resource = ? AND locker_id = ?", resource, lockerID).
		Count(&owned).Error; err != nil {
		return nil, fmt.Errorf("confirm control resource lease %q: %w", resource, err)
	}
	if owned != 1 {
		return nil, controllock.ErrLocked
	}
	return &controllock.Token{Resource: resource, LockerID: lockerID, LeaseTTL: ttl}, nil
}

func (l *GORMLocker) Renew(ctx context.Context, token *controllock.Token) error {
	if token == nil || token.LeaseTTL <= 0 {
		return fmt.Errorf("renew control resource: token with positive lease TTL is required")
	}
	now := time.Now().UTC()
	result := l.db.WithContext(ctx).Model(&resourceLockRow{}).
		Where("resource = ? AND locker_id = ?", token.Resource, token.LockerID).
		Updates(map[string]any{"expires_at": now.Add(token.LeaseTTL), "updated_at": now})
	if result.Error != nil {
		return fmt.Errorf("renew control resource %q: %w", token.Resource, result.Error)
	}
	if result.RowsAffected != 1 {
		return fmt.Errorf("%w: %s", controllock.ErrLockLost, token.Resource)
	}
	return nil
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
