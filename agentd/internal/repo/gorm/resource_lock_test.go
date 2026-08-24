package gorm

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	controllock "github.com/compforge/agentd/agentd/internal/lock"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestGORMLockerCoordinatesReplicasAndAllowsReacquireAfterRelease(t *testing.T) {
	database := newResourceLockTestDatabase(t)
	first, _ := NewGORMLocker(database, "replica-a")
	second, _ := NewGORMLocker(database, "replica-b")
	token, err := first.Lock(context.Background(), "worker-pool", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.Lock(context.Background(), "worker-pool", time.Minute); !errors.Is(err, controllock.ErrLocked) {
		t.Fatalf("second Lock error = %v, want ErrLocked", err)
	}
	if err := first.Unlock(context.Background(), token); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Lock(context.Background(), "worker-pool", time.Minute); err != nil {
		t.Fatalf("Lock after release: %v", err)
	}
}

func TestGORMLockerRenewExtendsOwnedLease(t *testing.T) {
	database := newResourceLockTestDatabase(t)
	locker, _ := NewGORMLocker(database, "replica-a")
	token, err := locker.Lock(context.Background(), "worker-pool", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	var before resourceLockRow
	if err := database.Where("resource = ?", token.Resource).First(&before).Error; err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond)
	if err := locker.Renew(context.Background(), token); err != nil {
		t.Fatal(err)
	}
	var after resourceLockRow
	if err := database.Where("resource = ?", token.Resource).First(&after).Error; err != nil {
		t.Fatal(err)
	}
	if !after.ExpiresAt.After(before.ExpiresAt) {
		t.Fatalf("renewed expiration = %v, want after %v", after.ExpiresAt, before.ExpiresAt)
	}
}

func TestGORMLockerExpiredLeaseHasOneTakeoverWinner(t *testing.T) {
	database := newResourceLockTestDatabase(t)
	now := time.Now().UTC()
	const resource = "worker-pool"
	if err := database.Create(&resourceLockRow{
		Resource: resource, LockerID: "expired-owner", ExpiresAt: now.Add(-time.Minute),
		CreatedAt: now.Add(-2 * time.Minute), UpdatedAt: now.Add(-time.Minute),
	}).Error; err != nil {
		t.Fatal(err)
	}

	first, _ := NewGORMLocker(database, "replica-a")
	second, _ := NewGORMLocker(database, "replica-b")
	start := make(chan struct{})
	results := make(chan struct {
		token *controllock.Token
		err   error
	}, 2)
	for _, locker := range []*GORMLocker{first, second} {
		go func() {
			<-start
			token, err := locker.Lock(context.Background(), resource, time.Minute)
			results <- struct {
				token *controllock.Token
				err   error
			}{token: token, err: err}
		}()
	}
	close(start)

	winners := 0
	var winnerToken *controllock.Token
	for range 2 {
		result := <-results
		switch {
		case result.err == nil && result.token != nil:
			winners++
			winnerToken = result.token
		case errors.Is(result.err, controllock.ErrLocked):
		default:
			t.Fatalf("takeover error = %v, token = %#v", result.err, result.token)
		}
	}
	if winners != 1 {
		t.Fatalf("takeover winners = %d, want 1", winners)
	}
	if err := first.Renew(context.Background(), &controllock.Token{
		Resource: resource, LockerID: "expired-owner", LeaseTTL: time.Minute,
	}); !errors.Is(err, controllock.ErrLockLost) {
		t.Fatalf("stale Renew error = %v, want ErrLockLost", err)
	}
	if err := first.Renew(context.Background(), winnerToken); err != nil {
		t.Fatalf("winner Renew: %v", err)
	}

	// An expired holder can finish late, but its fenced unlock must not delete
	// the successor's live lease.
	if err := first.Unlock(context.Background(), &controllock.Token{
		Resource: resource, LockerID: "expired-owner",
	}); err != nil {
		t.Fatal(err)
	}
	third, _ := NewGORMLocker(database, "replica-c")
	if _, err := third.Lock(context.Background(), resource, time.Minute); !errors.Is(err, controllock.ErrLocked) {
		t.Fatalf("Lock after stale unlock error = %v, want ErrLocked", err)
	}
}

func newResourceLockTestDatabase(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := filepath.Join(t.TempDir(), "agentd.db") + "?_busy_timeout=5000&_journal_mode=WAL"
	database, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewGORM(database); err != nil {
		t.Fatal(err)
	}
	return database
}
