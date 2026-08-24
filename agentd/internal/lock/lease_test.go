package lock

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type leaseTestLocker struct {
	mu                 sync.Mutex
	remainingConflicts int
	lockCalls          int
	renewErr           error
	renewed            chan struct{}
	unlocked           bool
}

func (l *leaseTestLocker) Lock(_ context.Context, resource string, ttl time.Duration) (*Token, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lockCalls++
	if l.remainingConflicts > 0 {
		l.remainingConflicts--
		return nil, ErrLocked
	}
	return &Token{Resource: resource, LockerID: "test", LeaseTTL: ttl}, nil
}

func (l *leaseTestLocker) Renew(context.Context, *Token) error {
	if l.renewed != nil {
		select {
		case l.renewed <- struct{}{}:
		default:
		}
	}
	return l.renewErr
}

func (l *leaseTestLocker) Unlock(context.Context, *Token) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.unlocked = true
	return nil
}

func TestWithLeaseRetriesRenewsAndReleases(t *testing.T) {
	locker := &leaseTestLocker{remainingConflicts: 2, renewed: make(chan struct{}, 1)}
	err := WithLease(context.Background(), locker, "worker-pool", LeaseOptions{
		TTL: 50 * time.Millisecond, RetryInterval: time.Millisecond,
		HeartbeatInterval: 5 * time.Millisecond, ReleaseTimeout: time.Second,
	}, func(context.Context) error {
		select {
		case <-locker.renewed:
			return nil
		case <-time.After(time.Second):
			return errors.New("timed out waiting for lease renewal")
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if locker.lockCalls != 3 {
		t.Fatalf("Lock calls = %d, want 3", locker.lockCalls)
	}
	if !locker.unlocked {
		t.Fatal("lease was not released")
	}
}

func TestWithLeaseCancelsWorkWhenRenewalLosesOwnership(t *testing.T) {
	locker := &leaseTestLocker{renewErr: ErrLockLost}
	err := WithLease(context.Background(), locker, "worker-pool", LeaseOptions{
		TTL: 50 * time.Millisecond, HeartbeatInterval: time.Millisecond,
	}, func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})
	if !errors.Is(err, ErrLockLost) {
		t.Fatalf("WithLease error = %v, want ErrLockLost", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("WithLease error = %v, want canceled work", err)
	}
	if !locker.unlocked {
		t.Fatal("lost lease was not released")
	}
}
