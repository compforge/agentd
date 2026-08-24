package lock

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type LeaseOptions struct {
	TTL               time.Duration
	RetryInterval     time.Duration
	HeartbeatInterval time.Duration
	ReleaseTimeout    time.Duration
	OnAcquired        func(waited time.Duration)
}

// WithLease waits for a lease, renews it while fn runs, and releases it when fn
// finishes. Losing ownership cancels fn's context and is returned to the caller.
func WithLease(
	ctx context.Context,
	locker LeaseLocker,
	resource string,
	options LeaseOptions,
	fn func(context.Context) error,
) (resultErr error) {
	if locker == nil || fn == nil {
		return fmt.Errorf("run with lease: locker and function are required")
	}
	if err := normalizeLeaseOptions(&options); err != nil {
		return err
	}

	startedAt := time.Now()
	token, err := waitForLease(ctx, locker, resource, options)
	if err != nil {
		return err
	}
	if options.OnAcquired != nil {
		options.OnAcquired(time.Since(startedAt))
	}

	leaseCtx, cancel := context.WithCancel(ctx)
	renewResult := make(chan error, 1)
	go renewLease(leaseCtx, cancel, locker, token, options.HeartbeatInterval, renewResult)
	defer func() {
		cancel()
		renewErr := <-renewResult
		releaseCtx, releaseCancel := context.WithTimeout(context.Background(), options.ReleaseTimeout)
		unlockErr := locker.Unlock(releaseCtx, token)
		releaseCancel()
		resultErr = errors.Join(resultErr, renewErr, unlockErr)
	}()

	return fn(leaseCtx)
}

func normalizeLeaseOptions(options *LeaseOptions) error {
	if options.TTL <= 0 {
		return fmt.Errorf("run with lease: positive TTL is required")
	}
	if options.RetryInterval <= 0 {
		options.RetryInterval = 50 * time.Millisecond
	}
	if options.HeartbeatInterval <= 0 {
		options.HeartbeatInterval = options.TTL / 3
	}
	if options.HeartbeatInterval <= 0 || options.HeartbeatInterval >= options.TTL {
		return fmt.Errorf("run with lease: heartbeat interval must be positive and shorter than TTL")
	}
	if options.ReleaseTimeout <= 0 {
		options.ReleaseTimeout = 5 * time.Second
	}
	return nil
}

func waitForLease(
	ctx context.Context,
	locker LeaseLocker,
	resource string,
	options LeaseOptions,
) (*Token, error) {
	for {
		token, err := locker.Lock(ctx, resource, options.TTL)
		if err == nil {
			return token, nil
		}
		if !errors.Is(err, ErrLocked) {
			return nil, fmt.Errorf("lock resource %q: %w", resource, err)
		}
		timer := time.NewTimer(options.RetryInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func renewLease(
	ctx context.Context,
	cancel context.CancelFunc,
	locker LeaseLocker,
	token *Token,
	interval time.Duration,
	result chan<- error,
) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			result <- nil
			return
		case <-ticker.C:
			if err := locker.Renew(ctx, token); err != nil {
				cancel()
				result <- fmt.Errorf("renew resource %q lease: %w", token.Resource, err)
				return
			}
		}
	}
}
