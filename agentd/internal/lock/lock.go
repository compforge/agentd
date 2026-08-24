// Package lock defines short distributed leases used by control-plane reconcilers.
package lock

import (
	"context"
	"errors"
	"time"
)

var (
	ErrLocked   = errors.New("resource is locked")
	ErrLockLost = errors.New("resource lock ownership lost")
)

type Token struct {
	Resource string
	LockerID string
	LeaseTTL time.Duration
}

// Locker coordinates one short reconcile decision across agentd replicas.
// Callers must not hold the lease while waiting for Kubernetes resources.
type Locker interface {
	Lock(context.Context, string, time.Duration) (*Token, error)
	Unlock(context.Context, *Token) error
}

// LeaseLocker can extend a held lease. Renew must return ErrLockLost when the
// token no longer owns the resource.
type LeaseLocker interface {
	Locker
	Renew(context.Context, *Token) error
}
