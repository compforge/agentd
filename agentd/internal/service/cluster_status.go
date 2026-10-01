package service

import (
	"fmt"
	"time"

	"github.com/compforge/agentd/agentd/internal/model"
)

// ClusterStatusReader exposes recent infrastructure hints without making the
// service depend on Kubernetes. False does not certify available capacity.
type ClusterStatusReader interface {
	ResourcePressure() bool
	SchedulingBlocked() bool
}

type Option func(*Service)

func WithClusterStatus(reader ClusterStatusReader) Option {
	return func(s *Service) { s.clusterStatus = reader }
}

func (a *Service) unavailableWorker(worker model.Worker, status model.WorkerObserverStatus, now time.Time) error {
	unavailable := fmt.Errorf("%w: Worker %q has no fresh ready endpoint", ErrUnavailable, worker.ID)
	// Only refine a confirmed provisioning wait. Stale/missing observations,
	// ready-node startup failures and already-active Worker outages remain
	// generic unavailability even if unrelated Pods report cluster pressure.
	if a.clusterStatus == nil || worker.Phase != model.WorkerPhaseCreating ||
		status.ObservedAt.IsZero() || status.ObservedAt.After(now) || now.Sub(status.ObservedAt) > a.observationTimeout ||
		(status.Exists && !status.Unschedulable) {
		return unavailable
	}
	if a.clusterStatus.ResourcePressure() {
		return fmt.Errorf("%w: %w; recent cluster resource pressure while waiting for Worker provisioning; provisioning will retry", unavailable, ErrClusterNoCapacity)
	}
	if a.clusterStatus.SchedulingBlocked() {
		return fmt.Errorf("%w: %w; recent cluster scheduling blockage while waiting for Worker provisioning; provisioning will retry", unavailable, ErrWorkerUnschedulable)
	}
	return unavailable
}
