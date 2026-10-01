package cluster

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/cache"
)

const signalTTL = 90 * time.Second

type resourceStatus struct {
	Pressure  bool
	ExpiresAt time.Time
}

// Observer owns process-local diagnostic hints for one managed Worker pool.
// Expiry bounds stale evidence after a lost watch or failed API request. False
// means no recent signal, not proof that the cluster has available capacity.
// Signals never replace authoritative placement or Worker readiness facts.
type Observer struct {
	mu                     sync.Mutex
	quota                  resourceStatus
	schedulingResource     resourceStatus
	schedulingBlockedUntil time.Time
	warnings               *cache.LRUExpireCache
	logger                 *slog.Logger
	now                    func() time.Time
}

func NewObserver(logger *slog.Logger) *Observer {
	if logger == nil {
		logger = slog.Default()
	}
	return &Observer{logger: logger, now: time.Now, warnings: cache.NewLRUExpireCache(1024)}
}

func (o *Observer) ResourcePressure() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	now := o.now()
	return (o.quota.Pressure && now.Before(o.quota.ExpiresAt)) ||
		(o.schedulingResource.Pressure && now.Before(o.schedulingResource.ExpiresAt))
}

func (o *Observer) SchedulingBlocked() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.now().Before(o.schedulingBlockedUntil)
}

func (o *Observer) OnPodCreated(ctx context.Context, pod *corev1.Pod, err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if err == nil {
		// Admission success clears quota evidence only; another Pod may still be
		// waiting for a node. AlreadyExists is not a successful admission.
		if o.quota.Pressure && o.now().Before(o.quota.ExpiresAt) {
			o.logger.InfoContext(ctx, "Worker Pod admission cleared cluster quota pressure")
		}
		o.quota = resourceStatus{}
		return
	}
	if !apierrors.IsForbidden(err) || !strings.Contains(strings.ToLower(err.Error()), "exceeded quota:") {
		return
	}
	now := o.now()
	changed := !o.quota.Pressure || !now.Before(o.quota.ExpiresAt)
	o.quota = resourceStatus{Pressure: true, ExpiresAt: now.Add(signalTTL)}
	if changed {
		o.logger.WarnContext(ctx, "cluster resource pressure from Worker Pod admission",
			"namespace", pod.Namespace, "pod", pod.Name, "error", err, "expires_at", o.quota.ExpiresAt)
	}
}

// OnPodUpdated accepts an informer event or a successful direct Pod read.
// Cache scans must not refresh evidence: a disconnected watch can retain stale
// conditions indefinitely. Existing live LIST calls also feed this hook.
func (o *Observer) OnPodUpdated(ctx context.Context, pod *corev1.Pod) {
	if pod == nil || pod.DeletionTimestamp != nil || pod.Spec.NodeName != "" ||
		pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		return
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type != corev1.PodScheduled || condition.Status == corev1.ConditionTrue {
			continue
		}
		o.mu.Lock()
		defer o.mu.Unlock()
		now := o.now()
		if condition.Status == corev1.ConditionFalse {
			o.schedulingBlockedUntil = now.Add(signalTTL)
			message := strings.ToLower(condition.Message)
			if condition.Reason == corev1.PodReasonUnschedulable &&
				(strings.Contains(message, "insufficient ") || strings.Contains(message, "too many pods")) {
				o.schedulingResource = resourceStatus{Pressure: true, ExpiresAt: now.Add(signalTTL)}
			}
		}
		key := pod.Namespace + "/" + pod.Name + "/" + string(pod.UID)
		signature := string(condition.Status) + "/" + condition.Reason + "/" + condition.Message
		previous, found := o.warnings.Get(key)
		if !found || previous != signature {
			o.warnings.Add(key, signature, 5*time.Minute)
			o.logger.WarnContext(ctx, "Worker Pod cannot be scheduled",
				"namespace", pod.Namespace, "pod", pod.Name, "pod_uid", pod.UID,
				"status", condition.Status, "reason", condition.Reason, "message", condition.Message,
				"resource_pressure", o.schedulingResource.Pressure && now.Before(o.schedulingResource.ExpiresAt))
		}
		return
	}
}
