// Package cluster collects recent Kubernetes capacity and scheduling signals.
// It observes infrastructure; it does not own Worker or Session lifecycle.
package cluster

import (
	"context"
	corev1 "k8s.io/api/core/v1"
)

// PodHooks accepts facts from namespace-scoped observations and local actions.
// Implementations must be concurrency-safe, bounded, and perform no network I/O.
// A create hook requires the attempted Pod and reports the actual API result,
// not an idempotent Ensure result.
type PodHooks interface {
	OnPodCreated(context.Context, *corev1.Pod, error)
	OnPodUpdated(context.Context, *corev1.Pod)
}
