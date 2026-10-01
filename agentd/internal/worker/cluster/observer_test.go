package cluster

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func pendingPod(message string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "worker", Namespace: "test", UID: "uid"},
		Status: corev1.PodStatus{Phase: corev1.PodPending, Conditions: []corev1.PodCondition{{
			Type: corev1.PodScheduled, Status: corev1.ConditionFalse,
			Reason: corev1.PodReasonUnschedulable, Message: message,
		}}}}
}

func quotaError() error {
	return apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "worker", fmt.Errorf("exceeded quota: compute, requested: requests.memory=2Gi"))
}

func TestAdmissionSignalsExpireAndClearIndependently(t *testing.T) {
	ctx := context.Background()
	o := NewObserver(slog.New(slog.NewTextHandler(io.Discard, nil)))
	now := time.Now()
	o.now = func() time.Time { return now }
	pod := pendingPod("0/2 nodes available: Insufficient memory")
	o.OnPodCreated(ctx, pod, quotaError())
	if !o.ResourcePressure() || o.SchedulingBlocked() {
		t.Fatal("quota must only mark resource pressure")
	}
	o.OnPodCreated(ctx, pod, apierrors.NewAlreadyExists(schema.GroupResource{Resource: "pods"}, pod.Name))
	if !o.ResourcePressure() {
		t.Fatal("AlreadyExists cleared quota pressure")
	}
	o.OnPodCreated(ctx, pod, nil)
	if o.ResourcePressure() {
		t.Fatal("successful admission did not clear quota pressure")
	}
	o.OnPodCreated(ctx, pod, quotaError())
	now = now.Add(signalTTL)
	if o.ResourcePressure() {
		t.Fatal("quota pressure did not expire")
	}
	o.OnPodCreated(ctx, pod, quotaError())
	o.OnPodUpdated(ctx, pod)
	o.OnPodCreated(ctx, pod, nil)
	if !o.ResourcePressure() || !o.SchedulingBlocked() {
		t.Fatal("admission cleared another Pod's scheduling signal")
	}
	// Scheduling success must not clear unrelated pending Pods' signals either.
	scheduled := pod.DeepCopy()
	scheduled.Spec.NodeName = "node"
	o.OnPodUpdated(ctx, scheduled)
	if !o.ResourcePressure() || !o.SchedulingBlocked() {
		t.Fatal("one scheduled Pod cleared shared evidence")
	}
	now = now.Add(signalTTL)
	if o.ResourcePressure() || o.SchedulingBlocked() {
		t.Fatal("scheduling signals did not expire")
	}
}

func TestPodSchedulingClassification(t *testing.T) {
	for _, tc := range []struct {
		name, message     string
		pressure, blocked bool
		mutate            func(*corev1.Pod)
	}{
		{name: "cpu", message: "0/2 nodes: Insufficient cpu", pressure: true, blocked: true},
		{name: "memory", message: "Insufficient memory", pressure: true, blocked: true},
		{name: "storage", message: "Insufficient ephemeral-storage", pressure: true, blocked: true},
		{name: "extended resource", message: "Insufficient example.com/gpu", pressure: true, blocked: true},
		{name: "pod slots", message: "Too many pods", pressure: true, blocked: true},
		{name: "affinity", message: "didn't match Pod's node affinity", blocked: true},
		{name: "taint", message: "had untolerated taint", blocked: true},
		{name: "pending without condition", mutate: func(p *corev1.Pod) { p.Status.Conditions = nil }},
		{name: "unknown", message: "Insufficient cpu", mutate: func(p *corev1.Pod) { p.Status.Conditions[0].Status = corev1.ConditionUnknown }},
		{name: "bound", message: "Insufficient cpu", mutate: func(p *corev1.Pod) { p.Spec.NodeName = "node" }},
		{name: "deleting", message: "Insufficient cpu", mutate: func(p *corev1.Pod) { v := metav1.Now(); p.DeletionTimestamp = &v }},
		{name: "terminal", message: "Insufficient cpu", mutate: func(p *corev1.Pod) { p.Status.Phase = corev1.PodFailed }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := NewObserver(slog.New(slog.NewTextHandler(io.Discard, nil)))
			p := pendingPod(tc.message)
			if tc.mutate != nil {
				tc.mutate(p)
			}
			o.OnPodUpdated(context.Background(), p)
			if o.ResourcePressure() != tc.pressure || o.SchedulingBlocked() != tc.blocked {
				t.Fatalf("pressure=%v blocked=%v", o.ResourcePressure(), o.SchedulingBlocked())
			}
		})
	}
}

func TestNonQuotaErrorsDoNotImplyPressure(t *testing.T) {
	o := NewObserver(slog.New(slog.NewTextHandler(io.Discard, nil)))
	for _, err := range []error{
		context.DeadlineExceeded,
		apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "worker", fmt.Errorf("RBAC denied")),
		fmt.Errorf("exceeded quota: not a Kubernetes forbidden response"),
	} {
		o.OnPodCreated(context.Background(), pendingPod(""), err)
		if o.ResourcePressure() {
			t.Fatalf("unrelated error marked pressure: %v", err)
		}
	}
}

func TestWarningsDeduplicateButObservationsRefresh(t *testing.T) {
	var logs bytes.Buffer
	o := NewObserver(slog.New(slog.NewTextHandler(&logs, nil)))
	now := time.Now()
	o.now = func() time.Time { return now }
	p := pendingPod("Insufficient cpu")
	for range 3 {
		o.OnPodUpdated(context.Background(), p)
		now = now.Add(signalTTL / 2)
	}
	if !o.ResourcePressure() {
		t.Fatal("duplicate observations failed to refresh pressure")
	}
	if strings.Count(logs.String(), "Worker Pod cannot be scheduled") != 1 {
		t.Fatalf("duplicate warnings: %s", logs.String())
	}
	p.Status.Conditions[0].Message = "had untolerated taint"
	o.OnPodUpdated(context.Background(), p)
	if strings.Count(logs.String(), "Worker Pod cannot be scheduled") != 2 {
		t.Fatal("changed scheduling reason was not logged")
	}
}

func TestConcurrentHooksAndReaders(t *testing.T) {
	o := NewObserver(slog.New(slog.NewTextHandler(io.Discard, nil)))
	p := pendingPod("Insufficient cpu")
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 50 {
				o.OnPodCreated(context.Background(), p, quotaError())
				o.OnPodUpdated(context.Background(), p)
				o.ResourcePressure()
				o.SchedulingBlocked()
				o.OnPodCreated(context.Background(), p, nil)
			}
		}()
	}
	wg.Wait()
	if !o.ResourcePressure() || !o.SchedulingBlocked() {
		t.Fatal("lost scheduling evidence")
	}
}
