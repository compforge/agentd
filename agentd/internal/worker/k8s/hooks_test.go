package k8s

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/compforge/agentd/agentd/internal/worker/cluster"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestEnsureReportsActualAdmissionResult(t *testing.T) {
	ctx := context.Background()
	observer := cluster.NewObserver(slog.New(slog.NewTextHandler(io.Discard, nil)))
	client := fake.NewClientset()
	rejected := true
	client.PrependReactor("create", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
		if rejected {
			return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "worker", fmt.Errorf("exceeded quota: memory"))
		}
		return false, nil, nil
	})
	substrate, err := New(client, Config{Namespace: "default", RequestTimeout: time.Second, QPS: 5, Burst: 10, PodHooks: observer})
	if err != nil {
		t.Fatal(err)
	}
	template := corev1.PodTemplateSpec{}
	if err := substrate.EnsureWorkerPod(ctx, "worker", "worker", template); err == nil || !observer.ResourcePressure() {
		t.Fatalf("quota result was not observed: %v", err)
	}
	rejected = false
	if err := substrate.EnsureWorkerPod(ctx, "worker", "worker", template); err != nil || observer.ResourcePressure() {
		t.Fatalf("successful admission did not clear quota: %v", err)
	}
	// Merely GETting a pre-existing Pod is not a new successful admission.
	observer.OnPodCreated(ctx, &corev1.Pod{}, apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "other", fmt.Errorf("exceeded quota: memory")))
	if err := substrate.EnsureWorkerPod(ctx, "worker", "worker", template); err != nil || !observer.ResourcePressure() {
		t.Fatalf("idempotent Ensure cleared quota: %v", err)
	}
}

func TestInformerAndLiveListFeedSchedulingHooks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := pod("worker", "uid", "", false, map[string]string{ManagedLabel: "true", WorkerIDLabel: "worker"})
	p.Status.Phase = corev1.PodPending
	p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: corev1.PodReasonUnschedulable, Message: "untolerated taint"}}
	client := fake.NewClientset(p)
	observer := cluster.NewObserver(slog.New(slog.NewTextHandler(io.Discard, nil)))
	substrate, err := New(client, Config{Namespace: "default", RequestTimeout: time.Second, QPS: 5, Burst: 10, PodHooks: observer})
	if err != nil {
		t.Fatal(err)
	}
	informer := substrate.NewAgentletPodInformer()
	notifications := make(chan struct{}, 10)
	if err := informer.Start(ctx, func() {
		select {
		case notifications <- struct{}{}:
		default:
		}
	}); err != nil {
		t.Fatal(err)
	}
	waitForInformerEvent(t, notifications)
	if !observer.SchedulingBlocked() || observer.ResourcePressure() {
		t.Fatal("initial informer ADD did not observe scheduling blockage")
	}
	p.Status.Conditions[0].Message = "Insufficient cpu"
	if _, err := client.CoreV1().Pods("default").UpdateStatus(ctx, p, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	waitForInformerEvent(t, notifications)
	if !observer.ResourcePressure() {
		t.Fatal("informer UPDATE did not report pressure")
	}
	// A separate observer without a watch receives the same fact from the
	// existing capacity planner's live LIST. No cluster-scoped watch is needed.
	listObserver := cluster.NewObserver(slog.New(slog.NewTextHandler(io.Discard, nil)))
	listClient, err := New(client, Config{Namespace: "default", RequestTimeout: time.Second, QPS: 5, Burst: 10, PodHooks: listObserver})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := listClient.ListAgentletPods(ctx); err != nil {
		t.Fatal(err)
	}
	if !listObserver.ResourcePressure() {
		t.Fatal("live LIST did not feed scheduling hook")
	}
}

func TestSnapshotRejectsStaleSchedulingFailureAfterBinding(t *testing.T) {
	p := pod("worker", "uid", "", false, map[string]string{ManagedLabel: "true", WorkerIDLabel: "worker"})
	p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: "SchedulerError"}}
	if !snapshotFromPod(p).Unschedulable {
		t.Fatal("explicit scheduling failure not projected")
	}
	p.Spec.NodeName = "node"
	if snapshotFromPod(p).Unschedulable {
		t.Fatal("bound Pod retained stale scheduling failure")
	}
}
