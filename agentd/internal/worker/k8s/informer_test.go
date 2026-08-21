package k8s

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestPodInformerCachesAndSignalsManagedPods(t *testing.T) {
	client := fake.NewClientset(
		pod("worker-a", "uid-a", "10.0.0.1", false, map[string]string{
			"app": "agentlet", ManagedLabel: "true", WorkerIDLabel: "worker-a",
		}),
		pod("other", "uid-other", "10.0.0.2", true, map[string]string{"app": "other"}),
	)
	substrate, err := New(client, Config{
		Namespace: "default", LabelSelector: "app=agentlet",
		RequestTimeout: time.Second, QPS: 5, Burst: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	informer := substrate.NewAgentletPodInformer()
	notifications := make(chan struct{}, 10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := informer.Start(ctx, func() {
		select {
		case notifications <- struct{}{}:
		default:
		}
	}); err != nil {
		t.Fatal(err)
	}

	assertInformerWorkers(t, informer, []string{"worker-a"})
	drain(notifications)
	created := pod("worker-b", "uid-b", "10.0.0.3", true, map[string]string{
		"app": "agentlet", ManagedLabel: "true", WorkerIDLabel: "worker-b",
	})
	if _, err := client.CoreV1().Pods("default").Create(ctx, created, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	waitForInformerEvent(t, notifications)
	assertInformerWorkers(t, informer, []string{"worker-a", "worker-b"})

	if err := client.CoreV1().Pods("default").Delete(ctx, "worker-a", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	waitForInformerEvent(t, notifications)
	assertInformerWorkers(t, informer, []string{"worker-b"})
}

func assertInformerWorkers(t *testing.T, informer *PodInformer, want []string) {
	t.Helper()
	snapshots, err := informer.ListAgentletPods(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got := make([]string, 0, len(snapshots))
	for _, snapshot := range snapshots {
		got = append(got, snapshot.ID)
	}
	if len(got) != len(want) {
		t.Fatalf("informer Workers = %v, want %v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("informer Workers = %v, want %v", got, want)
		}
	}
}

func waitForInformerEvent(t *testing.T, notifications <-chan struct{}) {
	t.Helper()
	select {
	case <-notifications:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Pod informer event")
	}
}

func drain(notifications <-chan struct{}) {
	for {
		select {
		case <-notifications:
		default:
			return
		}
	}
}
