package k8s

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

func TestListAgentletPods(t *testing.T) {
	client := fake.NewClientset(
		pod("worker-b", "uid-b", "10.0.0.2", true, map[string]string{
			"app": "agentlet", ManagedLabel: "true", WorkerIDLabel: "worker-b",
		}),
		pod("worker-a", "uid-a", "10.0.0.1", false, map[string]string{
			"app": "agentlet", ManagedLabel: "true", WorkerIDLabel: "worker-a",
		}),
		pod("other", "uid-other", "10.0.0.3", true, map[string]string{"app": "other"}),
	)
	substrate, err := New(client, Config{
		Namespace: "default", LabelSelector: "app=agentlet",
		RequestTimeout: time.Second, QPS: 5, Burst: 10,
	})
	if err != nil {
		t.Fatal(err)
	}

	snapshots, err := substrate.ListAgentletPods(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshots) != 2 || snapshots[0].ID != "worker-a" || snapshots[0].UID != "uid-a" || snapshots[0].Ready ||
		snapshots[1].ID != "worker-b" || snapshots[1].UID != "uid-b" || !snapshots[1].Ready {
		t.Fatalf("ListAgentletPods() = %+v", snapshots)
	}
}

func pod(name, uid, ip string, ready bool, labels map[string]string) *corev1.Pod {
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", UID: types.UID(uid), Labels: labels},
		Status: corev1.PodStatus{PodIP: ip, Conditions: []corev1.PodCondition{{
			Type: corev1.PodReady, Status: status,
		}}},
	}
}
