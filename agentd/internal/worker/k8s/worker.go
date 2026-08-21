package k8s

import (
	"context"
	"fmt"
	"sort"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	ManagedLabel  = "agentd.compforge.dev/managed"
	WorkerIDLabel = "agentd.compforge.dev/worker-id"
)

type PodSnapshot struct {
	ID            string
	UID           string
	Name          string
	Managed       bool
	IP            string
	Phase         corev1.PodPhase
	Ready         bool
	Unschedulable bool
}

func (c *Client) ListAgentletPods(ctx context.Context) ([]PodSnapshot, error) {
	pods, err := c.client.CoreV1().Pods(c.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: c.labelSelector,
	})
	if err != nil {
		return nil, fmt.Errorf("list Agentlet Pods in namespace %q: %w", c.namespace, err)
	}
	snapshots := make([]PodSnapshot, 0, len(pods.Items))
	for i := range pods.Items {
		snapshots = append(snapshots, snapshotFromPod(&pods.Items[i]))
	}
	sortPodSnapshots(snapshots)
	return snapshots, nil
}

func snapshotFromPod(pod *corev1.Pod) PodSnapshot {
	return PodSnapshot{
		ID: pod.Labels[WorkerIDLabel], UID: string(pod.UID), Name: pod.Name, IP: pod.Status.PodIP,
		Managed: pod.Labels[ManagedLabel] == "true",
		Phase:   pod.Status.Phase, Ready: pod.DeletionTimestamp == nil && podReady(pod),
		Unschedulable: podUnschedulable(pod),
	}
}

func sortPodSnapshots(snapshots []PodSnapshot) {
	sort.Slice(snapshots, func(i, j int) bool {
		if snapshots[i].ID == snapshots[j].ID {
			return snapshots[i].Name < snapshots[j].Name
		}
		return snapshots[i].ID < snapshots[j].ID
	})
}

func (c *Client) EnsureWorkerPod(
	ctx context.Context,
	workerID string,
	name string,
	template corev1.PodTemplateSpec,
) error {
	pods := c.client.CoreV1().Pods(c.namespace)
	existing, err := pods.Get(ctx, name, metav1.GetOptions{})
	if err == nil {
		return validateWorkerPodOwnership(existing, workerID)
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("get Worker Pod %q: %w", name, err)
	}
	pod := &corev1.Pod{ObjectMeta: *template.ObjectMeta.DeepCopy(), Spec: *template.Spec.DeepCopy()}
	pod.Name = name
	pod.Namespace = c.namespace
	if pod.Labels == nil {
		pod.Labels = map[string]string{}
	}
	pod.Labels[ManagedLabel] = "true"
	pod.Labels[WorkerIDLabel] = workerID
	created, err := pods.Create(ctx, pod, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		existing, getErr := pods.Get(ctx, name, metav1.GetOptions{})
		if getErr != nil {
			return fmt.Errorf("read concurrently created Worker Pod %q: %w", name, getErr)
		}
		return validateWorkerPodOwnership(existing, workerID)
	}
	if err != nil {
		return fmt.Errorf("create Worker Pod %q: %w", name, err)
	}
	return validateWorkerPodOwnership(created, workerID)
}

func (c *Client) DeleteWorkerPod(ctx context.Context, name string) error {
	propagation := metav1.DeletePropagationBackground
	if err := c.client.CoreV1().Pods(c.namespace).Delete(ctx, name, metav1.DeleteOptions{
		PropagationPolicy: &propagation,
	}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete Worker Pod %q: %w", name, err)
	}
	return nil
}

func validateWorkerPodOwnership(pod *corev1.Pod, workerID string) error {
	if pod.Labels[ManagedLabel] != "true" || pod.Labels[WorkerIDLabel] != workerID {
		return fmt.Errorf("ensure Worker Pod %q: existing Pod has different ownership", pod.Name)
	}
	return nil
}

func podUnschedulable(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodScheduled {
			return condition.Status == corev1.ConditionFalse && condition.Reason == corev1.PodReasonUnschedulable
		}
	}
	return false
}

func podReady(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}
