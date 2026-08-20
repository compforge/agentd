// Package k8s provides the Kubernetes substrate used by the agentd control plane.
package k8s

import (
	"context"
	"fmt"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type Config struct {
	Namespace      string
	LabelSelector  string
	RequestTimeout time.Duration
	QPS            float32
	Burst          int
}

type PodSnapshot struct {
	ID    string
	Name  string
	IP    string
	Ready bool
}

type Client struct {
	client        kubernetes.Interface
	namespace     string
	labelSelector string
}

func NewInCluster(config Config) (*Client, error) {
	restConfig, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("load in-cluster Kubernetes config: %w", err)
	}
	restConfig.Timeout = config.RequestTimeout
	restConfig.QPS = config.QPS
	restConfig.Burst = config.Burst
	client, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("create Kubernetes client: %w", err)
	}
	return New(client, config)
}

func New(client kubernetes.Interface, config Config) (*Client, error) {
	if client == nil {
		return nil, fmt.Errorf("create Kubernetes substrate: client is required")
	}
	if config.Namespace == "" {
		return nil, fmt.Errorf("create Kubernetes substrate: namespace is required")
	}
	if config.LabelSelector == "" {
		return nil, fmt.Errorf("create Kubernetes substrate: label selector is required")
	}
	if config.RequestTimeout <= 0 {
		return nil, fmt.Errorf("create Kubernetes substrate: request timeout must be positive")
	}
	if config.QPS <= 0 || config.Burst <= 0 {
		return nil, fmt.Errorf("create Kubernetes substrate: QPS and burst must be positive")
	}
	return &Client{client: client, namespace: config.Namespace, labelSelector: config.LabelSelector}, nil
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
		pod := &pods.Items[i]
		if pod.UID == "" {
			return nil, fmt.Errorf("observe Agentlet Pod %q: UID is empty", pod.Name)
		}
		snapshots = append(snapshots, PodSnapshot{
			ID: string(pod.UID), Name: pod.Name, IP: pod.Status.PodIP,
			Ready: pod.DeletionTimestamp == nil && podReady(pod),
		})
	}
	sort.Slice(snapshots, func(i, j int) bool { return snapshots[i].ID < snapshots[j].ID })
	return snapshots, nil
}

func podReady(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}
