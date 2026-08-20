package observer

import (
	"context"
	"fmt"
	"net"
	"strconv"

	"github.com/compforge/agentd/agentd/internal/k8s"
)

type WorkerSnapshot struct {
	ID            string
	PodUID        string
	Name          string
	Endpoint      string
	PodPhase      string
	Ready         bool
	Unschedulable bool
	Capacity      int
}

type Source interface {
	ListWorkers(context.Context) ([]WorkerSnapshot, error)
}

type podSource interface {
	ListAgentletPods(context.Context) ([]k8s.PodSnapshot, error)
}

type KubernetesSource struct {
	pods     podSource
	port     int
	capacity int
}

func NewKubernetesSource(pods podSource, port, capacity int) (*KubernetesSource, error) {
	if pods == nil {
		return nil, fmt.Errorf("create Kubernetes Worker source: Pod source is required")
	}
	if port <= 0 || port > 65535 {
		return nil, fmt.Errorf("create Kubernetes Worker source: port must be between 1 and 65535")
	}
	if capacity <= 0 {
		return nil, fmt.Errorf("create Kubernetes Worker source: capacity must be positive")
	}
	return &KubernetesSource{pods: pods, port: port, capacity: capacity}, nil
}

func (s *KubernetesSource) ListWorkers(ctx context.Context) ([]WorkerSnapshot, error) {
	pods, err := s.pods.ListAgentletPods(ctx)
	if err != nil {
		return nil, err
	}
	workers := make([]WorkerSnapshot, 0, len(pods))
	for _, pod := range pods {
		if pod.ID == "" {
			continue
		}
		endpoint := ""
		if pod.IP != "" {
			endpoint = "http://" + net.JoinHostPort(pod.IP, strconv.Itoa(s.port))
		}
		workers = append(workers, WorkerSnapshot{
			ID: pod.ID, PodUID: pod.UID, Name: pod.Name, Endpoint: endpoint,
			PodPhase: string(pod.Phase), Ready: pod.Ready && endpoint != "",
			Unschedulable: pod.Unschedulable, Capacity: s.capacity,
		})
	}
	return workers, nil
}
