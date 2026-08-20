package k8s

import (
	"context"
	"fmt"

	"github.com/compforge/agentd/agentd/internal/model"
	corev1 "k8s.io/api/core/v1"
)

type WorkerProvisioner struct {
	client   *Client
	template corev1.PodTemplateSpec
}

func NewWorkerProvisioner(client *Client, template corev1.PodTemplateSpec) (*WorkerProvisioner, error) {
	if client == nil {
		return nil, fmt.Errorf("create Kubernetes Worker Provisioner: client is required")
	}
	if len(template.Spec.Containers) == 0 {
		return nil, fmt.Errorf("create Kubernetes Worker Provisioner: Pod template requires containers")
	}
	return &WorkerProvisioner{client: client, template: template}, nil
}

func (p *WorkerProvisioner) Ensure(ctx context.Context, worker model.Worker) error {
	return p.client.EnsureWorkerPod(ctx, worker.ID, worker.Name, p.template)
}

func (p *WorkerProvisioner) Destroy(ctx context.Context, worker model.Worker) error {
	return p.client.DeleteWorkerPod(ctx, worker.Name)
}
