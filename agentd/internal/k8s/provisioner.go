package k8s

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/compforge/agentd/agentd/internal/model"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"
)

type WorkerProvisioner struct {
	client   *Client
	template corev1.PodTemplateSpec
}

func LoadPodTemplate(path string) (corev1.PodTemplateSpec, error) {
	if strings.TrimSpace(path) == "" {
		return corev1.PodTemplateSpec{}, fmt.Errorf("load Worker Pod template: path is required")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return corev1.PodTemplateSpec{}, fmt.Errorf("load Worker Pod template %q: %w", path, err)
	}
	var template corev1.PodTemplateSpec
	if err := yaml.UnmarshalStrict(raw, &template); err != nil {
		return corev1.PodTemplateSpec{}, fmt.Errorf("decode Worker Pod template %q: %w", path, err)
	}
	return template, nil
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
