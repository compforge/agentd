//go:build e2e

package e2e_test

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/compforge/case-harness/go/kube"
)

func kubernetesClientResult(namespace string) (*kube.Client, error) {
	options := kube.Options{
		Namespace: namespace, RequestTimeout: 10 * time.Second, QPS: 5, Burst: 10,
	}
	if os.Getenv("AGENTD_E2E_KUBE_IN_CLUSTER") == "1" {
		if strings.TrimSpace(os.Getenv("AGENTD_E2E_KUBECONFIG")) != "" {
			return nil, errors.New(
				"AGENTD_E2E_KUBECONFIG and AGENTD_E2E_KUBE_IN_CLUSTER=1 are mutually exclusive",
			)
		}
		client, err := kube.InCluster(options)
		if err != nil {
			return nil, fmt.Errorf("create in-cluster Kubernetes E2E client: %w", err)
		}
		return client, nil
	}
	kubeconfig := strings.TrimSpace(os.Getenv("AGENTD_E2E_KUBECONFIG"))
	if kubeconfig == "" {
		return nil, errors.New("AGENTD_E2E_KUBECONFIG is required unless AGENTD_E2E_KUBE_IN_CLUSTER=1")
	}
	client, err := kube.FromKubeconfig(
		kubeconfig,
		strings.TrimSpace(os.Getenv("AGENTD_E2E_KUBE_CONTEXT")),
		options,
	)
	if err != nil {
		return nil, fmt.Errorf("create Kubernetes E2E client: %w", err)
	}
	return client, nil
}
