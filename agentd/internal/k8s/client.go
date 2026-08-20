// Package k8s provides the Kubernetes substrate used by the agentd control plane.
package k8s

import (
	"fmt"
	"strings"
	"time"

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
	if config.RequestTimeout <= 0 {
		return nil, fmt.Errorf("create Kubernetes substrate: request timeout must be positive")
	}
	if config.QPS <= 0 || config.Burst <= 0 {
		return nil, fmt.Errorf("create Kubernetes substrate: QPS and burst must be positive")
	}
	selector := ManagedLabel + "=true"
	if strings.TrimSpace(config.LabelSelector) != "" {
		selector += "," + config.LabelSelector
	}
	return &Client{client: client, namespace: config.Namespace, labelSelector: selector}, nil
}
