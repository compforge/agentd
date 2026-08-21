//go:build e2e

package e2e_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/compforge/case-harness/go/e2e/caserun"
	"github.com/compforge/case-harness/go/kube"
)

const (
	managedWorkerLabel = "agentd.compforge.dev/managed"
	workerIDLabel      = "agentd.compforge.dev/worker-id"
)

func TestManagedAgentResumesAfterWorkerReplacement(t *testing.T) {
	state := workerReplacementCaseState{}
	result := caserun.Run(
		context.Background(),
		caserun.Ref("agentd-managed-agent", "worker_replacement_resume"),
		nil,
		&state,
		caserun.Definition[workerReplacementCaseState]{
			Prepare: prepareWorkerReplacementCase,
			Execute: executeWorkerReplacementCase,
			Judge: func(_ context.Context, state *workerReplacementCaseState) error {
				if state.managed.agentMessages < 2 {
					return caserun.Fail(fmt.Sprintf(
						"agent messages = %d, want at least 2",
						state.managed.agentMessages,
					))
				}
				if _, reused := state.previousWorkerIDs[state.replacementWorkerID]; reused {
					return caserun.Fail(fmt.Sprintf(
						"replacement Pod reused old logical Worker %q",
						state.replacementWorkerID,
					))
				}
				return nil
			},
			Budgets: systemCaseBudgets,
			Facets:  map[string]string{"boundary": "system", "recovery": "worker-replacement"},
		},
	)
	recordSystemCase(t, result)
}

type workerReplacementCaseState struct {
	managed             managedAgentCaseState
	cluster             *kube.Client
	workerSelector      string
	pollInterval        time.Duration
	previousWorkerIDs   map[string]struct{}
	replacementWorkerID string
}

func prepareWorkerReplacementCase(ctx context.Context, state *workerReplacementCaseState) error {
	disruption, err := readWorkerDisruptionEnv()
	if err != nil {
		return err
	}
	if err := prepareManagedAgentCase(ctx, &state.managed); err != nil {
		return err
	}
	state.cluster = disruption.cluster
	state.workerSelector = disruption.workerSelector
	state.pollInterval = disruption.pollInterval
	return nil
}

func executeWorkerReplacementCase(ctx context.Context, state *workerReplacementCaseState) error {
	ctx, cancel := context.WithTimeout(ctx, state.managed.config.timeout)
	defer cancel()
	if err := runTurnResult(
		ctx,
		state.managed.client,
		state.managed.sessionID,
		"Run the required sandbox check before Worker replacement.",
	); err != nil {
		return err
	}

	previous, workerIDs, err := managedWorkersResult(ctx, state.cluster, state.workerSelector)
	if err != nil {
		return err
	}
	state.previousWorkerIDs = workerIDs
	for _, pod := range previous {
		if err := state.cluster.DeletePod(ctx, pod.Ref()); err != nil {
			return fmt.Errorf("delete Worker Pod %q: %w", pod.Name, err)
		}
	}

	replacement, err := state.cluster.WaitReplacement(ctx, state.workerSelector, previous, state.pollInterval)
	if err != nil {
		return fmt.Errorf("wait for replacement Worker Pod: %w", err)
	}
	replacement, err = state.cluster.WaitReady(ctx, replacement.Ref(), state.pollInterval)
	if err != nil {
		return fmt.Errorf("wait for replacement Worker Pod %q to become ready: %w", replacement.Name, err)
	}
	if replacement.Labels[managedWorkerLabel] != "true" {
		return fmt.Errorf("replacement Pod %q is not an agentd-managed Worker", replacement.Name)
	}
	state.replacementWorkerID = strings.TrimSpace(replacement.Labels[workerIDLabel])
	if state.replacementWorkerID == "" {
		return fmt.Errorf("replacement Pod %q has no %s label", replacement.Name, workerIDLabel)
	}

	if err := runTurnResult(
		ctx,
		state.managed.client,
		state.managed.sessionID,
		"Run the required sandbox check after Worker replacement.",
	); err != nil {
		return err
	}
	messages, err := agentMessagesResult(ctx, state.managed.client, state.managed.sessionID)
	state.managed.agentMessages = len(messages)
	return err
}

func managedWorkersResult(
	ctx context.Context,
	cluster *kube.Client,
	selector string,
) ([]kube.Pod, map[string]struct{}, error) {
	pods, err := cluster.ListPods(ctx, selector)
	if err != nil {
		return nil, nil, fmt.Errorf("list Worker Pods: %w", err)
	}
	if len(pods) == 0 {
		return nil, nil, errors.New("worker selector matched no Pods after the first turn")
	}
	workerIDs := make(map[string]struct{}, len(pods))
	for _, pod := range pods {
		if pod.Labels[managedWorkerLabel] != "true" {
			return nil, nil, fmt.Errorf(
				"worker selector matched unmanaged Pod %q; refusing disruption",
				pod.Name,
			)
		}
		workerID := strings.TrimSpace(pod.Labels[workerIDLabel])
		if workerID == "" {
			return nil, nil, fmt.Errorf(
				"worker selector matched Pod %q without %s; refusing disruption",
				pod.Name,
				workerIDLabel,
			)
		}
		workerIDs[workerID] = struct{}{}
	}
	return pods, workerIDs, nil
}

type workerDisruptionConfig struct {
	cluster        *kube.Client
	workerSelector string
	pollInterval   time.Duration
}

func readWorkerDisruptionEnv() (workerDisruptionConfig, error) {
	if os.Getenv("AGENTD_E2E_ALLOW_WORKER_DISRUPTION") != "1" {
		return workerDisruptionConfig{}, caserun.Skip(
			"AGENTD_E2E_ALLOW_WORKER_DISRUPTION is not 1; skipping disruptive Worker replacement e2e",
		)
	}
	namespace := strings.TrimSpace(os.Getenv("AGENTD_E2E_KUBE_NAMESPACE"))
	if namespace == "" {
		return workerDisruptionConfig{}, errors.New("AGENTD_E2E_KUBE_NAMESPACE is required for Worker disruption")
	}
	selector := strings.TrimSpace(os.Getenv("AGENTD_E2E_WORKER_SELECTOR"))
	if selector == "" {
		return workerDisruptionConfig{}, errors.New("AGENTD_E2E_WORKER_SELECTOR is required for Worker disruption")
	}

	options := kube.Options{
		Namespace:      namespace,
		RequestTimeout: 10 * time.Second,
		QPS:            5,
		Burst:          10,
	}
	var (
		cluster *kube.Client
		err     error
	)
	if os.Getenv("AGENTD_E2E_KUBE_IN_CLUSTER") == "1" {
		if strings.TrimSpace(os.Getenv("AGENTD_E2E_KUBECONFIG")) != "" {
			return workerDisruptionConfig{}, errors.New(
				"AGENTD_E2E_KUBECONFIG and AGENTD_E2E_KUBE_IN_CLUSTER=1 are mutually exclusive",
			)
		}
		cluster, err = kube.InCluster(options)
	} else {
		kubeconfig := strings.TrimSpace(os.Getenv("AGENTD_E2E_KUBECONFIG"))
		if kubeconfig == "" {
			return workerDisruptionConfig{}, errors.New(
				"AGENTD_E2E_KUBECONFIG is required unless AGENTD_E2E_KUBE_IN_CLUSTER=1",
			)
		}
		cluster, err = kube.FromKubeconfig(
			kubeconfig,
			strings.TrimSpace(os.Getenv("AGENTD_E2E_KUBE_CONTEXT")),
			options,
		)
	}
	if err != nil {
		return workerDisruptionConfig{}, fmt.Errorf("create Kubernetes E2E client: %w", err)
	}
	return workerDisruptionConfig{
		cluster: cluster, workerSelector: selector, pollInterval: 500 * time.Millisecond,
	}, nil
}
