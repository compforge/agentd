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

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/compforge/case-harness/go/e2e/caserun"
	"github.com/compforge/case-harness/go/kube"
)

const controlPlaneComponentLabel = "app.kubernetes.io/component"

func TestManagedAgentRecoversAfterControlPlaneRestartBeforeDispatch(t *testing.T) {
	state := controlPlaneRestartCaseState{}
	result := caserun.Run(
		context.Background(),
		caserun.Ref("agentd-managed-agent", "control_plane_restart_before_dispatch"),
		nil,
		&state,
		caserun.Definition[controlPlaneRestartCaseState]{
			Prepare: prepareControlPlaneRestartCase,
			Execute: executeControlPlaneRestartBeforeDispatchCase,
			Judge: func(_ context.Context, state *controlPlaneRestartCaseState) error {
				if state.userMessages != 1 || state.agentMessages != 1 {
					return caserun.Fail(fmt.Sprintf(
						"message counts = user:%d agent:%d, want user:1 agent:1",
						state.userMessages, state.agentMessages,
					))
				}
				if !state.controlPlaneReplaced {
					return caserun.Fail("Control Plane Pod was not replaced")
				}
				return nil
			},
			Cleanup: verifyControlPlaneRestartCleanup,
			Budgets: systemCaseBudgets,
			Facets: map[string]string{
				"boundary": "system", "recovery": "control-plane-restart", "fault_window": "before-dispatch",
			},
		},
	)
	recordSystemCase(t, result)
}

func TestManagedAgentContinuesDuringControlPlaneRestart(t *testing.T) {
	state := controlPlaneRestartCaseState{duringTurn: true}
	result := caserun.Run(
		context.Background(),
		caserun.Ref("agentd-managed-agent", "control_plane_restart_during_turn"),
		nil,
		&state,
		caserun.Definition[controlPlaneRestartCaseState]{
			Prepare: prepareControlPlaneRestartCase,
			Execute: executeControlPlaneRestartDuringTurnCase,
			Judge: func(_ context.Context, state *controlPlaneRestartCaseState) error {
				if state.userMessages != 1 || state.agentMessages != 1 {
					return caserun.Fail(fmt.Sprintf(
						"message counts = user:%d agent:%d, want user:1 agent:1",
						state.userMessages, state.agentMessages,
					))
				}
				if !state.controlPlaneReplaced {
					return caserun.Fail("Control Plane Pod was not replaced")
				}
				if !state.workerSetStable {
					return caserun.Fail("Worker Pods changed while only the Control Plane was restarted")
				}
				return nil
			},
			Cleanup: verifyControlPlaneRestartCleanup,
			Budgets: systemCaseBudgets,
			Facets: map[string]string{
				"boundary": "system", "recovery": "control-plane-restart", "fault_window": "during-turn",
			},
		},
	)
	recordSystemCase(t, result)
}

type controlPlaneRestartCaseState struct {
	managed              managedAgentCaseState
	cluster              *kube.Client
	controlPlaneSelector string
	workerSelector       string
	pollInterval         time.Duration
	duringTurn           bool
	controlPlaneReplaced bool
	workerSetStable      bool
	userMessages         int
	agentMessages        int
}

func prepareControlPlaneRestartCase(ctx context.Context, state *controlPlaneRestartCaseState) error {
	disruption, err := readControlPlaneDisruptionEnv()
	if err != nil {
		return err
	}
	systemPrompt := sandboxResumeSystemPrompt
	if state.duringTurn {
		systemPrompt = midTurnSystemPrompt
		if strings.TrimSpace(disruption.workerSelector) == "" {
			return errors.New("AGENTD_E2E_WORKER_SELECTOR is required for during-Turn Control Plane restart")
		}
	}
	if err := prepareManagedAgentCaseWithSystem(ctx, &state.managed, systemPrompt); err != nil {
		return err
	}
	state.cluster = disruption.cluster
	state.controlPlaneSelector = disruption.controlPlaneSelector
	state.workerSelector = disruption.workerSelector
	state.pollInterval = disruption.pollInterval
	return nil
}

func executeControlPlaneRestartBeforeDispatchCase(
	ctx context.Context,
	state *controlPlaneRestartCaseState,
) error {
	ctx, cancel := context.WithTimeout(ctx, state.managed.config.timeout)
	defer cancel()

	if err := sendTurnResult(
		ctx,
		state.managed.client,
		state.managed.sessionID,
		"Run the sandbox check after durable ingress survives a Control Plane restart.",
	); err != nil {
		return err
	}
	if err := restartControlPlaneResult(ctx, state); err != nil {
		return err
	}
	if err := waitForTurnResult(
		ctx,
		state.managed.client,
		state.managed.sessionID,
		"Control Plane restart before dispatch",
		0,
	); err != nil {
		return err
	}
	var err error
	state.userMessages, state.agentMessages, err = managedMessageCountsResult(
		ctx, state.managed.client, state.managed.sessionID,
	)
	return err
}

func executeControlPlaneRestartDuringTurnCase(
	ctx context.Context,
	state *controlPlaneRestartCaseState,
) error {
	ctx, cancel := context.WithTimeout(ctx, state.managed.config.timeout)
	defer cancel()

	if err := sendTurnResult(
		ctx,
		state.managed.client,
		state.managed.sessionID,
		"Run the long sandbox operation while the Control Plane restarts.",
	); err != nil {
		return err
	}
	if err := waitForRunningSessionResult(ctx, state.managed.client, state.managed.sessionID); err != nil {
		return err
	}
	beforeWorkers, beforeWorkerIDs, err := managedWorkersResult(
		ctx, state.cluster, state.workerSelector,
	)
	if err != nil {
		return err
	}
	if err := restartControlPlaneResult(ctx, state); err != nil {
		return err
	}
	if err := waitForTurnResult(
		ctx,
		state.managed.client,
		state.managed.sessionID,
		"Control Plane restart during Turn",
		0,
	); err != nil {
		return err
	}
	afterWorkers, afterWorkerIDs, err := managedWorkersResult(ctx, state.cluster, state.workerSelector)
	if err != nil {
		return err
	}
	state.workerSetStable = samePodSet(beforeWorkers, afterWorkers) && sameStringSet(beforeWorkerIDs, afterWorkerIDs)
	state.userMessages, state.agentMessages, err = managedMessageCountsResult(
		ctx, state.managed.client, state.managed.sessionID,
	)
	return err
}

func restartControlPlaneResult(ctx context.Context, state *controlPlaneRestartCaseState) error {
	previous, err := controlPlanePodsResult(ctx, state.cluster, state.controlPlaneSelector)
	if err != nil {
		return err
	}
	if err := state.cluster.ForceDeletePod(ctx, previous[0].Ref()); err != nil {
		return fmt.Errorf("force-delete Control Plane Pod %q: %w", previous[0].Name, err)
	}
	replacement, err := state.cluster.WaitReplacement(
		ctx, state.controlPlaneSelector, previous, state.pollInterval,
	)
	if err != nil {
		return fmt.Errorf("wait for replacement Control Plane Pod: %w", err)
	}
	replacement, err = state.cluster.WaitReady(ctx, replacement.Ref(), state.pollInterval)
	if err != nil {
		return fmt.Errorf("wait for replacement Control Plane Pod %q to become ready: %w", replacement.Name, err)
	}
	if replacement.Labels[controlPlaneComponentLabel] != "control-plane" {
		return fmt.Errorf("replacement Pod %q is not a Control Plane", replacement.Name)
	}
	if err := waitForManagedAPIResult(ctx, state.managed.client, state.managed.sessionID, state.pollInterval); err != nil {
		return err
	}
	state.controlPlaneReplaced = replacement.UID != previous[0].UID
	return nil
}

func controlPlanePodsResult(ctx context.Context, cluster *kube.Client, selector string) ([]kube.Pod, error) {
	pods, err := cluster.ListPods(ctx, selector)
	if err != nil {
		return nil, fmt.Errorf("list Control Plane Pods: %w", err)
	}
	if len(pods) != 1 {
		return nil, fmt.Errorf("Control Plane selector matched %d Pods, want exactly 1 for restart E2E", len(pods))
	}
	if !pods[0].Ready || pods[0].Deleting {
		return nil, fmt.Errorf("Control Plane Pod %q is not steadily Ready", pods[0].Name)
	}
	if pods[0].Labels[controlPlaneComponentLabel] != "control-plane" {
		return nil, fmt.Errorf(
			"Control Plane selector matched Pod %q without %s=control-plane; refusing disruption",
			pods[0].Name,
			controlPlaneComponentLabel,
		)
	}
	return pods, nil
}

func waitForManagedAPIResult(
	ctx context.Context,
	client *anthropic.Client,
	sessionID string,
	interval time.Duration,
) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if _, err := client.Beta.Sessions.Get(ctx, sessionID, anthropic.BetaSessionGetParams{}); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for Managed Agents API after Control Plane restart: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func verifyControlPlaneRestartCleanup(ctx context.Context, state *controlPlaneRestartCaseState) error {
	if state.cluster == nil || state.controlPlaneSelector == "" {
		return nil
	}
	_, err := controlPlanePodsResult(ctx, state.cluster, state.controlPlaneSelector)
	if err != nil {
		return fmt.Errorf("verify Control Plane recovery: %w", err)
	}
	return nil
}

func samePodSet(left, right []kube.Pod) bool {
	if len(left) != len(right) {
		return false
	}
	values := make(map[string]struct{}, len(left))
	for _, pod := range left {
		values[string(pod.UID)] = struct{}{}
	}
	for _, pod := range right {
		if _, exists := values[string(pod.UID)]; !exists {
			return false
		}
	}
	return true
}

func sameStringSet(left, right map[string]struct{}) bool {
	if len(left) != len(right) {
		return false
	}
	for value := range left {
		if _, exists := right[value]; !exists {
			return false
		}
	}
	return true
}

type controlPlaneDisruptionConfig struct {
	cluster              *kube.Client
	controlPlaneSelector string
	workerSelector       string
	pollInterval         time.Duration
}

func readControlPlaneDisruptionEnv() (controlPlaneDisruptionConfig, error) {
	if os.Getenv("AGENTD_E2E_ALLOW_CONTROL_PLANE_DISRUPTION") != "1" {
		return controlPlaneDisruptionConfig{}, caserun.Skip(
			"AGENTD_E2E_ALLOW_CONTROL_PLANE_DISRUPTION is not 1; skipping Control Plane restart e2e",
		)
	}
	return readControlPlaneKubernetesEnv()
}

func readControlPlaneKubernetesEnv() (controlPlaneDisruptionConfig, error) {
	namespace := strings.TrimSpace(os.Getenv("AGENTD_E2E_KUBE_NAMESPACE"))
	if namespace == "" {
		return controlPlaneDisruptionConfig{}, errors.New(
			"AGENTD_E2E_KUBE_NAMESPACE is required for Control Plane disruption",
		)
	}
	selector := strings.TrimSpace(os.Getenv("AGENTD_E2E_CONTROL_PLANE_SELECTOR"))
	if selector == "" {
		return controlPlaneDisruptionConfig{}, errors.New(
			"AGENTD_E2E_CONTROL_PLANE_SELECTOR is required for Control Plane disruption",
		)
	}
	cluster, err := kubernetesClientResult(namespace)
	if err != nil {
		return controlPlaneDisruptionConfig{}, err
	}
	return controlPlaneDisruptionConfig{
		cluster: cluster, controlPlaneSelector: selector,
		workerSelector: strings.TrimSpace(os.Getenv("AGENTD_E2E_WORKER_SELECTOR")),
		pollInterval:   500 * time.Millisecond,
	}, nil
}
