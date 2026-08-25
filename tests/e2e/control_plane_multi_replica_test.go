//go:build e2e

package e2e_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/compforge/case-harness/go/e2e/caserun"
	"github.com/compforge/case-harness/go/kube"
)

func TestManagedAgentConvergesAcrossControlPlaneReplicaLoss(t *testing.T) {
	state := controlPlaneMultiReplicaCaseState{}
	result := caserun.Run(
		context.Background(),
		caserun.Ref("agentd-managed-agent", "control_plane_multi_replica_failover"),
		nil,
		&state,
		caserun.Definition[controlPlaneMultiReplicaCaseState]{
			Prepare: prepareControlPlaneMultiReplicaCase,
			Execute: executeControlPlaneMultiReplicaCase,
			Judge: func(_ context.Context, state *controlPlaneMultiReplicaCaseState) error {
				if state.userMessages != 3 || state.agentMessages != 3 {
					return caserun.Fail(fmt.Sprintf(
						"message counts = user:%d agent:%d, want user:3 agent:3",
						state.userMessages, state.agentMessages,
					))
				}
				if !state.acceptedWithOneReplica {
					return caserun.Fail("surviving Control Plane did not accept demand before replacement became Ready")
				}
				if !state.replacementRejoined {
					return caserun.Fail("replacement Control Plane did not rejoin the two-replica set")
				}
				if !state.workerSetStable {
					return caserun.Fail("Worker Pods changed during Control Plane-only failover")
				}
				return nil
			},
			Cleanup: verifyControlPlaneMultiReplicaCleanup,
			Budgets: systemCaseBudgets,
			Facets: map[string]string{
				"boundary": "system", "recovery": "control-plane-restart", "fault_window": "single-replica",
			},
		},
	)
	recordSystemCase(t, result)
}

type controlPlaneMultiReplicaCaseState struct {
	managed                managedAgentCaseState
	cluster                *kube.Client
	controlPlaneSelector   string
	workerSelector         string
	pollInterval           time.Duration
	initialControlPlanes   []kube.Pod
	acceptedWithOneReplica bool
	replacementRejoined    bool
	workerSetStable        bool
	userMessages           int
	agentMessages          int
}

func prepareControlPlaneMultiReplicaCase(
	ctx context.Context,
	state *controlPlaneMultiReplicaCaseState,
) error {
	if os.Getenv("AGENTD_E2E_ALLOW_CONTROL_PLANE_FAILOVER") != "1" {
		return caserun.Skip(
			"AGENTD_E2E_ALLOW_CONTROL_PLANE_FAILOVER is not 1; skipping multi-replica Control Plane e2e",
		)
	}
	disruption, err := readControlPlaneKubernetesEnv()
	if err != nil {
		return err
	}
	if disruption.workerSelector == "" {
		return errors.New("AGENTD_E2E_WORKER_SELECTOR is required for Control Plane failover")
	}
	controlPlanes, err := steadyControlPlanePodsResult(
		ctx, disruption.cluster, disruption.controlPlaneSelector, 2,
	)
	if err != nil {
		return err
	}
	state.cluster = disruption.cluster
	state.controlPlaneSelector = disruption.controlPlaneSelector
	state.workerSelector = disruption.workerSelector
	state.pollInterval = disruption.pollInterval
	state.initialControlPlanes = controlPlanes
	return prepareManagedAgentCase(ctx, &state.managed)
}

func executeControlPlaneMultiReplicaCase(
	ctx context.Context,
	state *controlPlaneMultiReplicaCaseState,
) error {
	ctx, cancel := context.WithTimeout(ctx, state.managed.config.timeout)
	defer cancel()

	if err := runTurnResult(
		ctx, state.managed.client, state.managed.sessionID,
		"Run the sandbox check while both Control Plane replicas are Ready.",
	); err != nil {
		return err
	}
	beforeWorkers, beforeWorkerIDs, err := managedWorkersResult(
		ctx, state.cluster, state.workerSelector,
	)
	if err != nil {
		return err
	}

	deleted := state.initialControlPlanes[0]
	survivor := state.initialControlPlanes[1]
	if err := state.cluster.ForceDeletePod(ctx, deleted.Ref()); err != nil {
		return fmt.Errorf("force-delete one Control Plane Pod %q: %w", deleted.Name, err)
	}
	singleReplica, err := waitForControlPlaneReadyCountResult(
		ctx, state.cluster, state.controlPlaneSelector, 1, state.pollInterval,
	)
	if err != nil {
		return err
	}
	if !containsPodUID(singleReplica, survivor) {
		return errors.New("original surviving Control Plane was not the sole Ready replica")
	}

	// A fresh client cannot reuse a connection pinned to the deleted Pod. At
	// this point the Service has exactly one Ready backend, so successful ingress
	// proves that the surviving peer can serve and reconcile without a leader.
	state.managed.client = newManagedAgentClient(state.managed.config)
	if err := waitForManagedAPIResult(
		ctx, state.managed.client, state.managed.sessionID, state.pollInterval,
	); err != nil {
		return err
	}
	if err := sendTurnResult(
		ctx, state.managed.client, state.managed.sessionID,
		"Run the sandbox check while only one Control Plane replica is Ready.",
	); err != nil {
		return err
	}
	singleReplica, err = readyControlPlanePodsResult(
		ctx, state.cluster, state.controlPlaneSelector, 1,
	)
	if err != nil {
		return fmt.Errorf("verify single-replica acceptance window: %w", err)
	}
	if !containsPodUID(singleReplica, survivor) {
		return errors.New("replacement became the serving replica during the single-replica acceptance window")
	}
	state.acceptedWithOneReplica = true
	if err := waitForTurnResult(
		ctx, state.managed.client, state.managed.sessionID, "single-replica Control Plane failover", 1,
	); err != nil {
		return err
	}

	replacement, err := state.cluster.WaitReplacement(
		ctx, state.controlPlaneSelector, state.initialControlPlanes, state.pollInterval,
	)
	if err != nil {
		return fmt.Errorf("wait for replacement Control Plane replica: %w", err)
	}
	if _, err := state.cluster.WaitReady(ctx, replacement.Ref(), state.pollInterval); err != nil {
		return fmt.Errorf("wait for replacement Control Plane replica %q to become Ready: %w", replacement.Name, err)
	}
	finalControlPlanes, err := waitForSteadyControlPlanePodsResult(
		ctx, state.cluster, state.controlPlaneSelector, 2, state.pollInterval,
	)
	if err != nil {
		return err
	}
	state.replacementRejoined = containsPodUID(finalControlPlanes, replacement) &&
		containsPodUID(finalControlPlanes, survivor)

	if err := runTurnResult(
		ctx, state.managed.client, state.managed.sessionID,
		"Run the sandbox check after the replacement Control Plane rejoins.",
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

func readyControlPlanePodsResult(
	ctx context.Context,
	cluster *kube.Client,
	selector string,
	expected int,
) ([]kube.Pod, error) {
	pods, err := controlPlanePodSnapshotResult(ctx, cluster, selector)
	if err != nil {
		return nil, err
	}
	return readyControlPlanesResult(pods, expected)
}

func steadyControlPlanePodsResult(
	ctx context.Context,
	cluster *kube.Client,
	selector string,
	expected int,
) ([]kube.Pod, error) {
	pods, err := controlPlanePodSnapshotResult(ctx, cluster, selector)
	if err != nil {
		return nil, err
	}
	if len(pods) != expected {
		return nil, fmt.Errorf("Control Plane Pods = %d, want exactly %d", len(pods), expected)
	}
	return readyControlPlanesResult(pods, expected)
}

func controlPlanePodSnapshotResult(
	ctx context.Context,
	cluster *kube.Client,
	selector string,
) ([]kube.Pod, error) {
	pods, err := cluster.ListPods(ctx, selector)
	if err != nil {
		return nil, fmt.Errorf("list Control Plane Pods: %w", err)
	}
	for _, pod := range pods {
		if pod.Labels[controlPlaneComponentLabel] != "control-plane" {
			return nil, fmt.Errorf(
				"Control Plane selector matched Pod %q without %s=control-plane; refusing disruption",
				pod.Name, controlPlaneComponentLabel,
			)
		}
	}
	return pods, nil
}

func readyControlPlanesResult(pods []kube.Pod, expected int) ([]kube.Pod, error) {
	ready := make([]kube.Pod, 0, len(pods))
	for _, pod := range pods {
		if pod.Ready && !pod.Deleting {
			ready = append(ready, pod)
		}
	}
	if len(ready) != expected {
		return nil, fmt.Errorf("Ready Control Plane Pods = %d, want %d", len(ready), expected)
	}
	return ready, nil
}

func waitForSteadyControlPlanePodsResult(
	ctx context.Context,
	cluster *kube.Client,
	selector string,
	expected int,
	interval time.Duration,
) ([]kube.Pod, error) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		ready, err := steadyControlPlanePodsResult(ctx, cluster, selector, expected)
		if err == nil {
			return ready, nil
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("wait for steady %d-replica Control Plane: %w", expected, ctx.Err())
		case <-ticker.C:
		}
	}
}

func waitForControlPlaneReadyCountResult(
	ctx context.Context,
	cluster *kube.Client,
	selector string,
	expected int,
	interval time.Duration,
) ([]kube.Pod, error) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		ready, err := readyControlPlanePodsResult(ctx, cluster, selector, expected)
		if err == nil {
			return ready, nil
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("wait for %d Ready Control Plane Pods: %w", expected, ctx.Err())
		case <-ticker.C:
		}
	}
}

func verifyControlPlaneMultiReplicaCleanup(
	ctx context.Context,
	state *controlPlaneMultiReplicaCaseState,
) error {
	if state.cluster == nil || state.controlPlaneSelector == "" {
		return nil
	}
	_, err := waitForSteadyControlPlanePodsResult(
		ctx, state.cluster, state.controlPlaneSelector, 2, state.pollInterval,
	)
	if err != nil {
		return fmt.Errorf("verify two-replica Control Plane recovery: %w", err)
	}
	return nil
}

func containsPodUID(pods []kube.Pod, target kube.Pod) bool {
	for _, pod := range pods {
		if pod.UID == target.UID {
			return true
		}
	}
	return false
}
