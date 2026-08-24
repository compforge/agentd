//go:build e2e

package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/packages/param"
	"github.com/compforge/case-harness/go/e2e/caserun"
	"github.com/compforge/case-harness/go/kube"
)

const midTurnSystemPrompt = "For every user request, call bash exactly once with command `sleep 90; printf AGENTD_E2E_CHAOS_TOOL_FINISHED`. If that tool fails or is denied, do not call another tool. Answer exactly AGENTD_E2E_OK after the tool result."

func TestManagedAgentRecoversAfterMidTurnWorkerLoss(t *testing.T) {
	runMidTurnWorkerDisruptionCase(t, "mid_turn_worker_loss", "mid-turn", true)
}

func TestManagedAgentDrainsOnWorkerTermination(t *testing.T) {
	runMidTurnWorkerDisruptionCase(t, "agentlet_graceful_drain", "graceful-drain", false)
}

func runMidTurnWorkerDisruptionCase(t *testing.T, caseID, recoveryFacet string, forceDelete bool) {
	t.Helper()
	state := midTurnWorkerDisruptionCaseState{forceDelete: forceDelete}
	result := caserun.Run(
		context.Background(),
		caserun.Ref("agentd-managed-agent", caseID),
		nil,
		&state,
		caserun.Definition[midTurnWorkerDisruptionCaseState]{
			Prepare: prepareMidTurnWorkerDisruptionCase,
			Execute: executeMidTurnWorkerDisruptionCase,
			Judge: func(_ context.Context, state *midTurnWorkerDisruptionCaseState) error {
				if state.userMessages != 1 || state.agentMessages != 1 {
					return caserun.Fail(fmt.Sprintf(
						"message counts = user:%d agent:%d, want user:1 agent:1",
						state.userMessages, state.agentMessages,
					))
				}
				if _, reused := state.previousWorkerIDs[state.replacementWorkerID]; reused {
					return caserun.Fail(fmt.Sprintf(
						"replacement Pod reused old logical Worker %q",
						state.replacementWorkerID,
					))
				}
				if !state.safeOutcome {
					return caserun.Fail("mid-turn Worker loss did not converge to one safe answer")
				}
				return nil
			},
			Budgets: systemCaseBudgets,
			Facets:  map[string]string{"boundary": "system", "recovery": recoveryFacet},
		},
	)
	recordSystemCase(t, result)
}

type midTurnWorkerDisruptionCaseState struct {
	managed             managedAgentCaseState
	cluster             *kube.Client
	workerSelector      string
	pollInterval        time.Duration
	previousWorkerIDs   map[string]struct{}
	replacementWorkerID string
	userMessages        int
	agentMessages       int
	safeOutcome         bool
	forceDelete         bool
}

func prepareMidTurnWorkerDisruptionCase(ctx context.Context, state *midTurnWorkerDisruptionCaseState) error {
	disruption, err := readWorkerDisruptionEnv()
	if err != nil {
		return err
	}
	if err := prepareManagedAgentCaseWithSystem(ctx, &state.managed, midTurnSystemPrompt); err != nil {
		return err
	}
	state.cluster = disruption.cluster
	state.workerSelector = disruption.workerSelector
	state.pollInterval = disruption.pollInterval
	return nil
}

func executeMidTurnWorkerDisruptionCase(ctx context.Context, state *midTurnWorkerDisruptionCaseState) error {
	ctx, cancel := context.WithTimeout(ctx, state.managed.config.timeout)
	defer cancel()

	messages, err := agentMessagesResult(ctx, state.managed.client, state.managed.sessionID)
	if err != nil {
		return err
	}
	before := len(messages)
	if err := sendTurnResult(
		ctx, state.managed.client, state.managed.sessionID,
		"Run the long sandbox operation for the mid-turn Worker-loss check.",
	); err != nil {
		return err
	}
	if err := waitForRunningSessionResult(ctx, state.managed.client, state.managed.sessionID); err != nil {
		return err
	}

	previous, workerIDs, err := managedWorkersResult(ctx, state.cluster, state.workerSelector)
	if err != nil {
		return err
	}
	state.previousWorkerIDs = workerIDs
	for _, pod := range previous {
		if state.forceDelete {
			if err := state.cluster.ForceDeletePod(ctx, pod.Ref()); err != nil {
				return fmt.Errorf("force-delete running Worker Pod %q: %w", pod.Name, err)
			}
			continue
		}
		if err := state.cluster.DeletePod(ctx, pod.Ref()); err != nil {
			return fmt.Errorf("gracefully delete running Worker Pod %q: %w", pod.Name, err)
		}
	}
	state.replacementWorkerID, err = replacementWorkerIDResult(
		ctx, state.cluster, state.workerSelector, previous, state.pollInterval,
	)
	if err != nil {
		return err
	}

	toolUseID, completed, err := waitForMidTurnSettlementResult(
		ctx, state.managed.client, state.managed.sessionID, before,
	)
	if err != nil {
		return err
	}
	if !completed {
		if err := denyToolUseResult(ctx, state.managed.client, state.managed.sessionID, toolUseID); err != nil {
			return err
		}
		if err := waitForTurnResult(
			ctx, state.managed.client, state.managed.sessionID, "mid-turn tool reconciliation", before,
		); err != nil {
			return err
		}
	}

	state.userMessages, state.agentMessages, err = managedMessageCountsResult(
		ctx, state.managed.client, state.managed.sessionID,
	)
	state.safeOutcome = err == nil && state.userMessages == 1 && state.agentMessages == 1
	return err
}

func waitForRunningSessionResult(
	ctx context.Context,
	client *anthropic.Client,
	sessionID string,
) error {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		current, err := client.Beta.Sessions.Get(ctx, sessionID, anthropic.BetaSessionGetParams{})
		if err != nil {
			return fmt.Errorf("get Session while waiting for running state: %w", err)
		}
		if current.Status == anthropic.BetaManagedAgentsSessionStatusRunning {
			return nil
		}
		if current.Status == anthropic.BetaManagedAgentsSessionStatusTerminated {
			return fmt.Errorf("Session terminated before mid-turn disruption")
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for running Session: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func waitForMidTurnSettlementResult(
	ctx context.Context,
	client *anthropic.Client,
	sessionID string,
	beforeAgentMessages int,
) (string, bool, error) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		page, err := client.Beta.Sessions.Events.List(ctx, sessionID, anthropic.BetaSessionEventListParams{})
		if err != nil {
			return "", false, fmt.Errorf("list Session Events after Worker disruption: %w", err)
		}
		agentMessages := 0
		for _, event := range page.Data {
			if event.Type == "agent.message" {
				agentMessages++
				if agentMessages > beforeAgentMessages && strings.Contains(event.RawJSON(), "AGENTD_E2E_OK") {
					return "", true, nil
				}
			}
			if event.Type != "session.status_idle" {
				continue
			}
			var idle struct {
				StopReason struct {
					Type     string   `json:"type"`
					EventIDs []string `json:"event_ids"`
				} `json:"stop_reason"`
			}
			if err := json.Unmarshal([]byte(event.RawJSON()), &idle); err != nil {
				return "", false, fmt.Errorf("decode idle Session Event: %w", err)
			}
			switch idle.StopReason.Type {
			case "requires_action":
				if len(idle.StopReason.EventIDs) != 1 {
					return "", false, fmt.Errorf(
						"required action Event IDs = %v, want exactly one",
						idle.StopReason.EventIDs,
					)
				}
				return idle.StopReason.EventIDs[0], false, nil
			case "retries_exhausted":
				return "", false, fmt.Errorf("Session exhausted the disrupted Turn instead of recovering it")
			}
		}
		select {
		case <-ctx.Done():
			return "", false, fmt.Errorf("wait for mid-turn recovery: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func denyToolUseResult(
	ctx context.Context,
	client *anthropic.Client,
	sessionID string,
	toolUseID string,
) error {
	_, err := client.Beta.Sessions.Events.Send(ctx, sessionID, anthropic.BetaSessionEventSendParams{
		Events: []anthropic.BetaManagedAgentsEventParamsUnion{{
			OfUserToolConfirmation: &anthropic.BetaManagedAgentsUserToolConfirmationEventParams{
				Type:        anthropic.BetaManagedAgentsUserToolConfirmationEventParamsTypeUserToolConfirmation,
				Result:      anthropic.BetaManagedAgentsUserToolConfirmationEventParamsResultDeny,
				ToolUseID:   toolUseID,
				DenyMessage: param.NewOpt("mid-turn E2E reconciled the unknown tool outcome without replay"),
			},
		}},
	})
	if err != nil {
		return fmt.Errorf("deny unresolved tool use %q: %w", toolUseID, err)
	}
	return nil
}

func managedMessageCountsResult(
	ctx context.Context,
	client *anthropic.Client,
	sessionID string,
) (int, int, error) {
	page, err := client.Beta.Sessions.Events.List(ctx, sessionID, anthropic.BetaSessionEventListParams{})
	if err != nil {
		return 0, 0, fmt.Errorf("list final Session Events: %w", err)
	}
	userMessages := 0
	agentMessages := 0
	for _, event := range page.Data {
		switch event.Type {
		case "user.message":
			userMessages++
		case "agent.message":
			agentMessages++
		}
	}
	return userMessages, agentMessages, nil
}
