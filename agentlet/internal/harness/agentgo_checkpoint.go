package harness

import (
	"context"
	"encoding/json"
	"fmt"

	agentledger "github.com/compforge/agent-ledger/go"
	agentgoadapter "github.com/compforge/agent-ledger/go/adapters/agentgo"
	"github.com/compforge/agentgo"
)

// agentGoCheckpoint mirrors only the public, portable fields in the AgentGo
// Adapter checkpoint envelope. Agentlet inspects the snapshot to make public
// output projection idempotent; execution replay remains Adapter-owned.
type agentGoCheckpoint struct {
	Snapshot       agentgo.AgentSnapshot `codec:"snapshot"`
	ExecutionScope string                `codec:"execution_scope"`
	ScopeComplete  bool                  `codec:"scope_complete"`
}

func (r *AgentGoRunner) loadAgentGoCheckpoint(
	ctx context.Context,
	resumeRef string,
	checkpointKey string,
	expectedRevision int64,
) (agentGoCheckpoint, agentledger.Checkpoint, bool, error) {
	checkpoint, exists, err := r.resolveCheckpoint(ctx, resumeRef, checkpointKey, expectedRevision)
	if err != nil || !exists {
		return agentGoCheckpoint{}, checkpoint, exists, err
	}
	if checkpoint.Format != agentgoadapter.CheckpointFormat {
		return agentGoCheckpoint{}, agentledger.Checkpoint{}, false, fmt.Errorf(
			"unsupported AgentGo checkpoint format %q", checkpoint.Format,
		)
	}
	codec, err := agentgo.NewCodec()
	if err != nil {
		return agentGoCheckpoint{}, agentledger.Checkpoint{}, false, fmt.Errorf("create AgentGo checkpoint codec: %w", err)
	}
	encoded, err := json.Marshal(checkpoint.State)
	if err != nil {
		return agentGoCheckpoint{}, agentledger.Checkpoint{}, false, fmt.Errorf("encode AgentGo checkpoint: %w", err)
	}
	var native agentGoCheckpoint
	if err := codec.Unmarshal(encoded, &native); err != nil {
		return agentGoCheckpoint{}, agentledger.Checkpoint{}, false, fmt.Errorf(
			"decode AgentGo checkpoint revision %d: %w", checkpoint.Revision, err,
		)
	}
	if native.ExecutionScope == "" {
		return agentGoCheckpoint{}, agentledger.Checkpoint{}, false, fmt.Errorf(
			"AgentGo checkpoint revision %d has no execution scope", checkpoint.Revision,
		)
	}
	return native, checkpoint, true, nil
}

// resolveCheckpoint validates the Control State recovery point, then advances
// to a newer checkpoint that the previous Worker persisted before agentd could
// observe it.
//
// +spec=`A Control State ResumeRef is a validated lower bound; recovery adopts a newer checkpoint only from the same Session key and revision chain`
// +case:id=mid_turn_worker_loss,desc=`force-delete a Worker after it persists Harness state but before agentd observes the ResumePoint`,expect=`the replacement Agentlet adopts the newer same-Session checkpoint and reconciles Ledger Attempts`,forbid=`starting again from stale Control State or consuming the input as retries_exhausted`,group=system
// +why=`Checkpoint persistence precedes asynchronous Control State observation, so abrupt Worker loss can legitimately leave the shared Checkpoint Store ahead of agentd`
// +link=agentd/docs/agentlet.md
// +link=tests/e2e/cases/managed-agent.yaml
func (r *AgentGoRunner) resolveCheckpoint(
	ctx context.Context,
	resumeRef string,
	checkpointKey string,
	expectedRevision int64,
) (agentledger.Checkpoint, bool, error) {
	if expectedRevision < 0 {
		return agentledger.Checkpoint{}, false, fmt.Errorf(
			"AgentGo checkpoint revision must not be negative: %d", expectedRevision,
		)
	}
	if expectedRevision == 0 {
		if resumeRef != checkpointKey {
			return agentledger.Checkpoint{}, false, fmt.Errorf(
				"AgentGo initial checkpoint reference %q does not match key %q", resumeRef, checkpointKey,
			)
		}
	} else {
		controlCheckpoint, exists, err := r.config.Checkpoints.GetCheckpoint(ctx, resumeRef)
		if err != nil {
			return agentledger.Checkpoint{}, false, fmt.Errorf("load AgentGo control checkpoint: %w", err)
		}
		if !exists {
			return agentledger.Checkpoint{}, false, fmt.Errorf("AgentGo control checkpoint %q does not exist", resumeRef)
		}
		if controlCheckpoint.Key != checkpointKey {
			return agentledger.Checkpoint{}, false, fmt.Errorf(
				"AgentGo control checkpoint belongs to %q, want %q", controlCheckpoint.Key, checkpointKey,
			)
		}
		if controlCheckpoint.Revision != expectedRevision {
			return agentledger.Checkpoint{}, false, fmt.Errorf(
				"AgentGo control checkpoint revision mismatch: control=%d checkpoint=%d",
				expectedRevision,
				controlCheckpoint.Revision,
			)
		}
	}

	latest, exists, err := r.config.Checkpoints.LoadLatestCheckpoint(ctx, checkpointKey)
	if err != nil {
		return agentledger.Checkpoint{}, false, fmt.Errorf("load latest AgentGo checkpoint: %w", err)
	}
	if !exists {
		if expectedRevision == 0 {
			return agentledger.Checkpoint{}, false, nil
		}
		return agentledger.Checkpoint{}, false, fmt.Errorf(
			"AgentGo checkpoint key %q has no revisions", checkpointKey,
		)
	}
	if latest.Revision < expectedRevision {
		return agentledger.Checkpoint{}, false, fmt.Errorf(
			"AgentGo latest checkpoint revision %d is behind control revision %d",
			latest.Revision,
			expectedRevision,
		)
	}
	if latest.Revision == expectedRevision && expectedRevision > 0 && latest.ID != resumeRef {
		return agentledger.Checkpoint{}, false, fmt.Errorf(
			"AgentGo checkpoint revision %d has unexpected identity %q", latest.Revision, latest.ID,
		)
	}
	return latest, true, nil
}

func (r *AgentGoRunner) latestResumePoint(
	ctx context.Context,
	checkpointKey string,
	fallback TurnResult,
) (TurnResult, error) {
	checkpoint, exists, err := r.config.Checkpoints.LoadLatestCheckpoint(ctx, checkpointKey)
	if err != nil {
		return fallback, fmt.Errorf("load AgentGo resume point: %w", err)
	}
	if !exists {
		return fallback, nil
	}
	if checkpoint.Key != checkpointKey || checkpoint.Format != agentgoadapter.CheckpointFormat {
		return fallback, fmt.Errorf("latest AgentGo checkpoint does not match Session recovery contract")
	}
	return TurnResult{ResumeRef: checkpoint.ID, ResumeRevision: checkpoint.Revision}, nil
}
