// Package reconciler converges durable Session input into Agentlet execution.
package reconciler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/compforge/agentd/agentd/internal/model"
	"github.com/compforge/agentd/agentd/internal/service"
	"github.com/compforge/agentd/agentd/internal/session/connector"
	managedevent "github.com/compforge/agentd/internal/event"
)

type Control interface {
	ListSessions(context.Context) ([]model.Session, error)
	ReconcilePlacement(context.Context, string, bool) (model.Session, error)
	CurrentExecution(context.Context, string) (service.ExecutionTarget, error)
}

type EventSource interface {
	PendingInputs(context.Context, string) ([]managedevent.ManagedEvent, error)
}

type DataPlane interface {
	Ensure(context.Context, connector.Target) error
	Wake(context.Context, connector.Target) error
}

type WorkerNotifier interface {
	Notify()
}

type Config struct {
	Interval       time.Duration
	RequestTimeout time.Duration
	Concurrency    int
	Logger         *slog.Logger
}

// Reconciler treats the shared Ledger as durable execution demand. Notify is
// only a latency optimization; periodic scans recover after a lost notification
// or control-plane restart.
type Reconciler struct {
	control        Control
	events         EventSource
	dataPlane      DataPlane
	workerNotifier WorkerNotifier
	interval       time.Duration
	requestTimeout time.Duration
	concurrency    int
	logger         *slog.Logger
	notifications  chan struct{}
}

func New(
	control Control,
	events EventSource,
	dataPlane DataPlane,
	workerNotifier WorkerNotifier,
	config Config,
) (*Reconciler, error) {
	if control == nil || events == nil || dataPlane == nil {
		return nil, fmt.Errorf("create Session Reconciler: control, event source, and data plane are required")
	}
	if config.Interval <= 0 || config.RequestTimeout <= 0 {
		return nil, fmt.Errorf("create Session Reconciler: interval and request timeout must be positive")
	}
	if config.Concurrency <= 0 {
		return nil, fmt.Errorf("create Session Reconciler: concurrency must be positive")
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	return &Reconciler{
		control: control, events: events, dataPlane: dataPlane, workerNotifier: workerNotifier,
		interval: config.Interval, requestTimeout: config.RequestTimeout,
		concurrency: config.Concurrency, logger: config.Logger,
		notifications: make(chan struct{}, 1),
	}, nil
}

func (r *Reconciler) Notify() {
	select {
	case r.notifications <- struct{}{}:
	default:
	}
}

func (r *Reconciler) Run(ctx context.Context) {
	r.reconcileAndLog(ctx)
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.reconcileAndLog(ctx)
		case <-r.notifications:
			r.reconcileAndLog(ctx)
		}
	}
}

// Reconcile is the single owner of Session placement actions. It consumes
// durable input, Worker facts, and Agentlet observations to release stable
// placements, place pending work, and wake the selected Agentlet.
//
// +spec=`An unprocessed durable user Event remains executable after notification loss or control-plane restart; placement changes occur only after a stable execution boundary or confirmed Worker loss`
// +case:id=worker_replacement_resume,desc=`A durable Session resumes on a new logical Worker after confirmed Worker loss`,input=`replace-worker-between-sandbox-turns`,expect=`two completed turns on different logical Workers`,forbid=`lost durable input or reuse of the retired Worker`,group=system
// +case:id=mid_turn_worker_loss,desc=`A Worker disappears while its Session is running an unknown-effect sandbox tool`,input=`force-delete-the-assigned-worker-during-one-turn`,expect=`a replacement Worker resumes the durable input and either completes safely or asks for exact tool reconciliation before producing one answer`,forbid=`lost or duplicate input, automatic replay of an unresolved side effect, or Session termination`,group=system
// +link=agentd/docs/agentd.md
// +link=tests/e2e/cases/managed-agent.yaml
func (r *Reconciler) Reconcile(ctx context.Context) error {
	sessions, err := r.control.ListSessions(ctx)
	if err != nil {
		return fmt.Errorf("list Sessions for execution reconciliation: %w", err)
	}
	jobs := make(chan model.Session)
	var (
		workerSet       sync.WaitGroup
		errorLock       sync.Mutex
		reconcileErrors []error
	)
	for range min(r.concurrency, len(sessions)) {
		workerSet.Add(1)
		go func() {
			defer workerSet.Done()
			for session := range jobs {
				if err := r.reconcileSession(ctx, session); err != nil {
					errorLock.Lock()
					reconcileErrors = append(reconcileErrors, err)
					errorLock.Unlock()
				}
			}
		}()
	}
	for _, session := range sessions {
		select {
		case jobs <- session:
		case <-ctx.Done():
			close(jobs)
			workerSet.Wait()
			return errors.Join(append(reconcileErrors, ctx.Err())...)
		}
	}
	close(jobs)
	workerSet.Wait()
	return errors.Join(reconcileErrors...)
}

func (r *Reconciler) reconcileSession(ctx context.Context, session model.Session) error {
	requestCtx, cancel := context.WithTimeout(ctx, r.requestTimeout)
	defer cancel()
	pending, err := r.events.PendingInputs(requestCtx, session.ID)
	if err != nil {
		return fmt.Errorf("read pending input for Session %q: %w", session.ID, err)
	}
	reconciled, err := r.control.ReconcilePlacement(requestCtx, session.ID, len(pending) > 0)
	if errors.Is(err, service.ErrNoCapacity) || errors.Is(err, service.ErrNoAssignment) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("reconcile placement for Session %q: %w", session.ID, err)
	}
	if reconciled.Placement.Bound() && reconciled.Placement.Fence != session.Placement.Fence &&
		r.workerNotifier != nil {
		// A changed placement may point at a newly published Worker row. Wake the
		// Worker Reconciler after the transaction commits; a redundant wake for
		// an already-ready Worker is harmless and coalesced.
		r.workerNotifier.Notify()
	}
	if len(pending) == 0 || reconciled.Status == model.SessionStatusTerminated ||
		!reconciled.Placement.Bound() {
		return nil
	}
	execution, err := r.control.CurrentExecution(requestCtx, session.ID)
	if errors.Is(err, service.ErrUnavailable) || errors.Is(err, service.ErrNoAssignment) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("resolve execution for Session %q: %w", session.ID, err)
	}
	target := connector.Target{Endpoint: execution.Endpoint, Work: execution.Work}
	if err := r.dataPlane.Ensure(requestCtx, target); err != nil {
		return fmt.Errorf("ensure Work for Session %q: %w", session.ID, err)
	}
	if err := r.dataPlane.Wake(requestCtx, target); err != nil {
		return fmt.Errorf("wake Session %q: %w", session.ID, err)
	}
	r.logger.InfoContext(ctx, "woke Session on Agentlet",
		"session_id", session.ID, "worker_id", execution.Work.WorkerID,
		"placement_fence", execution.Work.AssignmentID)
	return nil
}

func (r *Reconciler) reconcileAndLog(ctx context.Context) {
	if err := r.Reconcile(ctx); err != nil && ctx.Err() == nil {
		r.logger.ErrorContext(ctx, "reconcile Session execution", "error", err)
	}
}
