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
	PrepareExecution(context.Context, string) (service.ExecutionTarget, error)
	CurrentExecution(context.Context, string) (service.ExecutionTarget, error)
}

type EventSource interface {
	UnprocessedUserMessages(context.Context, string) ([]managedevent.ManagedEvent, error)
}

type DataPlane interface {
	Ensure(context.Context, connector.Target) error
	Wake(context.Context, connector.Target) error
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
	interval       time.Duration
	requestTimeout time.Duration
	concurrency    int
	logger         *slog.Logger
	notifications  chan struct{}
}

func New(control Control, events EventSource, dataPlane DataPlane, config Config) (*Reconciler, error) {
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
		control: control, events: events, dataPlane: dataPlane,
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

// Reconcile ensures one current Work and one wake signal for every Session
// with durable, unprocessed user input. It never moves an existing Assignment
// merely because its Agentlet is temporarily unreachable. Worker failure
// reconciliation is a separate control-plane concern.
//
// +spec=`An unprocessed durable user Event remains executable after notification loss or control-plane restart; reconciliation assigns unbound Sessions but retries existing Assignments in place`
// +link=agentd/docs/agentd.md
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
		if session.Status == model.SessionStatusTerminated {
			continue
		}
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
	pending, err := r.events.UnprocessedUserMessages(requestCtx, session.ID)
	if err != nil {
		return fmt.Errorf("read pending input for Session %q: %w", session.ID, err)
	}
	if len(pending) == 0 {
		return nil
	}

	var execution service.ExecutionTarget
	if session.AssignmentID == "" || session.WorkerID == "" {
		execution, err = r.control.PrepareExecution(requestCtx, session.ID)
	} else {
		execution, err = r.control.CurrentExecution(requestCtx, session.ID)
	}
	if errors.Is(err, service.ErrNoCapacity) || errors.Is(err, service.ErrNoAssignment) {
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
	return nil
}

func (r *Reconciler) reconcileAndLog(ctx context.Context) {
	if err := r.Reconcile(ctx); err != nil && ctx.Err() == nil {
		r.logger.ErrorContext(ctx, "reconcile Session execution", "error", err)
	}
}
