// Package observer reconciles Agentlet execution facts into Session Control State.
package observer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/compforge/agentd/agentd/internal/model"
	"github.com/compforge/agentd/agentd/internal/service"
	"github.com/compforge/agentd/internal/executionapi"
)

type Source interface {
	ObserveSession(context.Context, model.Session) (executionapi.SessionState, error)
}

type Sink interface {
	ListSessions(context.Context) ([]model.Session, error)
	ObserveSession(context.Context, string, model.SessionObserverStatus) (model.Session, error)
}

type Config struct {
	Interval       time.Duration
	RequestTimeout time.Duration
	Concurrency    int
	Logger         *slog.Logger
}

// Observer polls only Sessions with a current Assignment. It never installs a
// Work: observing a missing or unreachable Agentlet must not create execution.
type Observer struct {
	source         Source
	sink           Sink
	interval       time.Duration
	requestTimeout time.Duration
	concurrency    int
	logger         *slog.Logger
}

func New(source Source, sink Sink, config Config) (*Observer, error) {
	if source == nil || sink == nil {
		return nil, fmt.Errorf("create Session Observer: source and sink are required")
	}
	if config.Interval <= 0 || config.RequestTimeout <= 0 {
		return nil, fmt.Errorf("create Session Observer: interval and request timeout must be positive")
	}
	if config.Concurrency <= 0 {
		return nil, fmt.Errorf("create Session Observer: concurrency must be positive")
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	return &Observer{
		source: source, sink: sink, interval: config.Interval,
		requestTimeout: config.RequestTimeout, concurrency: config.Concurrency, logger: config.Logger,
	}, nil
}

func (o *Observer) Run(ctx context.Context) {
	o.reconcileAndLog(ctx)
	ticker := time.NewTicker(o.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			o.reconcileAndLog(ctx)
		}
	}
}

func (o *Observer) Reconcile(ctx context.Context) error {
	sessions, err := o.sink.ListSessions(ctx)
	if err != nil {
		return fmt.Errorf("list Sessions for observation: %w", err)
	}
	jobs := make(chan model.Session)
	var (
		workerSet      sync.WaitGroup
		errorLock      sync.Mutex
		observedErrors []error
	)
	for range min(o.concurrency, len(sessions)) {
		workerSet.Add(1)
		go func() {
			defer workerSet.Done()
			for session := range jobs {
				if err := o.observe(ctx, session); err != nil {
					errorLock.Lock()
					observedErrors = append(observedErrors, err)
					errorLock.Unlock()
				}
			}
		}()
	}
	for _, session := range sessions {
		if session.AssignmentID == "" || session.WorkerID == "" {
			continue
		}
		select {
		case jobs <- session:
		case <-ctx.Done():
			close(jobs)
			workerSet.Wait()
			return errors.Join(append(observedErrors, ctx.Err())...)
		}
	}
	close(jobs)
	workerSet.Wait()
	return errors.Join(observedErrors...)
}

func (o *Observer) observe(ctx context.Context, session model.Session) error {
	// Fence the snapshot at request start. A slower response from an older poll
	// cannot overwrite an observation whose request started later.
	observedAt := time.Now().UTC()
	requestCtx, cancel := context.WithTimeout(ctx, o.requestTimeout)
	defer cancel()
	state, err := o.source.ObserveSession(requestCtx, session)
	if err != nil {
		return fmt.Errorf("observe Session %q: %w", session.ID, err)
	}
	_, err = o.sink.ObserveSession(ctx, session.ID, model.SessionObserverStatus{
		ObservedAt: observedAt, AssignmentID: state.AssignmentID, Exists: true,
		Status: model.SessionStatus(state.Status), ResumeRef: state.ResumeRef,
		ResumeRevision: state.ResumeRevision,
	})
	if errors.Is(err, service.ErrConflict) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("persist Session %q observation: %w", session.ID, err)
	}
	return nil
}

func (o *Observer) reconcileAndLog(ctx context.Context) {
	if err := o.Reconcile(ctx); err != nil && ctx.Err() == nil {
		o.logger.ErrorContext(ctx, "reconcile Session observations", "error", err)
	}
}
