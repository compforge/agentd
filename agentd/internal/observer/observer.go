// Package observer reconciles Worker facts from their runtime source.
package observer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	control "github.com/compforge/agentd/agentd/internal/app"
)

type Sink interface {
	ListWorkers(context.Context) ([]control.Worker, error)
	ObserveWorker(context.Context, control.Worker) (control.Worker, error)
}

type Config struct {
	Interval       time.Duration
	RequestTimeout time.Duration
	Logger         *slog.Logger
}

// Observer owns the periodic source-to-Worker reconciliation loop. It only
// records observed facts; Kubernetes owns Pod lifecycle and Scheduler owns
// placement decisions.
type Observer struct {
	source         Source
	sink           Sink
	interval       time.Duration
	requestTimeout time.Duration
	logger         *slog.Logger
}

func New(source Source, sink Sink, config Config) (*Observer, error) {
	if source == nil || sink == nil {
		return nil, fmt.Errorf("create Worker Observer: source and sink are required")
	}
	if config.Interval <= 0 || config.RequestTimeout <= 0 {
		return nil, fmt.Errorf("create Worker Observer: interval and request timeout must be positive")
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	return &Observer{
		source: source, sink: sink, interval: config.Interval,
		requestTimeout: config.RequestTimeout, logger: config.Logger,
	}, nil
}

func (o *Observer) Run(ctx context.Context) {
	o.reconcileWithTimeout(ctx)
	ticker := time.NewTicker(o.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			o.reconcileWithTimeout(ctx)
		}
	}
}

func (o *Observer) Reconcile(ctx context.Context) error {
	snapshots, err := o.source.ListWorkers(ctx)
	if err != nil {
		return fmt.Errorf("list Worker source: %w", err)
	}
	existing, err := o.sink.ListWorkers(ctx)
	if err != nil {
		return err
	}

	observedAt := time.Now().UTC()
	seen := make(map[string]struct{}, len(snapshots))
	var reconcileErrors []error
	for _, snapshot := range snapshots {
		seen[snapshot.ID] = struct{}{}
		if err := o.observe(ctx, control.Worker{
			ID: snapshot.ID, Name: snapshot.Name, MaxRuns: snapshot.MaxRuns,
		}, control.WorkerObserverStatus{
			ObservedAt: observedAt, Exists: true, Ready: snapshot.Ready, Endpoint: snapshot.Endpoint,
		}); err != nil {
			reconcileErrors = append(reconcileErrors, err)
		}
	}
	for _, worker := range existing {
		if _, ok := seen[worker.ID]; ok {
			continue
		}
		if err := o.observe(ctx, control.Worker{
			ID: worker.ID, Name: worker.Name, MaxRuns: worker.MaxRuns,
		}, control.WorkerObserverStatus{ObservedAt: observedAt, Exists: false}); err != nil {
			reconcileErrors = append(reconcileErrors, err)
		}
	}
	return errors.Join(reconcileErrors...)
}

func (o *Observer) observe(ctx context.Context, worker control.Worker, status control.WorkerObserverStatus) error {
	raw, err := json.Marshal(status)
	if err != nil {
		return fmt.Errorf("encode observation for Worker %q: %w", worker.ID, err)
	}
	worker.ObserverStatus = raw
	if _, err := o.sink.ObserveWorker(ctx, worker); err != nil {
		return fmt.Errorf("persist observation for Worker %q: %w", worker.ID, err)
	}
	return nil
}

func (o *Observer) reconcileWithTimeout(ctx context.Context) {
	reconcileCtx, cancel := context.WithTimeout(ctx, o.requestTimeout)
	defer cancel()
	if err := o.Reconcile(reconcileCtx); err != nil && ctx.Err() == nil {
		o.logger.ErrorContext(ctx, "reconcile Worker observations", "error", err)
	}
}
