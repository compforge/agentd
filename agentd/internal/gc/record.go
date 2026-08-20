package gc

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

type RetiredWorkerStore interface {
	DeleteRetiredWorkersBefore(context.Context, time.Time, int) (int64, error)
}

type RecordConfig struct {
	Interval       time.Duration
	RequestTimeout time.Duration
	Retention      time.Duration
	BatchSize      int
	Logger         *slog.Logger
}

// RecordGC removes aged terminal Worker rows. Pod deletion and lifecycle
// transitions remain owned by PodGC; this controller only maintains metadata.
// +case:id=retired_worker_record_retention,desc=`retire and destroy an idle Worker Pod, observe its absence, then age its record past retention`,expect=`the terminal record remains observable during retention and is deleted after expiration`,forbid=`deleting a present, active, fresh, or Session-bound Worker record`
// +link=agentd/tests/e2e
type RecordGC struct {
	workers RetiredWorkerStore
	config  RecordConfig
}

func NewRecordGC(workers RetiredWorkerStore, config RecordConfig) (*RecordGC, error) {
	if workers == nil {
		return nil, fmt.Errorf("create Worker Record GC: store is required")
	}
	if config.Interval <= 0 || config.RequestTimeout <= 0 || config.Retention <= 0 {
		return nil, fmt.Errorf("create Worker Record GC: durations must be positive")
	}
	if config.BatchSize <= 0 {
		return nil, fmt.Errorf("create Worker Record GC: batch size must be positive")
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	return &RecordGC{workers: workers, config: config}, nil
}

func (g *RecordGC) Run(ctx context.Context) {
	g.sweepWithTimeout(ctx)
	ticker := time.NewTicker(g.config.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			g.sweepWithTimeout(ctx)
		}
	}
}

func (g *RecordGC) Sweep(ctx context.Context) (int64, error) {
	cutoff := time.Now().UTC().Add(-g.config.Retention)
	deleted, err := g.workers.DeleteRetiredWorkersBefore(ctx, cutoff, g.config.BatchSize)
	if err != nil {
		return deleted, fmt.Errorf("delete retired Worker records: %w", err)
	}
	return deleted, nil
}

func (g *RecordGC) sweepWithTimeout(ctx context.Context) {
	sweepCtx, cancel := context.WithTimeout(ctx, g.config.RequestTimeout)
	defer cancel()
	deleted, err := g.Sweep(sweepCtx)
	if err != nil {
		if ctx.Err() == nil {
			g.config.Logger.ErrorContext(ctx, "sweep retired Worker records", "error", err)
		}
		return
	}
	if deleted > 0 {
		g.config.Logger.InfoContext(ctx, "deleted retired Worker records", "count", deleted)
	}
}
