// Package scheduler owns Session-to-Worker placement policy.
package scheduler

import (
	"strings"
	"time"
)

type Reason string

const (
	ReasonExisting   Reason = "existing"
	ReasonAvailable  Reason = "available"
	ReasonNoCapacity Reason = "no_capacity"
)

type Observation struct {
	ObservedAt time.Time
	Exists     bool
	Ready      bool
	Endpoint   string
}

type Candidate struct {
	WorkerID      string
	Capacity      int
	AssignedCount int64
	Observation   Observation
}

type Decision struct {
	WorkerID string
	Reason   Reason
}

// Scheduler owns only Session-to-Worker placement policy. Worker Observer and
// Assignment storage contribute facts; the application layer applies the
// returned decision. Schedule performs no I/O.
type Scheduler struct {
	observationMaxAge time.Duration
}

func New(observationMaxAge time.Duration) *Scheduler {
	return &Scheduler{observationMaxAge: observationMaxAge}
}

// Schedule retains a schedulable existing placement. Otherwise it selects the
// least-loaded Worker with free capacity, using Worker ID as a stable tie-break.
func (s *Scheduler) Schedule(now time.Time, existingWorkerID string, candidates []Candidate) Decision {
	if existingWorkerID != "" {
		for _, candidate := range candidates {
			if candidate.WorkerID == existingWorkerID && s.schedulable(now, candidate) {
				return Decision{WorkerID: existingWorkerID, Reason: ReasonExisting}
			}
		}
	}

	best := Candidate{AssignedCount: -1}
	for _, candidate := range candidates {
		if !s.schedulable(now, candidate) || candidate.AssignedCount >= int64(candidate.Capacity) {
			continue
		}
		if best.AssignedCount < 0 || candidate.AssignedCount < best.AssignedCount ||
			(candidate.AssignedCount == best.AssignedCount && candidate.WorkerID < best.WorkerID) {
			best = candidate
		}
	}
	if best.AssignedCount < 0 {
		return Decision{Reason: ReasonNoCapacity}
	}
	return Decision{WorkerID: best.WorkerID, Reason: ReasonAvailable}
}

func (s *Scheduler) schedulable(now time.Time, candidate Candidate) bool {
	observation := candidate.Observation
	if !observation.Exists || !observation.Ready || strings.TrimSpace(observation.Endpoint) == "" {
		return false
	}
	if observation.ObservedAt.IsZero() || observation.ObservedAt.After(now) {
		return false
	}
	return now.Sub(observation.ObservedAt) <= s.observationMaxAge
}
