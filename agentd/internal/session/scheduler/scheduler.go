// Package scheduler owns Session-to-Worker placement policy.
package scheduler

import (
	"sort"
	"strings"
	"time"
)

type Reason string

const (
	ReasonExisting   Reason = "existing"
	ReasonAffinity   Reason = "affinity"
	ReasonAvailable  Reason = "available"
	ReasonNoCapacity Reason = "no_capacity"

	lastWorkerAffinityBonus = 10
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
	Score    Score
}

type Score struct {
	CapacityHeadroom int
	LastWorker       int
	Total            int
}

type placementCandidate struct {
	workerID string
	score    Score
}

// Scheduler owns only Session-to-Worker placement policy. Worker Observer and
// Assignment storage contribute facts; the service layer applies the
// returned decision. Schedule performs no I/O.
type Scheduler struct {
	observationMaxAge time.Duration
}

func New(observationMaxAge time.Duration) *Scheduler {
	return &Scheduler{observationMaxAge: observationMaxAge}
}

// Schedule retains a schedulable existing placement. Unbound Sessions score
// every admitted candidate by capacity headroom and a small last-Worker bonus;
// affinity reduces needless movement without overriding material load skew.
func (s *Scheduler) Schedule(
	now time.Time,
	existingWorkerID string,
	lastWorkerID string,
	candidates []Candidate,
) Decision {
	if existingWorkerID != "" {
		for _, candidate := range candidates {
			if candidate.WorkerID == existingWorkerID && s.schedulable(now, candidate) {
				return Decision{WorkerID: existingWorkerID, Reason: ReasonExisting}
			}
		}
	}
	admitted := make([]placementCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		if !s.schedulable(now, candidate) || candidate.AssignedCount >= int64(candidate.Capacity) {
			continue
		}
		admitted = append(admitted, placementCandidate{
			workerID: candidate.WorkerID,
			score:    scoreCandidate(lastWorkerID, candidate),
		})
	}
	if len(admitted) == 0 {
		return Decision{Reason: ReasonNoCapacity}
	}
	sort.SliceStable(admitted, func(i, j int) bool {
		if admitted[i].score.Total != admitted[j].score.Total {
			return admitted[i].score.Total > admitted[j].score.Total
		}
		return admitted[i].workerID < admitted[j].workerID
	})
	reason := ReasonAvailable
	if admitted[0].score.LastWorker > 0 {
		reason = ReasonAffinity
	}
	return Decision{WorkerID: admitted[0].workerID, Reason: reason, Score: admitted[0].score}
}

func scoreCandidate(lastWorkerID string, candidate Candidate) Score {
	available := int64(candidate.Capacity) - candidate.AssignedCount
	headroom := int(available * 100 / int64(candidate.Capacity))
	score := Score{CapacityHeadroom: headroom}
	if candidate.WorkerID == lastWorkerID {
		score.LastWorker = lastWorkerAffinityBonus
	}
	score.Total = score.CapacityHeadroom + score.LastWorker
	return score
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
