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

	currentAssignmentBonus  = 20
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
	CapacityHeadroom  int
	CurrentAssignment int
	LastWorker        int
	Total             int
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

// Schedule scores every admitted candidate by projected capacity headroom.
// The current Assignment and last Worker add decreasing soft-affinity bonuses;
// both reduce needless movement without overriding material load skew.
func (s *Scheduler) Schedule(
	now time.Time,
	existingWorkerID string,
	lastWorkerID string,
	candidates []Candidate,
) Decision {
	admitted := make([]placementCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		if !s.schedulable(now, candidate) || candidate.Capacity <= 0 {
			continue
		}
		isCurrent := candidate.WorkerID == existingWorkerID
		if !isCurrent && candidate.AssignedCount >= int64(candidate.Capacity) {
			continue
		}
		admitted = append(admitted, placementCandidate{
			workerID: candidate.WorkerID,
			score:    scoreCandidate(existingWorkerID, lastWorkerID, candidate),
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
	if admitted[0].score.CurrentAssignment > 0 {
		reason = ReasonExisting
	} else if admitted[0].score.LastWorker > 0 {
		reason = ReasonAffinity
	}
	return Decision{WorkerID: admitted[0].workerID, Reason: reason, Score: admitted[0].score}
}

func scoreCandidate(existingWorkerID, lastWorkerID string, candidate Candidate) Score {
	projectedAssigned := candidate.AssignedCount
	if candidate.WorkerID != existingWorkerID {
		projectedAssigned++
	}
	available := int64(candidate.Capacity) - projectedAssigned
	if available < 0 {
		available = 0
	}
	headroom := int(available * 100 / int64(candidate.Capacity))
	score := Score{CapacityHeadroom: headroom}
	if candidate.WorkerID == existingWorkerID {
		score.CurrentAssignment = currentAssignmentBonus
	} else if candidate.WorkerID == lastWorkerID {
		score.LastWorker = lastWorkerAffinityBonus
	}
	score.Total = score.CapacityHeadroom + score.CurrentAssignment + score.LastWorker
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
