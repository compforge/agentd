package scheduler

import (
	"testing"
	"time"
)

func TestScheduleRetainsExistingWorker(t *testing.T) {
	now := time.Now().UTC()
	decision := New(time.Minute).Schedule(now, "worker-b", []Candidate{
		readyCandidate("worker-a", now, 0, 4),
		readyCandidate("worker-b", now, 4, 4),
	})
	if decision.WorkerID != "worker-b" || decision.Reason != ReasonExisting {
		t.Fatalf("Schedule() = %+v, want existing worker-b", decision)
	}
}

func TestScheduleSelectsLeastLoadedWorkerDeterministically(t *testing.T) {
	now := time.Now().UTC()
	decision := New(time.Minute).Schedule(now, "", []Candidate{
		readyCandidate("worker-c", now, 2, 4),
		readyCandidate("worker-b", now, 1, 4),
		readyCandidate("worker-a", now, 1, 4),
	})
	if decision.WorkerID != "worker-a" || decision.Reason != ReasonAvailable {
		t.Fatalf("Schedule() = %+v, want available worker-a", decision)
	}
}

func TestScheduleSkipsUnavailableWorkers(t *testing.T) {
	now := time.Now().UTC()
	stale := readyCandidate("worker-stale", now.Add(-time.Hour), 0, 4)
	full := readyCandidate("worker-full", now, 4, 4)
	notReady := readyCandidate("worker-not-ready", now, 0, 4)
	notReady.Observation.Ready = false

	decision := New(time.Minute).Schedule(now, "", []Candidate{stale, full, notReady})
	if decision.WorkerID != "" || decision.Reason != ReasonNoCapacity {
		t.Fatalf("Schedule() = %+v, want no capacity", decision)
	}
}

func readyCandidate(workerID string, observedAt time.Time, assignedRuns int64, maxRuns int) Candidate {
	return Candidate{
		WorkerID: workerID, MaxRuns: maxRuns, AssignedRuns: assignedRuns,
		Observation: Observation{
			ObservedAt: observedAt, Exists: true, Ready: true, Endpoint: "http://" + workerID,
		},
	}
}
