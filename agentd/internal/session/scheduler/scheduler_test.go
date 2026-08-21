package scheduler

import (
	"testing"
	"time"
)

func TestSchedulePrefersCurrentWorkerAtEqualProjectedLoad(t *testing.T) {
	now := time.Now().UTC()
	decision := New(time.Minute).Schedule(now, "worker-b", "", []Candidate{
		readyCandidate("worker-a", now, 1, 4),
		readyCandidate("worker-b", now, 2, 4),
	})
	if decision.WorkerID != "worker-b" || decision.Reason != ReasonExisting {
		t.Fatalf("Schedule() = %+v, want existing worker-b", decision)
	}
}

func TestScheduleMovesFromCurrentWorkerWhenHeadroomOutweighsAffinity(t *testing.T) {
	now := time.Now().UTC()
	decision := New(time.Minute).Schedule(now, "worker-b", "worker-b", []Candidate{
		readyCandidate("worker-a", now, 0, 4),
		readyCandidate("worker-b", now, 4, 4),
	})
	if decision.WorkerID != "worker-a" || decision.Reason != ReasonAvailable {
		t.Fatalf("Schedule() = %+v, want available worker-a", decision)
	}
}

func TestSchedulePrefersLastWorkerWithCapacity(t *testing.T) {
	now := time.Now().UTC()
	decision := New(time.Minute).Schedule(now, "", "worker-b", []Candidate{
		readyCandidate("worker-a", now, 2, 4),
		readyCandidate("worker-b", now, 2, 4),
	})
	if decision.WorkerID != "worker-b" || decision.Reason != ReasonAffinity {
		t.Fatalf("Schedule() = %+v, want affinity worker-b", decision)
	}
}

func TestScheduleMovesWhenHeadroomOutweighsAffinity(t *testing.T) {
	now := time.Now().UTC()
	decision := New(time.Minute).Schedule(now, "", "worker-b", []Candidate{
		readyCandidate("worker-a", now, 0, 4),
		readyCandidate("worker-b", now, 2, 4),
	})
	if decision.WorkerID != "worker-a" || decision.Reason != ReasonAvailable {
		t.Fatalf("Schedule() = %+v, want available worker-a", decision)
	}
}

func TestScheduleSelectsLeastLoadedWorkerDeterministically(t *testing.T) {
	now := time.Now().UTC()
	decision := New(time.Minute).Schedule(now, "", "", []Candidate{
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

	decision := New(time.Minute).Schedule(now, "", "worker-full", []Candidate{stale, full, notReady})
	if decision.WorkerID != "" || decision.Reason != ReasonNoCapacity {
		t.Fatalf("Schedule() = %+v, want no capacity", decision)
	}
}

func readyCandidate(workerID string, observedAt time.Time, assignedCount int64, capacity int) Candidate {
	return Candidate{
		WorkerID: workerID, Capacity: capacity, AssignedCount: assignedCount,
		Observation: Observation{
			ObservedAt: observedAt, Exists: true, Ready: true, Endpoint: "http://" + workerID,
		},
	}
}
