package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/compforge/agentd/agentd/internal/model"
	"github.com/compforge/agentd/agentd/internal/service"
)

type clusterStatus struct{ pressure, blocked bool }

func (s clusterStatus) ResourcePressure() bool  { return s.pressure }
func (s clusterStatus) SchedulingBlocked() bool { return s.blocked }

func TestCurrentExecutionRefinesOnlyConfirmedProvisioningWait(t *testing.T) {
	now := time.Now().UTC()
	missing := model.WorkerObserverStatus{ObservedAt: now, Exists: false}
	pending := model.WorkerObserverStatus{ObservedAt: now, Exists: true, PodPhase: "Pending", Unschedulable: true}
	for _, tc := range []struct {
		name        string
		phase       model.WorkerPhase
		observation model.WorkerObserverStatus
		signal      clusterStatus
		want        error
	}{
		{"quota", model.WorkerPhaseCreating, missing, clusterStatus{pressure: true}, service.ErrClusterNoCapacity},
		{"scheduler resource shortage", model.WorkerPhaseCreating, pending, clusterStatus{true, true}, service.ErrClusterNoCapacity},
		{"scheduler constraints", model.WorkerPhaseCreating, pending, clusterStatus{blocked: true}, service.ErrWorkerUnschedulable},
		{"no evidence", model.WorkerPhaseCreating, pending, clusterStatus{}, nil},
		{"unknown observation", model.WorkerPhaseCreating, model.WorkerObserverStatus{}, clusterStatus{true, true}, nil},
		{"stale observation", model.WorkerPhaseCreating, model.WorkerObserverStatus{ObservedAt: now.Add(-time.Hour)}, clusterStatus{true, true}, nil},
		{"future observation", model.WorkerPhaseCreating, model.WorkerObserverStatus{ObservedAt: now.Add(time.Hour)}, clusterStatus{true, true}, nil},
		{"scheduled but not ready", model.WorkerPhaseCreating, model.WorkerObserverStatus{ObservedAt: now, Exists: true, PodPhase: "Pending"}, clusterStatus{true, true}, nil},
		{"active worker outage", model.WorkerPhaseActive, pending, clusterStatus{true, true}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, repository := newTestControl(t)
			application, err := service.New(repository, time.Minute, 1, service.WithClusterStatus(tc.signal))
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			raw, err := json.Marshal(tc.observation)
			if err != nil {
				t.Fatal(err)
			}
			if err := repository.PutWorker(ctx, model.Worker{ID: "worker", Name: "worker", Capacity: 1, Phase: tc.phase, ObserverStatus: raw, CreatedAt: now, UpdatedAt: now}); err != nil {
				t.Fatal(err)
			}
			original := model.Session{ID: "session", Status: model.SessionStatusRescheduling, Placement: model.SessionPlacement{WorkerID: "worker", Fence: "fence"}, CreatedAt: now, UpdatedAt: now}
			if err := repository.PutSession(ctx, original); err != nil {
				t.Fatal(err)
			}
			_, err = application.CurrentExecution(ctx, "session")
			if !errors.Is(err, service.ErrUnavailable) {
				t.Fatalf("lost retryable unavailability: %v", err)
			}
			if tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatalf("error=%v want=%v", err, tc.want)
			}
			if tc.want == nil && (errors.Is(err, service.ErrClusterNoCapacity) || errors.Is(err, service.ErrWorkerUnschedulable)) {
				t.Fatalf("unrelated outage reclassified: %v", err)
			}
			current, err := repository.GetSession(ctx, "session")
			if err != nil {
				t.Fatal(err)
			}
			if current.Status != original.Status || current.Placement != original.Placement {
				t.Fatal("diagnosis changed placement or terminal state")
			}
		})
	}
}
