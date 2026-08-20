package model

import (
	"encoding/json"
	"time"
)

type WorkerPhase string

const (
	WorkerPhaseCreating WorkerPhase = "creating"
	WorkerPhaseActive   WorkerPhase = "active"
	WorkerPhaseDraining WorkerPhase = "draining"
	WorkerPhaseRetired  WorkerPhase = "retired"
)

// Worker is one managed Agentlet Pod and the capacity unit agentd schedules.
type Worker struct {
	ID             string
	Name           string
	Capacity       int
	Phase          WorkerPhase
	ObserverStatus json.RawMessage
	IdleSince      *time.Time
	AbsentAt       *time.Time
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

type WorkerObserverStatus struct {
	ObservedAt    time.Time `json:"observed_at"`
	Exists        bool      `json:"exists"`
	Ready         bool      `json:"ready"`
	Endpoint      string    `json:"endpoint,omitempty"`
	PodUID        string    `json:"pod_uid,omitempty"`
	PodPhase      string    `json:"pod_phase,omitempty"`
	Unschedulable bool      `json:"unschedulable,omitempty"`
}
