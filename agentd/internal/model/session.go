package model

import (
	"encoding/json"
	"time"
)

type SessionStatus string

const (
	SessionStatusIdle         SessionStatus = "idle"
	SessionStatusRunning      SessionStatus = "running"
	SessionStatusRescheduling SessionStatus = "rescheduling"
	SessionStatusTerminated   SessionStatus = "terminated"
)

// Session is durable Control State and its current Worker placement. Durable
// execution demand comes from unprocessed Events; rescheduling without a
// placement projects that demand as pending capacity.
// LastWorkerID is a non-owning affinity hint and never reserves Worker capacity.
type Session struct {
	ID             string
	AgentID        string
	AgentVersionID string
	EnvironmentID  string
	Title          string
	Metadata       map[string]string
	Status         SessionStatus
	Revision       int64
	Harness        string
	HarnessVersion string
	ResumeRef      string
	ResumeRevision int64
	ObserverStatus json.RawMessage
	Placement      SessionPlacement
	LastWorkerID   string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// SessionPlacement is the current execution location embedded in Session
// Control State. Fence changes whenever execution moves to another Worker.
type SessionPlacement struct {
	WorkerID string
	Fence    string
	PlacedAt *time.Time
}

func (p SessionPlacement) Bound() bool {
	return p.WorkerID != "" && p.Fence != ""
}

// SessionObserverStatus is the latest Agentlet fact for one placement. The
// fence prevents a delayed response from being mistaken for replacement Work.
type SessionObserverStatus struct {
	ObservedAt     time.Time     `json:"observed_at"`
	PlacementFence string        `json:"assignment_id"`
	Exists         bool          `json:"exists"`
	Status         SessionStatus `json:"status,omitempty"`
	ResumeRef      string        `json:"resume_ref,omitempty"`
	ResumeRevision int64         `json:"resume_revision"`
}
