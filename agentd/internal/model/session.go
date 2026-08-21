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

// Session is both durable execution demand and its current Worker binding.
// A rescheduling Session without a Worker is pending capacity. AssignmentID is
// regenerated whenever the Session moves to another Worker and acts as a fence.
// LastWorkerID is a non-owning affinity hint and never reserves Worker capacity.
type Session struct {
	ID             string
	AgentID        string
	AgentVersion   int64
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
	AssignmentID   string
	WorkerID       string
	LastWorkerID   string
	AssignedAt     *time.Time
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// SessionObserverStatus is the latest Agentlet fact for one Assignment. The
// AssignmentID is part of the observation so a delayed response cannot be
// mistaken for the state of a replacement Work.
type SessionObserverStatus struct {
	ObservedAt     time.Time     `json:"observed_at"`
	AssignmentID   string        `json:"assignment_id"`
	Exists         bool          `json:"exists"`
	Status         SessionStatus `json:"status,omitempty"`
	ResumeRef      string        `json:"resume_ref,omitempty"`
	ResumeRevision int64         `json:"resume_revision"`
}

// Assignment is the current execution ownership of one managed Session. It is
// derived from Session binding fields and is not stored in a separate table.
type Assignment struct {
	ID        string
	SessionID string
	WorkerID  string
	CreatedAt time.Time
	UpdatedAt time.Time
}
