package model

import "time"

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
	AssignmentID   string
	WorkerID       string
	AssignedAt     *time.Time
	CreatedAt      time.Time
	UpdatedAt      time.Time
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
