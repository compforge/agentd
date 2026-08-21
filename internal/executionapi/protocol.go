// Package executionapi defines the private HTTP contract between agentd and Agentlet.
package executionapi

import "time"

const (
	AssignmentHeader = "X-Agentd-Assignment-ID"
	WorkerHeader     = "X-Agentd-Worker-ID"
)

// WorkSpec is the complete control-plane slice an Agentlet needs to execute a
// Session. Agentlet treats it as a replaceable snapshot, never as global truth.
type WorkSpec struct {
	AssignmentID string              `json:"assignment_id"`
	WorkerID     string              `json:"worker_id"`
	Session      SessionSnapshot     `json:"session"`
	Agent        AgentSnapshot       `json:"agent"`
	Environment  EnvironmentSnapshot `json:"environment"`
}

type AgentSnapshot struct {
	ID          string           `json:"id"`
	Name        string           `json:"name"`
	Description string           `json:"description"`
	Model       ModelSnapshot    `json:"model"`
	System      string           `json:"system"`
	Tools       []map[string]any `json:"tools"`
	Version     int64            `json:"version"`
}

// ModelSnapshot contains the external model connection needed by the assigned
// Harness. It is private execution data and must never be projected as an Event.
type ModelSnapshot struct {
	ID         string `json:"id"`
	Provider   string `json:"provider"`
	UpstreamID string `json:"upstream_id"`
	BaseURL    string `json:"base_url,omitempty"`
	APIKey     string `json:"api_key"`
}

type EnvironmentSnapshot struct {
	ID     string         `json:"id"`
	Config map[string]any `json:"config"`
}

type SessionSnapshot struct {
	ID             string            `json:"id"`
	EnvironmentID  string            `json:"environment_id"`
	Title          string            `json:"title"`
	Metadata       map[string]string `json:"metadata"`
	Status         string            `json:"status"`
	Revision       int64             `json:"revision"`
	Harness        string            `json:"harness"`
	HarnessVersion string            `json:"harness_version"`
	ResumeRef      string            `json:"resume_ref"`
	ResumeRevision int64             `json:"resume_revision"`
	CreatedAt      time.Time         `json:"created_at"`
	UpdatedAt      time.Time         `json:"updated_at"`
}

// SessionState is Agentlet's observed execution state for the current
// Assignment. agentd accepts it only while the Assignment fence still matches.
type SessionState struct {
	AssignmentID   string `json:"assignment_id"`
	Status         string `json:"status"`
	ResumeRef      string `json:"resume_ref"`
	ResumeRevision int64  `json:"resume_revision"`
}
