package model

import "time"

type Agent struct {
	ID          string
	VersionID   string
	Name        string
	Description string
	ModelID     string
	System      string
	Tools       []map[string]any
	Metadata    map[string]string
	Version     int64
	ArchivedAt  *time.Time
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// Model is a control-plane registration for an external model service.
// APIKey is write-only at the public API boundary and is only sent to the
// assigned Agentlet as part of an internal WorkSpec.
type Model struct {
	ID         string
	Provider   string
	UpstreamID string
	BaseURL    string
	APIKey     string
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

type Environment struct {
	ID          string
	Name        string
	Description string
	Config      map[string]any
	Metadata    map[string]string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}
