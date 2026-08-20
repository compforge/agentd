package model

import "time"

type Agent struct {
	ID          string
	Name        string
	Description string
	ModelID     string
	System      string
	Tools       []map[string]any
	Metadata    map[string]string
	Version     int64
	CreatedAt   time.Time
	UpdatedAt   time.Time
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
