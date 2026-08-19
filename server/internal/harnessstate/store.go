package harnessstate

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

var ErrConflict = errors.New("harness state revision conflict")

// Record is an opaque, versioned Harness State mutation. The Harness Adapter,
// not the store, owns Format and Data semantics.
type Record struct {
	Revision    int64           `json:"revision"`
	Format      string          `json:"format"`
	Data        json.RawMessage `json:"data"`
	CommittedAt time.Time       `json:"committed_at"`
}

type Store interface {
	Append(context.Context, string, int64, string, json.RawMessage) (Record, error)
	Load(context.Context, string) ([]Record, error)
}
