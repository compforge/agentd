package model

import "time"

// IdempotencyIdentity names a request within a resource type. Keys are opaque and
// case-sensitive; RequestDigest detects accidental key reuse with other input.
type IdempotencyIdentity struct {
	IdempotencyKey string
	ResourceType   string
	RequestDigest  string
}

// IdempotencyResponse is an opaque API receipt, not the current resource state.
type IdempotencyResponse struct {
	StatusCode int
	Body       []byte
}

type IdempotencyRecord struct {
	ID string
	IdempotencyIdentity
	IdempotencyResponse
	CreatedAt time.Time
}
