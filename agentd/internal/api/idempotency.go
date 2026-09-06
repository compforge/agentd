package api

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"

	"github.com/compforge/agentd/agentd/internal/model"
	"github.com/compforge/agentd/agentd/internal/service"
)

// sessionIdempotencyIdentity fingerprints the original input, not a resolved
// Agent version or mutable resource. JSON whitespace and object order do not
// change identity; array order and all supplied fields do.
func sessionIdempotencyIdentity(key, body []byte) (model.IdempotencyIdentity, error) {
	return requestIdempotencyIdentity(key, body, model.ResourceTypeSession, "")
}

func requestIdempotencyIdentity(key, body []byte, resourceType model.ResourceType, sessionID string) (model.IdempotencyIdentity, error) {
	if len(key) == 0 {
		return model.IdempotencyIdentity{}, nil
	}
	if len(key) > 255 {
		return model.IdempotencyIdentity{}, fmt.Errorf("%w: %s must be at most 255 bytes", service.ErrInvalid, idempotencyKeyHeader)
	}
	for _, char := range key {
		if char < 33 || char > 126 {
			return model.IdempotencyIdentity{}, fmt.Errorf("%w: %s must contain visible ASCII characters", service.ErrInvalid, idempotencyKeyHeader)
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var input any
	if err := decoder.Decode(&input); err != nil {
		return model.IdempotencyIdentity{}, fmt.Errorf("%w: invalid request JSON", service.ErrInvalid)
	}
	if sessionID != "" {
		// The target Session is request content, not another key scope. Reusing an
		// Event key for a different Session must not silently target that Session.
		input = map[string]any{"session_id": sessionID, "body": input}
	}
	canonical, err := json.Marshal(input)
	if err != nil {
		return model.IdempotencyIdentity{}, err
	}
	digest := sha256.Sum256(canonical)
	return model.IdempotencyIdentity{ResourceType: resourceType, IdempotencyKey: string(key), RequestDigest: fmt.Sprintf("%x", digest)}, nil
}
