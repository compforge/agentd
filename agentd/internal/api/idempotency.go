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
	if len(key) == 0 {
		return model.IdempotencyIdentity{}, nil
	}
	if len(key) > 255 {
		return model.IdempotencyIdentity{}, fmt.Errorf("%w: Idempotency-Key must be at most 255 bytes", service.ErrInvalid)
	}
	for _, char := range key {
		if char < 33 || char > 126 {
			return model.IdempotencyIdentity{}, fmt.Errorf("%w: Idempotency-Key must contain visible ASCII characters", service.ErrInvalid)
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var input any
	if err := decoder.Decode(&input); err != nil {
		return model.IdempotencyIdentity{}, fmt.Errorf("%w: invalid request JSON", service.ErrInvalid)
	}
	canonical, err := json.Marshal(input)
	if err != nil {
		return model.IdempotencyIdentity{}, err
	}
	digest := sha256.Sum256(canonical)
	return model.IdempotencyIdentity{ResourceType: "session", IdempotencyKey: string(key), RequestDigest: fmt.Sprintf("%x", digest)}, nil
}
