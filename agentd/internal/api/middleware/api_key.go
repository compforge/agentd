package middleware

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"

	hertzapp "github.com/cloudwego/hertz/pkg/app"
)

const APIKeyHeader = "x-api-key"

var errAuthentication = errors.New("invalid x-api-key")

type APIKey struct {
	digest [sha256.Size]byte
}

func NewAPIKey(value string) APIKey {
	return APIKey{digest: sha256.Sum256([]byte(value))}
}

// Handle protects the public resource API while keeping health probes
// independent of client credentials.
//
// +spec=`Every public /v1 request requires the configured x-api-key; /healthz remains anonymous and authentication failures use the public error envelope`
// +case:id=api_key_authentication,desc=`call a public API without a key, with a wrong key, and with the configured key`,expect=`the first two requests return authentication errors and the configured key succeeds`,forbid=`protecting health probes or accepting an invalid key`,group=system
// +why=`A deployment-level key closes the control-plane trust boundary without prematurely introducing tenant identity or authorization into the resource model`
// +link=agentd/docs/kernel.md
// +link=tests/e2e/cases/managed-agent.yaml
func (apiKey APIKey) Handle(ctx context.Context, request *hertzapp.RequestContext) {
	if string(request.Request.URI().Path()) == "/healthz" {
		request.Next(ctx)
		return
	}
	provided := request.Request.Header.Peek(APIKeyHeader)
	providedDigest := sha256.Sum256(provided)
	if len(provided) > 0 && subtle.ConstantTimeCompare(providedDigest[:], apiKey.digest[:]) == 1 {
		request.Next(ctx)
		return
	}

	_ = request.Error(errAuthentication)
	request.Abort()
}
