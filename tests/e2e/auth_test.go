//go:build e2e

package e2e_test

import (
	"context"
	"errors"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/compforge/case-harness/go/e2e/caserun"
)

func TestControlPlaneRequiresAPIKey(t *testing.T) {
	state := apiKeyAuthCaseState{}
	result := caserun.Run(
		context.Background(),
		caserun.Ref("agentd-managed-agent", "api_key_authentication"),
		nil,
		&state,
		caserun.Definition[apiKeyAuthCaseState]{
			Prepare: func(_ context.Context, state *apiKeyAuthCaseState) error {
				state.baseURL = strings.TrimRight(strings.TrimSpace(os.Getenv("AGENTD_E2E_BASE_URL")), "/")
				if state.baseURL == "" {
					if os.Getenv("AGENTD_REQUIRE_E2E") == "1" {
						return errors.New("AGENTD_E2E_BASE_URL is required")
					}
					return caserun.Skip("AGENTD_E2E_BASE_URL is not set; skipping live API authentication e2e")
				}
				state.apiKey = valueOr(os.Getenv("AGENTD_E2E_API_KEY"), "test")
				return nil
			},
			Execute: func(ctx context.Context, state *apiKeyAuthCaseState) error {
				var err error
				state.missing, err = authenticationStatus(ctx, state.baseURL, "")
				if err != nil {
					return err
				}
				state.wrong, err = authenticationStatus(ctx, state.baseURL, state.apiKey+"-wrong")
				if err != nil {
					return err
				}
				state.correct, err = authenticationStatus(ctx, state.baseURL, state.apiKey)
				return err
			},
			Judge: func(_ context.Context, state *apiKeyAuthCaseState) error {
				if state.missing != http.StatusUnauthorized || state.wrong != http.StatusUnauthorized ||
					state.correct != http.StatusOK {
					return caserun.Fail("public API did not enforce the configured x-api-key")
				}
				return nil
			},
			Budgets: systemCaseBudgets,
			Facets:  map[string]string{"boundary": "control-plane", "security": "authentication"},
		},
	)
	systemRun.Assert(t, result)
}

type apiKeyAuthCaseState struct {
	baseURL string
	apiKey  string
	missing int
	wrong   int
	correct int
}

func authenticationStatus(ctx context.Context, baseURL, apiKey string) (int, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/v1/models", nil)
	if err != nil {
		return 0, err
	}
	if apiKey != "" {
		request.Header.Set("x-api-key", apiKey)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return 0, err
	}
	response.Body.Close()
	return response.StatusCode, nil
}
