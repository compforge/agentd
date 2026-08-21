//go:build e2e

package e2e_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/compforge/case-harness/go/e2e/caserun"
)

func TestModelCredentialRemainsWriteOnly(t *testing.T) {
	state := modelSecretCaseState{}
	result := caserun.Run(
		context.Background(),
		caserun.Ref("agentd-managed-agent", "model_secret_redaction"),
		nil,
		&state,
		caserun.Definition[modelSecretCaseState]{
			Prepare: func(_ context.Context, state *modelSecretCaseState) error {
				config, err := readEnv()
				if err != nil {
					return err
				}
				state.config = config
				state.resourceID = "agentd-e2e-redaction-" + fmt.Sprintf("%d", time.Now().UTC().UnixNano())
				return nil
			},
			Execute: func(ctx context.Context, state *modelSecretCaseState) error {
				var err error
				state.responses = make([][]byte, 3)
				state.responses[0], err = registerModelResult(ctx, state.config, state.resourceID)
				if err != nil {
					return err
				}
				state.responses[1], err = modelRequestResult(
					ctx, state.config, http.MethodGet, "/v1/models/"+state.resourceID, nil,
				)
				if err != nil {
					return err
				}
				state.responses[2], err = modelRequestResult(ctx, state.config, http.MethodGet, "/v1/models", nil)
				return err
			},
			Judge: func(_ context.Context, state *modelSecretCaseState) error {
				for index, response := range state.responses {
					if !bytes.Contains(response, []byte(state.resourceID)) {
						return caserun.Fail(fmt.Sprintf("Model response %d omitted resource id", index))
					}
					if !bytes.Contains(response, []byte(`"api_key_configured":true`)) {
						return caserun.Fail(fmt.Sprintf("Model response %d did not report configured credential", index))
					}
					if bytes.Contains(response, []byte(state.config.modelAPIKey)) ||
						bytes.Contains(response, []byte(`"api_key":`)) {
						return caserun.Fail(fmt.Sprintf("Model response %d exposed api_key", index))
					}
				}
				return nil
			},
			Budgets: systemCaseBudgets,
			Facets:  map[string]string{"boundary": "control-plane", "security": "credential"},
		},
	)
	systemRun.Assert(t, result)
}

type modelSecretCaseState struct {
	config     envConfig
	resourceID string
	responses  [][]byte
}
