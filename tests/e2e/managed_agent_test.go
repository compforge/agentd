//go:build e2e

package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"
	"github.com/anthropics/anthropic-sdk-go/packages/param"
	"github.com/compforge/case-harness/go/e2e/caserun"
)

func TestManagedAgentResumesAcrossSandboxTurns(t *testing.T) {
	state := managedAgentCaseState{}
	result := caserun.Run(
		context.Background(),
		caserun.Ref("agentd-managed-agent", "sandbox_resume"),
		nil,
		&state,
		caserun.Definition[managedAgentCaseState]{
			Prepare: prepareManagedAgentCase,
			Execute: func(ctx context.Context, state *managedAgentCaseState) error {
				ctx, cancel := context.WithTimeout(ctx, state.config.timeout)
				defer cancel()
				if err := runTurnResult(ctx, state.client, state.sessionID, "Run the required sandbox check for turn one."); err != nil {
					return err
				}
				if err := runTurnResult(ctx, state.client, state.sessionID, "Run the required sandbox check again for turn two."); err != nil {
					return err
				}
				messages, err := agentMessagesResult(ctx, state.client, state.sessionID)
				state.agentMessages = len(messages)
				return err
			},
			Judge: func(_ context.Context, state *managedAgentCaseState) error {
				if state.agentMessages < 2 {
					return caserun.Fail(fmt.Sprintf("agent messages = %d, want at least 2", state.agentMessages))
				}
				return nil
			},
			Budgets: systemCaseBudgets,
			Facets:  map[string]string{"boundary": "system", "recovery": "checkpoint"},
		},
	)
	recordSystemCase(t, result)
}

const sandboxResumeSystemPrompt = "For every user request, call bash exactly once with command `printf AGENTD_E2E_SANDBOX_OK`. After the tool succeeds, answer exactly AGENTD_E2E_OK."

type managedAgentCaseState struct {
	config        envConfig
	client        *anthropic.Client
	sessionID     string
	agentMessages int
}

func prepareManagedAgentCase(ctx context.Context, state *managedAgentCaseState) error {
	return prepareManagedAgentCaseWithSystem(ctx, state, sandboxResumeSystemPrompt)
}

func prepareManagedAgentCaseWithSystem(
	ctx context.Context,
	state *managedAgentCaseState,
	systemPrompt string,
) error {
	config, err := readEnv()
	if err != nil {
		return err
	}
	client := newManagedAgentClient(config)
	suffix := fmt.Sprintf("%d", time.Now().UTC().UnixNano())
	modelResourceID := "agentd-e2e-model-" + suffix
	if _, err := registerModelResult(ctx, config, modelResourceID); err != nil {
		return err
	}
	agent, err := client.Beta.Agents.New(ctx, anthropic.BetaAgentNewParams{
		Name:   "agentd-e2e-" + suffix,
		Model:  anthropic.BetaManagedAgentsModelConfigParams{ID: anthropic.BetaManagedAgentsModel(modelResourceID)},
		System: anthropic.String(systemPrompt),
		Tools: []anthropic.BetaAgentNewParamsToolUnion{{
			OfAgentToolset20260401: &anthropic.BetaManagedAgentsAgentToolset20260401Params{
				Type: anthropic.BetaManagedAgentsAgentToolset20260401ParamsTypeAgentToolset20260401,
			},
		}},
	})
	if err != nil {
		return fmt.Errorf("create Agent: %w", err)
	}
	unrestricted := anthropic.NewBetaUnrestrictedNetworkParam()
	environment, err := client.Beta.Environments.New(ctx, anthropic.BetaEnvironmentNewParams{
		Name: "agentd-e2e-" + suffix,
		Config: anthropic.BetaEnvironmentNewParamsConfigUnion{OfCloud: &anthropic.BetaCloudConfigParams{
			Networking: anthropic.BetaCloudConfigParamsNetworkingUnion{OfUnrestricted: &unrestricted},
		}},
	})
	if err != nil {
		return fmt.Errorf("create Environment: %w", err)
	}
	session, err := client.Beta.Sessions.New(ctx, anthropic.BetaSessionNewParams{
		Agent:         anthropic.BetaSessionNewParamsAgentUnion{OfString: param.NewOpt(agent.ID)},
		EnvironmentID: environment.ID,
		Title:         anthropic.String("agentd system e2e " + suffix),
	})
	if err != nil {
		return fmt.Errorf("create Session: %w", err)
	}
	state.config = config
	state.client = client
	state.sessionID = session.ID
	return nil
}

func newManagedAgentClient(config envConfig) *anthropic.Client {
	client := anthropic.NewClient(
		option.WithoutEnvironmentDefaults(),
		option.WithAPIKey(config.apiKey),
		option.WithBaseURL(config.baseURL),
	)
	return &client
}

func registerModelResult(ctx context.Context, config envConfig, resourceID string) ([]byte, error) {
	payload, err := json.Marshal(map[string]string{
		"id": resourceID, "provider": config.modelProvider, "model": config.model,
		"base_url": config.modelBaseURL, "api_key": config.modelAPIKey,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal Model: %w", err)
	}
	body, err := modelRequestResult(ctx, config, http.MethodPost, "/v1/models", payload)
	if err != nil {
		return nil, err
	}
	if bytes.Contains(body, []byte(config.modelAPIKey)) {
		return nil, errors.New("create Model response exposed api_key")
	}
	return body, nil
}

func modelRequestResult(
	ctx context.Context,
	config envConfig,
	method string,
	path string,
	payload []byte,
) ([]byte, error) {
	request, err := http.NewRequestWithContext(ctx, method, config.baseURL+path, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("build Model request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Api-Key", config.apiKey)
	response, err := (&http.Client{Timeout: 30 * time.Second}).Do(request)
	if err != nil {
		return nil, fmt.Errorf("request Model: %w", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("read Model response: %w", err)
	}
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Model request status=%d body=%s", response.StatusCode, body)
	}
	return body, nil
}

func runTurnResult(ctx context.Context, client *anthropic.Client, sessionID, prompt string) error {
	messages, err := agentMessagesResult(ctx, client, sessionID)
	if err != nil {
		return err
	}
	before := len(messages)
	if err := sendTurnResult(ctx, client, sessionID, prompt); err != nil {
		return err
	}
	return waitForTurnResult(ctx, client, sessionID, prompt, before)
}

func sendTurnResult(ctx context.Context, client *anthropic.Client, sessionID, prompt string) error {
	_, err := client.Beta.Sessions.Events.Send(ctx, sessionID, anthropic.BetaSessionEventSendParams{
		Events: []anthropic.BetaManagedAgentsEventParamsUnion{{
			OfUserMessage: &anthropic.BetaManagedAgentsUserMessageEventParams{
				Type: anthropic.BetaManagedAgentsUserMessageEventParamsTypeUserMessage,
				Content: []anthropic.BetaManagedAgentsUserMessageEventParamsContentUnion{{
					OfText: &anthropic.BetaManagedAgentsTextBlockParam{
						Type: anthropic.BetaManagedAgentsTextBlockTypeText,
						Text: prompt,
					},
				}},
			},
		}},
	})
	if err != nil {
		return fmt.Errorf("send user Event: %w", err)
	}
	return nil
}

func waitForTurnResult(
	ctx context.Context,
	client *anthropic.Client,
	sessionID string,
	prompt string,
	before int,
) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		current, err := client.Beta.Sessions.Get(ctx, sessionID, anthropic.BetaSessionGetParams{})
		if err != nil {
			return fmt.Errorf("get Session while waiting: %w", err)
		}
		if current.Status == anthropic.BetaManagedAgentsSessionStatusTerminated {
			return fmt.Errorf("Session terminated while processing %q", prompt)
		}
		messages, err := agentMessagesResult(ctx, client, sessionID)
		if err != nil {
			return err
		}
		if len(messages) > before && current.Status == anthropic.BetaManagedAgentsSessionStatusIdle {
			if !strings.Contains(messages[len(messages)-1].RawJSON(), "AGENTD_E2E_OK") {
				return fmt.Errorf("final agent.message did not contain the sandbox turn marker: %s", messages[len(messages)-1].RawJSON())
			}
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for completed turn: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func agentMessagesResult(
	ctx context.Context,
	client *anthropic.Client,
	sessionID string,
) ([]anthropic.BetaManagedAgentsSessionEventUnion, error) {
	page, err := client.Beta.Sessions.Events.List(ctx, sessionID, anthropic.BetaSessionEventListParams{})
	if err != nil {
		return nil, fmt.Errorf("list Session Events: %w", err)
	}
	messages := make([]anthropic.BetaManagedAgentsSessionEventUnion, 0)
	for _, event := range page.Data {
		if event.Type == "agent.message" {
			messages = append(messages, event)
		}
	}
	return messages, nil
}

type envConfig struct {
	baseURL       string
	apiKey        string
	model         string
	modelProvider string
	modelBaseURL  string
	modelAPIKey   string
	timeout       time.Duration
}

func readEnv() (envConfig, error) {
	baseURL := strings.TrimRight(strings.TrimSpace(os.Getenv("AGENTD_E2E_BASE_URL")), "/")
	if baseURL == "" {
		if os.Getenv("AGENTD_REQUIRE_E2E") == "1" {
			return envConfig{}, errors.New("AGENTD_E2E_BASE_URL is required")
		}
		return envConfig{}, caserun.Skip("AGENTD_E2E_BASE_URL is not set; skipping live system e2e")
	}
	modelAPIKey := strings.TrimSpace(os.Getenv("AGENTD_E2E_MODEL_API_KEY"))
	if modelAPIKey == "" {
		modelAPIKey = strings.TrimSpace(os.Getenv("ANTHROPIC_API_KEY"))
	}
	if modelAPIKey == "" {
		if os.Getenv("AGENTD_REQUIRE_E2E") == "1" {
			return envConfig{}, errors.New("AGENTD_E2E_MODEL_API_KEY is required")
		}
		return envConfig{}, caserun.Skip("AGENTD_E2E_MODEL_API_KEY is not set; skipping live system e2e")
	}
	timeout := 10 * time.Minute
	if raw := os.Getenv("AGENTD_E2E_TIMEOUT"); raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil || parsed <= 0 {
			return envConfig{}, fmt.Errorf("AGENTD_E2E_TIMEOUT must be a positive duration, got %q", raw)
		}
		timeout = parsed
	}
	return envConfig{
		baseURL:       baseURL,
		apiKey:        valueOr(os.Getenv("AGENTD_E2E_API_KEY"), "test"),
		model:         valueOr(os.Getenv("AGENTD_E2E_MODEL"), "claude-sonnet-4-6"),
		modelProvider: valueOr(os.Getenv("AGENTD_E2E_MODEL_PROVIDER"), "anthropic"),
		modelBaseURL:  strings.TrimSpace(os.Getenv("AGENTD_E2E_MODEL_BASE_URL")),
		modelAPIKey:   modelAPIKey,
		timeout:       timeout,
	}, nil
}

func valueOr(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
