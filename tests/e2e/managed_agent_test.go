//go:build e2e

package e2e_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"
	"github.com/anthropics/anthropic-sdk-go/packages/param"
)

func TestManagedAgentResumesAcrossSandboxTurns(t *testing.T) {
	config := loadEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), config.timeout)
	defer cancel()

	client := anthropic.NewClient(
		option.WithoutEnvironmentDefaults(),
		option.WithAPIKey(config.apiKey),
		option.WithBaseURL(config.baseURL),
	)
	suffix := fmt.Sprintf("%d", time.Now().UTC().UnixNano())
	agent, err := client.Beta.Agents.New(ctx, anthropic.BetaAgentNewParams{
		Name:   "agentd-e2e-" + suffix,
		Model:  anthropic.BetaManagedAgentsModelConfigParams{ID: config.model},
		System: anthropic.String("For every user request, call bash exactly once with command `printf AGENTD_E2E_SANDBOX_OK`. After the tool succeeds, answer exactly AGENTD_E2E_OK."),
		Tools: []anthropic.BetaAgentNewParamsToolUnion{{
			OfAgentToolset20260401: &anthropic.BetaManagedAgentsAgentToolset20260401Params{
				Type: anthropic.BetaManagedAgentsAgentToolset20260401ParamsTypeAgentToolset20260401,
			},
		}},
	})
	if err != nil {
		t.Fatalf("create Agent: %v", err)
	}
	unrestricted := anthropic.NewBetaUnrestrictedNetworkParam()
	environment, err := client.Beta.Environments.New(ctx, anthropic.BetaEnvironmentNewParams{
		Name: "agentd-e2e-" + suffix,
		Config: anthropic.BetaEnvironmentNewParamsConfigUnion{OfCloud: &anthropic.BetaCloudConfigParams{
			Networking: anthropic.BetaCloudConfigParamsNetworkingUnion{OfUnrestricted: &unrestricted},
		}},
	})
	if err != nil {
		t.Fatalf("create Environment: %v", err)
	}
	session, err := client.Beta.Sessions.New(ctx, anthropic.BetaSessionNewParams{
		Agent:         anthropic.BetaSessionNewParamsAgentUnion{OfString: param.NewOpt(agent.ID)},
		EnvironmentID: environment.ID,
		Title:         anthropic.String("agentd system e2e " + suffix),
	})
	if err != nil {
		t.Fatalf("create Session: %v", err)
	}

	runTurn(t, ctx, &client, session.ID, "Run the required sandbox check for turn one.")
	runTurn(t, ctx, &client, session.ID, "Run the required sandbox check again for turn two.")
}

func runTurn(t *testing.T, ctx context.Context, client *anthropic.Client, sessionID, prompt string) {
	t.Helper()
	before := len(agentMessages(t, ctx, client, sessionID))
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
		t.Fatalf("send user Event: %v", err)
	}

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		current, err := client.Beta.Sessions.Get(ctx, sessionID, anthropic.BetaSessionGetParams{})
		if err != nil {
			t.Fatalf("get Session while waiting: %v", err)
		}
		if current.Status == anthropic.BetaManagedAgentsSessionStatusTerminated {
			t.Fatalf("Session terminated while processing %q", prompt)
		}
		messages := agentMessages(t, ctx, client, sessionID)
		if len(messages) > before && current.Status == anthropic.BetaManagedAgentsSessionStatusIdle {
			if !strings.Contains(messages[len(messages)-1].RawJSON(), "AGENTD_E2E_OK") {
				t.Fatalf("final agent.message did not contain the sandbox turn marker: %s", messages[len(messages)-1].RawJSON())
			}
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for completed turn: %v", ctx.Err())
		case <-ticker.C:
		}
	}
}

func agentMessages(
	t *testing.T,
	ctx context.Context,
	client *anthropic.Client,
	sessionID string,
) []anthropic.BetaManagedAgentsSessionEventUnion {
	t.Helper()
	page, err := client.Beta.Sessions.Events.List(ctx, sessionID, anthropic.BetaSessionEventListParams{})
	if err != nil {
		t.Fatalf("list Session Events: %v", err)
	}
	messages := make([]anthropic.BetaManagedAgentsSessionEventUnion, 0)
	for _, event := range page.Data {
		if event.Type == "agent.message" {
			messages = append(messages, event)
		}
	}
	return messages
}

type envConfig struct {
	baseURL string
	apiKey  string
	model   string
	timeout time.Duration
}

func loadEnv(t *testing.T) envConfig {
	t.Helper()
	baseURL := strings.TrimRight(strings.TrimSpace(os.Getenv("AGENTD_E2E_BASE_URL")), "/")
	if baseURL == "" {
		if os.Getenv("AGENTD_REQUIRE_E2E") == "1" {
			t.Fatal("AGENTD_E2E_BASE_URL is required")
		}
		t.Skip("AGENTD_E2E_BASE_URL is not set; skipping live system e2e")
	}
	timeout := 10 * time.Minute
	if raw := os.Getenv("AGENTD_E2E_TIMEOUT"); raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil || parsed <= 0 {
			t.Fatalf("AGENTD_E2E_TIMEOUT must be a positive duration, got %q", raw)
		}
		timeout = parsed
	}
	return envConfig{
		baseURL: baseURL,
		apiKey:  valueOr(os.Getenv("AGENTD_E2E_API_KEY"), "test"),
		model:   valueOr(os.Getenv("AGENTD_E2E_MODEL"), "claude-sonnet-4-6"),
		timeout: timeout,
	}
}

func valueOr(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
