package observer

import (
	"context"
	"fmt"

	"github.com/compforge/agentd/agentd/internal/model"
	"github.com/compforge/agentd/agentd/internal/service"
	"github.com/compforge/agentd/agentd/internal/session/connector"
	"github.com/compforge/agentd/internal/executionapi"
)

type executionResolver interface {
	CurrentExecution(context.Context, string) (service.ExecutionTarget, error)
}

type stateClient interface {
	SessionState(context.Context, connector.Target) (executionapi.SessionState, error)
}

type AgentletSource struct {
	resolver executionResolver
	client   stateClient
}

func NewAgentletSource(resolver executionResolver, client stateClient) (*AgentletSource, error) {
	if resolver == nil || client == nil {
		return nil, fmt.Errorf("create Agentlet Session source: resolver and client are required")
	}
	return &AgentletSource{resolver: resolver, client: client}, nil
}

func (s *AgentletSource) ObserveSession(
	ctx context.Context,
	session model.Session,
) (executionapi.SessionState, error) {
	target, err := s.resolver.CurrentExecution(ctx, session.ID)
	if err != nil {
		return executionapi.SessionState{}, err
	}
	return s.client.SessionState(ctx, connector.Target{Endpoint: target.Endpoint, Work: target.Work})
}
