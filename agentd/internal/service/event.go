package service

import (
	"context"
	"errors"
	"fmt"

	agentledger "github.com/compforge/agent-ledger/go"
	"github.com/compforge/agentd/agentd/internal/model"
	managedevent "github.com/compforge/agentd/internal/event"
)

// AcceptUserMessage runs inside the resource transaction supplied by Idempotency.
// The immutable Ledger Event itself is the receipt; no HTTP snapshot is needed.
// +spec=`A keyed user.message is accepted once, including concurrent retries; replay precedes mutable Session and tool-state validation`
// +link=agentd/docs/agentd.md#资源请求与一致性
func (a *Service) AcceptUserMessage(ctx context.Context, events *managedevent.Log, sessionID string, identity model.IdempotencyIdentity, content any) (managedevent.ManagedEvent, bool, error) {
	if identity.ResourceType != model.ResourceTypeEvent || identity.IdempotencyKey == "" {
		return nil, false, fmt.Errorf("%w: an Event idempotency key is required", ErrInvalid)
	}
	session, err := a.repository.GetSessionForUpdate(ctx, sessionID)
	if err != nil {
		return nil, false, err
	}
	requestID := managedevent.UserMessageRequestID(identity.IdempotencyKey)
	previous, digest, exists, err := events.FindUserMessageRequest(ctx, sessionID, requestID)
	if err != nil {
		return nil, false, err
	}
	if exists {
		if digest != identity.RequestDigest {
			return nil, false, ErrIdempotencyConflict
		}
		return previous, true, nil
	}
	if session.ArchivedAt != nil || session.Status == model.SessionStatusTerminated {
		return nil, false, fmt.Errorf("%w: Session %q is terminated or archived", ErrConflict, sessionID)
	}
	blocking, err := events.UnresolvedToolUses(ctx, sessionID)
	if err != nil {
		return nil, false, err
	}
	if len(blocking) > 0 {
		return nil, false, fmt.Errorf("%w: Session requires tool action before accepting a new message", ErrConflict)
	}
	accepted, err := events.AppendUserMessageRequest(ctx, sessionID, requestID, identity.RequestDigest, content)
	if errors.Is(err, agentledger.ErrIdempotencyViolation) {
		return nil, false, ErrIdempotencyConflict
	}
	if err != nil {
		return nil, false, fmt.Errorf("persist user.message: %w", err)
	}
	return accepted, false, nil
}
