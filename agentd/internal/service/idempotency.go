package service

import (
	"context"
	"errors"
	"fmt"

	"github.com/compforge/agentd/agentd/internal/model"
	"github.com/compforge/agentd/agentd/internal/repo"
	managedevent "github.com/compforge/agentd/internal/event"
)

var ErrIdempotencyConflict = errors.New("idempotency key reused with different content")

// Idempotency guards a resource command; it is not a CRUD service for keys.
// Resource handlers retain their validation, business logic and response shape.
type Idempotency struct {
	store   repo.IdempotencyStore
	control *Service
}

func NewIdempotency(store repo.IdempotencyStore, control *Service) *Idempotency {
	return &Idempotency{store: store, control: control}
}

// Run executes a database-only resource command and its API projection in one transaction.
// The API supplies encoding so service/repo never depend on wire view types.
// An empty identity uses the same atomic command path without storing a receipt.
// +spec=`Repeated keys within a resource type replay the original successful response without rerunning the command; mismatched content is rejected`
// +why=`A durable receipt is insufficient if input can commit separately before the receipt exists`
// +link=agentd/docs/idempotency.md
func (s *Idempotency) Run(ctx context.Context, identity model.IdempotencyIdentity, command func(context.Context, *Service, *managedevent.Log) (model.IdempotencyResponse, error)) (model.IdempotencyResponse, bool, error) {
	if identity.IdempotencyKey != "" {
		previous, err := s.store.GetIdempotencyKey(ctx, identity)
		if err == nil {
			return replayIdempotency(identity, previous)
		}
		if !errors.Is(err, repo.ErrNotFound) {
			return model.IdempotencyResponse{}, false, err
		}
	}
	var response model.IdempotencyResponse
	err := s.store.Transaction(ctx, func(txCtx context.Context, repository repo.Repository, events *managedevent.Log, receipts repo.IdempotencyRepository) error {
		if identity.IdempotencyKey != "" {
			if err := receipts.CreateIdempotencyKey(txCtx, identity); err != nil {
				return err
			}
		}
		control := *s.control
		control.repository = repository
		var err error
		response, err = command(txCtx, &control, events)
		if err != nil {
			return err
		}
		if identity.IdempotencyKey == "" {
			return nil
		}
		return receipts.SaveIdempotencyResponse(txCtx, identity, response)
	})
	if errors.Is(err, repo.ErrIdempotencyKeyExists) {
		// A duplicate INSERT may wait for the winning transaction. Read only after
		// our transaction has rolled back, outside its repeatable-read snapshot.
		previous, readErr := s.store.GetIdempotencyKey(ctx, identity)
		if readErr != nil {
			return model.IdempotencyResponse{}, false, fmt.Errorf("read concurrent idempotency record: %w", readErr)
		}
		return replayIdempotency(identity, previous)
	}
	if err != nil {
		return model.IdempotencyResponse{}, false, err
	}
	return response, false, nil
}

func replayIdempotency(identity model.IdempotencyIdentity, receipt model.IdempotencyRecord) (model.IdempotencyResponse, bool, error) {
	if identity.RequestDigest != receipt.RequestDigest {
		return model.IdempotencyResponse{}, false, ErrIdempotencyConflict
	}
	return receipt.IdempotencyResponse, true, nil
}
