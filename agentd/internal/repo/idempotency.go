package repo

import (
	"context"
	"errors"

	"github.com/compforge/agentd/agentd/internal/model"
	managedevent "github.com/compforge/agentd/internal/event"
)

var ErrIdempotencyKeyExists = errors.New("idempotency key already exists")

type IdempotencyRepository interface {
	GetIdempotencyKey(context.Context, model.IdempotencyIdentity) (model.IdempotencyRecord, error)
	CreateIdempotencyKey(context.Context, model.IdempotencyIdentity) error
	SaveIdempotencyResponse(context.Context, model.IdempotencyIdentity, model.IdempotencyResponse) error
}

// IdempotencyStore binds receipt, control state and Ledger writes to one commit.
// The callback must not make network calls or let transaction-bound values escape.
type IdempotencyStore interface {
	GetIdempotencyKey(context.Context, model.IdempotencyIdentity) (model.IdempotencyRecord, error)
	Transaction(context.Context, func(context.Context, Repository, *managedevent.Log, IdempotencyRepository) error) error
}
