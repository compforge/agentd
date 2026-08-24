package api

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/compforge/agentd/agentd/internal/api/view"
	"github.com/compforge/agentd/agentd/internal/service"
)

const (
	defaultPageLimit  = 20
	maxPageLimit      = 100
	pageCursorVersion = 1

	agentCursor        = "agent"
	agentVersionCursor = "agent_version"
	modelCursor        = "model"
	environmentCursor  = "environment"
	sessionCursor      = "session"
	eventCursor        = "event"
)

type pageCursor struct {
	Version       int                   `json:"v"`
	Kind          string                `json:"kind"`
	Direction     service.PageDirection `json:"direction"`
	Descending    bool                  `json:"descending,omitempty"`
	CreatedAt     *time.Time            `json:"created_at,omitempty"`
	ID            string                `json:"id,omitempty"`
	VersionNumber *int64                `json:"version,omitempty"`
	Sequence      *int64                `json:"sequence,omitempty"`
}

// parsePage turns an opaque, resource-bound keyset cursor into a service query.
// A client-controlled cursor can choose a starting key, but never an unbounded
// OFFSET or limit.
//
// +spec=`All list endpoints use resource-bound opaque keyset cursors, default to 20 items, reject limits above 100, and never expose cursor encoding below the API boundary`
// +link=repo://agentd/docs/kernel.md
func parsePage(request view.PageRequest, kind string, descending bool) (service.PageQuery, error) {
	limit, err := pageLimit(request.Limit)
	if err != nil {
		return service.PageQuery{}, err
	}
	query := service.PageQuery{Limit: limit, Descending: descending, Direction: service.PageAfter}
	if request.Page == "" {
		return query, nil
	}
	cursor, err := decodePageCursor(request.Page)
	if err != nil || cursor.Kind != kind || cursor.Descending != descending ||
		(cursor.Direction != service.PageAfter && cursor.Direction != service.PageBefore) {
		return service.PageQuery{}, invalidPageCursor()
	}
	if cursor.Direction == service.PageBefore && kind != sessionCursor {
		return service.PageQuery{}, invalidPageCursor()
	}
	query.Direction = cursor.Direction
	switch kind {
	case agentVersionCursor:
		if cursor.Direction != service.PageAfter || cursor.VersionNumber == nil || *cursor.VersionNumber < 1 {
			return service.PageQuery{}, invalidPageCursor()
		}
		query.Anchor = &service.PageAnchor{Version: *cursor.VersionNumber}
	default:
		if cursor.CreatedAt == nil || cursor.CreatedAt.IsZero() || cursor.ID == "" {
			return service.PageQuery{}, invalidPageCursor()
		}
		query.Anchor = &service.PageAnchor{CreatedAt: *cursor.CreatedAt, ID: cursor.ID}
	}
	return query, nil
}

func pageLinks(
	kind string,
	query service.PageQuery,
	hasMore bool,
	first service.PageAnchor,
	last service.PageAnchor,
) (next, previous *string) {
	if first.ID == "" && first.Version == 0 {
		return nil, nil
	}
	if query.Direction == service.PageBefore {
		if query.Anchor != nil {
			next = encodePageCursor(kind, query.Descending, service.PageAfter, last)
		}
		if hasMore {
			previous = encodePageCursor(kind, query.Descending, service.PageBefore, first)
		}
		return next, previous
	}
	if hasMore {
		next = encodePageCursor(kind, query.Descending, service.PageAfter, last)
	}
	if query.Anchor != nil {
		previous = encodePageCursor(kind, query.Descending, service.PageBefore, first)
	}
	return next, previous
}

func encodePageCursor(kind string, descending bool, direction service.PageDirection, anchor service.PageAnchor) *string {
	cursor := pageCursor{
		Version: pageCursorVersion, Kind: kind, Direction: direction, Descending: descending,
	}
	if kind == agentVersionCursor {
		cursor.VersionNumber = &anchor.Version
	} else {
		cursor.CreatedAt = &anchor.CreatedAt
		cursor.ID = anchor.ID
	}
	return encodeCursor(cursor)
}

func parseEventPage(request view.PageRequest) (limit int, afterSeq int64, err error) {
	limit, err = pageLimit(request.Limit)
	if err != nil || request.Page == "" {
		return limit, 0, err
	}
	cursor, decodeErr := decodePageCursor(request.Page)
	if decodeErr != nil || cursor.Kind != eventCursor || cursor.Direction != service.PageAfter ||
		cursor.Descending || cursor.Sequence == nil || *cursor.Sequence < 0 {
		return 0, 0, invalidPageCursor()
	}
	return limit, *cursor.Sequence, nil
}

func encodeEventCursor(sequence int64) *string {
	return encodeCursor(pageCursor{
		Version: pageCursorVersion, Kind: eventCursor, Direction: service.PageAfter, Sequence: &sequence,
	})
}

func pageLimit(limit int) (int, error) {
	if limit == 0 {
		return defaultPageLimit, nil
	}
	if limit < 1 || limit > maxPageLimit {
		return 0, fmt.Errorf("%w: limit must be between 1 and %d", service.ErrInvalid, maxPageLimit)
	}
	return limit, nil
}

func decodePageCursor(value string) (pageCursor, error) {
	encoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return pageCursor{}, err
	}
	var cursor pageCursor
	if err := json.Unmarshal(encoded, &cursor); err != nil || cursor.Version != pageCursorVersion {
		return pageCursor{}, invalidPageCursor()
	}
	return cursor, nil
}

func encodeCursor(cursor pageCursor) *string {
	encoded, _ := json.Marshal(cursor)
	value := base64.RawURLEncoding.EncodeToString(encoded)
	return &value
}

func invalidPageCursor() error {
	return fmt.Errorf("%w: invalid page cursor", service.ErrInvalid)
}
