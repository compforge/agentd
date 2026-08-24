package api

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/compforge/agentd/agentd/internal/api/view"
	"github.com/compforge/agentd/agentd/internal/service"
)

const (
	defaultPageLimit  = 20
	maxPageLimit      = 100
	pageCursorVersion = 1
)

type pageCursor struct {
	Version int `json:"v"`
	Offset  int `json:"offset"`
}

// parsePage keeps the public cursor opaque while exposing only bounded
// offset/limit values to the service and Repository layers.
//
// +spec=`All list endpoints use one opaque cursor format, default to 20 items, reject limits above 100, and never expose cursor encoding below the API boundary`
// +link=repo://agentd/docs/kernel.md
func parsePage(request view.PageRequest) (service.PageQuery, error) {
	query := service.PageQuery{Limit: request.Limit}
	if query.Limit == 0 {
		query.Limit = defaultPageLimit
	}
	if query.Limit < 1 || query.Limit > maxPageLimit {
		return service.PageQuery{}, fmt.Errorf("%w: limit must be between 1 and %d", service.ErrInvalid, maxPageLimit)
	}
	if request.Page == "" {
		return query, nil
	}
	encoded, err := base64.RawURLEncoding.DecodeString(request.Page)
	if err != nil {
		return service.PageQuery{}, fmt.Errorf("%w: invalid page cursor", service.ErrInvalid)
	}
	var cursor pageCursor
	if err := json.Unmarshal(encoded, &cursor); err != nil ||
		cursor.Version != pageCursorVersion || cursor.Offset < 0 {
		return service.PageQuery{}, fmt.Errorf("%w: invalid page cursor", service.ErrInvalid)
	}
	query.Offset = cursor.Offset
	return query, nil
}

func pageLinks(query service.PageQuery, hasMore bool) (next, previous *string) {
	if hasMore {
		next = encodePageCursor(query.Offset + query.Limit)
	}
	if query.Offset > 0 {
		previous = encodePageCursor(max(query.Offset-query.Limit, 0))
	}
	return next, previous
}

func encodePageCursor(offset int) *string {
	encoded, _ := json.Marshal(pageCursor{Version: pageCursorVersion, Offset: offset})
	value := base64.RawURLEncoding.EncodeToString(encoded)
	return &value
}

func slicePage[T any](items []T, query service.PageQuery) service.Page[T] {
	if query.Offset >= len(items) {
		return service.Page[T]{Items: []T{}}
	}
	end := min(query.Offset+query.Limit+1, len(items))
	values := items[query.Offset:end]
	page := service.Page[T]{Items: values}
	if len(values) > query.Limit {
		page.Items = values[:query.Limit]
		page.HasMore = true
	}
	return page
}
