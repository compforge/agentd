package api

import (
	"encoding/base64"
	"testing"
	"time"

	"github.com/compforge/agentd/agentd/internal/api/view"
	"github.com/compforge/agentd/agentd/internal/service"
)

func TestPageCursorRoundTrip(t *testing.T) {
	query, err := parsePage(view.PageRequest{}, sessionCursor, true)
	if err != nil {
		t.Fatal(err)
	}
	if query.Limit != defaultPageLimit || query.Anchor != nil || query.Direction != service.PageAfter {
		t.Fatalf("default page query = %#v", query)
	}

	first := service.PageAnchor{CreatedAt: time.Date(2026, 8, 24, 1, 0, 0, 0, time.UTC), ID: "session-1"}
	last := service.PageAnchor{CreatedAt: time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC), ID: "session-2"}
	next, previous := pageLinks(sessionCursor, query, true, first, last)
	if next == nil || previous != nil {
		t.Fatalf("first page links next=%v previous=%v", next, previous)
	}
	query, err = parsePage(view.PageRequest{Limit: query.Limit, Page: *next}, sessionCursor, true)
	if err != nil {
		t.Fatal(err)
	}
	if query.Anchor == nil || query.Anchor.ID != last.ID || query.Direction != service.PageAfter {
		t.Fatalf("decoded page query = %#v", query)
	}
	_, previous = pageLinks(sessionCursor, query, false, first, last)
	if previous == nil {
		t.Fatal("second page has no previous cursor")
	}
	backward, err := parsePage(view.PageRequest{Limit: query.Limit, Page: *previous}, sessionCursor, true)
	if err != nil || backward.Direction != service.PageBefore || backward.Anchor.ID != first.ID {
		t.Fatalf("backward page query = %#v, %v", backward, err)
	}
}

func TestPageRequestRejectsInvalidValuesAndForeignCursors(t *testing.T) {
	oldOffsetCursor := base64.RawURLEncoding.EncodeToString([]byte(`{"v":1,"offset":1000000000}`))
	for _, input := range []view.PageRequest{
		{Limit: -1},
		{Limit: maxPageLimit + 1},
		{Page: "not-a-cursor"},
		{Page: oldOffsetCursor},
	} {
		if _, err := parsePage(input, sessionCursor, true); err == nil {
			t.Fatalf("parsePage(%#v) succeeded", input)
		}
	}

	modelPage := encodePageCursor(modelCursor, false, service.PageAfter, service.PageAnchor{
		CreatedAt: time.Now().UTC(), ID: "model-1",
	})
	if _, err := parsePage(view.PageRequest{Page: *modelPage}, sessionCursor, false); err == nil {
		t.Fatal("Session list accepted a Model cursor")
	}
}

func TestEventCursorRoundTrip(t *testing.T) {
	cursor := encodeEventCursor(42)
	limit, sequence, err := parseEventPage(view.PageRequest{Limit: 10, Page: *cursor})
	if err != nil || limit != 10 || sequence != 42 {
		t.Fatalf("event page = limit %d sequence %d, %v", limit, sequence, err)
	}
}
