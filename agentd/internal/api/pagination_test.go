package api

import (
	"testing"

	"github.com/compforge/agentd/agentd/internal/api/view"
)

func TestPageCursorRoundTrip(t *testing.T) {
	query, err := parsePage(view.PageRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if query.Limit != defaultPageLimit || query.Offset != 0 {
		t.Fatalf("default page query = %#v", query)
	}

	next, previous := pageLinks(query, true)
	if next == nil || previous != nil {
		t.Fatalf("first page links next=%v previous=%v", next, previous)
	}
	query, err = parsePage(view.PageRequest{Limit: query.Limit, Page: *next})
	if err != nil {
		t.Fatal(err)
	}
	if query.Offset != defaultPageLimit {
		t.Fatalf("decoded page query = %#v", query)
	}
	_, previous = pageLinks(query, false)
	if previous == nil {
		t.Fatal("second page has no previous cursor")
	}
}

func TestPageRequestRejectsInvalidValues(t *testing.T) {
	for _, input := range []view.PageRequest{
		{Limit: -1},
		{Limit: maxPageLimit + 1},
		{Page: "not-a-cursor"},
	} {
		if _, err := parsePage(input); err == nil {
			t.Fatalf("parsePage(%#v) succeeded", input)
		}
	}
}
