package httpapi

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

// The zero instant is a valid RFC3339Nano timestamp and must work as an
// explicit bound, distinct from an omitted bound.
const zeroInstant = "0001-01-01T00:00:00Z"

// seedBoundaryEvents stores events at a boundary instant, one nanosecond
// after it, and well after it.
func seedBoundaryEvents(t *testing.T, server *httptest.Server) {
	t.Helper()
	status, out := post(t, server, `{"events":[
		{"id":"edge","service":"s","severity":"info","message":"m","at":"2026-10-01T09:00:00Z"},
		{"id":"edge-plus-1ns","service":"s","severity":"info","message":"m","at":"2026-10-01T09:00:00.000000001Z"},
		{"id":"later","service":"s","severity":"info","message":"m","at":"2026-10-02T09:00:00Z"}
	]}`)
	if status != http.StatusOK {
		t.Fatalf("seed: %d %v", status, out)
	}
}

// getIDs issues GET /events and returns the status and the event ids.
func getIDs(t *testing.T, server *httptest.Server, query string) (int, []string) {
	t.Helper()
	status, rows := get(t, server, query)
	if status != http.StatusOK {
		return status, nil
	}
	arr, ok := rows.([]any)
	if !ok {
		t.Fatalf("events is not an array: %v", rows)
	}
	ids := make([]string, len(arr))
	for i, row := range arr {
		ids[i] = row.(map[string]any)["id"].(string)
	}
	return status, ids
}

func TestZeroInstantUntilIsApplied(t *testing.T) {
	server, _, _ := newTestServer(t)
	seedBoundaryEvents(t, server)

	// until=0001-01-01T00:00:00Z excludes every stored event, all of which
	// are later than that instant.
	status, ids := getIDs(t, server, "?until="+zeroInstant)
	if status != http.StatusOK {
		t.Fatalf("status: %d", status)
	}
	if len(ids) != 0 {
		t.Fatalf("events after the until bound must not appear: %v", ids)
	}

	// since at the zero instant keeps everything (all events are later).
	status, ids = getIDs(t, server, "?since="+zeroInstant)
	if status != http.StatusOK || len(ids) != 3 {
		t.Fatalf("since at the zero instant should keep all events: %d %v", status, ids)
	}

	// since later than the zero-instant until is a 400 with an error string.
	status, out := get(t, server, "?since=2026-10-01T00:00:00Z&until="+zeroInstant)
	if status != http.StatusBadRequest {
		t.Fatalf("since after until: want 400, got %d %v", status, out)
	}
	if msg, _ := out.(map[string]any)["error"].(string); msg == "" {
		t.Fatal("since after until should return an error string")
	}

	// Equal bounds at the zero instant are allowed.
	status, _ = getIDs(t, server, "?since="+zeroInstant+"&until="+zeroInstant)
	if status != http.StatusOK {
		t.Fatalf("equal zero-instant bounds should be allowed, got %d", status)
	}
}

func TestZeroInstantUntilEquivalentTimezone(t *testing.T) {
	server, _, _ := newTestServer(t)
	seedBoundaryEvents(t, server)

	// The same instant spelled in another zone must behave identically.
	for _, q := range []string{"0001-01-01T00:00:00Z", "0000-12-31T19:00:00-05:00"} {
		status, ids := getIDs(t, server, "?until="+url.QueryEscape(q))
		if status != http.StatusOK {
			t.Fatalf("%q: status %d", q, status)
		}
		if len(ids) != 0 {
			t.Fatalf("%q: events after the bound must not appear: %v", q, ids)
		}
	}
}

func TestBoundaryNanosecondPrecision(t *testing.T) {
	server, _, _ := newTestServer(t)
	seedBoundaryEvents(t, server)

	// Inclusive until keeps the event at the exact instant but drops the one
	// a nanosecond later.
	status, ids := getIDs(t, server, "?until=2026-10-01T09:00:00Z")
	if status != http.StatusOK || len(ids) != 1 || ids[0] != "edge" {
		t.Fatalf("inclusive until: %d %v", status, ids)
	}

	// Inclusive since drops the event a nanosecond earlier.
	status, ids = getIDs(t, server, "?since=2026-10-01T09:00:00.000000001Z")
	if status != http.StatusOK || len(ids) != 2 || ids[0] != "edge-plus-1ns" {
		t.Fatalf("inclusive since: %d %v", status, ids)
	}
}

func TestPageZeroInstantBoundIsRemembered(t *testing.T) {
	server, _, _ := newTestServer(t)
	seedBoundaryEvents(t, server)

	// First page with only an explicit zero-instant until: the snapshot
	// holds the empty result set.
	first := url.Values{}
	first.Set("limit", "1")
	first.Set("until", zeroInstant)
	status, out := page(t, server, "?"+first.Encode())
	if status != http.StatusOK {
		t.Fatalf("first page: %d %v", status, out)
	}
	events, cursor := pageEvents(t, out)
	if len(events) != 0 {
		t.Fatalf("first page should be empty: %v", events)
	}
	if cursor != "" {
		t.Fatalf("empty snapshot should have null next_cursor, got %q", cursor)
	}

	// A snapshot with real matches: since at the zero instant matches all.
	first = url.Values{}
	first.Set("limit", "2")
	first.Set("since", zeroInstant)
	status, out = page(t, server, "?"+first.Encode())
	if status != http.StatusOK {
		t.Fatalf("first page: %d %v", status, out)
	}
	events, cursor = pageEvents(t, out)
	if len(events) != 2 || cursor == "" {
		t.Fatalf("first page: %v cursor %q", events, cursor)
	}

	// Continuation with only cursor+limit reuses the saved zero-instant
	// bound and returns the rest.
	status, out = page(t, server, "?limit=2&cursor="+cursor)
	if status != http.StatusOK {
		t.Fatalf("continuation: %d %v", status, out)
	}
	events, _ = pageEvents(t, out)
	if len(events) != 1 || events[0]["id"] != "later" {
		t.Fatalf("continuation should use the saved filters: %v", events)
	}

	// Continuation spelling the same instant in another zone is accepted.
	status, out = page(t, server, "?"+url.Values{
		"limit":  {"2"},
		"cursor": {cursor},
		"since":  {"0000-12-31T19:00:00-05:00"},
	}.Encode())
	if status != http.StatusOK {
		t.Fatalf("equivalent timezone should be accepted: %d %v", status, out)
	}

	// Continuation that adds any other filter is a different set: 400.
	status, out = page(t, server, "?"+url.Values{
		"limit":   {"2"},
		"cursor":  {cursor},
		"service": {"s"},
	}.Encode())
	if status != http.StatusBadRequest {
		t.Fatalf("adding a filter should be 400, got %d %v", status, out)
	}
	if msg, _ := out["error"].(string); msg == "" {
		t.Fatal("mismatched filters should return an error string")
	}
}

func TestPageZeroInstantBoundVsMissingBound(t *testing.T) {
	server, _, _ := newTestServer(t)
	seedBoundaryEvents(t, server)

	// First page with an explicit zero-instant since and a real until.
	first := url.Values{}
	first.Set("limit", "1")
	first.Set("since", zeroInstant)
	first.Set("until", "2026-10-02T00:00:00Z")
	status, out := page(t, server, "?"+first.Encode())
	if status != http.StatusOK {
		t.Fatalf("first page: %d %v", status, out)
	}
	events, cursor := pageEvents(t, out)
	if len(events) != 1 || events[0]["id"] != "edge" || cursor == "" {
		t.Fatalf("first page: %v cursor %q", events, cursor)
	}

	// Swapping the until for the zero instant is a different filter set and
	// must be rejected, even though both exclude the same later events.
	status, out = page(t, server, "?"+url.Values{
		"limit":  {"1"},
		"cursor": {cursor},
		"since":  {zeroInstant},
		"until":  {zeroInstant},
	}.Encode())
	if status != http.StatusBadRequest {
		t.Fatalf("different until should be 400, got %d %v", status, out)
	}

	// Dropping the until entirely is also a different filter set.
	status, out = page(t, server, "?"+url.Values{
		"limit":  {"1"},
		"cursor": {cursor},
		"since":  {zeroInstant},
	}.Encode())
	if status != http.StatusBadRequest {
		t.Fatalf("dropping until should be 400, got %d %v", status, out)
	}

	// Repeating the exact first-page filters continues from the saved spot.
	status, out = page(t, server, "?"+url.Values{
		"limit":  {"1"},
		"cursor": {cursor},
		"since":  {zeroInstant},
		"until":  {"2026-10-02T00:00:00Z"},
	}.Encode())
	if status != http.StatusOK {
		t.Fatalf("matching filters should continue: %d %v", status, out)
	}
	events, _ = pageEvents(t, out)
	if len(events) != 1 || events[0]["id"] != "edge-plus-1ns" {
		t.Fatalf("continuation from saved position: %v", events)
	}
}

func TestPageZeroInstantSinceAfterUntil(t *testing.T) {
	server, _, _ := newTestServer(t)
	seedBoundaryEvents(t, server)

	// A since later than the zero-instant until is rejected on a first page.
	status, out := page(t, server, "?since=2026-10-01T00:00:00Z&until="+zeroInstant)
	if status != http.StatusBadRequest {
		t.Fatalf("want 400, got %d %v", status, out)
	}
	if msg, _ := out["error"].(string); msg == "" {
		t.Fatal("since after until should return an error string")
	}
}
