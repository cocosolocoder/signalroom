package httpapi

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

// page issues a GET /events/page request and returns the status and body.
func page(t *testing.T, server *httptest.Server, query string) (int, map[string]any) {
	t.Helper()
	resp, err := http.Get(server.URL + "/events/page" + query)
	if err != nil {
		t.Fatalf("page: %v", err)
	}
	defer resp.Body.Close()
	return decode(t, resp)
}

// pageEvents extracts the events array and next_cursor from a page response.
// A null next_cursor is returned as the empty string.
func pageEvents(t *testing.T, out map[string]any) ([]map[string]any, string) {
	t.Helper()
	raw, ok := out["events"].([]any)
	if !ok {
		t.Fatalf("events is not an array: %v", out)
	}
	events := make([]map[string]any, len(raw))
	for i, e := range raw {
		events[i] = e.(map[string]any)
	}
	var cursor string
	if c, ok := out["next_cursor"].(string); ok {
		cursor = c
	}
	return events, cursor
}

func seedEvents(t *testing.T, server *httptest.Server, n int) {
	t.Helper()
	body := `{"events":[`
	for i := 0; i < n; i++ {
		if i > 0 {
			body += ","
		}
		body += `{"id":"e` + itoa(i) + `","service":"s","severity":"info","message":"m","at":"2026-10-01T09:` + pad2(i) + `:00Z"}`
	}
	body += `]}`
	if status, _ := post(t, server, body); status != http.StatusOK {
		t.Fatalf("seed %d events: status %d", n, status)
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

func pad2(n int) string {
	if n < 10 {
		return "0" + itoa(n)
	}
	return itoa(n)
}

func TestPageBasicPaging(t *testing.T) {
	server, _, _ := newTestServer(t)
	seedEvents(t, server, 5)

	// Page 1: limit 2.
	status, out := page(t, server, "?limit=2")
	if status != http.StatusOK {
		t.Fatalf("page 1: %d %v", status, out)
	}
	events, cursor := pageEvents(t, out)
	if len(events) != 2 || events[0]["id"] != "e0" || events[1]["id"] != "e1" {
		t.Fatalf("page 1: %v", events)
	}
	if cursor == "" {
		t.Fatal("page 1 should have a next_cursor")
	}

	// Page 2.
	status, out = page(t, server, "?limit=2&cursor="+cursor)
	if status != http.StatusOK {
		t.Fatalf("page 2: %d %v", status, out)
	}
	events, cursor = pageEvents(t, out)
	if len(events) != 2 || events[0]["id"] != "e2" || events[1]["id"] != "e3" {
		t.Fatalf("page 2: %v", events)
	}
	if cursor == "" {
		t.Fatal("page 2 should have a next_cursor")
	}

	// Page 3 (last, partial).
	status, out = page(t, server, "?limit=2&cursor="+cursor)
	if status != http.StatusOK {
		t.Fatalf("page 3: %d %v", status, out)
	}
	events, cursor = pageEvents(t, out)
	if len(events) != 1 || events[0]["id"] != "e4" {
		t.Fatalf("page 3: %v", events)
	}
	if cursor != "" {
		t.Fatalf("page 3 should have null next_cursor, got %q", cursor)
	}
}

func TestPageDefaultLimit(t *testing.T) {
	server, _, _ := newTestServer(t)
	seedEvents(t, server, 3)

	status, out := page(t, server, "")
	if status != http.StatusOK {
		t.Fatalf("status: %d %v", status, out)
	}
	events, cursor := pageEvents(t, out)
	if len(events) != 3 {
		t.Fatalf("default limit should return all 3, got %d", len(events))
	}
	if cursor != "" {
		t.Fatal("no more records, next_cursor should be null")
	}
}

func TestPageLimitValidation(t *testing.T) {
	server, _, _ := newTestServer(t)
	for _, q := range []string{
		"limit=0",
		"limit=1001",
		"limit=-1",
		"limit=1.5",
		"limit=abc",
		"limit=",
		"limit=1&limit=2",
		"limit=%201",
		"limit=100000000000000000000",
	} {
		status, out := page(t, server, "?"+q)
		if status != http.StatusBadRequest {
			t.Fatalf("%q: want 400, got %d %v", q, status, out)
		}
		if msg, _ := out["error"].(string); msg == "" {
			t.Fatalf("%q: missing error string", q)
		}
	}
}

func TestPageCursorValidation(t *testing.T) {
	server, _, _ := newTestServer(t)
	post(t, server, `{"events":[{"id":"e1","service":"s","severity":"info","message":"m","at":"2026-10-01T09:00:00Z"}]}`)

	for _, q := range []string{
		"cursor=",
		"cursor=not-a-cursor",
		"cursor=aaa&cursor=bbb",
		"cursor=eyJ2IjoxfQ.aaaa",
	} {
		status, out := page(t, server, "?"+q)
		if status != http.StatusBadRequest {
			t.Fatalf("%q: want 400, got %d %v", q, status, out)
		}
		if msg, _ := out["error"].(string); msg == "" {
			t.Fatalf("%q: missing error string", q)
		}
	}
}

func TestPageSnapshotStableAcrossWrites(t *testing.T) {
	server, _, _ := newTestServer(t)
	post(t, server, `{"events":[
		{"id":"e1","service":"s","severity":"info","message":"m","at":"2026-10-01T09:00:00Z"},
		{"id":"e2","service":"s","severity":"info","message":"m","at":"2026-10-01T09:02:00Z"}
	]}`)

	// Page 1: limit 1.
	status, out := page(t, server, "?limit=1")
	if status != http.StatusOK {
		t.Fatalf("page 1: %d %v", status, out)
	}
	events, cursor := pageEvents(t, out)
	if len(events) != 1 || events[0]["id"] != "e1" {
		t.Fatalf("page 1: %v", events)
	}

	// Ingest new events after the snapshot: one earlier, one at the same
	// instant as an existing event. Neither must appear in page 2.
	post(t, server, `{"events":[{"id":"e0","service":"s","severity":"info","message":"m","at":"2026-10-01T08:00:00Z"}]}`)
	post(t, server, `{"events":[{"id":"e1b","service":"s","severity":"info","message":"m","at":"2026-10-01T09:00:00Z"}]}`)

	status, out = page(t, server, "?limit=1&cursor="+cursor)
	events, cursor = pageEvents(t, out)
	if len(events) != 1 || events[0]["id"] != "e2" {
		t.Fatalf("page 2 must not include post-snapshot events: %v", events)
	}
	if cursor != "" {
		t.Fatal("page 2 should be the last")
	}

	// A fresh first page sees all 4 events.
	status, out = page(t, server, "?limit=10")
	events, _ = pageEvents(t, out)
	if len(events) != 4 {
		t.Fatalf("fresh query should see 4 events, got %d", len(events))
	}
}

func TestPageQueryContinuation(t *testing.T) {
	server, _, _ := newTestServer(t)
	post(t, server, `{"events":[
		{"id":"e1","service":"gateway","severity":"critical","message":"m","at":"2026-10-01T09:00:00Z","labels":{"env":"prod","region":"cn"}},
		{"id":"e2","service":"api","severity":"info","message":"m","at":"2026-10-01T09:01:00Z","labels":{"env":"prod"}},
		{"id":"e3","service":"gateway","severity":"critical","message":"m","at":"2026-10-01T09:02:00Z","labels":{"env":"prod","region":"cn"}}
	]}`)

	// First page with filters.
	firstQ := url.Values{}
	firstQ.Set("limit", "1")
	firstQ.Set("service", "gateway")
	firstQ.Set("severity", "critical")
	firstQ.Add("label", "env=prod")
	firstQ.Add("label", "region=cn")
	firstQ.Set("since", "2026-10-01T00:00:00Z")
	firstQ.Set("until", "2026-10-02T00:00:00Z")
	status, out := page(t, server, "?"+firstQ.Encode())
	events, cursor := pageEvents(t, out)
	if len(events) != 1 || events[0]["id"] != "e1" {
		t.Fatalf("first page: %v", events)
	}

	// Continuation with no filters uses the snapshot query.
	status, out = page(t, server, "?limit=1&cursor="+cursor)
	events, cursor = pageEvents(t, out)
	if len(events) != 1 || events[0]["id"] != "e3" {
		t.Fatalf("continuation with no filters should use snapshot query: %v", events)
	}
	if cursor != "" {
		t.Fatal("e3 is the last matching event")
	}

	// Re-fetch first page for a fresh cursor.
	status, out = page(t, server, "?"+firstQ.Encode())
	_, cursor = pageEvents(t, out)

	// Continuation with equivalent filters: whitespace, severity case,
	// label order, and timezone representation must all be accepted.
	equiv := url.Values{}
	equiv.Set("limit", "1")
	equiv.Set("cursor", cursor)
	equiv.Set("service", " gateway ")
	equiv.Set("severity", "CRITICAL")
	equiv.Add("label", " region = cn ")
	equiv.Add("label", " env = prod ")
	equiv.Set("since", "2026-10-01T08:00:00+08:00")
	equiv.Set("until", "2026-10-02T08:00:00+08:00")
	status, out = page(t, server, "?"+equiv.Encode())
	if status != http.StatusOK {
		t.Fatalf("equivalent filters should be accepted, got %d %v", status, out)
	}
	events, _ = pageEvents(t, out)
	if len(events) != 1 || events[0]["id"] != "e3" {
		t.Fatalf("equivalent continuation: %v", events)
	}

	// Continuation with different filters → 400.
	diff := url.Values{}
	diff.Set("limit", "1")
	diff.Set("cursor", cursor)
	diff.Set("service", "api")
	status, out = page(t, server, "?"+diff.Encode())
	if status != http.StatusBadRequest {
		t.Fatalf("different filters should be 400, got %d %v", status, out)
	}
	if msg, _ := out["error"].(string); msg == "" {
		t.Fatal("different filters should return an error string")
	}
}

func TestPageLimitChange(t *testing.T) {
	server, _, _ := newTestServer(t)
	seedEvents(t, server, 5)

	// Page 1: limit 2.
	status, out := page(t, server, "?limit=2")
	if status != http.StatusOK {
		t.Fatalf("page 1: %d %v", status, out)
	}
	events, cursor := pageEvents(t, out)
	if len(events) != 2 {
		t.Fatalf("page 1: %v", events)
	}

	// Page 2: limit 3 (changed). Should get e2, e3, e4.
	status, out = page(t, server, "?limit=3&cursor="+cursor)
	events, cursor = pageEvents(t, out)
	if len(events) != 3 || events[0]["id"] != "e2" || events[1]["id"] != "e3" || events[2]["id"] != "e4" {
		t.Fatalf("page 2 with changed limit: %v", events)
	}
	if cursor != "" {
		t.Fatal("should be last page")
	}
}

func TestPageSameCursorRepeated(t *testing.T) {
	server, _, _ := newTestServer(t)
	seedEvents(t, server, 3)

	status, out := page(t, server, "?limit=1")
	if status != http.StatusOK {
		t.Fatalf("page: %d %v", status, out)
	}
	_, cursor := pageEvents(t, out)

	// Request the same cursor twice.
	status, out1 := page(t, server, "?limit=1&cursor="+cursor)
	events1, cursor1 := pageEvents(t, out1)
	status, out2 := page(t, server, "?limit=1&cursor="+cursor)
	events2, cursor2 := pageEvents(t, out2)

	if events1[0]["id"] != "e1" || events2[0]["id"] != "e1" {
		t.Fatalf("same cursor should give the same record: %v %v", events1, events2)
	}
	if cursor1 != cursor2 {
		t.Fatalf("same cursor should give the same next cursor: %q %q", cursor1, cursor2)
	}
}

func TestPageAllRecordsExactlyOnce(t *testing.T) {
	server, _, _ := newTestServer(t)
	seedEvents(t, server, 10)

	seen := make(map[string]bool)
	cursor := ""
	pages := 0
	for {
		q := "?limit=3"
		if cursor != "" {
			q += "&cursor=" + cursor
		}
		status, out := page(t, server, q)
		if status != http.StatusOK {
			t.Fatalf("page %d: %d %v", pages, status, out)
		}
		events, next := pageEvents(t, out)
		for _, e := range events {
			id := e["id"].(string)
			if seen[id] {
				t.Fatalf("event %s appeared twice", id)
			}
			seen[id] = true
		}
		pages++
		if next == "" {
			break
		}
		cursor = next
		if pages > 100 {
			t.Fatal("too many pages")
		}
	}
	if len(seen) != 10 {
		t.Fatalf("should see all 10 events, got %d", len(seen))
	}
}

func TestPageEmptyResult(t *testing.T) {
	server, _, _ := newTestServer(t)
	status, out := page(t, server, "?service=nope")
	if status != http.StatusOK {
		t.Fatalf("status: %d %v", status, out)
	}
	events, cursor := pageEvents(t, out)
	if len(events) != 0 {
		t.Fatalf("empty result should be [], got %v", events)
	}
	if cursor != "" {
		t.Fatalf("empty result should have null next_cursor, got %q", cursor)
	}
}

func TestPageLabelsInResponse(t *testing.T) {
	server, _, _ := newTestServer(t)
	post(t, server, `{"events":[{"id":"e1","service":"s","severity":"info","message":"m","at":"2026-10-01T09:00:00Z","labels":{"env":"prod","version":"v2"}}]}`)
	status, out := page(t, server, "")
	if status != http.StatusOK {
		t.Fatalf("status: %d %v", status, out)
	}
	events, _ := pageEvents(t, out)
	if len(events) != 1 {
		t.Fatalf("events: %v", events)
	}
	labels, ok := events[0]["labels"].(map[string]any)
	if !ok || labels["env"] != "prod" || labels["version"] != "v2" {
		t.Fatalf("labels missing or wrong: %v", events[0])
	}
}

func TestPage503WhenPoisoned(t *testing.T) {
	server, store, _ := newTestServer(t)
	store.failNext = true
	post(t, server, `{"events":[{"id":"a","service":"s","severity":"info","message":"m","at":"2026-10-01T09:00:00Z"}]}`)

	status, out := page(t, server, "")
	if status != http.StatusServiceUnavailable {
		t.Fatalf("poisoned store should be 503, got %d %v", status, out)
	}
}

func TestPageExplicitZeroSincePreservedAcrossPages(t *testing.T) {
	server, _, _ := newTestServer(t)
	seedEvents(t, server, 5)

	// First page with an explicit since at the zero instant. It matches all
	// events but, unlike a missing since, must be saved as a real bound.
	status, out := page(t, server, "?limit=2&since=0001-01-01T00:00:00Z")
	if status != http.StatusOK {
		t.Fatalf("page 1: %d %v", status, out)
	}
	events, cursor := pageEvents(t, out)
	if len(events) != 2 || events[0]["id"] != "e0" || events[1]["id"] != "e1" {
		t.Fatalf("page 1: %v", events)
	}
	if cursor == "" {
		t.Fatal("page 1 should have a next_cursor")
	}

	// Continuation with only cursor and limit keeps the saved zero bound.
	status, out = page(t, server, "?limit=2&cursor="+cursor)
	if status != http.StatusOK {
		t.Fatalf("page 2: %d %v", status, out)
	}
	events, _ = pageEvents(t, out)
	if len(events) != 2 || events[0]["id"] != "e2" || events[1]["id"] != "e3" {
		t.Fatalf("continuation must reuse the saved since: %v", events)
	}
}

func TestPageExplicitZeroUntilFiltersAndStaysEmpty(t *testing.T) {
	server, _, _ := newTestServer(t)
	seedEvents(t, server, 3)

	// until at the zero instant is an explicit bound: all 2026 events are
	// after it, so the result is the usual empty form with no cursor.
	status, out := page(t, server, "?until=0001-01-01T00:00:00Z")
	if status != http.StatusOK {
		t.Fatalf("status: %d %v", status, out)
	}
	events, next := pageEvents(t, out)
	if len(events) != 0 {
		t.Fatalf("zero until must return [], got %v", events)
	}
	if next != "" {
		t.Fatalf("zero until must have null next_cursor, got %q", next)
	}

	// A boundary one nanosecond tighter also excludes the event exactly a
	// nanosecond after it.
	status, out = page(t, server, "?until=2026-10-01T08:59:59.999999999Z")
	if status != http.StatusOK {
		t.Fatalf("status: %d %v", status, out)
	}
	if events, _ = pageEvents(t, out); len(events) != 0 {
		t.Fatalf("event a nanosecond after until must be excluded: %v", events)
	}
}

func TestPageZeroBoundPresenceIsPartOfFilterSet(t *testing.T) {
	server, _, _ := newTestServer(t)
	seedEvents(t, server, 5)

	first := url.Values{}
	first.Set("limit", "2")
	first.Set("service", "s")
	first.Set("since", "0001-01-01T00:00:00Z")
	status, out := page(t, server, "?"+first.Encode())
	if status != http.StatusOK {
		t.Fatalf("first page: %d %v", status, out)
	}
	events, cursor := pageEvents(t, out)
	if len(events) != 2 || events[0]["id"] != "e0" || events[1]["id"] != "e1" {
		t.Fatalf("first page: %v", events)
	}

	// Carrying filters but omitting the since makes its full filter set a
	// missing bound, which differs from the explicit zero instant even
	// though both would select the same events.
	missing := url.Values{}
	missing.Set("limit", "2")
	missing.Set("cursor", cursor)
	missing.Set("service", "s")
	status, out = page(t, server, "?"+missing.Encode())
	if status != http.StatusBadRequest {
		t.Fatalf("missing bound vs explicit zero must be 400, got %d %v", status, out)
	}
	if msg, _ := out["error"].(string); msg == "" {
		t.Fatal("filter mismatch needs a non-empty error")
	}

	// Expressing the same zero instant in another time zone is the same
	// condition, so paging continues from where the first page stopped.
	equiv := url.Values{}
	equiv.Set("limit", "2")
	equiv.Set("cursor", cursor)
	equiv.Set("service", "s")
	equiv.Set("since", "0001-01-01T08:00:00+08:00")
	status, out = page(t, server, "?"+equiv.Encode())
	if status != http.StatusOK {
		t.Fatalf("equivalent timezone should continue, got %d %v", status, out)
	}
	events, _ = pageEvents(t, out)
	if len(events) != 2 || events[0]["id"] != "e2" || events[1]["id"] != "e3" {
		t.Fatalf("equivalent continuation must resume from e2: %v", events)
	}

	// Carrying an explicit until where the snapshot had none is likewise a
	// different condition.
	added := url.Values{}
	added.Set("limit", "2")
	added.Set("cursor", cursor)
	added.Set("service", "s")
	added.Set("since", "0001-01-01T00:00:00Z")
	added.Set("until", "0001-01-01T00:00:00Z")
	status, out = page(t, server, "?"+added.Encode())
	if status != http.StatusBadRequest {
		t.Fatalf("adding an until bound must be 400, got %d %v", status, out)
	}
}

func TestPageMethodNotAllowed(t *testing.T) {
	server, _, _ := newTestServer(t)
	resp, err := http.Post(server.URL+"/events/page", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("POST on /events/page should be 405, got %d", resp.StatusCode)
	}
}
