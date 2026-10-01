package httpapi

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/cocosolocoder/signalroom/internal/events"
)

type pageBody struct {
	Events     []map[string]any `json:"events"`
	NextCursor *string          `json:"next_cursor"`
	Error      string           `json:"error"`
}

func getPage(t *testing.T, server *httptest.Server, query string) (int, pageBody) {
	t.Helper()
	resp, err := http.Get(server.URL + "/events/page" + query)
	if err != nil {
		t.Fatalf("get page: %v", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var body pageBody
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	return resp.StatusCode, body
}

func pageIDs(body pageBody) string {
	parts := make([]string, len(body.Events))
	for i, event := range body.Events {
		parts[i] = event["id"].(string)
	}
	return strings.Join(parts, ",")
}

// seedTimeline posts n events e0..e(n-1) at one-minute intervals from 09:00,
// all service "svc", alternating critical/info severity.
func seedTimeline(t *testing.T, server *httptest.Server, n int) {
	t.Helper()
	var b strings.Builder
	b.WriteString(`{"events":[`)
	for i := range n {
		if i > 0 {
			b.WriteByte(',')
		}
		severity := "info"
		if i%2 == 0 {
			severity = "critical"
		}
		fmt.Fprintf(&b, `{"id":"e%d","service":"svc","severity":%q,"message":"m","at":"2026-10-01T09:%02d:00Z"}`, i, severity, i)
	}
	b.WriteString(`]}`)
	if status, out := post(t, server, b.String()); status != http.StatusOK {
		t.Fatalf("seed status %d: %v", status, out)
	}
}

// walkPages pages through the whole result set with the given per-page limit
// and returns every event id in order.
func walkPages(t *testing.T, server *httptest.Server, firstQuery string, limit int) string {
	t.Helper()
	var got []string
	query := firstQuery
	cursor := ""
	for {
		var q string
		if cursor == "" {
			q = query
		} else {
			q = "?cursor=" + url.QueryEscape(cursor)
			if limit != 0 {
				q += fmt.Sprintf("&limit=%d", limit)
			}
		}
		status, body := getPage(t, server, q)
		if status != http.StatusOK {
			t.Fatalf("page status %d: %s", status, body.Error)
		}
		for _, event := range body.Events {
			got = append(got, event["id"].(string))
		}
		if body.NextCursor == nil {
			return strings.Join(got, ",")
		}
		cursor = *body.NextCursor
	}
}

func TestPageBasicPagination(t *testing.T) {
	server, _, _ := newTestServer(t)
	seedTimeline(t, server, 5)

	status, first := getPage(t, server, "?limit=2")
	if status != http.StatusOK || len(first.Events) != 2 {
		t.Fatalf("first page: %d %+v", status, first)
	}
	if pageIDs(first) != "e0,e1" || first.NextCursor == nil {
		t.Fatalf("first page contents/cursor: %+v", first)
	}

	status, second := getPage(t, server, "?cursor="+url.QueryEscape(*first.NextCursor)+"&limit=2")
	if status != http.StatusOK || pageIDs(second) != "e2,e3" || second.NextCursor == nil {
		t.Fatalf("second page: %d %+v", status, second)
	}

	status, third := getPage(t, server, "?cursor="+url.QueryEscape(*second.NextCursor)+"&limit=2")
	if status != http.StatusOK || pageIDs(third) != "e4" {
		t.Fatalf("third page: %d %+v", status, third)
	}
	if third.NextCursor != nil {
		t.Fatalf("end cursor must be null, got %q", *third.NextCursor)
	}
}

func TestPageWalksEntireSetExactlyOnce(t *testing.T) {
	server, _, _ := newTestServer(t)
	seedTimeline(t, server, 7)
	for _, limit := range []int{1, 2, 3, 7, 100} {
		if got := walkPages(t, server, fmt.Sprintf("?limit=%d", limit), limit); got != "e0,e1,e2,e3,e4,e5,e6" {
			t.Fatalf("limit=%d walked %s", limit, got)
		}
	}
}

func TestPageDefaultLimitAndEmptyResult(t *testing.T) {
	server, _, _ := newTestServer(t)

	// Empty timeline: events is [] (not null) and no cursor.
	status, body := getPage(t, server, "")
	if status != http.StatusOK || body.NextCursor != nil || len(body.Events) != 0 {
		t.Fatalf("empty page: %d %+v", status, body)
	}
	if raw := rawPageBody(t, server, ""); !strings.Contains(raw, `"events":[]`) {
		t.Fatalf("empty events must encode as []: %s", raw)
	}

	seedTimeline(t, server, 5)
	status, body = getPage(t, server, "?service=nonexistent")
	if status != http.StatusOK || len(body.Events) != 0 || body.NextCursor != nil {
		t.Fatalf("empty filtered page: %d %+v", status, body)
	}

	// Default limit of 100 returns everything in one page.
	status, body = getPage(t, server, "")
	if status != http.StatusOK || len(body.Events) != 5 || body.NextCursor != nil {
		t.Fatalf("default limit page: %d %+v", status, body)
	}
}

func rawPageBody(t *testing.T, server *httptest.Server, query string) string {
	t.Helper()
	resp, err := http.Get(server.URL + "/events/page" + query)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func TestPageLimitValidation(t *testing.T) {
	server, _, _ := newTestServer(t)
	seedTimeline(t, server, 2)

	bad := []string{
		"limit=0",
		"limit=-1",
		"limit=1001",
		"limit=1.5",
		"limit=abc",
		"limit=0x1",
		"limit=",
		"limit=%20",
		"limit=1%20",
		"limit=1&limit=2",
		"limit=99999999999999999999",
	}
	for _, q := range bad {
		status, body := getPage(t, server, "?"+q)
		if status != http.StatusBadRequest || body.Error == "" {
			t.Fatalf("%s want 400 with error, got %d %q", q, status, body.Error)
		}
	}

	// Boundary values and leading zeros (decimal integers, like the compare
	// step parameter) are accepted.
	for _, q := range []string{"limit=1", "limit=1000", "limit=01"} {
		if status, body := getPage(t, server, "?"+q); status != http.StatusOK {
			t.Fatalf("%s want 200, got %d %s", q, status, body.Error)
		}
	}
}

func TestPageFiltersMatchEventsEndpoint(t *testing.T) {
	server, _, _ := newTestServer(t)
	seedTimeline(t, server, 6) // e0 critical, e1 info, e2 critical, ...

	cases := map[string]string{
		"severity": "?severity=critical&limit=2",
		"service":  "?service=svc&limit=3",
		"range":    "?since=2026-10-01T09:01:00Z&until=2026-10-01T09:03:00Z&limit=2",
	}
	want := map[string]string{
		"severity": "e0,e2,e4",
		"service":  "e0,e1,e2,e3,e4,e5",
		"range":    "e1,e2,e3",
	}
	for name, q := range cases {
		if got := walkPages(t, server, q, 2); got != want[name] {
			t.Fatalf("%s: got %s want %s", name, got, want[name])
		}
	}

	// Invalid filters on the first page are 400 exactly like GET /events.
	for _, q := range []string{
		"?since=banana",
		"?since=2026-10-01T10:00:00Z&until=2026-10-01T09:00:00Z",
		"?label=env",
	} {
		if status, body := getPage(t, server, q); status != http.StatusBadRequest || body.Error == "" {
			t.Fatalf("%s want 400, got %d %q", q, status, body.Error)
		}
	}
}

func TestPageKeepsFullLabels(t *testing.T) {
	server, _, _ := newTestServer(t)
	body := `{"events":[
		{"id":"a","service":"svc","severity":"info","message":"m","at":"2026-10-01T09:00:00Z","labels":{"env":"prod","version":"v2"}},
		{"id":"b","service":"svc","severity":"info","message":"m","at":"2026-10-01T09:01:00Z"}
	]}`
	if status, out := post(t, server, body); status != http.StatusOK {
		t.Fatalf("seed: %d %v", status, out)
	}
	status, page := getPage(t, server, "?limit=1")
	if status != http.StatusOK {
		t.Fatalf("status %d", status)
	}
	labels, ok := page.Events[0]["labels"].(map[string]any)
	if !ok || labels["env"] != "prod" || labels["version"] != "v2" {
		t.Fatalf("labels must be fully preserved: %+v", page.Events[0])
	}

	status, last := getPage(t, server, "?cursor="+url.QueryEscape(*page.NextCursor)+"&limit=1")
	if status != http.StatusOK || pageIDs(last) != "b" {
		t.Fatalf("second page: %d %+v", status, last)
	}
	if _, present := last.Events[0]["labels"]; present {
		t.Fatalf("label-less event must omit labels: %+v", last.Events[0])
	}
}

func TestPageContinuationFilterEquivalence(t *testing.T) {
	server, _, _ := newTestServer(t)
	body := `{"events":[
		{"id":"a","service":"gateway","severity":"critical","message":"m","at":"2026-10-01T09:00:00Z","labels":{"env":"prod","version":"v2"}},
		{"id":"b","service":"gateway","severity":"info","message":"m","at":"2026-10-01T09:05:00Z","labels":{"env":"prod","version":"v2"}},
		{"id":"c","service":"gateway","severity":"critical","message":"m","at":"2026-10-01T09:10:00Z","labels":{"env":"prod","version":"v2"}}
	]}`
	if status, out := post(t, server, body); status != http.StatusOK {
		t.Fatalf("seed: %d %v", status, out)
	}

	status, first := getPage(t, server,
		"?service=%20gateway%20&severity=CRITICAL&label=env=prod&label=version=v2&since=2026-10-01T09:00:00Z&limit=1")
	if status != http.StatusOK || pageIDs(first) != "a" {
		t.Fatalf("first page: %d %+v", status, first)
	}

	// Equivalent re-statement: reordered labels, whitespace, severity case,
	// same instant in another time zone.
	equiv := "?cursor=" + url.QueryEscape(*first.NextCursor) +
		"&service=gateway&severity=critical" +
		"&label=version=%20v2%20&label=env=%20prod%20" +
		"&since=2026-10-01T17:00:00%2B08:00&limit=1"
	status, second := getPage(t, server, equiv)
	if status != http.StatusOK || pageIDs(second) != "c" {
		t.Fatalf("equivalent filters must continue, got %d %+v", status, second)
	}

	// A different single condition, a partial restatement, and an added
	// condition are all 400.
	base := "?cursor=" + url.QueryEscape(*first.NextCursor)
	for _, suffix := range []string{
		"&service=other",
		"&service=gateway",
		"&severity=critical",
		"&service=gateway&severity=critical&label=env=prod&label=version=v2&since=2026-10-01T09:00:00Z&until=2026-10-01T09:10:00Z",
	} {
		status, bad := getPage(t, server, base+suffix)
		if status != http.StatusBadRequest || bad.Error == "" {
			t.Fatalf("%s want 400, got %d %q", suffix, status, bad.Error)
		}
	}

	// No filters at all resumes with the frozen ones.
	status, resumed := getPage(t, server, base+"&limit=1")
	if status != http.StatusOK || pageIDs(resumed) != "c" {
		t.Fatalf("filter-less continuation: %d %+v", status, resumed)
	}
}

func TestPageSnapshotIsolationFromNewIngests(t *testing.T) {
	server, _, tl := newTestServer(t)
	seedTimeline(t, server, 6) // critical: e0, e2, e4 at 09:00, 09:02, 09:04

	// Freeze a filtered set (critical: e0,e2,e4) one event at a time.
	status, first := getPage(t, server, "?severity=critical&limit=1")
	if status != http.StatusOK || pageIDs(first) != "e0" {
		t.Fatalf("first page: %d %+v", status, first)
	}

	// While paging, new events arrive: one earlier than everything and
	// matching the filter, one at the same instant as an existing match, and
	// one replay of an identical event.
	intruders := `{"events":[
		{"id":"early","service":"svc","severity":"critical","message":"m","at":"2026-10-01T08:00:00Z"},
		{"id":"sametime","service":"svc","severity":"critical","message":"m","at":"2026-10-01T09:02:00Z"},
		{"id":"e0","service":"svc","severity":"critical","message":"m","at":"2026-10-01T09:00:00Z"}
	]}`
	if status, out := post(t, server, intruders); status != http.StatusOK {
		t.Fatalf("intruders: %d %v", status, out)
	}
	if got := tl.Query(events.Query{Severity: "critical"}); len(got) != 5 {
		t.Fatalf("fresh query must see the new commits, got %d", len(got))
	}

	status, second := getPage(t, server, "?cursor="+url.QueryEscape(*first.NextCursor)+"&limit=1")
	if status != http.StatusOK || pageIDs(second) != "e2" {
		t.Fatalf("frozen set must not admit intruders: %d %+v", status, second)
	}
	if second.NextCursor == nil {
		t.Fatalf("frozen set still holds e4, cursor must not be null")
	}

	// The same cursor is reusable: repeated requests return the same page and
	// the same next cursor, and concurrent clients never consume progress.
	cursor := *first.NextCursor
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			st, body := getPage(t, server, "?cursor="+url.QueryEscape(cursor)+"&limit=1")
			if st != http.StatusOK || pageIDs(body) != "e2" || body.NextCursor == nil {
				t.Errorf("shared cursor page: %d %+v", st, body)
			}
		}()
	}
	wg.Wait()

	// The frozen walk still finishes on e4 without any intruder.
	status, third := getPage(t, server, "?cursor="+url.QueryEscape(*second.NextCursor)+"&limit=1")
	if status != http.StatusOK || pageIDs(third) != "e4" || third.NextCursor != nil {
		t.Fatalf("frozen walk must end at e4: %d %+v", status, third)
	}

	// A fresh first page now includes the new commits. e2 and sametime share
	// the 09:02 instant, so the id tie-break puts e2 first.
	status, restarted := getPage(t, server, "?severity=critical&limit=10")
	if status != http.StatusOK || pageIDs(restarted) != "early,e0,e2,sametime,e4" {
		t.Fatalf("new first page must see the current set: %d %+v", status, restarted)
	}
}

func TestPageLimitCanChangeMidWalk(t *testing.T) {
	server, _, _ := newTestServer(t)
	seedTimeline(t, server, 5)

	status, first := getPage(t, server, "?limit=1")
	if status != http.StatusOK || pageIDs(first) != "e0" {
		t.Fatalf("first: %d %+v", status, first)
	}
	// Switch to a larger limit from the same position.
	status, second := getPage(t, server, "?cursor="+url.QueryEscape(*first.NextCursor)+"&limit=3")
	if status != http.StatusOK || pageIDs(second) != "e1,e2,e3" {
		t.Fatalf("larger limit from same position: %d %+v", status, second)
	}
	// And shrink again.
	status, third := getPage(t, server, "?cursor="+url.QueryEscape(*second.NextCursor)+"&limit=1")
	if status != http.StatusOK || pageIDs(third) != "e4" || third.NextCursor != nil {
		t.Fatalf("smaller limit to the end: %d %+v", status, third)
	}
}

func TestPageCursorRepeatedIsStable(t *testing.T) {
	server, _, _ := newTestServer(t)
	seedTimeline(t, server, 3)
	_, first := getPage(t, server, "?limit=1")
	cursor := *first.NextCursor

	firstNext := ""
	for i := range 4 {
		status, body := getPage(t, server, "?cursor="+url.QueryEscape(cursor)+"&limit=1")
		if status != http.StatusOK || pageIDs(body) != "e1" || body.NextCursor == nil {
			t.Fatalf("repeat %d: %d %+v", i, status, body)
		}
		if firstNext == "" {
			firstNext = *body.NextCursor
		} else if *body.NextCursor != firstNext {
			t.Fatalf("repeat %d changed the next cursor", i)
		}
	}
}

func TestPageRejectsBadCursors(t *testing.T) {
	server, _, _ := newTestServer(t)
	seedTimeline(t, server, 2)
	_, first := getPage(t, server, "?limit=1")
	goodCursor := *first.NextCursor
	goodPayload, err := verifyTestCursor(goodCursor)
	if err != nil {
		t.Fatal(err)
	}
	signWith := func(secret []byte, payload []byte) string {
		mac := hmac.New(sha256.New, secret)
		mac.Write(payload)
		enc := base64.RawURLEncoding
		return enc.EncodeToString(payload) + "." + enc.EncodeToString(mac.Sum(nil))
	}

	// Flip a payload character: the first character carries six significant
	// bits, so this changes the payload and breaks the signature.
	tampered := flipFirstChar(goodCursor)

	bad := []string{
		"",
		"nodot",
		".",
		"abc.",
		".abc",
		"a.b.c",
		"!!!.!!!",
		tampered,
		// Signed by a different directory's secret.
		signWith([]byte("other-secret-other-secret-other!!"), goodPayload),
		// Well-signed by us but an out-of-range offset.
		marshalAndSignCursor(t, cursorData{
			Version: cursorVersion,
			IDs:     []string{"e0", "e1"},
			Offset:  5,
		}),
		// Well-signed but unknown version.
		marshalAndSignCursor(t, cursorData{
			Version: 99,
			IDs:     []string{"e0"},
		}),
		// Well-signed but references an event this directory never saw.
		marshalAndSignCursor(t, cursorData{
			Version: cursorVersion,
			IDs:     []string{"foreign-event"},
		}),
		// Well-signed but payload is not JSON.
		signTestCursor([]byte("not-json")),
	}
	for i, token := range bad {
		status, body := getPage(t, server, "?cursor="+url.QueryEscape(token)+"&limit=1")
		if status != http.StatusBadRequest || body.Error == "" {
			t.Fatalf("bad cursor %d want 400, got %d %q (token=%q)", i, status, body.Error, token)
		}
	}

	// Repeated or empty cursor parameters are also 400.
	for _, q := range []string{
		"?cursor=&limit=1",
		"?cursor=a&cursor=b",
	} {
		if status, body := getPage(t, server, q); status != http.StatusBadRequest || body.Error == "" {
			t.Fatalf("%q want 400, got %d %q", q, status, body.Error)
		}
	}
}

func marshalAndSignCursor(t *testing.T, data cursorData) string {
	t.Helper()
	raw, err := json.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}
	return signTestCursor(raw)
}

// flipFirstChar changes the first base64url character to a different valid
// one; it carries six significant payload bits, so the change alters the
// payload bytes and invalidates the signature.
func flipFirstChar(token string) string {
	b := []byte(token)
	if b[0] == 'A' {
		b[0] = 'B'
	} else {
		b[0] = 'A'
	}
	return string(b)
}

func TestPageReturns503WhenStoragePoisoned(t *testing.T) {
	server, store, _ := newTestServer(t)
	seedTimeline(t, server, 2)
	_, first := getPage(t, server, "?limit=1")
	cursor := *first.NextCursor

	store.failNext = true
	if status, out := post(t, server, `{"events":[{"id":"z","service":"s","severity":"info","message":"m","at":"2026-10-01T09:10:00Z"}]}`); status != http.StatusServiceUnavailable {
		t.Fatalf("poisoning write: %d %v", status, out)
	}
	for _, q := range []string{"", "?limit=1", "?cursor=" + url.QueryEscape(cursor)} {
		if status, body := getPage(t, server, q); status != http.StatusServiceUnavailable || body.Error == "" {
			t.Fatalf("%q want 503, got %d %q", q, status, body.Error)
		}
	}
}

func TestPageMethodNotAllowed(t *testing.T) {
	server, _, _ := newTestServer(t)
	req, _ := http.NewRequest(http.MethodPost, server.URL+"/events/page", strings.NewReader(""))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("want 405, got %d", resp.StatusCode)
	}
}
