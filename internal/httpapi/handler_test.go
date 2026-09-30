package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/cocosolocoder/signalroom/internal/events"
)

type fakeStorage struct {
	mu       sync.Mutex
	failNext bool
	poisoned bool
	appended []int
}

func (f *fakeStorage) Append(batch []events.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.poisoned {
		return errors.New("poisoned")
	}
	if f.failNext {
		f.poisoned = true
		f.failNext = false
		return errors.New("simulated write failure")
	}
	f.appended = append(f.appended, len(batch))
	return nil
}

func (f *fakeStorage) Poisoned() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.poisoned
}

func newTestServer(t *testing.T) (*httptest.Server, *fakeStorage, *events.Timeline) {
	t.Helper()
	tl := events.NewTimeline()
	store := &fakeStorage{}
	server := httptest.NewServer(NewHandler(tl, store))
	t.Cleanup(server.Close)
	return server, store, tl
}

func post(t *testing.T, server *httptest.Server, body string) (int, map[string]any) {
	t.Helper()
	resp, err := http.Post(server.URL+"/events", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	return decode(t, resp)
}

func decode(t *testing.T, resp *http.Response) (int, map[string]any) {
	t.Helper()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	return resp.StatusCode, out
}

func get(t *testing.T, server *httptest.Server, query string) (int, any) {
	t.Helper()
	resp, err := http.Get(server.URL + "/events" + query)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var anyBody any
	if err := json.Unmarshal(raw, &anyBody); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	return resp.StatusCode, anyBody
}

func TestPostCreatesReplaysAndPersists(t *testing.T) {
	server, store, _ := newTestServer(t)
	body := `{"events":[
		{"id":"e1","service":"gateway","severity":" CRITICAL ","message":" spike ","at":"2026-10-01T10:00:00.123456789+08:00"},
		{"id":"e2","service":"api","severity":"info","message":"ok","at":"2026-10-01T09:00:00Z"}
	]}`
	status, out := post(t, server, body)
	if status != http.StatusOK {
		t.Fatalf("status=%d body=%v", status, out)
	}
	if out["created"].(float64) != 2 || out["replayed"].(float64) != 0 {
		t.Fatalf("unexpected counts: %v", out)
	}
	if len(store.appended) != 1 || store.appended[0] != 2 {
		t.Fatalf("expected one durable batch of 2, got %v", store.appended)
	}

	// Identical retry (same instant expressed in UTC) is fully replayed.
	retry := `{"events":[{"id":"e1","service":"gateway","severity":"critical","message":"spike","at":"2026-10-01T02:00:00.123456789Z"}]}`
	status, out = post(t, server, retry)
	if status != http.StatusOK {
		t.Fatalf("retry status=%d body=%v", status, out)
	}
	if out["created"].(float64) != 0 || out["replayed"].(float64) != 1 {
		t.Fatalf("retry counts: %v", out)
	}
	if len(store.appended) != 1 {
		t.Fatal("replay must not touch storage")
	}

	status, rows := get(t, server, "")
	arr := rows.([]any)
	if status != http.StatusOK || len(arr) != 2 {
		t.Fatalf("get: status=%d rows=%v", status, arr)
	}
	// e1 is 10:00+08 == 02:00 UTC, so it precedes e2 at 09:00 UTC.
	first := arr[0].(map[string]any)
	if first["id"] != "e1" || first["severity"] != "critical" || first["message"] != "spike" {
		t.Fatalf("ordering/normalization wrong: %v", first)
	}
	if got := arr[1].(map[string]any)["at"]; got != "2026-10-01T09:00:00Z" {
		t.Fatalf("second row timestamp: %v", got)
	}
}

func TestPostRejectsBadInput(t *testing.T) {
	server, store, _ := newTestServer(t)
	cases := map[string]string{
		"not json":            `{not json`,
		"missing events":      `{}`,
		"events not array":    `{"events":{}}`,
		"empty array":         `{"events":[]}`,
		"unknown top field":   `{"events":[],"extra":1}`,
		"unknown event field": `{"events":[{"id":"a","service":"s","severity":"info","message":"m","at":"2026-10-01T09:00:00Z","x":1}]}`,
		"missing id":          `{"events":[{"service":"s","severity":"info","message":"m","at":"2026-10-01T09:00:00Z"}]}`,
		"blank message":       `{"events":[{"id":"a","service":"s","severity":"info","message":"   ","at":"2026-10-01T09:00:00Z"}]}`,
		"bad timestamp":       `{"events":[{"id":"a","service":"s","severity":"info","message":"m","at":"not-a-time"}]}`,
		"trailing tokens":     `{"events":[{"id":"a","service":"s","severity":"info","message":"m","at":"2026-10-01T09:00:00Z"}]}junk`,
		"null body":           `null`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			status, out := post(t, server, body)
			if status != http.StatusBadRequest {
				t.Fatalf("want 400, got %d (%v)", status, out)
			}
			if msg, _ := out["error"].(string); msg == "" {
				t.Fatal("error response needs a non-empty error string")
			}
		})
	}
	if len(store.appended) != 0 {
		t.Fatalf("rejected requests must not persist anything: %v", store.appended)
	}
}

func TestPostConflicts(t *testing.T) {
	server, store, _ := newTestServer(t)
	seed := `{"events":[{"id":"dup","service":"s","severity":"info","message":"v1","at":"2026-10-01T09:00:00Z"}]}`
	if status, out := post(t, server, seed); status != http.StatusOK {
		t.Fatalf("seed: %d %v", status, out)
	}

	differentContent := `{"events":[{"id":"dup","service":"s","severity":"info","message":"v2","at":"2026-10-01T09:00:00Z"}]}`
	if status, out := post(t, server, differentContent); status != http.StatusConflict {
		t.Fatalf("want 409, got %d %v", status, out)
	}
	withinBatch := `{"events":[
		{"id":"a","service":"s","severity":"info","message":"one","at":"2026-10-01T09:00:00Z"},
		{"id":"a","service":"s2","severity":"info","message":"two","at":"2026-10-01T09:01:00Z"}
	]}`
	if status, out := post(t, server, withinBatch); status != http.StatusConflict {
		t.Fatalf("want 409 for in-batch duplicate, got %d %v", status, out)
	}
	if len(store.appended) != 1 {
		t.Fatalf("conflicts must not persist: %v", store.appended)
	}
}

func TestPostValidationPrecedesConflict(t *testing.T) {
	server, store, _ := newTestServer(t)
	post(t, server, `{"events":[{"id":"dup","service":"s","severity":"info","message":"v1","at":"2026-10-01T09:00:00Z"}]}`)
	mixed := `{"events":[
		{"id":"dup","service":"s","severity":"info","message":"v2","at":"2026-10-01T09:00:00Z"},
		{"id":"bad","service":"s","severity":"info","message":"","at":"2026-10-01T09:01:00Z"}
	]}`
	if status, _ := post(t, server, mixed); status != http.StatusBadRequest {
		t.Fatalf("validation must win over conflict, got %d", status)
	}
	if len(store.appended) != 1 {
		t.Fatalf("rejected batch must not persist: %v", store.appended)
	}
}

func TestGetFiltersRangesAndEmpty(t *testing.T) {
	server, _, _ := newTestServer(t)
	body := `{"events":[
		{"id":"a","service":"gateway","severity":"critical","message":"m","at":"2026-10-01T09:00:00Z"},
		{"id":"b","service":"api","severity":"info","message":"m","at":"2026-10-01T09:00:00Z"},
		{"id":"c","service":"api","severity":"info","message":"m","at":"2026-10-01T10:30:00Z"}
	]}`
	if status, _ := post(t, server, body); status != http.StatusOK {
		t.Fatal("seed")
	}

	status, rows := get(t, server, "?service=api")
	if status != http.StatusOK || len(rows.([]any)) != 2 {
		t.Fatalf("service filter: %d %v", status, rows)
	}
	status, rows = get(t, server, "?severity=CRITICAL")
	if status != http.StatusOK || len(rows.([]any)) != 1 || rows.([]any)[0].(map[string]any)["id"] != "a" {
		t.Fatalf("severity filter: %d %v", status, rows)
	}
	// Both ends inclusive: since == until == 09:00 keeps both 09:00 events.
	status, rows = get(t, server, "?since=2026-10-01T09:00:00Z&until=2026-10-01T09:00:00Z")
	if status != http.StatusOK || len(rows.([]any)) != 2 {
		t.Fatalf("inclusive endpoints: %d %v", status, rows)
	}
	status, rows = get(t, server, "?until=2026-10-01T09:00:00Z")
	if status != http.StatusOK || len(rows.([]any)) != 2 {
		t.Fatalf("open start: %d %v", status, rows)
	}
	status, out := get(t, server, "?service=nope")
	if status != http.StatusOK {
		t.Fatalf("empty status: %d", status)
	}
	arr, ok := out.([]any)
	if !ok || len(arr) != 0 {
		t.Fatalf("empty result must be [], got %v", out)
	}

	for _, q := range []string{
		"?since=2026-10-01T10:00:00Z&until=2026-10-01T09:00:00Z",
		"?since=banana",
		"?until=2026-13-99T00:00:00Z",
	} {
		resp, err := http.Get(server.URL + "/events" + q)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		var errBody map[string]any
		_ = json.Unmarshal(raw, &errBody)
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("%s want 400 got %d", q, resp.StatusCode)
		}
		if msg, _ := errBody["error"].(string); msg == "" {
			t.Fatalf("%s missing error string", q)
		}
	}
}

func TestUnknownRouteAndMethod(t *testing.T) {
	server, _, _ := newTestServer(t)
	resp, err := http.Get(server.URL + "/nope")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("want 404, got %d", resp.StatusCode)
	}
	req, _ := http.NewRequest(http.MethodDelete, server.URL+"/events", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("want 405, got %d", resp.StatusCode)
	}
}

func TestStorageFailureReturns503UntilRestart(t *testing.T) {
	server, store, _ := newTestServer(t)
	store.failNext = true
	status, out := post(t, server, `{"events":[{"id":"a","service":"s","severity":"info","message":"m","at":"2026-10-01T09:00:00Z"}]}`)
	if status != http.StatusServiceUnavailable {
		t.Fatalf("failed write must be 503, got %d %v", status, out)
	}
	// Subsequent requests of every kind keep failing in this process.
	if status, _ = post(t, server, `{"events":[{"id":"b","service":"s","severity":"info","message":"m","at":"2026-10-01T09:01:00Z"}]}`); status != http.StatusServiceUnavailable {
		t.Fatalf("later POST must be 503, got %d", status)
	}
	if status, _ = get(t, server, ""); status != http.StatusServiceUnavailable {
		t.Fatalf("GET must be 503 after storage failure, got %d", status)
	}
}

func TestEmptyTimelineReturnsEmptyArray(t *testing.T) {
	server, _, _ := newTestServer(t)
	status, rows := get(t, server, "")
	if status != http.StatusOK {
		t.Fatalf("status %d", status)
	}
	arr, ok := rows.([]any)
	if !ok || len(arr) != 0 {
		t.Fatalf("want [], got %v", rows)
	}
}
