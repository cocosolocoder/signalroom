package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/cocosolocoder/signalroom/internal/events"
	"github.com/cocosolocoder/signalroom/internal/logfile"
)

type fakeStorage struct {
	mu       sync.Mutex
	failNext bool
	poisoned bool
	appended []int

	// stall, when non-nil, blocks one Append (with the timeline write lock
	// held) until the channel is closed; stallEntered is closed once that
	// Append reaches the wait. armStall sets both up.
	stall        chan struct{}
	stallEntered chan struct{}

	cursorKey []byte
	snapshots map[string][]byte
}

// armStall makes the next Append block until the returned release channel is
// closed. The returned entered channel closes once that Append is waiting, so
// a test knows the batch is held mid commit.
func (f *fakeStorage) armStall() (release, entered chan struct{}) {
	release = make(chan struct{})
	entered = make(chan struct{})
	f.mu.Lock()
	f.stall = release
	f.stallEntered = entered
	f.mu.Unlock()
	return release, entered
}

func (f *fakeStorage) Append(batch []events.Event) error {
	f.mu.Lock()
	stall := f.stall
	entered := f.stallEntered
	f.stall = nil
	f.stallEntered = nil
	f.mu.Unlock()
	if stall != nil {
		// Only the first Append blocks: the stalled submission still holds
		// the timeline lock, so no other Append can reach here concurrently.
		close(entered)
		<-stall
	}
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

func (f *fakeStorage) CursorKey() []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cursorKey
}

func (f *fakeStorage) SaveSnapshot(id string, payload []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.snapshots == nil {
		f.snapshots = make(map[string][]byte)
	}
	f.snapshots[id] = append([]byte(nil), payload...)
	return nil
}

func (f *fakeStorage) LoadSnapshot(id string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	payload, ok := f.snapshots[id]
	if !ok {
		return nil, errors.New("snapshot not found")
	}
	return append([]byte(nil), payload...), nil
}

func newTestServer(t *testing.T) (*httptest.Server, *fakeStorage, *events.Timeline) {
	t.Helper()
	tl := events.NewTimeline()
	store := &fakeStorage{cursorKey: []byte("test-cursor-key-0123456789ab")}
	server := httptest.NewServer(NewHandler(tl, store, store))
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

func TestPostRejectsDuplicateJSONFields(t *testing.T) {
	server, store, _ := newTestServer(t)
	const at = `"at":"2026-10-01T09:00:00Z"`
	cases := map[string]string{
		"duplicate top events":        `{"events":[],"events":[{"id":"a","service":"s","severity":"info","message":"m",` + at + `}]}`,
		"duplicate event id":          `{"events":[{"id":"a","id":"a","service":"s","severity":"info","message":"m",` + at + `}]}`,
		"duplicate null then id":      `{"events":[{"id":null,"id":"a","service":"s","severity":"info","message":"m",` + at + `}]}`,
		"duplicate service":           `{"events":[{"id":"a","service":"s1","service":"s2","severity":"info","message":"m",` + at + `}]}`,
		"bad labels then good labels": `{"events":[{"id":"a","service":"s","severity":"info","message":"m",` + at + `,"labels":{"env":1},"labels":{"env":"prod"}}]}`,
		"null then real labels":       `{"events":[{"id":"a","service":"s","severity":"info","message":"m",` + at + `,"labels":null,"labels":{"env":"prod"}}]}`,
		"unicode field escape":        `{"events":[{"id":"a","\u0069\u0064":"a","service":"s","severity":"info","message":"m",` + at + `}]}`,
		"unicode label escape":        `{"events":[{"id":"a","service":"s","severity":"info","message":"m",` + at + `,"labels":{"\u0065nv":"a","env":"b"}}]}`,
		"dup field in later event": `{"events":[
			{"id":"a","service":"s","severity":"info","message":"m",` + at + `},
			{"id":"b","id":"c","service":"s","severity":"info","message":"m",` + at + `}
		]}`,
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
		t.Fatalf("ambiguous requests must not persist anything: %v", store.appended)
	}
	if status, rows := get(t, server, ""); status != http.StatusOK || len(rows.([]any)) != 0 {
		t.Fatalf("rejected events must not be queryable: %d %v", status, rows)
	}

	// A rejected input is not a storage failure: a well-formed batch after
	// it goes through normally, including its created/replayed counts.
	good := `{"events":[{"id":"g1","service":"s","severity":" INFO ","message":" hi ","at":"2026-10-01T09:00:00Z"}]}`
	if status, out := post(t, server, good); status != http.StatusOK || out["created"].(float64) != 1 || out["replayed"].(float64) != 0 {
		t.Fatalf("valid batch after rejected ones must succeed: %d %v", status, out)
	}
	if status, out := post(t, server, good); status != http.StatusOK || out["created"].(float64) != 0 || out["replayed"].(float64) != 1 {
		t.Fatalf("retry counts changed: %d %v", status, out)
	}
	status, rows := get(t, server, "")
	arr := rows.([]any)
	if status != http.StatusOK || len(arr) != 1 {
		t.Fatalf("only the valid event should be stored: %d %v", status, rows)
	}
	only := arr[0].(map[string]any)
	if only["id"] != "g1" || only["severity"] != "info" || only["message"] != "hi" {
		t.Fatalf("normalization changed: %v", only)
	}
}

func TestPostDuplicateFieldPrecedesConflict(t *testing.T) {
	server, store, _ := newTestServer(t)
	seed := `{"events":[{"id":"dup","service":"s","severity":"info","message":"v1","at":"2026-10-01T09:00:00Z"}]}`
	if status, out := post(t, server, seed); status != http.StatusOK {
		t.Fatalf("seed: %d %v", status, out)
	}
	// Both a 409 id conflict and a duplicated field: invalid input wins.
	mixed := `{"events":[{"id":"dup","service":"s","severity":"info","message":"v2","at":"2026-10-01T09:00:00Z","at":"2026-10-01T09:00:00Z"}]}`
	if status, out := post(t, server, mixed); status != http.StatusBadRequest {
		t.Fatalf("want 400 before 409, got %d %v", status, out)
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

func TestGetExplicitZeroBounds(t *testing.T) {
	server, _, _ := newTestServer(t)
	body := `{"events":[
		{"id":"a","service":"s","severity":"info","message":"m","at":"2026-10-01T09:00:00Z"},
		{"id":"b","service":"s","severity":"info","message":"m","at":"2026-10-01T09:00:00.000000001Z"}
	]}`
	if status, _ := post(t, server, body); status != http.StatusOK {
		t.Fatal("seed")
	}

	// until at the zero instant is an explicit upper bound, not "unbounded":
	// no event after year 1 may appear.
	status, rows := get(t, server, "?until=0001-01-01T00:00:00Z")
	if status != http.StatusOK {
		t.Fatalf("zero until status: %d %v", status, rows)
	}
	if arr, ok := rows.([]any); !ok || len(arr) != 0 {
		t.Fatalf("zero until must return [], got %v", rows)
	}

	// An event one nanosecond outside a boundary is excluded.
	status, rows = get(t, server, "?until=2026-10-01T09:00:00Z")
	if status != http.StatusOK || len(rows.([]any)) != 1 || rows.([]any)[0].(map[string]any)["id"] != "a" {
		t.Fatalf("until inclusive by the nanosecond: %d %v", status, rows)
	}

	// since == until at an ordinary instant is allowed and inclusive.
	status, rows = get(t, server, "?since=2026-10-01T09:00:00Z&until=2026-10-01T09:00:00Z")
	if status != http.StatusOK || len(rows.([]any)) != 1 || rows.([]any)[0].(map[string]any)["id"] != "a" {
		t.Fatalf("equal endpoints: %d %v", status, rows)
	}

	// since strictly after until, including at the zero instant, is a 400.
	for _, q := range []string{
		"?since=2026-10-01T09:00:00Z&until=0001-01-01T00:00:00Z",
		"?since=0001-01-01T00:00:00.000000001Z&until=0001-01-01T00:00:00Z",
	} {
		status, out := get(t, server, q)
		if status != http.StatusBadRequest {
			t.Fatalf("%s want 400, got %d %v", q, status, out)
		}
		if m, _ := out.(map[string]any)["error"].(string); m == "" {
			t.Fatalf("%s missing non-empty error", q)
		}
	}

	// Equivalent timezone spellings of the same zero instant filter alike.
	status, rows = get(t, server, "?until=0000-12-31T19:00:00-05:00")
	if status != http.StatusOK || len(rows.([]any)) != 0 {
		t.Fatalf("equivalent zero-instant timezone spelling: %d %v", status, rows)
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

// newRealStorageServer builds a handler backed by a real on-disk log, so
// capacity checks see the actual encoded frame sizes. It returns the server
// and the data directory.
func newRealStorageServer(t *testing.T) (*httptest.Server, string) {
	t.Helper()
	dir := t.TempDir()
	log, _, err := logfile.Open(dir)
	if err != nil {
		t.Fatalf("open log: %v", err)
	}
	t.Cleanup(func() { log.Close() })
	server := httptest.NewServer(NewHandler(events.NewTimeline(), log, log))
	t.Cleanup(server.Close)
	return server, dir
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	return info.Size()
}

func eventJSON(id, message string) string {
	return `{"id":"` + id + `","service":"s","severity":"info","message":"` + message + `","at":"2026-10-01T09:00:00Z"}`
}

func TestOversizeBatchIs400NotStorageFailure(t *testing.T) {
	server, dir := newRealStorageServer(t)
	logPath := filepath.Join(dir, "events.log")

	// A 12 MiB message of '<' fits the 16 MiB request body limit, but each
	// character is saved as the 6-byte escape \u003c, pushing the encoded
	// batch of new events past the 64 MiB save capacity.
	big := `{"events":[` + eventJSON("big", strings.Repeat("<", 12<<20)) + `]}`
	status, out := post(t, server, big)
	if status != http.StatusBadRequest {
		t.Fatalf("oversize batch must be 400, got %d %v", status, out)
	}
	msg, _ := out["error"].(string)
	if msg == "" {
		t.Fatal("error must be non-empty")
	}
	if !strings.Contains(msg, "capacity") || strings.Contains(msg, "restart") {
		t.Fatalf("error must explain the capacity rejection without a restart hint, got %q", msg)
	}

	// The whole batch was rejected without touching storage: no record was
	// written, nothing is visible, and the log is not poisoned.
	if got := fileSize(t, logPath); got != 0 {
		t.Fatalf("rejected batch must not append a record, log is %d bytes", got)
	}
	if status, rows := get(t, server, ""); status != http.StatusOK || len(rows.([]any)) != 0 {
		t.Fatalf("rejected events must not be queryable: %d %v", status, rows)
	}

	// A later in-capacity batch succeeds without a restart, and the rejected
	// id is still free to use.
	status, out = post(t, server, `{"events":[`+eventJSON("big", "small")+`]}`)
	if status != http.StatusOK || out["created"].(float64) != 1 || out["replayed"].(float64) != 0 {
		t.Fatalf("in-capacity retry of the rejected id must succeed: %d %v", status, out)
	}
}

func TestBatchCapacityCountsOnlyNewEvents(t *testing.T) {
	server, dir := newRealStorageServer(t)
	logPath := filepath.Join(dir, "events.log")

	// Seed an event whose 6 MiB '<' message encodes to ~36 MiB.
	seed := `{"events":[` + eventJSON("a", strings.Repeat("<", 6<<20)) + `]}`
	if status, out := post(t, server, seed); status != http.StatusOK || out["created"].(float64) != 1 {
		t.Fatalf("seed: %d %v", status, out)
	}
	seedSize := fileSize(t, logPath)

	// A batch of identical retries only is accepted as a replay and appends
	// no new record.
	if status, out := post(t, server, seed); status != http.StatusOK || out["created"].(float64) != 0 || out["replayed"].(float64) != 1 {
		t.Fatalf("pure replay: %d %v", status, out)
	}
	if got := fileSize(t, logPath); got != seedSize {
		t.Fatalf("pure replay must not append a record: log grew from %d to %d", seedSize, got)
	}

	// The stored event (~36 MiB encoded) plus a new ~36 MiB event would
	// exceed 64 MiB if retries counted toward the capacity. Only new events
	// count, so the mixed batch is accepted.
	mixed := `{"events":[` + eventJSON("a", strings.Repeat("<", 6<<20)) + `,` + eventJSON("b", strings.Repeat("<", 6<<20)) + `]}`
	if status, out := post(t, server, mixed); status != http.StatusOK || out["created"].(float64) != 1 || out["replayed"].(float64) != 1 {
		t.Fatalf("mixed retry+new batch within new-event capacity must succeed: %d %v", status, out)
	}
}

func TestCapacityCheckRunsAfterValidationAndConflict(t *testing.T) {
	server, _ := newRealStorageServer(t)
	big := strings.Repeat("<", 12<<20)

	seed := `{"events":[` + eventJSON("a", "m") + `]}`
	if status, _ := post(t, server, seed); status != http.StatusOK {
		t.Fatalf("seed: %d", status)
	}

	// An invalid event in an oversize batch still reports the validation
	// problem, not the capacity one.
	invalid := `{"events":[{"id":"b","service":"s","severity":"info","message":"","at":"2026-10-01T09:00:00Z"},` + eventJSON("c", big) + `]}`
	status, out := post(t, server, invalid)
	if status != http.StatusBadRequest {
		t.Fatalf("invalid oversize batch must be 400, got %d %v", status, out)
	}
	if msg := out["error"].(string); !strings.Contains(msg, "required") {
		t.Fatalf("validation must win over capacity, got %q", msg)
	}

	// A conflicting id in an oversize batch still reports the conflict.
	conflict := `{"events":[` + eventJSON("a", "different") + `,` + eventJSON("c", big) + `]}`
	if status, _ := post(t, server, conflict); status != http.StatusConflict {
		t.Fatalf("conflicting oversize batch must be 409, got %d", status)
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
