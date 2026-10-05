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
	"github.com/cocosolocoder/signalroom/internal/metrics"
)

type fakeMetricStorage struct {
	mu       sync.Mutex
	failNext bool
	poisoned bool
	batches  [][]metrics.Sample

	// stall, when non-nil, blocks one Append (with the metric store write
	// lock held) until the channel is closed; stallEntered closes once that
	// Append reaches the wait. armStall sets both up.
	stall        chan struct{}
	stallEntered chan struct{}
}

// armStall makes the next Append block until the returned release channel is
// closed. The returned entered channel closes once that Append is waiting, so
// a test knows the batch is held mid commit.
func (f *fakeMetricStorage) armStall() (release, entered chan struct{}) {
	release = make(chan struct{})
	entered = make(chan struct{})
	f.mu.Lock()
	f.stall = release
	f.stallEntered = entered
	f.mu.Unlock()
	return release, entered
}

func (f *fakeMetricStorage) Append(batch []metrics.Sample) error {
	f.mu.Lock()
	stall := f.stall
	entered := f.stallEntered
	f.stall = nil
	f.stallEntered = nil
	f.mu.Unlock()
	if stall != nil {
		// Only the first Append blocks: the stalled submission still holds
		// the metric store lock, so no other Append can reach here at once.
		close(entered)
		<-stall
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failNext {
		f.poisoned = true
		f.failNext = false
		return errors.New("simulated metric write failure")
	}
	f.batches = append(f.batches, append([]metrics.Sample(nil), batch...))
	return nil
}

func (f *fakeMetricStorage) Poisoned() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.poisoned
}

func newMetricsServer(t *testing.T, store *metrics.Store, mstore *fakeMetricStorage) *httptest.Server {
	t.Helper()
	if store == nil {
		store = metrics.NewStore()
	}
	if mstore == nil {
		mstore = &fakeMetricStorage{}
	}
	tl := events.NewTimeline()
	eventStore := &fakeStorage{cursorKey: []byte("test-cursor-key-0123456789ab")}
	server := httptest.NewServer(NewHandler(tl, eventStore, eventStore, WithMetrics(store, mstore)))
	t.Cleanup(server.Close)
	return server
}

func metricsDo(t *testing.T, server *httptest.Server, method, path, body string) (int, map[string]any) {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, server.URL+path, reader)
	if err != nil {
		t.Fatal(err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode %q (%d): %v", raw, resp.StatusCode, err)
	}
	return resp.StatusCode, out
}

func postSamples(t *testing.T, server *httptest.Server, body string) map[string]any {
	t.Helper()
	status, out := metricsDo(t, server, http.MethodPost, "/metrics", body)
	if status >= 400 {
		t.Fatalf("POST /metrics %d: %v", status, out)
	}
	return out
}

func TestPostMetricsCreatedAndReplayed(t *testing.T) {
	server := newMetricsServer(t, nil, nil)
	body := `{"samples":[
		{"id":"s1","service":"gw","name":"requests","at":"2026-10-01T10:00:00Z","value":1},
		{"id":"s2","service":"gw","name":"requests","at":"2026-10-01T10:00:01Z","value":2,"labels":{"env":"prod"}}
	]}`
	out := postSamples(t, server, body)
	if int(number(out["created"])) != 2 || int(number(out["replayed"])) != 0 {
		t.Fatalf("initial: %v", out)
	}

	// Retry with 1.0 numeric equivalence.
	retry := `{"samples":[
		{"id":"s1","service":"gw","name":"requests","at":"2026-10-01T10:00:00Z","value":1.0},
		{"id":"s2","service":"gw","name":"requests","at":"2026-10-01T18:00:01+08:00","value":2,"labels":{"env":"prod"}}
	]}`
	out = postSamples(t, server, retry)
	if int(number(out["created"])) != 0 || int(number(out["replayed"])) != 2 {
		t.Fatalf("retry: %v", out)
	}
}

func TestPostMetricsValidationAndConflict(t *testing.T) {
	server := newMetricsServer(t, nil, nil)
	postSamples(t, server, `{"samples":[{"id":"s1","service":"gw","name":"requests","at":"2026-10-01T10:00:00Z","value":1}]}`)

	cases := []struct {
		name   string
		body   string
		status int
	}{
		{"empty array", `{"samples":[]}`, http.StatusBadRequest},
		{"missing samples", `{}`, http.StatusBadRequest},
		{"unknown field", `{"samples":[{"id":"s9","service":"gw","name":"requests","at":"2026-10-01T10:00:00Z","value":1}],"x":1}`, http.StatusBadRequest},
		{"blank id", `{"samples":[{"id":"  ","service":"gw","name":"requests","at":"2026-10-01T10:00:00Z","value":1}]}`, http.StatusBadRequest},
		{"negative value", `{"samples":[{"id":"s9","service":"gw","name":"requests","at":"2026-10-01T10:00:00Z","value":-1}]}`, http.StatusBadRequest},
		{"string value", `{"samples":[{"id":"s9","service":"gw","name":"requests","at":"2026-10-01T10:00:00Z","value":"1"}]}`, http.StatusBadRequest},
		{"bad time", `{"samples":[{"id":"s9","service":"gw","name":"requests","at":"nope","value":1}]}`, http.StatusBadRequest},
		{"duplicate json field", `{"samples":[{"id":"s9","id":"s10","service":"gw","name":"requests","at":"2026-10-01T10:00:00Z","value":1}]}`, http.StatusBadRequest},
		{"changed content", `{"samples":[{"id":"s1","service":"gw","name":"requests","at":"2026-10-01T10:00:00Z","value":5}]}`, http.StatusConflict},
		{"series time clash", `{"samples":[{"id":"s2","service":"gw","name":"requests","at":"2026-10-01T10:00:00Z","value":1}]}`, http.StatusConflict},
		{"in-batch dup id", `{"samples":[{"id":"d","service":"gw","name":"requests","at":"2026-10-01T10:00:01Z","value":1},{"id":"d","service":"gw","name":"requests","at":"2026-10-01T10:00:02Z","value":2}]}`, http.StatusConflict},
	}
	for _, tc := range cases {
		status, out := metricsDo(t, server, http.MethodPost, "/metrics", tc.body)
		if status != tc.status {
			t.Fatalf("%s: want %d, got %d (%v)", tc.name, tc.status, status, out)
		}
		if out["error"] == "" {
			t.Fatalf("%s: non-empty error required, got %v", tc.name, out)
		}
	}
}

func TestPostMetricsValidationPrecedesConflict(t *testing.T) {
	server := newMetricsServer(t, nil, nil)
	postSamples(t, server, `{"samples":[{"id":"s1","service":"gw","name":"requests","at":"2026-10-01T10:00:00Z","value":1}]}`)
	status, _ := metricsDo(t, server, http.MethodPost, "/metrics", `{"samples":[
		{"id":"s1","service":"gw","name":"requests","at":"2026-10-01T10:00:00Z","value":5},
		{"id":" ","service":"gw","name":"requests","at":"2026-10-01T10:00:01Z","value":1}
	]}`)
	if status != http.StatusBadRequest {
		t.Fatalf("validation must precede conflict, got %d", status)
	}
}

func TestPostMetricsFailurePoisonsOnlyMetrics(t *testing.T) {
	mstore := &fakeMetricStorage{failNext: true}
	server := newMetricsServer(t, metrics.NewStore(), mstore)

	status, _ := metricsDo(t, server, http.MethodPost, "/metrics",
		`{"samples":[{"id":"s1","service":"gw","name":"requests","at":"2026-10-01T10:00:00Z","value":1}]}`)
	if status != http.StatusServiceUnavailable {
		t.Fatalf("write failure must be 503, got %d", status)
	}
	// The failed batch is invisible and every later metric request stays 503.
	status, _ = metricsDo(t, server, http.MethodPost, "/metrics",
		`{"samples":[{"id":"s1","service":"gw","name":"requests","at":"2026-10-01T10:00:00Z","value":1}]}`)
	if status != http.StatusServiceUnavailable {
		t.Fatalf("later request must stay 503, got %d", status)
	}
	status, _ = metricsDo(t, server, http.MethodGet,
		"/metrics/aggregate?name=requests&since=2026-10-01T10:00:00Z&until=2026-10-01T11:00:00Z&step=60", "")
	if status != http.StatusServiceUnavailable {
		t.Fatalf("aggregate must stay 503, got %d", status)
	}
	// Events are unaffected.
	resp, err := http.Get(server.URL + "/events")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("events must stay available, got %d", resp.StatusCode)
	}
}

func aggregateGet(t *testing.T, server *httptest.Server, query string) (int, []map[string]any) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, server.URL+"/metrics/aggregate"+query, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode >= 400 {
		var errBody map[string]any
		_ = json.Unmarshal(raw, &errBody)
		return resp.StatusCode, []map[string]any{{"_error": errBody}}
	}
	var out []map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	return resp.StatusCode, out
}

func TestPostMetricsDuplicateFieldsRejected(t *testing.T) {
	server := newMetricsServer(t, nil, nil)

	// uEscape renders a JSON \uXXXX escape without spelling it out in the
	// source literal, so it stays a literal backslash-u sequence in the body.
	uEscape := func(hexdigits, tail string) string {
		return string('\\') + "u" + hexdigits + tail
	}
	valid := `{"id":"s9","service":"gw","name":"requests","at":"2026-10-01T10:00:00Z","value":1}`

	cases := []struct {
		name string
		body string
	}{
		{"outer field twice", `{"samples":[` + valid + `],"samples":[` + valid + `]}`},
		{"outer field unicode escaped", `{"samples":[` + valid + `],"` + uEscape("0073", "amples") + `":[` + valid + `]}`},
		{"sample field twice, same value", `{"samples":[{"id":"s9","id":"s9","service":"gw","name":"requests","at":"2026-10-01T10:00:00Z","value":1}]}`},
		{"sample field null then legal", `{"samples":[{"id":null,"id":"s9","service":"gw","name":"requests","at":"2026-10-01T10:00:00Z","value":1}]}`},
		{"sample field unicode escaped", `{"samples":[{"id":"s9","` + uEscape("0069", "d") + `":"s10","service":"gw","name":"requests","at":"2026-10-01T10:00:00Z","value":1}]}`},
		{"labels name twice, same value", `{"samples":[{"id":"s9","service":"gw","name":"requests","at":"2026-10-01T10:00:00Z","value":1,"labels":{"env":"prod","env":"prod"}}]}`},
		{"labels name unicode escaped", `{"samples":[{"id":"s9","service":"gw","name":"requests","at":"2026-10-01T10:00:00Z","value":1,"labels":{"env":"prod","` + uEscape("0065", "nv") + `":"dev"}}]}`},
	}
	for _, tc := range cases {
		status, out := metricsDo(t, server, http.MethodPost, "/metrics", tc.body)
		if status != http.StatusBadRequest {
			t.Fatalf("%s: want 400, got %d (%v)", tc.name, status, out)
		}
		if msg, _ := out["error"].(string); msg == "" {
			t.Fatalf("%s: non-empty error required, got %v", tc.name, out)
		}
		// A rejected request must not look like an ingestion result.
		if _, present := out["created"]; present {
			t.Fatalf("%s: created count must be absent, got %v", tc.name, out)
		}
		if _, present := out["replayed"]; present {
			t.Fatalf("%s: replayed count must be absent, got %v", tc.name, out)
		}
	}

	// The same names in different objects are not duplicates: distinct
	// samples repeat field names and separate labels objects both carry env.
	body := `{"samples":[
		{"id":"a","service":"gw","name":"requests","at":"2026-10-01T10:00:00Z","value":1,"labels":{"env":"prod"}},
		{"id":"b","service":"gw","name":"requests","at":"2026-10-01T10:00:01Z","value":2,"labels":{"env":"staging"}}
	]}`
	if out := postSamples(t, server, body); int(number(out["created"])) != 2 {
		t.Fatalf("repeated names in separate objects must ingest: %v", out)
	}
}

func TestPostMetricsDuplicateFieldRollsBackWholeBatch(t *testing.T) {
	mstore := &fakeMetricStorage{}
	server := newMetricsServer(t, metrics.NewStore(), mstore)

	// One stored point in the window we will aggregate.
	postSamples(t, server, `{"samples":[
		{"id":"s0","service":"gw","name":"requests","at":"2026-10-01T10:00:00Z","value":10}
	]}`)

	window := "?name=requests&service=gw&since=2026-10-01T10:00:00Z&until=2026-10-01T10:01:00Z&step=60"
	firstSeg := func(t *testing.T) map[string]any {
		t.Helper()
		status, out := aggregateGet(t, server, window)
		if status != http.StatusOK || len(out) != 1 {
			t.Fatalf("aggregate %d: %v", status, out)
		}
		segs := out[0]["segments"].([]any)
		if len(segs) != 1 {
			t.Fatalf("one segment: %v", segs)
		}
		return segs[0].(map[string]any)
	}
	before := firstSeg(t)
	if int(number(before["count"])) != 1 || before["delta"].(float64) != 0 {
		t.Fatalf("baseline: %v", before)
	}

	// A valid new sample followed by a sample with a duplicated field: the
	// whole request is input-shaped rejection, and the leading sample must
	// neither reach durable storage nor take its id or timestamp.
	status, out := metricsDo(t, server, http.MethodPost, "/metrics", `{"samples":[
		{"id":"s1","service":"gw","name":"requests","at":"2026-10-01T10:00:30Z","value":12},
		{"id":"x1","id":"x2","service":"gw","name":"requests","at":"2026-10-01T10:00:40Z","value":1}
	]}`)
	if status != http.StatusBadRequest || out["error"] == "" {
		t.Fatalf("trailing duplicate field must be 400, got %d %v", status, out)
	}
	if len(mstore.batches) != 1 {
		t.Fatalf("rejected batch must never reach durable storage, appended %d batches", len(mstore.batches))
	}
	if seg := firstSeg(t); seg["count"] != before["count"] || seg["delta"] != before["delta"] {
		t.Fatalf("stored series changed after rejection: before %v, after %v", before, seg)
	}

	// The same batch carrying content that would 409 against the stored
	// sample still answers 400: format rejection precedes conflict handling.
	status, out = metricsDo(t, server, http.MethodPost, "/metrics", `{"samples":[
		{"id":"s0","service":"gw","name":"requests","at":"2026-10-01T10:00:00Z","value":99},
		{"id":"x1","id":"x2","service":"gw","name":"requests","at":"2026-10-01T10:00:40Z","value":1}
	]}`)
	if status != http.StatusBadRequest || out["error"] == "" {
		t.Fatalf("duplicate field plus stored conflict must stay 400, got %d %v", status, out)
	}
	if seg := firstSeg(t); seg["count"] != before["count"] || seg["delta"] != before["delta"] {
		t.Fatalf("stored series changed after mixed rejection: before %v, after %v", before, seg)
	}

	// With the duplicate removed, the previously rejected id and timestamp
	// are free and the samples ingest normally.
	out = postSamples(t, server, `{"samples":[
		{"id":"s1","service":"gw","name":"requests","at":"2026-10-01T10:00:30Z","value":12},
		{"id":"s2","service":"gw","name":"requests","at":"2026-10-01T10:01:00Z","value":15}
	]}`)
	if int(number(out["created"])) != 2 || int(number(out["replayed"])) != 0 {
		t.Fatalf("recovery ingestion: %v", out)
	}

	// Half-open window: s1 joins s0 in [10:00,10:01) with delta +2; the point
	// at exactly 10:01 stays out of that window and lands in the next one.
	seg := firstSeg(t)
	if int(number(seg["count"])) != 2 || seg["delta"].(float64) != 2 {
		t.Fatalf("window after recovery: %v", seg)
	}
	nextWindow := "?name=requests&service=gw&since=2026-10-01T10:01:00Z&until=2026-10-01T10:02:00Z&step=60"
	nextStatus, nextAgg := aggregateGet(t, server, nextWindow)
	if nextStatus != http.StatusOK || len(nextAgg) != 1 {
		t.Fatalf("next window: %d %v", nextStatus, nextAgg)
	}
	nextSeg := nextAgg[0]["segments"].([]any)[0].(map[string]any)
	if int(number(nextSeg["count"])) != 1 || nextSeg["delta"].(float64) != 3 {
		t.Fatalf("point at window end must move to next segment (+3): %v", nextSeg)
	}
}

func TestPostMetricsEmptyLabelShapesAccepted(t *testing.T) {
	server := newMetricsServer(t, nil, nil)
	bodies := []string{
		`{"samples":[{"id":"n1","service":"gw","name":"requests","at":"2026-10-01T10:00:00Z","value":1}]}`,
		`{"samples":[{"id":"n2","service":"gw","name":"requests","at":"2026-10-01T10:00:01Z","value":2,"labels":null}]}`,
		`{"samples":[{"id":"n3","service":"gw","name":"requests","at":"2026-10-01T10:00:02Z","value":3,"labels":{}}]}`,
	}
	for i, body := range bodies {
		out := postSamples(t, server, body)
		if int(number(out["created"])) != 1 {
			t.Fatalf("case %d: %v", i, out)
		}
	}
	status, agg := aggregateGet(t, server,
		"?name=requests&service=gw&since=2026-10-01T10:00:00Z&until=2026-10-01T10:01:00Z&step=60")
	if status != http.StatusOK || len(agg) != 1 {
		t.Fatalf("aggregate: %d %v", status, agg)
	}
	if _, hasLabels := agg[0]["labels"]; hasLabels {
		t.Fatalf("all three shapes mean no labels: %v", agg[0])
	}
	if int(number(agg[0]["segments"].([]any)[0].(map[string]any)["count"])) != 3 {
		t.Fatalf("all three samples share the label-less series: %v", agg[0])
	}
}

func TestPostMetricsDuplicateIDAcrossSamplesIsConflict(t *testing.T) {
	// Two well-formed samples sharing one id is a content conflict (409), not
	// a duplicate JSON field (400): the repeated name lives in different
	// objects, while one object naming id twice is still a format error.
	server := newMetricsServer(t, nil, nil)
	postSamples(t, server, `{"samples":[{"id":"s1","service":"gw","name":"requests","at":"2026-10-01T10:00:00Z","value":1}]}`)

	dupIDAcrossSamples := `{"samples":[
		{"id":"d","service":"gw","name":"requests","at":"2026-10-01T10:00:01Z","value":1},
		{"id":"d","service":"gw","name":"requests","at":"2026-10-01T10:00:02Z","value":2}
	]}`
	if status, _ := metricsDo(t, server, http.MethodPost, "/metrics", dupIDAcrossSamples); status != http.StatusConflict {
		t.Fatalf("repeated id across samples must be 409, got %d", status)
	}
	dupFieldInOneSample := `{"samples":[
		{"id":"f","service":"gw","name":"requests","at":"2026-10-01T10:00:01Z","value":1},
		{"id":"g","id":"h","service":"gw","name":"requests","at":"2026-10-01T10:00:02Z","value":2}
	]}`
	if status, _ := metricsDo(t, server, http.MethodPost, "/metrics", dupFieldInOneSample); status != http.StatusBadRequest {
		t.Fatalf("repeated field within one sample must be 400, got %d", status)
	}
}

func TestGetMetricsAggregateShapeAndDeltas(t *testing.T) {
	server := newMetricsServer(t, nil, nil)
	postSamples(t, server, `{"samples":[
		{"id":"p","service":"gw","name":"requests","at":"2026-10-01T09:59:00Z","value":100},
		{"id":"a","service":"gw","name":"requests","at":"2026-10-01T10:00:00Z","value":105},
		{"id":"b","service":"gw","name":"requests","at":"2026-10-01T10:00:30Z","value":3},
		{"id":"c","service":"gw","name":"requests","at":"2026-10-01T10:01:30Z","value":4}
	]}`)

	status, out := aggregateGet(t, server,
		"?name=requests&since=2026-10-01T10:00:00Z&until=2026-10-01T10:02:00Z&step=60")
	if status != http.StatusOK {
		t.Fatalf("status %d: %v", status, out)
	}
	if len(out) != 1 {
		t.Fatalf("want one series, got %d", len(out))
	}
	if out[0]["service"] != "gw" {
		t.Fatalf("service: %v", out[0]["service"])
	}
	if _, hasLabels := out[0]["labels"]; hasLabels {
		t.Fatalf("label-less series must omit labels: %v", out[0])
	}
	segments, ok := out[0]["segments"].([]any)
	if !ok || len(segments) != 2 {
		t.Fatalf("segments: %v", out[0]["segments"])
	}
	first := segments[0].(map[string]any)
	if first["since"] != "2026-10-01T10:00:00Z" || first["until"] != "2026-10-01T10:01:00Z" {
		t.Fatalf("boundaries: %v %v", first["since"], first["until"])
	}
	if int(number(first["count"])) != 2 {
		t.Fatalf("first count: %v", first["count"])
	}
	if first["delta"].(float64) != 8 { // +5 rise, then reset to 3
		t.Fatalf("first delta: %v", first["delta"])
	}
}

func TestGetMetricsAggregateEmptyIsArray(t *testing.T) {
	server := newMetricsServer(t, nil, nil)
	status, raw := httpGetRaw(t, server,
		"/metrics/aggregate?name=requests&since=2026-10-01T10:00:00Z&until=2026-10-01T10:02:00Z&step=60")
	if status != http.StatusOK {
		t.Fatalf("status %d: %s", status, raw)
	}
	if strings.TrimSpace(raw) != "[]" {
		t.Fatalf("no match must be an empty array, got %q", raw)
	}
}

func httpGetRaw(t *testing.T, server *httptest.Server, path string) (int, string) {
	t.Helper()
	resp, err := http.Get(server.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(raw)
}

func TestGetMetricsAggregateValidation(t *testing.T) {
	server := newMetricsServer(t, nil, nil)
	cases := []string{
		"since=2026-10-01T10:00:00Z&until=2026-10-01T10:02:00Z&step=60",       // missing name
		"name=requests&until=2026-10-01T10:02:00Z&step=60",                    // missing since
		"name=requests&since=2026-10-01T10:00:00Z&step=60",                    // missing until
		"name=requests&since=2026-10-01T10:00:00Z&until=2026-10-01T10:02:00Z", // missing step
		"name=requests&since=bogus&until=2026-10-01T10:02:00Z&step=60",
		"name=requests&since=2026-10-01T10:02:00Z&until=2026-10-01T10:00:00Z&step=60", // inverted
		"name=requests&since=2026-10-01T10:00:00Z&until=2026-10-01T10:02:00Z&step=0",
		"name=requests&since=2026-10-01T10:00:00Z&until=2026-10-01T10:02:00Z&step=86401",
		"name=requests&since=2026-10-01T10:00:00Z&until=2026-10-01T10:02:00Z&step=60&step=61", // repeated
		"name=requests&name=other&since=2026-10-01T10:00:00Z&until=2026-10-01T10:02:00Z&step=60",
		"name=requests&since=2026-10-01T10:00:00Z&until=2026-10-01T10:02:00Z&step=1&label=badlabel", // bad label
	}
	for i, query := range cases {
		status, out := aggregateGet(t, server, "?"+query)
		if status != http.StatusBadRequest {
			t.Fatalf("case %d: want 400, got %d (%s)", i, status, query)
		}
		if errMsg, _ := out[0]["_error"].(map[string]any); errMsg["error"] == nil || errMsg["error"] == "" {
			t.Fatalf("case %d: non-empty error required", i)
		}
	}

	// Over 10000 segments: a window of 10001 seconds with step 1.
	big := "?name=requests&since=2026-10-01T10:00:00Z&until=2026-10-01T12:46:41Z&step=1"
	status, _ := aggregateGet(t, server, big)
	if status != http.StatusBadRequest {
		t.Fatalf("segment cap must be 400, got %d", status)
	}
}

func TestGetMetricsAggregateNonFiniteDeltaIs422(t *testing.T) {
	server := newMetricsServer(t, nil, nil)
	postSamples(t, server, `{"samples":[
		{"id":"p","service":"gw","name":"requests","at":"2026-10-01T09:59:00Z","value":0},
		{"id":"a","service":"gw","name":"requests","at":"2026-10-01T10:00:00Z","value":1.7e308},
		{"id":"b","service":"gw","name":"requests","at":"2026-10-01T10:00:30Z","value":1},
		{"id":"c","service":"gw","name":"requests","at":"2026-10-01T10:00:40Z","value":1.7e308}
	]}`)
	status, out := aggregateGet(t, server,
		"?name=requests&since=2026-10-01T10:00:00Z&until=2026-10-01T10:02:00Z&step=60")
	if status != http.StatusUnprocessableEntity {
		t.Fatalf("non-finite delta must be 422, got %d (%v)", status, out)
	}
}

// windowCount runs an aggregate query expected to match exactly one series
// and returns the number of samples in its segments. It is the queryable
// evidence that ingestion stored (or did not store) a point.
func windowCount(t *testing.T, server *httptest.Server, query string) int {
	t.Helper()
	status, out := aggregateGet(t, server, query)
	if status != http.StatusOK || len(out) != 1 {
		t.Fatalf("aggregate expected one series, got %d: %v", status, out)
	}
	total := 0
	for _, seg := range out[0]["segments"].([]any) {
		total += int(number(seg.(map[string]any)["count"]))
	}
	return total
}

const (
	wideOldAt    = "1600-01-01T00:00:00Z"
	wideOldAltTZ = "1600-01-01T08:00:00+08:00"
	wideFutureAt = "2184-07-20T23:34:33.709551616Z"
	// wideFutureAltTZ is the same absolute instant as wideFutureAt written
	// with a seven-hour-behind offset.
	wideFutureAltTZ  = "2184-07-20T16:34:33.709551616-07:00"
	wideOldWindow    = "?name=wide&service=gw&since=1599-12-31T23:59:59Z&until=1600-01-01T00:00:01Z&step=2"
	wideFutureWindow = "?name=wide&service=gw&since=2184-07-20T23:34:33Z&until=2184-07-20T23:34:34Z&step=1"
)

func TestPostMetricsWideDatePointsAccepted(t *testing.T) {
	// These two instants are centuries apart but share one naive UnixNano
	// value because the pre-1678 seconds overflow; both orders must ingest.
	for _, reverse := range []bool{false, true} {
		t.Run("order", func(t *testing.T) {
			server := newMetricsServer(t, nil, nil)
			body := `{"samples":[
				{"id":"old","service":"gw","name":"wide","at":"` + wideOldAt + `","value":1},
				{"id":"future","service":"gw","name":"wide","at":"` + wideFutureAt + `","value":2}
			]}`
			if reverse {
				body = `{"samples":[
					{"id":"future","service":"gw","name":"wide","at":"` + wideFutureAt + `","value":2},
					{"id":"old","service":"gw","name":"wide","at":"` + wideOldAt + `","value":1}
				]}`
			}
			out := postSamples(t, server, body)
			if int(number(out["created"])) != 2 || int(number(out["replayed"])) != 0 {
				t.Fatalf("wide pair must create two points (reverse=%v): %v", reverse, out)
			}
			if count := windowCount(t, server, wideOldWindow); count != 1 {
				t.Fatalf("old-era short window must hold one point, got %d (reverse=%v)", count, reverse)
			}
			if count := windowCount(t, server, wideFutureWindow); count != 1 {
				t.Fatalf("future-era short window must hold one point, got %d (reverse=%v)", count, reverse)
			}

			// Resubmitting both with the same ids/contents replays both, even
			// across the centuries: created stays zero and counts are unchanged.
			out = postSamples(t, server, body)
			if int(number(out["created"])) != 0 || int(number(out["replayed"])) != 2 {
				t.Fatalf("identical wide batch must replay (reverse=%v): %v", reverse, out)
			}
			if count := windowCount(t, server, wideOldWindow); count != 1 {
				t.Fatalf("replay changed old-era count: %d", count)
			}
			if count := windowCount(t, server, wideFutureWindow); count != 1 {
				t.Fatalf("replay changed future-era count: %d", count)
			}
		})
	}
}

func TestPostMetricsPost2262DateAccepted(t *testing.T) {
	// A legal date past the UnixNano-safe year 2262 must not be bounded out.
	server := newMetricsServer(t, nil, nil)
	out := postSamples(t, server, `{"samples":[
		{"id":"far","service":"gw","name":"wide","at":"2300-01-02T03:04:05.678901234Z","value":9}
	]}`)
	if int(number(out["created"])) != 1 || int(number(out["replayed"])) != 0 {
		t.Fatalf("post-2262 sample: %v", out)
	}
	query := "?name=wide&service=gw&since=2300-01-02T03:04:05Z&until=2300-01-02T03:04:06Z&step=1"
	if count := windowCount(t, server, query); count != 1 {
		t.Fatalf("post-2262 point must be queryable, got %d", count)
	}
}

func TestPostMetricsWideDateTimezoneClashes(t *testing.T) {
	server := newMetricsServer(t, nil, nil)
	postSamples(t, server, `{"samples":[
		{"id":"owner","service":"gw","name":"wide","at":"`+wideOldAt+`","value":1}
	]}`)

	// A different id occupying the same series/instant in another timezone
	// spelling clashes with the already-accepted point.
	status, out := metricsDo(t, server, http.MethodPost, "/metrics", `{"samples":[
		{"id":"intruder","service":"gw","name":"wide","at":"`+wideOldAltTZ+`","value":7}
	]}`)
	if status != http.StatusConflict || out["error"] == "" {
		t.Fatalf("timezone-equivalent clash against stored point must be 409 with error, got %d %v", status, out)
	}

	// The same clash entirely inside one batch is also refused.
	status, out = metricsDo(t, server, http.MethodPost, "/metrics", `{"samples":[
		{"id":"f1","service":"gw","name":"wide","at":"`+wideFutureAt+`","value":1},
		{"id":"f2","service":"gw","name":"wide","at":"`+wideFutureAltTZ+`","value":2}
	]}`)
	if status != http.StatusConflict || out["error"] == "" {
		t.Fatalf("in-batch timezone clash must be 409 with error, got %d %v", status, out)
	}

	// Nothing from either rejection became visible: old window still 1, the
	// future window still has no series.
	if count := windowCount(t, server, wideOldWindow); count != 1 {
		t.Fatalf("rejections must not add points, old window count=%d", count)
	}
	if status, agg := aggregateGet(t, server, wideFutureWindow); status != http.StatusOK || len(agg) != 0 {
		t.Fatalf("rejected future batch must be absent, got %d %v", status, agg)
	}
}

func TestPostMetricsWideDateReplayAndNanosecondMove(t *testing.T) {
	server := newMetricsServer(t, nil, nil)
	postSamples(t, server, `{"samples":[
		{"id":"r","service":"gw","name":"wide","at":"`+wideFutureAt+`","value":3}
	]}`)

	// Same id, identical content, only the timezone representation changed:
	// one replay and no new sample.
	out := postSamples(t, server, `{"samples":[
		{"id":"r","service":"gw","name":"wide","at":"`+wideFutureAltTZ+`","value":3}
	]}`)
	if int(number(out["created"])) != 0 || int(number(out["replayed"])) != 1 {
		t.Fatalf("timezone-only resubmission must replay one: %v", out)
	}
	if count := windowCount(t, server, wideFutureWindow); count != 1 {
		t.Fatalf("replay must not add a sample, count=%d", count)
	}

	// The same id whose instant really moves, even by one nanosecond, is a
	// conflict rather than a replay.
	for _, moved := range []string{
		"2184-07-20T23:34:33.709551617Z",
		"2184-07-20T23:34:33.709551615Z",
	} {
		status, out := metricsDo(t, server, http.MethodPost, "/metrics", `{"samples":[
			{"id":"r","service":"gw","name":"wide","at":"`+moved+`","value":3}
		]}`)
		if status != http.StatusConflict || out["error"] == "" {
			t.Fatalf("one-nanosecond move for %s must be 409 with error, got %d %v", moved, status, out)
		}
	}
	if count := windowCount(t, server, wideFutureWindow); count != 1 {
		t.Fatalf("failed moves must not add samples, count=%d", count)
	}
}

func TestPostMetricsWideDateMixedBatchRollsBack(t *testing.T) {
	server := newMetricsServer(t, nil, nil)
	postSamples(t, server, `{"samples":[
		{"id":"owner","service":"gw","name":"wide","at":"`+wideOldAt+`","value":1}
	]}`)

	// One conflicting sample plus one legal new sample: the whole batch is
	// rejected, leaving no trace of the legal sample.
	status, out := metricsDo(t, server, http.MethodPost, "/metrics", `{"samples":[
		{"id":"intruder","service":"gw","name":"wide","at":"`+wideOldAltTZ+`","value":9},
		{"id":"fresh","service":"gw","name":"wide","at":"`+wideFutureAt+`","value":2}
	]}`)
	if status != http.StatusConflict || out["error"] == "" {
		t.Fatalf("mixed batch must be 409 with error, got %d %v", status, out)
	}
	if count := windowCount(t, server, wideOldWindow); count != 1 {
		t.Fatalf("old window must be unchanged after rollback, count=%d", count)
	}
	if status, agg := aggregateGet(t, server, wideFutureWindow); status != http.StatusOK || len(agg) != 0 {
		t.Fatalf("legal sample must not survive the rollback, got %d %v", status, agg)
	}

	// The legal new sample submitted by itself is genuinely new.
	out = postSamples(t, server, `{"samples":[
		{"id":"fresh","service":"gw","name":"wide","at":"`+wideFutureAt+`","value":2}
	]}`)
	if int(number(out["created"])) != 1 || int(number(out["replayed"])) != 0 {
		t.Fatalf("standalone legal sample must create one: %v", out)
	}
	if count := windowCount(t, server, wideFutureWindow); count != 1 {
		t.Fatalf("future window must hold the separately ingested point, count=%d", count)
	}
}

func TestPostMetricsNanosecondApartPointsCoexist(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		t.Run("order", func(t *testing.T) {
			server := newMetricsServer(t, nil, nil)
			body := `{"samples":[
				{"id":"n0","service":"gw","name":"wide","at":"2026-10-01T10:00:00.123456789Z","value":1},
				{"id":"n1","service":"gw","name":"wide","at":"2026-10-01T10:00:00.123456790Z","value":2}
			]}`
			if reverse {
				body = `{"samples":[
					{"id":"n1","service":"gw","name":"wide","at":"2026-10-01T10:00:00.123456790Z","value":2},
					{"id":"n0","service":"gw","name":"wide","at":"2026-10-01T10:00:00.123456789Z","value":1}
				]}`
			}
			out := postSamples(t, server, body)
			if int(number(out["created"])) != 2 || int(number(out["replayed"])) != 0 {
				t.Fatalf("nanosecond-apart samples must both create (reverse=%v): %v", reverse, out)
			}
			query := "?name=wide&service=gw&since=2026-10-01T10:00:00Z&until=2026-10-01T10:00:01Z&step=1"
			if count := windowCount(t, server, query); count != 2 {
				t.Fatalf("one-second window must contain both points, got %d (reverse=%v)", count, reverse)
			}
		})
	}
}

func TestPostMetricsWideDateClashScopedToLabelSet(t *testing.T) {
	server := newMetricsServer(t, nil, nil)
	// Two series distinguished only by labels may each hold the same distant
	// instant.
	out := postSamples(t, server, `{"samples":[
		{"id":"p1","service":"gw","name":"wide","at":"`+wideOldAt+`","value":1,"labels":{"env":"prod"}},
		{"id":"s1","service":"gw","name":"wide","at":"`+wideOldAt+`","value":1,"labels":{"env":"staging"}}
	]}`)
	if int(number(out["created"])) != 2 {
		t.Fatalf("distinct label sets share an instant: %v", out)
	}

	// Another id on either label set at that instant still clashes.
	for _, label := range []string{"prod", "staging"} {
		status, out := metricsDo(t, server, http.MethodPost, "/metrics", `{"samples":[
			{"id":"other-`+label+`","service":"gw","name":"wide","at":"`+wideOldAltTZ+`","value":2,"labels":{"env":"`+label+`"}}
		]}`)
		if status != http.StatusConflict || out["error"] == "" {
			t.Fatalf("env=%s clash must be 409 with error, got %d %v", label, status, out)
		}
	}

	// Each label series is independently queryable at the shared instant.
	for _, label := range []string{"prod", "staging"} {
		query := "?name=wide&service=gw&label=env=" + label +
			"&since=1599-12-31T23:59:59Z&until=1600-01-01T00:00:01Z&step=2"
		if count := windowCount(t, server, query); count != 1 {
			t.Fatalf("env=%s series must hold one point, got %d", label, count)
		}
	}
}

func TestGetMetricsAggregateServiceAndLabelFilters(t *testing.T) {
	server := newMetricsServer(t, nil, nil)
	postSamples(t, server, `{"samples":[
		{"id":"a","service":"gw","name":"requests","at":"2026-10-01T10:00:00Z","value":1,"labels":{"env":"prod"}},
		{"id":"b","service":"gw","name":"requests","at":"2026-10-01T10:00:00Z","value":1,"labels":{"env":"staging"}},
		{"id":"c","service":"api","name":"requests","at":"2026-10-01T10:00:00Z","value":1}
	]}`)
	q := "?name=requests&service=gw&label=env=prod&since=2026-10-01T10:00:00Z&until=2026-10-01T10:01:00Z&step=60"
	status, out := aggregateGet(t, server, q)
	if status != http.StatusOK {
		t.Fatalf("status %d: %v", status, out)
	}
	if len(out) != 1 {
		t.Fatalf("filters should select one series, got %d", len(out))
	}
	labels := out[0]["labels"].(map[string]any)
	if labels["env"] != "prod" {
		t.Fatalf("labels: %v", labels)
	}
}
