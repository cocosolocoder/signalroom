package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cocosolocoder/signalroom/internal/events"
	"github.com/cocosolocoder/signalroom/internal/metrics"
)

type fakeMetricStorage struct {
	mu       sync.Mutex
	failNext bool
	poisoned bool
	batches  [][]metrics.Sample
}

func (f *fakeMetricStorage) Append(batch []metrics.Sample) error {
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

func newMetricsServer(t *testing.T, store *metrics.Store, mstore MetricStorage) *httptest.Server {
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

// Batch-visibility regression scenario: one metric name with two existing
// label-distinct series. The batch appends to both, introduces a third
// series, carries a late sample before the window start (changing the first
// in-window sample's predecessor), and includes a sample exactly at the
// window end (excluded). The window has three segments, so committing the
// batch moves the series set, per-segment counts, and per-segment deltas.
// Batch order is deliberately scrambled: results must follow sample time.
const (
	aggVisibilityPreBody = `{"samples":[
		{"id":"a0","service":"gw","name":"requests","at":"2026-10-01T09:59:50Z","value":100,"labels":{"env":"prod"}},
		{"id":"a1","service":"gw","name":"requests","at":"2026-10-01T10:00:05Z","value":110,"labels":{"env":"prod"}},
		{"id":"a2","service":"gw","name":"requests","at":"2026-10-01T10:00:30Z","value":120,"labels":{"env":"prod"}},
		{"id":"a3","service":"gw","name":"requests","at":"2026-10-01T10:01:00Z","value":140,"labels":{"env":"prod"}},
		{"id":"b1","service":"gw","name":"requests","at":"2026-10-01T10:00:10Z","value":50,"labels":{"env":"staging"}},
		{"id":"b2","service":"gw","name":"requests","at":"2026-10-01T10:00:25Z","value":55,"labels":{"env":"staging"}}
	]}`
	aggVisibilityBatchBody = `{"samples":[
		{"id":"c2","service":"gw","name":"requests","at":"2026-10-01T10:00:22Z","value":9,"labels":{"env":"canary"}},
		{"id":"a4","service":"gw","name":"requests","at":"2026-10-01T09:59:55Z","value":108,"labels":{"env":"prod"}},
		{"id":"b4","service":"gw","name":"requests","at":"2026-10-01T10:01:00Z","value":70,"labels":{"env":"staging"}},
		{"id":"a5","service":"gw","name":"requests","at":"2026-10-01T10:00:45Z","value":135,"labels":{"env":"prod"}},
		{"id":"c1","service":"gw","name":"requests","at":"2026-10-01T10:00:02Z","value":7,"labels":{"env":"canary"}},
		{"id":"b3","service":"gw","name":"requests","at":"2026-10-01T10:00:50Z","value":60,"labels":{"env":"staging"}}
	]}`
	aggVisibilityQuery = "?name=requests&since=2026-10-01T10:00:00Z&until=2026-10-01T10:01:00Z&step=20"
)

// wantAggSeries is the expected shape of one series in an aggregate response;
// env == "" means the series must omit labels.
type wantAggSeries struct {
	service string
	env     string
	counts  []int
	deltas  []float64
}

func checkAggregateBody(t *testing.T, got []map[string]any, want []wantAggSeries) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("series count: got %d want %d (%v)", len(got), len(want), got)
	}
	for i, w := range want {
		series := got[i]
		if series["service"] != w.service {
			t.Fatalf("series %d service: got %v want %s", i, series["service"], w.service)
		}
		labels, hasLabels := series["labels"].(map[string]any)
		if w.env == "" {
			if hasLabels {
				t.Fatalf("series %d must omit labels: %v", i, series)
			}
		} else if !hasLabels || labels["env"] != w.env {
			t.Fatalf("series %d labels: got %v want env=%s", i, series["labels"], w.env)
		}
		segments, ok := series["segments"].([]any)
		if !ok || len(segments) != len(w.counts) {
			t.Fatalf("series %d segments: %v", i, series["segments"])
		}
		for j, raw := range segments {
			segment, ok := raw.(map[string]any)
			if !ok {
				t.Fatalf("series %d segment %d: %v", i, j, raw)
			}
			if int(number(segment["count"])) != w.counts[j] || segment["delta"].(float64) != w.deltas[j] {
				t.Fatalf("series %d segment %d: got count=%v delta=%v, want %d/%v",
					i, j, segment["count"], segment["delta"], w.counts[j], w.deltas[j])
			}
		}
	}
}

// Series order is deterministic: service, then the canonical label set, whose
// length-prefixed rendering sorts "prod" < "canary" < "staging" here.
var (
	aggVisibilityPreState = []wantAggSeries{
		{service: "gw", env: "prod", counts: []int{1, 1, 0}, deltas: []float64{10, 10, 0}},
		{service: "gw", env: "staging", counts: []int{1, 1, 0}, deltas: []float64{0, 5, 0}},
	}
	aggVisibilityPostState = []wantAggSeries{
		// a4@09:59:55 (108) replaces a0 (100) as a1's predecessor: seg0 delta 2.
		// a5 fills seg2. a3@10:01:00 stays excluded.
		{service: "gw", env: "prod", counts: []int{1, 1, 1}, deltas: []float64{2, 10, 15}},
		{service: "gw", env: "canary", counts: []int{1, 1, 0}, deltas: []float64{0, 2, 0}},
		// b4@10:01:00 stays excluded; b3 fills seg2.
		{service: "gw", env: "staging", counts: []int{1, 1, 1}, deltas: []float64{0, 5, 5}},
	}
)

func TestGetMetricsAggregateCommittedBatchVisibility(t *testing.T) {
	server := newMetricsServer(t, nil, nil)
	postSamples(t, server, aggVisibilityPreBody)

	status, out := aggregateGet(t, server, aggVisibilityQuery)
	if status != http.StatusOK {
		t.Fatalf("pre-state status %d: %v", status, out)
	}
	checkAggregateBody(t, out, aggVisibilityPreState)
	segments := out[0]["segments"].([]any)
	first := segments[0].(map[string]any)
	last := segments[2].(map[string]any)
	if first["since"] != "2026-10-01T10:00:00Z" || first["until"] != "2026-10-01T10:00:20Z" ||
		last["since"] != "2026-10-01T10:00:40Z" || last["until"] != "2026-10-01T10:01:00Z" {
		t.Fatalf("segment boundaries: %v", segments)
	}

	out2 := postSamples(t, server, aggVisibilityBatchBody)
	if int(number(out2["created"])) != 6 || int(number(out2["replayed"])) != 0 {
		t.Fatalf("batch ingest: %v", out2)
	}

	// After the commit succeeds, the same query shows the batch's full effect.
	status, out = aggregateGet(t, server, aggVisibilityQuery)
	if status != http.StatusOK {
		t.Fatalf("post-state status %d: %v", status, out)
	}
	checkAggregateBody(t, out, aggVisibilityPostState)
}

// blockingMetricStorage blocks inside Append (the commit hook) once armed, so
// a test can hold a batch mid-commit while queries run.
type blockingMetricStorage struct {
	mu      sync.Mutex
	armed   bool
	entered chan struct{}
	release chan struct{}
}

func (s *blockingMetricStorage) Append(batch []metrics.Sample) error {
	s.mu.Lock()
	armed := s.armed
	s.mu.Unlock()
	if armed {
		close(s.entered)
		<-s.release
	}
	return nil
}

func (s *blockingMetricStorage) Poisoned() bool { return false }

func (s *blockingMetricStorage) arm() {
	s.mu.Lock()
	s.armed = true
	s.mu.Unlock()
}

// fetchAggregate is a non-fatal aggregateGet for use inside goroutines.
func fetchAggregate(server *httptest.Server, query string) (int, []map[string]any, error) {
	resp, err := http.Get(server.URL + "/metrics/aggregate" + query)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, nil, err
	}
	var out []map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, out, nil
}

func TestGetMetricsAggregateConcurrentWithIngest(t *testing.T) {
	storage := &blockingMetricStorage{entered: make(chan struct{}), release: make(chan struct{})}
	server := newMetricsServer(t, metrics.NewStore(), storage)
	postSamples(t, server, aggVisibilityPreBody)

	status, preState, err := fetchAggregate(server, aggVisibilityQuery)
	if err != nil || status != http.StatusOK {
		t.Fatalf("pre-state status %d err %v", status, err)
	}
	checkAggregateBody(t, preState, aggVisibilityPreState)

	storage.arm()

	// Commit the batch in the background; it blocks inside the commit hook.
	postDone := make(chan int, 1)
	go func() {
		req, err := http.NewRequest(http.MethodPost, server.URL+"/metrics", strings.NewReader(aggVisibilityBatchBody))
		if err != nil {
			postDone <- 0
			return
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			postDone <- 0
			return
		}
		resp.Body.Close()
		postDone <- resp.StatusCode
	}()

	// Queries racing the commit start may land on either side of it.
	type aggResult struct {
		status int
		body   []map[string]any
		err    error
	}
	const racingReaders = 3
	racing := make(chan aggResult, racingReaders)
	for i := 0; i < racingReaders; i++ {
		go func() {
			status, body, err := fetchAggregate(server, aggVisibilityQuery)
			racing <- aggResult{status, body, err}
		}()
	}

	<-storage.entered

	// Queries issued while the commit is in flight may wait for it; waiting
	// must not be an error, and no partial batch may leak into a response.
	const waitingReaders = 6
	waiting := make(chan aggResult, waitingReaders)
	for i := 0; i < waitingReaders; i++ {
		go func() {
			status, body, err := fetchAggregate(server, aggVisibilityQuery)
			waiting <- aggResult{status, body, err}
		}()
	}
	select {
	case r := <-waiting:
		t.Fatalf("query returned while commit in flight: %d %v", r.status, r.body)
	case <-time.After(50 * time.Millisecond):
	}

	close(storage.release)
	if status := <-postDone; status != http.StatusOK {
		t.Fatalf("batch POST status %d", status)
	}

	status, postState, err := fetchAggregate(server, aggVisibilityQuery)
	if err != nil || status != http.StatusOK {
		t.Fatalf("post-state status %d err %v", status, err)
	}
	checkAggregateBody(t, postState, aggVisibilityPostState)

	// Every racing response is exactly one of the two committed states.
	for i := 0; i < racingReaders; i++ {
		r := <-racing
		if r.err != nil || r.status != http.StatusOK {
			t.Fatalf("racing query: status %d err %v", r.status, r.err)
		}
		if !reflect.DeepEqual(r.body, preState) && !reflect.DeepEqual(r.body, postState) {
			t.Fatalf("racing query saw a mixed state: %v", r.body)
		}
	}
	// Every query that waited out the commit sees the complete new state.
	for i := 0; i < waitingReaders; i++ {
		select {
		case r := <-waiting:
			if r.err != nil || r.status != http.StatusOK {
				t.Fatalf("waiting query: status %d err %v", r.status, r.err)
			}
			if !reflect.DeepEqual(r.body, postState) {
				t.Fatalf("waiting query saw a mixed state: %v", r.body)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("query did not return after the commit was released")
		}
	}
}

func TestPostMetricsRejectedBatchLeavesAggregateUntouched(t *testing.T) {
	server := newMetricsServer(t, nil, nil)
	postSamples(t, server, aggVisibilityPreBody)

	status, before := aggregateGet(t, server, aggVisibilityQuery)
	if status != http.StatusOK {
		t.Fatalf("baseline status %d: %v", status, before)
	}

	// Valid new samples plus one that claims an existing series' absolute
	// instant under a different id: the whole batch is rejected.
	rejected := `{"samples":[
		{"id":"c1","service":"gw","name":"requests","at":"2026-10-01T10:00:02Z","value":7,"labels":{"env":"canary"}},
		{"id":"a5","service":"gw","name":"requests","at":"2026-10-01T10:00:45Z","value":135,"labels":{"env":"prod"}},
		{"id":"b3","service":"gw","name":"requests","at":"2026-10-01T10:00:50Z","value":60,"labels":{"env":"staging"}},
		{"id":"clash","service":"gw","name":"requests","at":"2026-10-01T10:00:05Z","value":999,"labels":{"env":"prod"}}
	]}`
	status, out := metricsDo(t, server, http.MethodPost, "/metrics", rejected)
	if status != http.StatusConflict {
		t.Fatalf("clashing batch: want 409, got %d (%v)", status, out)
	}
	if out["error"] == "" {
		t.Fatalf("conflict must carry an error message: %v", out)
	}

	status, after := aggregateGet(t, server, aggVisibilityQuery)
	if status != http.StatusOK {
		t.Fatalf("post-rejection status %d: %v", status, after)
	}
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("rejected batch changed the aggregate:\nbefore %v\nafter  %v", before, after)
	}

	// The valid siblings were not stored: re-posting them creates them.
	out = postSamples(t, server, `{"samples":[
		{"id":"c1","service":"gw","name":"requests","at":"2026-10-01T10:00:02Z","value":7,"labels":{"env":"canary"}},
		{"id":"a5","service":"gw","name":"requests","at":"2026-10-01T10:00:45Z","value":135,"labels":{"env":"prod"}},
		{"id":"b3","service":"gw","name":"requests","at":"2026-10-01T10:00:50Z","value":60,"labels":{"env":"staging"}}
	]}`)
	if int(number(out["created"])) != 3 || int(number(out["replayed"])) != 0 {
		t.Fatalf("valid siblings must not have been stored by the rejected batch: %v", out)
	}
}
