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
