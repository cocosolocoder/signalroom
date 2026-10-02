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

	"github.com/cocosolocoder/signalroom/internal/metrics"
)

type fakeMetricStorage struct {
	mu       sync.Mutex
	failNext bool
	poisoned bool
	appended []int
}

func (f *fakeMetricStorage) AppendBatch(batch []metrics.Sample) error {
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

func (f *fakeMetricStorage) Poisoned() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.poisoned
}

func newMetricsTestServer(t *testing.T) (*httptest.Server, *fakeMetricStorage, *metrics.Timeline) {
	t.Helper()
	tl := metrics.NewTimeline()
	store := &fakeMetricStorage{}
	server := httptest.NewServer(NewHandler(nil, nil, nil, WithMetrics(tl, store)))
	t.Cleanup(server.Close)
	return server, store, tl
}

func postMetrics(t *testing.T, server *httptest.Server, body string) (int, map[string]any) {
	t.Helper()
	resp, err := http.Post(server.URL+"/metrics", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post metrics: %v", err)
	}
	defer resp.Body.Close()
	return decode(t, resp)
}

func getMetricsAggregate(t *testing.T, server *httptest.Server, query string) (int, any) {
	t.Helper()
	resp, err := http.Get(server.URL + "/metrics/aggregate" + query)
	if err != nil {
		t.Fatalf("get aggregate: %v", err)
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

func TestPostMetricsCreatesAndReplays(t *testing.T) {
	server, store, _ := newMetricsTestServer(t)
	body := `{"samples":[
		{"id":"m1","service":"svc","name":"cpu","at":"2026-10-01T10:00:00Z","value":1},
		{"id":"m2","service":"svc","name":"cpu","at":"2026-10-01T10:01:00Z","value":2}
	]}`
	status, out := postMetrics(t, server, body)
	if status != http.StatusOK {
		t.Fatalf("status=%d body=%v", status, out)
	}
	if out["created"].(float64) != 2 || out["replayed"].(float64) != 0 {
		t.Fatalf("counts: %v", out)
	}
	if len(store.appended) != 1 || store.appended[0] != 2 {
		t.Fatalf("expected one durable batch of 2, got %v", store.appended)
	}

	// Retry with 1.0 and equivalent timezone: replays.
	retry := `{"samples":[
		{"id":"m1","service":"svc","name":"cpu","at":"2026-10-01T18:00:00+08:00","value":1.0},
		{"id":"m2","service":"svc","name":"cpu","at":"2026-10-01T10:01:00Z","value":2.0}
	]}`
	status, out = postMetrics(t, server, retry)
	if status != http.StatusOK {
		t.Fatalf("retry status=%d body=%v", status, out)
	}
	if out["created"].(float64) != 0 || out["replayed"].(float64) != 2 {
		t.Fatalf("retry counts: %v", out)
	}
	if len(store.appended) != 1 {
		t.Fatal("replay must not touch storage")
	}
}

func TestPostMetricsRejectsInvalid(t *testing.T) {
	server, _, _ := newMetricsTestServer(t)
	cases := []string{
		`{"samples":[]}`,
		`{"samples":[{"id":"m1","service":"svc","name":"cpu","at":"2026-10-01T10:00:00Z","value":1}],"extra":1}`,
		`{"samples":[{"id":"m1","service":"svc","name":"cpu","at":"2026-10-01T10:00:00Z","value":1,"id":"m2"}]}`,
		`{"samples":[{"id":"","service":"svc","name":"cpu","at":"2026-10-01T10:00:00Z","value":1}]}`,
		`{"samples":[{"id":"m1","service":"svc","name":"cpu","at":"2026-10-01T10:00:00Z","value":-1}]}`,
		`{"samples":[{"id":"m1","service":"svc","name":"cpu","at":"2026-10-01T10:00:00Z","value":"1"}]}`,
		`{"samples":[{"id":"m1","service":"svc","name":"cpu","at":"2026-10-01T10:00:00Z","value":1}],"samples":[]}`,
	}
	for i, body := range cases {
		status, _ := postMetrics(t, server, body)
		if status != http.StatusBadRequest {
			t.Fatalf("case %d: expected 400, got %d", i, status)
		}
	}
}

func TestPostMetricsConflict(t *testing.T) {
	server, _, _ := newMetricsTestServer(t)
	body := `{"samples":[{"id":"m1","service":"svc","name":"cpu","at":"2026-10-01T10:00:00Z","value":1}]}`
	postMetrics(t, server, body)

	// Same id, different value: 409.
	changed := `{"samples":[{"id":"m1","service":"svc","name":"cpu","at":"2026-10-01T10:00:00Z","value":2}]}`
	status, _ := postMetrics(t, server, changed)
	if status != http.StatusConflict {
		t.Fatalf("expected 409, got %d", status)
	}

	// Same series-time, different id: 409.
	diffID := `{"samples":[{"id":"m2","service":"svc","name":"cpu","at":"2026-10-01T10:00:00Z","value":5}]}`
	status, _ = postMetrics(t, server, diffID)
	if status != http.StatusConflict {
		t.Fatalf("expected 409 for series-time conflict, got %d", status)
	}

	// Duplicate id within batch: 409.
	dup := `{"samples":[
		{"id":"m3","service":"svc","name":"cpu","at":"2026-10-01T10:00:00Z","value":1},
		{"id":"m3","service":"svc","name":"cpu","at":"2026-10-01T10:01:00Z","value":2}
	]}`
	status, _ = postMetrics(t, server, dup)
	if status != http.StatusConflict {
		t.Fatalf("expected 409 for duplicate id in batch, got %d", status)
	}
}

func TestPostMetricsPoisonedReturns503(t *testing.T) {
	server, store, _ := newMetricsTestServer(t)
	store.failNext = true
	body := `{"samples":[{"id":"m1","service":"svc","name":"cpu","at":"2026-10-01T10:00:00Z","value":1}]}`
	status, _ := postMetrics(t, server, body)
	if status != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 after write failure, got %d", status)
	}
	// Subsequent requests also 503.
	status, _ = postMetrics(t, server, body)
	if status != http.StatusServiceUnavailable {
		t.Fatalf("expected persistent 503, got %d", status)
	}
}

func TestAggregateMetrics(t *testing.T) {
	server, _, _ := newMetricsTestServer(t)
	body := `{"samples":[
		{"id":"m1","service":"svc","name":"cpu","at":"2026-10-01T10:00:00Z","value":10},
		{"id":"m2","service":"svc","name":"cpu","at":"2026-10-01T10:01:00Z","value":15},
		{"id":"m3","service":"svc","name":"cpu","at":"2026-10-01T10:02:00Z","value":12},
		{"id":"m4","service":"svc","name":"cpu","at":"2026-10-01T10:03:00Z","value":20}
	]}`
	postMetrics(t, server, body)

	query := "?name=cpu&since=2026-10-01T10:00:00Z&until=2026-10-01T10:04:00Z&step=60"
	status, raw := getMetricsAggregate(t, server, query)
	if status != http.StatusOK {
		t.Fatalf("status=%d raw=%v", status, raw)
	}
	arr := raw.([]any)
	if len(arr) != 1 {
		t.Fatalf("expected 1 series, got %d", len(arr))
	}
	series := arr[0].(map[string]any)
	if series["service"] != "svc" {
		t.Fatalf("service: %v", series["service"])
	}
	segs := series["segments"].([]any)
	if len(segs) != 4 {
		t.Fatalf("expected 4 segments, got %d", len(segs))
	}
	// seg0: first sample, no predecessor → 0. seg1: 15-10=5.
	// seg2: 12<15 reset → 12. seg3: 20-12=8.
	want := []float64{0, 5, 12, 8}
	for i, w := range want {
		seg := segs[i].(map[string]any)
		if seg["increment"].(float64) != w {
			t.Fatalf("seg%d increment=%v, want %v", i, seg["increment"], w)
		}
	}
}

func TestAggregateMetricsValidation(t *testing.T) {
	server, _, _ := newMetricsTestServer(t)
	cases := []string{
		"?since=2026-10-01T10:00:00Z&until=2026-10-01T10:04:00Z&step=60",         // missing name
		"?name=cpu&until=2026-10-01T10:04:00Z&step=60",                          // missing since
		"?name=cpu&since=2026-10-01T10:00:00Z&step=60",                          // missing until
		"?name=cpu&since=2026-10-01T10:00:00Z&until=2026-10-01T10:04:00Z",       // missing step
		"?name=cpu&since=2026-10-01T10:00:00Z&until=2026-10-01T09:00:00Z&step=60", // inverted
		"?name=cpu&since=2026-10-01T10:00:00Z&until=2026-10-01T10:04:00Z&step=0",  // step 0
		"?name=cpu&since=2026-10-01T10:00:00Z&until=2026-10-01T10:04:00Z&step=86401", // step too large
		"?name=cpu&name=mem&since=2026-10-01T10:00:00Z&until=2026-10-01T10:04:00Z&step=60", // duplicate name
		"?name=cpu&since=bad&until=2026-10-01T10:04:00Z&step=60",                // bad timestamp
		"?name=cpu&since=2026-10-01T10:00:00Z&until=2026-10-01T10:04:00Z&step=60&service=a&service=b", // duplicate service
	}
	for i, query := range cases {
		status, _ := getMetricsAggregate(t, server, query)
		if status != http.StatusBadRequest {
			t.Fatalf("case %d: expected 400, got %d", i, status)
		}
	}
}

func TestAggregateMetricsNameExactMatch(t *testing.T) {
	server, _, _ := newMetricsTestServer(t)
	body := `{"samples":[
		{"id":"m1","service":"svc","name":"cpu","at":"2026-10-01T10:00:00Z","value":10}
	]}`
	postMetrics(t, server, body)

	// Exact match works.
	status, raw := getMetricsAggregate(t, server, "?name=cpu&since=2026-10-01T10:00:00Z&until=2026-10-01T10:01:00Z&step=60")
	if status != http.StatusOK {
		t.Fatalf("status=%d", status)
	}
	if len(raw.([]any)) != 1 {
		t.Fatalf("exact match should return 1 series, got %d", len(raw.([]any)))
	}

	// Padded name does not match (exact match, no trimming).
	status, raw = getMetricsAggregate(t, server, "?name=%20cpu%20&since=2026-10-01T10:00:00Z&until=2026-10-01T10:01:00Z&step=60")
	if status != http.StatusOK {
		t.Fatalf("status=%d", status)
	}
	if len(raw.([]any)) != 0 {
		t.Fatalf("padded name should not match, got %d series", len(raw.([]any)))
	}
}

func TestAggregateMetricsEmpty(t *testing.T) {
	server, _, _ := newMetricsTestServer(t)
	query := "?name=cpu&since=2026-10-01T10:00:00Z&until=2026-10-01T10:04:00Z&step=60"
	status, raw := getMetricsAggregate(t, server, query)
	if status != http.StatusOK {
		t.Fatalf("status=%d", status)
	}
	arr := raw.([]any)
	if len(arr) != 0 {
		t.Fatalf("expected empty array, got %v", arr)
	}
}

func TestAggregateMetricsSegmentLimit(t *testing.T) {
	server, _, _ := newMetricsTestServer(t)
	// 10001 segments.
	query := "?name=cpu&since=2026-10-01T10:00:00Z&until=2026-10-01T12:46:41Z&step=1"
	status, _ := getMetricsAggregate(t, server, query)
	if status != http.StatusBadRequest {
		t.Fatalf("expected 400 for too many segments, got %d", status)
	}
}

func TestAggregateMetricsNonFinite(t *testing.T) {
	server, _, _ := newMetricsTestServer(t)
	body := `{"samples":[
		{"id":"m1","service":"svc","name":"cpu","at":"2026-10-01T10:00:00Z","value":0},
		{"id":"m2","service":"svc","name":"cpu","at":"2026-10-01T10:00:01Z","value":1.7976931348623157e308},
		{"id":"m3","service":"svc","name":"cpu","at":"2026-10-01T10:00:02Z","value":0},
		{"id":"m4","service":"svc","name":"cpu","at":"2026-10-01T10:00:03Z","value":1.7976931348623157e308}
	]}`
	postMetrics(t, server, body)
	query := "?name=cpu&since=2026-10-01T10:00:00Z&until=2026-10-01T10:00:04Z&step=4"
	status, _ := getMetricsAggregate(t, server, query)
	if status != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422 for non-finite sum, got %d", status)
	}
}
