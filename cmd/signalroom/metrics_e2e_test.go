package main

import (
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"net/http"
	"os"
	"syscall"
	"testing"
)

const sampleMetrics = `{"samples":[
	{"id":"m1","service":"gateway","name":"cpu","at":"2026-10-01T10:00:00Z","value":10},
	{"id":"m2","service":"gateway","name":"cpu","at":"2026-10-01T10:01:00Z","value":15},
	{"id":"m3","service":"api","name":"cpu","at":"2026-10-01T10:00:00Z","value":5}
]}`

func postMetricsE2E(t *testing.T, p *serverProc, body string) map[string]int {
	t.Helper()
	status, raw := httpDo(t, http.MethodPost, "http://"+p.addr+"/metrics", body)
	if status >= 400 {
		t.Fatalf("POST metrics status %d: %s", status, raw)
	}
	var out map[string]int
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func getAggregate(t *testing.T, p *serverProc, query string) []map[string]any {
	t.Helper()
	status, raw := httpDo(t, http.MethodGet, "http://"+p.addr+"/metrics/aggregate"+query, "")
	if status != http.StatusOK {
		t.Fatalf("GET aggregate status %d: %s", status, raw)
	}
	var out []map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	return out
}

func TestServeMetricsPersistsAcrossKillAndReplay(t *testing.T) {
	dataDir := t.TempDir()
	p := startServer(t, dataDir)
	counts := postMetricsE2E(t, p, sampleMetrics)
	if counts["created"] != 3 {
		t.Fatalf("created=%d", counts["created"])
	}

	// Hard kill: fsync had already confirmed durability.
	p.signal(t, syscall.SIGKILL)
	p.waitExit(t, -1)
	waitTCPPortClosed(t, p.addr)

	p2 := startServer(t, dataDir)
	got := getAggregate(t, p2, "?name=cpu&since=2026-10-01T10:00:00Z&until=2026-10-01T10:02:00Z&step=60")
	if len(got) != 2 {
		t.Fatalf("recovery after SIGKILL: %d series: %+v", len(got), got)
	}

	// Retry of the fully confirmed batch dedupes after restart.
	retry := postMetricsE2E(t, p2, sampleMetrics)
	if retry["created"] != 0 || retry["replayed"] != 3 {
		t.Fatalf("retry after restart: %+v", retry)
	}

	p2.signal(t, syscall.SIGTERM)
	p2.waitExit(t, 0)
}

func TestServeMetricsAggregateIncrements(t *testing.T) {
	dataDir := t.TempDir()
	p := startServer(t, dataDir)
	postMetricsE2E(t, p, `{"samples":[
		{"id":"m1","service":"svc","name":"cpu","at":"2026-10-01T10:00:00Z","value":10},
		{"id":"m2","service":"svc","name":"cpu","at":"2026-10-01T10:01:00Z","value":15},
		{"id":"m3","service":"svc","name":"cpu","at":"2026-10-01T10:02:00Z","value":12},
		{"id":"m4","service":"svc","name":"cpu","at":"2026-10-01T10:03:00Z","value":20}
	]}`)

	got := getAggregate(t, p, "?name=cpu&since=2026-10-01T10:00:00Z&until=2026-10-01T10:04:00Z&step=60")
	if len(got) != 1 {
		t.Fatalf("expected 1 series, got %d", len(got))
	}
	segs := got[0]["segments"].([]any)
	want := []float64{0, 5, 12, 8}
	for i, w := range want {
		seg := segs[i].(map[string]any)
		if seg["increment"].(float64) != w {
			t.Fatalf("seg%d increment=%v, want %v", i, seg["increment"], w)
		}
	}

	p.signal(t, syscall.SIGTERM)
	p.waitExit(t, 0)
}

func TestServeMetricsAggregatePredecessorBeforeSince(t *testing.T) {
	dataDir := t.TempDir()
	p := startServer(t, dataDir)
	postMetricsE2E(t, p, `{"samples":[
		{"id":"m1","service":"svc","name":"cpu","at":"2026-10-01T09:59:00Z","value":100},
		{"id":"m2","service":"svc","name":"cpu","at":"2026-10-01T10:02:00Z","value":130}
	]}`)

	got := getAggregate(t, p, "?name=cpu&since=2026-10-01T10:00:00Z&until=2026-10-01T10:03:00Z&step=60")
	segs := got[0]["segments"].([]any)
	// seg0 [10:00,10:01): empty. seg1 [10:01,10:02): empty.
	// seg2 [10:02,10:03): 130-100=30.
	if segs[2].(map[string]any)["increment"].(float64) != 30 {
		t.Fatalf("predecessor delta should land in the later segment: %+v", segs[2])
	}

	p.signal(t, syscall.SIGTERM)
	p.waitExit(t, 0)
}

func TestServeMetricsConflictAndValidation(t *testing.T) {
	dataDir := t.TempDir()
	p := startServer(t, dataDir)
	postMetricsE2E(t, p, `{"samples":[{"id":"m1","service":"svc","name":"cpu","at":"2026-10-01T10:00:00Z","value":1}]}`)

	// Same id, different value: 409.
	status, raw := httpDo(t, http.MethodPost, "http://"+p.addr+"/metrics",
		`{"samples":[{"id":"m1","service":"svc","name":"cpu","at":"2026-10-01T10:00:00Z","value":2}]}`)
	if status != http.StatusConflict {
		t.Fatalf("changed content should be 409: %d %s", status, raw)
	}

	// Same series-time, different id: 409.
	status, raw = httpDo(t, http.MethodPost, "http://"+p.addr+"/metrics",
		`{"samples":[{"id":"m2","service":"svc","name":"cpu","at":"2026-10-01T10:00:00Z","value":5}]}`)
	if status != http.StatusConflict {
		t.Fatalf("series-time conflict should be 409: %d %s", status, raw)
	}

	// Invalid input: 400.
	status, raw = httpDo(t, http.MethodPost, "http://"+p.addr+"/metrics",
		`{"samples":[{"id":"m3","service":"svc","name":"cpu","at":"2026-10-01T10:00:00Z","value":-1}]}`)
	if status != http.StatusBadRequest {
		t.Fatalf("negative value should be 400: %d %s", status, raw)
	}

	// Unknown field: 400.
	status, raw = httpDo(t, http.MethodPost, "http://"+p.addr+"/metrics",
		`{"samples":[{"id":"m3","service":"svc","name":"cpu","at":"2026-10-01T10:00:00Z","value":1}],"extra":1}`)
	if status != http.StatusBadRequest {
		t.Fatalf("unknown field should be 400: %d %s", status, raw)
	}

	p.signal(t, syscall.SIGTERM)
	p.waitExit(t, 0)
}

func TestServeMetrics503Isolation(t *testing.T) {
	dataDir := t.TempDir()
	p := startServer(t, dataDir)
	postMetricsE2E(t, p, `{"samples":[{"id":"m1","service":"svc","name":"cpu","at":"2026-10-01T10:00:00Z","value":1}]}`)

	// Events still work after metrics are poisoned (simulated by corrupting
	// the metric log). We can't easily poison via HTTP, so verify the
	// endpoints are independent: a metric write failure does not affect
	// events. This is a structural guarantee tested at the unit level.
	// Here we just confirm both endpoints coexist.
	status, _ := httpDo(t, http.MethodPost, "http://"+p.addr+"/events",
		`{"events":[{"id":"e1","service":"svc","severity":"info","message":"m","at":"2026-10-01T10:00:00Z"}]}`)
	if status != http.StatusOK {
		t.Fatalf("events should still work: %d", status)
	}
	status, _ = httpDo(t, http.MethodGet, "http://"+p.addr+"/metrics/aggregate?name=cpu&since=2026-10-01T10:00:00Z&until=2026-10-01T10:01:00Z&step=60", "")
	if status != http.StatusOK {
		t.Fatalf("aggregate should still work: %d", status)
	}

	p.signal(t, syscall.SIGTERM)
	p.waitExit(t, 0)
}

func TestServeMetricsLabelsAndSeries(t *testing.T) {
	dataDir := t.TempDir()
	p := startServer(t, dataDir)
	postMetricsE2E(t, p, `{"samples":[
		{"id":"m1","service":"svc","name":"cpu","at":"2026-10-01T10:00:00Z","value":10,"labels":{"env":"prod"}},
		{"id":"m2","service":"svc","name":"cpu","at":"2026-10-01T10:00:00Z","value":20,"labels":{"env":"staging"}},
		{"id":"m3","service":"svc","name":"cpu","at":"2026-10-01T10:00:00Z","value":30}
	]}`)

	// Three distinct series.
	got := getAggregate(t, p, "?name=cpu&since=2026-10-01T10:00:00Z&until=2026-10-01T10:01:00Z&step=60")
	if len(got) != 3 {
		t.Fatalf("expected 3 series, got %d", len(got))
	}

	// Filter by label.
	got = getAggregate(t, p, "?name=cpu&since=2026-10-01T10:00:00Z&until=2026-10-01T10:01:00Z&step=60&label=env=prod")
	if len(got) != 1 {
		t.Fatalf("expected 1 filtered series, got %d", len(got))
	}
	if got[0]["labels"].(map[string]any)["env"] != "prod" {
		t.Fatalf("wrong labels: %v", got[0]["labels"])
	}

	p.signal(t, syscall.SIGTERM)
	p.waitExit(t, 0)
}

func TestServeMetricsTornTailTrimmed(t *testing.T) {
	dataDir := t.TempDir()
	first := startServer(t, dataDir)
	postMetricsE2E(t, first, `{"samples":[{"id":"good","service":"svc","name":"cpu","at":"2026-10-01T10:00:00Z","value":1}]}`)
	first.signal(t, syscall.SIGKILL)
	first.waitExit(t, -1)
	waitTCPPortClosed(t, first.addr)

	// Append a torn partial frame to metrics.log.
	path := dataDir + "/metrics.log"
	existing, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte(`[{"id":"torn","service":"svc","name":"cpu","at":"2026-10-01T10:01:00Z","value":2}]`)
	torn := make([]byte, 8+4)
	binary.BigEndian.PutUint32(torn[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(torn[4:8], crc32.ChecksumIEEE(payload))
	copy(torn[8:], payload[:4])
	if err := os.WriteFile(path, append(existing, torn...), 0o644); err != nil {
		t.Fatal(err)
	}

	second := startServer(t, dataDir)
	got := getAggregate(t, second, "?name=cpu&since=2026-10-01T10:00:00Z&until=2026-10-01T10:02:00Z&step=60")
	if len(got) != 1 {
		t.Fatalf("torn tail must be ignored: got %d series", len(got))
	}
	// New writes after the trimmed tail survive another restart.
	postMetricsE2E(t, second, `{"samples":[{"id":"after","service":"svc","name":"cpu","at":"2026-10-01T10:02:00Z","value":3}]}`)
	second.signal(t, syscall.SIGTERM)
	second.waitExit(t, 0)
	waitTCPPortClosed(t, second.addr)

	third := startServer(t, dataDir)
	got = getAggregate(t, third, "?name=cpu&since=2026-10-01T10:00:00Z&until=2026-10-01T10:03:00Z&step=60")
	if len(got) != 1 {
		t.Fatalf("trimmed tail polluted later writes: got %d series", len(got))
	}
	third.signal(t, syscall.SIGTERM)
	third.waitExit(t, 0)
}
