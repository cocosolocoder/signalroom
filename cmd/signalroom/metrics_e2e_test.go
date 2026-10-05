package main

import (
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"net/http"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// putFrame writes the shared length-prefixed, CRC-checked frame layout into a
// buffer sized for one payload.
func putFrame(frame, payload []byte) {
	binary.BigEndian.PutUint32(frame[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(payload))
	copy(frame[8:], payload)
}

func postMetrics(t *testing.T, p *serverProc, body string) map[string]int {
	t.Helper()
	status, raw := httpDo(t, http.MethodPost, "http://"+p.addr+"/metrics", body)
	if status >= 400 {
		t.Fatalf("POST /metrics status %d: %s", status, raw)
	}
	var out map[string]int
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func metricAggregate(t *testing.T, p *serverProc, query string) (int, []map[string]any) {
	t.Helper()
	status, raw := httpDo(t, http.MethodGet, "http://"+p.addr+"/metrics/aggregate"+query, "")
	if status >= 400 {
		var errBody map[string]any
		_ = json.Unmarshal(raw, &errBody)
		return status, []map[string]any{{"_error": errBody}}
	}
	var out []map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	return status, out
}

const sampleMetrics = `{"samples":[
	{"id":"m1","service":"gateway","name":"requests","at":"2026-10-01T10:00:00Z","value":100},
	{"id":"m2","service":"gateway","name":"requests","at":"2026-10-01T10:01:00Z","value":105}
]}`

func TestServeMetricsPersistAcrossKillAndDedupeRetry(t *testing.T) {
	dataDir := t.TempDir()
	p := startServer(t, dataDir)
	if counts := postMetrics(t, p, sampleMetrics); counts["created"] != 2 {
		t.Fatalf("created=%v", counts)
	}

	p.signal(t, syscall.SIGKILL)
	p.waitExit(t, -1)
	waitTCPPortClosed(t, p.addr)

	p2 := startServer(t, dataDir)
	status, got := metricAggregate(t, p2,
		"?name=requests&since=2026-10-01T10:00:00Z&until=2026-10-01T10:02:00Z&step=60")
	if status != http.StatusOK {
		t.Fatalf("aggregate after restart: %d %v", status, got)
	}
	if len(got) != 1 {
		t.Fatalf("one series: %v", got)
	}
	segments := got[0]["segments"].([]any)
	if segments[0].(map[string]any)["delta"].(float64) != 0 ||
		segments[1].(map[string]any)["delta"].(float64) != 5 {
		t.Fatalf("deltas must survive restart: %v", segments)
	}

	// The identical batch replays with zero created.
	if counts := postMetrics(t, p2, sampleMetrics); counts["created"] != 0 || counts["replayed"] != 2 {
		t.Fatalf("retry after restart: %v", counts)
	}
	p2.signal(t, syscall.SIGTERM)
	p2.waitExit(t, 0)
}

// postMetricsExpectError runs a POST /metrics that must fail with the given
// status and a non-empty error field.
func postMetricsExpectError(t *testing.T, p *serverProc, wantStatus int, body string) {
	t.Helper()
	status, raw := httpDo(t, http.MethodPost, "http://"+p.addr+"/metrics", body)
	if status != wantStatus {
		t.Fatalf("POST /metrics: want %d, got %d: %s", wantStatus, status, raw)
	}
	var errBody map[string]string
	if err := json.Unmarshal(raw, &errBody); err != nil || errBody["error"] == "" {
		t.Fatalf("status %d must carry a non-empty error, got %s", status, raw)
	}
}

// windowSampleCount aggregates one query expected to match exactly one series
// and returns the number of samples across its segments.
func windowSampleCount(t *testing.T, p *serverProc, query string) int {
	t.Helper()
	status, series := metricAggregate(t, p, query)
	if status != http.StatusOK || len(series) != 1 {
		t.Fatalf("aggregate %s: want one series, got %d %v", query, status, series)
	}
	segments := series[0]["segments"].([]any)
	total := 0
	for _, segment := range segments {
		total += int(segment.(map[string]any)["count"].(float64))
	}
	return total
}

// TestServeMetricsWideDateSamplesAcceptedAcrossEras is the acceptance
// guarantee for sampling-instant identity far outside the years for which
// time.Time.UnixNano stays exact. The two instants share one naive UnixNano
// value, so they must still be received as two distinct points in either
// submission order, survive a timezone-only retry as a replay, and remain
// independently queryable; a legal post-2262 date is accepted too.
func TestServeMetricsWideDateSamplesAcceptedAcrossEras(t *testing.T) {
	const (
		oldAt    = "1600-01-01T00:00:00Z"
		oldAltTZ = "1600-01-01T08:00:00+08:00" // same absolute instant as oldAt
		futureAt = "2184-07-20T23:34:33.709551616Z"
	)
	oldWindow := "?name=requests&service=gateway&since=1599-12-31T23:59:59Z&until=1600-01-01T00:00:01Z&step=2"
	futureWindow := "?name=requests&service=gateway&since=2184-07-20T23:34:33Z&until=2184-07-20T23:34:34Z&step=1"

	for _, reverse := range []bool{false, true} {
		t.Run("order", func(t *testing.T) {
			p := startServer(t, t.TempDir())
			body := `{"samples":[
				{"id":"era-old","service":"gateway","name":"requests","at":"` + oldAt + `","value":1},
				{"id":"era-new","service":"gateway","name":"requests","at":"` + futureAt + `","value":2}
			]}`
			if reverse {
				body = `{"samples":[
					{"id":"era-new","service":"gateway","name":"requests","at":"` + futureAt + `","value":2},
					{"id":"era-old","service":"gateway","name":"requests","at":"` + oldAt + `","value":1}
				]}`
			}
			if counts := postMetrics(t, p, body); counts["created"] != 2 || counts["replayed"] != 0 {
				t.Fatalf("distant legal dates must both be new (reverse=%v): %v", reverse, counts)
			}

			if count := windowSampleCount(t, p, oldWindow); count != 1 {
				t.Fatalf("old-era window must contain its one point, got %d (reverse=%v)", count, reverse)
			}
			if count := windowSampleCount(t, p, futureWindow); count != 1 {
				t.Fatalf("future-era window must contain its one point, got %d (reverse=%v)", count, reverse)
			}

			// The identical batch retries entirely as replays.
			if counts := postMetrics(t, p, body); counts["created"] != 0 || counts["replayed"] != 2 {
				t.Fatalf("identical batch must replay (reverse=%v): %v", reverse, counts)
			}

			// Same id, same content, only the timezone spelling differs: one
			// replay, no added sample.
			if counts := postMetrics(t, p, `{"samples":[
				{"id":"era-old","service":"gateway","name":"requests","at":"`+oldAltTZ+`","value":1}
			]}`); counts["created"] != 0 || counts["replayed"] != 1 {
				t.Fatalf("timezone-only resubmission must replay one: %v", counts)
			}
			if count := windowSampleCount(t, p, oldWindow); count != 1 {
				t.Fatalf("replay must not increase the old-era count, got %d", count)
			}
			if count := windowSampleCount(t, p, futureWindow); count != 1 {
				t.Fatalf("replay must not increase the future-era count, got %d", count)
			}
			p.signal(t, syscall.SIGTERM)
			p.waitExit(t, 0)
		})
	}

	// A legal date after 2262 is not bounded out and is independently
	// queryable.
	p := startServer(t, t.TempDir())
	if counts := postMetrics(t, p, `{"samples":[
		{"id":"far","service":"gateway","name":"requests","at":"2300-01-02T03:04:05.678901234Z","value":7}
	]}`); counts["created"] != 1 || counts["replayed"] != 0 {
		t.Fatalf("post-2262 date must be new: %v", counts)
	}
	farWindow := "?name=requests&service=gateway&since=2300-01-02T03:04:05Z&until=2300-01-02T03:04:06Z&step=1"
	if count := windowSampleCount(t, p, farWindow); count != 1 {
		t.Fatalf("post-2262 point must be queryable, got %d", count)
	}
	p.signal(t, syscall.SIGTERM)
	p.waitExit(t, 0)
}

// TestServeMetricsWideDateConflictsRejectWholeBatch covers the clash rules at
// distant dates: another timezone spelling is the same sampling point, a
// different id cannot occupy it either against stored data or inside one
// batch, mixed batches roll back completely, and the legal sample from a
// rejected batch still ingests when sent alone.
func TestServeMetricsWideDateConflictsRejectWholeBatch(t *testing.T) {
	const (
		oldAt        = "1600-01-01T00:00:00Z"
		oldAltTZ     = "1600-01-01T08:00:00+08:00"
		futureAt     = "2184-07-20T23:34:33.709551616Z"
		futureAltTZ  = "2184-07-20T16:34:33.709551616-07:00" // same instant as futureAt
		futureNextNS = "2184-07-20T23:34:33.709551617Z"      // one nanosecond later
	)
	oldWindow := "?name=requests&service=gateway&since=1599-12-31T23:59:59Z&until=1600-01-01T00:00:01Z&step=2"
	futureWindow := "?name=requests&service=gateway&since=2184-07-20T23:34:33Z&until=2184-07-20T23:34:34Z&step=1"

	p := startServer(t, t.TempDir())
	if counts := postMetrics(t, p, `{"samples":[
		{"id":"owner","service":"gateway","name":"requests","at":"`+oldAt+`","value":1}
	]}`); counts["created"] != 1 {
		t.Fatalf("seed: %v", counts)
	}

	// Clash with an already-accepted point via another timezone spelling.
	postMetricsExpectError(t, p, http.StatusConflict, `{"samples":[
		{"id":"intruder","service":"gateway","name":"requests","at":"`+oldAltTZ+`","value":5}
	]}`)

	// Same absolute future instant under two offsets inside one batch.
	postMetricsExpectError(t, p, http.StatusConflict, `{"samples":[
		{"id":"f1","service":"gateway","name":"requests","at":"`+futureAt+`","value":1},
		{"id":"f2","service":"gateway","name":"requests","at":"`+futureAltTZ+`","value":2}
	]}`)

	// One conflicting sample plus one legal new sample: the whole batch is
	// refused, with nothing left behind.
	postMetricsExpectError(t, p, http.StatusConflict, `{"samples":[
		{"id":"intruder2","service":"gateway","name":"requests","at":"`+oldAltTZ+`","value":9},
		{"id":"fresh","service":"gateway","name":"requests","at":"`+futureNextNS+`","value":3}
	]}`)
	if count := windowSampleCount(t, p, oldWindow); count != 1 {
		t.Fatalf("rollback must leave the old era unchanged, got %d", count)
	}
	if status, series := metricAggregate(t, p, futureWindow); status != http.StatusOK || len(series) != 0 {
		t.Fatalf("legal sample must not survive the rollback, got %d %v", status, series)
	}

	// The legal new sample sent on its own really is new.
	if counts := postMetrics(t, p, `{"samples":[
		{"id":"fresh","service":"gateway","name":"requests","at":"`+futureNextNS+`","value":3}
	]}`); counts["created"] != 1 || counts["replayed"] != 0 {
		t.Fatalf("standalone legal sample must create one: %v", counts)
	}
	if count := windowSampleCount(t, p, futureWindow); count != 1 {
		t.Fatalf("future era must hold the separately ingested point, got %d", count)
	}
	if count := windowSampleCount(t, p, oldWindow); count != 1 {
		t.Fatalf("old era must remain at one point, got %d", count)
	}

	// Same id with the instant really moved by one nanosecond is a conflict,
	// not a replay.
	postMetricsExpectError(t, p, http.StatusConflict, `{"samples":[
		{"id":"fresh","service":"gateway","name":"requests","at":"2184-07-20T23:34:33.709551618Z","value":3}
	]}`)
	if count := windowSampleCount(t, p, futureWindow); count != 1 {
		t.Fatalf("failed one-nanosecond move must not add a point, got %d", count)
	}

	p.signal(t, syscall.SIGTERM)
	p.waitExit(t, 0)
}

// TestServeMetricsNanosecondAndLabelIdentity covers one-second/nanosecond
// coexistence and the series scoping of clashes at a distant date: samples
// differing by even one nanosecond coexist, while clashes stay confined to
// the identical label set.
func TestServeMetricsNanosecondAndLabelIdentity(t *testing.T) {
	p := startServer(t, t.TempDir())

	// Two samples one nanosecond apart in the same second are both new.
	if counts := postMetrics(t, p, `{"samples":[
		{"id":"ns0","service":"gateway","name":"requests","at":"2026-10-01T10:00:00.111111111Z","value":1},
		{"id":"ns1","service":"gateway","name":"requests","at":"2026-10-01T10:00:00.111111112Z","value":2}
	]}`); counts["created"] != 2 || counts["replayed"] != 0 {
		t.Fatalf("nanosecond-apart samples must both be new: %v", counts)
	}
	secondWindow := "?name=requests&service=gateway&since=2026-10-01T10:00:00Z&until=2026-10-01T10:00:01Z&step=1"
	if count := windowSampleCount(t, p, secondWindow); count != 2 {
		t.Fatalf("the one-second window must contain both points, got %d", count)
	}

	// A timezone-only retry of one of them replays without adding a point.
	if counts := postMetrics(t, p, `{"samples":[
		{"id":"ns0","service":"gateway","name":"requests","at":"2026-10-01T18:00:00.111111111+08:00","value":1}
	]}`); counts["created"] != 0 || counts["replayed"] != 1 {
		t.Fatalf("timezone-only retry must replay one: %v", counts)
	}
	if count := windowSampleCount(t, p, secondWindow); count != 2 {
		t.Fatalf("replay must not add a point, got %d", count)
	}

	// Moving one id's instant by a single nanosecond conflicts.
	postMetrics(t, p, `{"samples":[
		{"id":"mv","service":"gateway","name":"requests","at":"2026-10-01T10:05:00.000000001Z","value":1}
	]}`)
	postMetricsExpectError(t, p, http.StatusConflict, `{"samples":[
		{"id":"mv","service":"gateway","name":"requests","at":"2026-10-01T10:05:00.000000002Z","value":1}
	]}`)

	// Different label sets are different series: both may hold the same
	// distant instant.
	if counts := postMetrics(t, p, `{"samples":[
		{"id":"lp","service":"gateway","name":"requests","at":"1600-01-01T00:00:00Z","value":1,"labels":{"env":"prod"}},
		{"id":"ls","service":"gateway","name":"requests","at":"1600-01-01T00:00:00Z","value":1,"labels":{"env":"staging"}}
	]}`); counts["created"] != 2 || counts["replayed"] != 0 {
		t.Fatalf("distinct label sets share an instant: %v", counts)
	}
	labelWindow := func(env string) string {
		return "?name=requests&service=gateway&label=env=" + env +
			"&since=1599-12-31T23:59:59Z&until=1600-01-01T00:00:01Z&step=2"
	}
	if count := windowSampleCount(t, p, labelWindow("prod")); count != 1 {
		t.Fatalf("prod series must hold one point, got %d", count)
	}
	if count := windowSampleCount(t, p, labelWindow("staging")); count != 1 {
		t.Fatalf("staging series must hold one point, got %d", count)
	}
	// Another id still clashes within one label series, even with an offset.
	postMetricsExpectError(t, p, http.StatusConflict, `{"samples":[
		{"id":"lp2","service":"gateway","name":"requests","at":"1600-01-01T08:00:00+08:00","value":2,"labels":{"env":"prod"}}
	]}`)

	p.signal(t, syscall.SIGTERM)
	p.waitExit(t, 0)
}

func TestServeMetricsLegacyDataDirStarts(t *testing.T) {
	// An old directory with only events.log must start and serve metrics.
	dataDir := t.TempDir()
	payload := []byte(`[{"id":"e1","service":"s","severity":"info","message":"m","at":"2026-10-01T09:00:00Z"}]`)
	frame := make([]byte, 8+len(payload))
	putFrame(frame, payload)
	if err := os.WriteFile(filepath.Join(dataDir, "events.log"), frame, 0o644); err != nil {
		t.Fatal(err)
	}
	p := startServer(t, dataDir)
	if counts := postMetrics(t, p, `{"samples":[{"id":"m1","service":"gw","name":"requests","at":"2026-10-01T10:00:00Z","value":1}]}`); counts["created"] != 1 {
		t.Fatalf("created=%v", counts)
	}
	status, got := metricAggregate(t, p,
		"?name=requests&since=2026-10-01T10:00:00Z&until=2026-10-01T10:01:00Z&step=60")
	if status != http.StatusOK || len(got) != 1 {
		t.Fatalf("aggregate on legacy dir: %d %v", status, got)
	}
	// Events still readable.
	if len(getEvents(t, p, "")) != 1 {
		t.Fatal("legacy events must remain readable")
	}
	p.signal(t, syscall.SIGTERM)
	p.waitExit(t, 0)
}
