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
