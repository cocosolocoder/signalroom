package main

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

// bigMetricsLtCount is a run of '<' large enough that its saved encoding
// escapes past the 64 MiB save capacity, while the literal characters keep
// the request body under the 16 MiB body limit (each '<' is one request byte
// but six saved bytes). The run rides in the metric name, keeping the sample
// id fixed and reusable.
const bigMetricsLtCount = 11_200_000

func oversizeMetricBody(id, at string) string {
	return `{"samples":[{"id":"` + id + `","service":"gateway","name":"` +
		strings.Repeat("<", bigMetricsLtCount) + `","at":"` + at + `","value":1}]}`
}

// TestServeMetricsOversizeBatchIs400AndStaysUsable is the end-to-end form of
// the reported bug: a legal, sub-16-MiB metric request that saves to over
// 64 MiB is a 400 (split or shrink the batch), not a 503 (storage failed,
// restart). The rejected batch leaves no durable record in metrics.log, the
// server keeps accepting batches and queries with no restart, and the
// rejected id is free in the same process and after a kill/recovery cycle.
func TestServeMetricsOversizeBatchIs400AndStaysUsable(t *testing.T) {
	dataDir := t.TempDir()
	p := startServer(t, dataDir)
	body := oversizeMetricBody("huge", "2026-10-01T09:00:00Z")
	if len(body) >= 16<<20 {
		t.Fatalf("setup: request body %d exceeds the 16 MiB body limit", len(body))
	}

	status, raw := httpDo(t, http.MethodPost, "http://"+p.addr+"/metrics", body)
	if status != http.StatusBadRequest {
		t.Fatalf("oversize batch must be 400, got %d: %s", status, truncate(raw))
	}
	var errBody map[string]string
	if err := json.Unmarshal(raw, &errBody); err != nil {
		t.Fatalf("decode error body %q: %v", raw, err)
	}
	msg := errBody["error"]
	if msg == "" {
		t.Fatal("error must be non-empty")
	}
	lower := strings.ToLower(msg)
	if !strings.Contains(msg, "save capacity") || (!strings.Contains(lower, "split") && !strings.Contains(lower, "reduce")) {
		t.Fatalf("error must name the save capacity and ask to split or reduce content, got %q", msg)
	}
	if strings.Contains(lower, "restart") || strings.Contains(lower, "unavailable") {
		t.Fatalf("oversize error must not demand a restart or blame storage, got %q", msg)
	}

	// Nothing from the rejected batch is queryable.
	if count := windowSampleCountEmptyOK(t, p,
		"?name=requests&service=gateway&since=2026-10-01T09:00:00Z&until=2026-10-01T10:00:00Z&step=60"); count != 0 {
		t.Fatalf("rejected samples must not be visible, got %d", count)
	}

	// The store is still usable with no restart: a normal batch commits.
	normal := `{"samples":[{"id":"ok","service":"gateway","name":"requests","at":"2026-10-01T09:01:00Z","value":1}]}`
	if counts := postMetrics(t, p, normal); counts["created"] != 1 {
		t.Fatalf("normal batch after rejection: %v", counts)
	}

	// The rejected id is reusable right away in a smaller batch.
	reuse := `{"samples":[{"id":"huge","service":"gateway","name":"requests","at":"2026-10-01T09:02:00Z","value":2}]}`
	if counts := postMetrics(t, p, reuse); counts["created"] != 1 {
		t.Fatalf("rejected id must be reusable: %v", counts)
	}

	// Kill without graceful shutdown; on recovery only the two accepted
	// samples may be present — the rejected batch must have left no frame.
	p.signal(t, syscall.SIGKILL)
	p.waitExit(t, -1)
	waitTCPPortClosed(t, p.addr)

	if onDisk, err := os.ReadFile(filepath.Join(dataDir, "metrics.log")); err != nil {
		t.Fatalf("read metrics log: %v", err)
	} else if strings.Count(string(onDisk), `"huge"`) != 1 {
		// One accepted frame now carries the small "huge" retry; the
		// oversize name never landed, so there must be no second hit.
		t.Fatalf("rejected batch left a durable record (huge hits=%d)", strings.Count(string(onDisk), `"huge"`))
	}

	p2 := startServer(t, dataDir)
	window := "?name=requests&service=gateway&since=2026-10-01T09:00:00Z&until=2026-10-01T09:03:00Z&step=60"
	if count := windowSampleCount(t, p2, window); count != 2 {
		t.Fatalf("after recovery want the 2 accepted samples, got %d", count)
	}
	// Reusing the id again after restart is a replay, not a conflict.
	if counts := postMetrics(t, p2, reuse); counts["created"] != 0 || counts["replayed"] != 1 {
		t.Fatalf("rejected-then-accepted id must replay after restart: %v", counts)
	}
	p2.signal(t, syscall.SIGTERM)
	p2.waitExit(t, 0)
}

// windowSampleCountEmptyOK is windowSampleCount but tolerates an empty result
// (zero matching series), which a well-formed aggregate returns as [].
func windowSampleCountEmptyOK(t *testing.T, p *serverProc, query string) int {
	t.Helper()
	status, series := metricAggregate(t, p, query)
	if status != http.StatusOK {
		t.Fatalf("aggregate %s: %d %v", query, status, series)
	}
	total := 0
	for _, s := range series {
		if _, hasErr := s["_error"]; hasErr {
			t.Fatalf("aggregate errored: %v", s)
		}
		for _, segment := range s["segments"].([]any) {
			total += int(segment.(map[string]any)["count"].(float64))
		}
	}
	return total
}
