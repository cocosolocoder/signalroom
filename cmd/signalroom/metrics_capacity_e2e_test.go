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

// bigMetricAmpCount is a run of '&' characters large enough that its saved
// encoding escapes past the 64 MiB save capacity, while the literal
// characters keep the request body under the 16 MiB body limit (each '&' is
// one request byte but six saved bytes). The run sits in the sample id,
// which has no length cap of its own.
const bigMetricAmpCount = 11_200_000

func oversizeMetricsBody(id, at string) string {
	return `{"samples":[{"id":"` + id + strings.Repeat("&", bigMetricAmpCount) +
		`","service":"svc","name":"requests","at":"` + at + `","value":1}]}`
}

// TestServeMetricsOversizeBatchIs400AndStaysUsable is the end-to-end form of
// the reported bug: a legal, sub-16-MiB request that saves to over 64 MiB is
// a 400 (split or shrink the batch), not a 503 (storage failed, restart).
// The rejected batch leaves no durable frame, the server keeps accepting
// batches with no restart, and the rejected id is free in the same process
// and after a kill/recovery cycle.
func TestServeMetricsOversizeBatchIs400AndStaysUsable(t *testing.T) {
	dataDir := t.TempDir()
	p := startServer(t, dataDir)
	body := oversizeMetricsBody("huge", "2026-10-01T09:00:00Z")
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
	low := strings.ToLower(msg)
	if !strings.Contains(msg, "save capacity") ||
		(!strings.Contains(low, "reduce") && !strings.Contains(low, "split")) {
		t.Fatalf("error must name the save capacity and ask to split or reduce, got %q", msg)
	}
	if strings.Contains(low, "restart") || strings.Contains(low, "unavailable") {
		t.Fatalf("oversize error must not demand a restart or claim storage failure, got %q", msg)
	}

	// Nothing from the rejected batch is queryable.
	if status, series := metricAggregate(t, p,
		"?name=requests&service=svc&since=2026-10-01T09:00:00Z&until=2026-10-01T09:01:00Z&step=60"); status != http.StatusOK || len(series) != 0 {
		t.Fatalf("rejected samples must not be visible, got %d %v", status, series)
	}

	// The store is still usable with no restart: a normal batch commits.
	normal := `{"samples":[{"id":"ok","service":"svc","name":"requests","at":"2026-10-01T09:01:00Z","value":1}]}`
	if counts := postMetrics(t, p, normal); counts["created"] != 1 {
		t.Fatalf("normal batch after rejection: %v", counts)
	}

	// The rejected id is reusable right away in a smaller batch.
	reuse := `{"samples":[{"id":"huge","service":"svc","name":"requests","at":"2026-10-01T09:02:00Z","value":2}]}`
	if counts := postMetrics(t, p, reuse); counts["created"] != 1 {
		t.Fatalf("rejected id must be reusable: %v", counts)
	}

	// Kill without graceful shutdown; the oversize batch must have left no
	// frame: only the small "huge" retry lands on disk, one frame total.
	p.signal(t, syscall.SIGKILL)
	p.waitExit(t, -1)
	waitTCPPortClosed(t, p.addr)

	onDisk, err := os.ReadFile(filepath.Join(dataDir, "metrics.log"))
	if err != nil {
		t.Fatalf("read metrics log: %v", err)
	}
	if strings.Count(string(onDisk), `"huge"`) != 1 {
		t.Fatalf("rejected batch left a durable record (huge hits=%d)", strings.Count(string(onDisk), `"huge"`))
	}
	if strings.Count(string(onDisk), strings.Repeat("&", 1000)) != 0 {
		t.Fatal("rejected batch's escaped content must never reach the log")
	}

	p2 := startServer(t, dataDir)
	if status, series := metricAggregate(t, p2,
		"?name=requests&service=svc&since=2026-10-01T09:00:00Z&until=2026-10-01T09:03:00Z&step=60"); status != http.StatusOK || len(series) != 1 {
		t.Fatalf("after recovery want one series, got %d: %v", status, series)
	} else {
		var total float64
		for _, seg := range series[0]["segments"].([]any) {
			total += seg.(map[string]any)["count"].(float64)
		}
		if total != 2 {
			t.Fatalf("after recovery want the 2 accepted samples, got %v", total)
		}
	}
	// Reusing the id again after restart is a replay, not a conflict.
	if counts := postMetrics(t, p2, reuse); counts["created"] != 0 || counts["replayed"] != 1 {
		t.Fatalf("rejected-then-accepted id must replay after restart: %v", counts)
	}

	// An oversize submission after restart is still a content 400, and
	// events keep working alongside it.
	if status, raw := httpDo(t, http.MethodPost, "http://"+p2.addr+"/metrics",
		oversizeMetricsBody("later", "2026-10-01T09:10:00Z")); status != http.StatusBadRequest {
		t.Fatalf("oversize after recovery must stay 400, got %d: %s", status, truncate(raw))
	}
	if len(getEvents(t, p2, "")) != 0 {
		t.Fatal("event endpoint must stay healthy")
	}
	p2.signal(t, syscall.SIGTERM)
	p2.waitExit(t, 0)
}

// TestServeMetricsOversizeConflictPrecedence confirms end to end that
// validation (400) and conflict (409) rejections outrank the save-capacity
// check: an oversized body that is also malformed or conflicting answers on
// the earlier rule, and nothing is written.
func TestServeMetricsOversizeConflictPrecedence(t *testing.T) {
	dataDir := t.TempDir()
	p := startServer(t, dataDir)
	if counts := postMetrics(t, p,
		`{"samples":[{"id":"s1","service":"svc","name":"requests","at":"2026-10-01T09:00:00Z","value":1}]}`); counts["created"] != 1 {
		t.Fatalf("seed: %v", counts)
	}

	// Oversize content plus a negative value: validation wins.
	badValue := `{"samples":[{"id":"x` + strings.Repeat("&", bigMetricAmpCount) +
		`","service":"svc","name":"requests","at":"2026-10-01T09:05:00Z","value":-1}]}`
	postMetricsExpectError(t, p, http.StatusBadRequest, badValue)

	// Oversize content under the same stored id but with different content
	// (the run sits in the metric name): id-content conflict wins.
	conflict := `{"samples":[{"id":"s1","service":"svc","name":"requests` +
		strings.Repeat("&", bigMetricAmpCount) +
		`","at":"2026-10-01T09:00:00Z","value":1}]}`
	postMetricsExpectError(t, p, http.StatusConflict, conflict)

	// Same oversize content under a fresh id is the capacity 400.
	status, raw := httpDo(t, http.MethodPost, "http://"+p.addr+"/metrics",
		oversizeMetricsBody("fine", "2026-10-01T09:05:00Z"))
	if status != http.StatusBadRequest {
		t.Fatalf("oversize must be 400, got %d: %s", status, truncate(raw))
	}

	// Only the seed sample survived all three rejections.
	if status, series := metricAggregate(t, p,
		"?name=requests&service=svc&since=2026-10-01T09:00:00Z&until=2026-10-01T09:06:00Z&step=60"); status != http.StatusOK {
		t.Fatalf("aggregate status %d", status)
	} else {
		var total float64
		for _, s := range series {
			for _, seg := range s["segments"].([]any) {
				total += seg.(map[string]any)["count"].(float64)
			}
		}
		if total != 1 {
			t.Fatalf("precedence rejections must add nothing, count=%v", total)
		}
	}
	p.signal(t, syscall.SIGTERM)
	p.waitExit(t, 0)
}
