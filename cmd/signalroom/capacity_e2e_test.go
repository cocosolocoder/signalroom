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

// bigLtCount is a run of '<' large enough that its saved encoding escapes
// past the 64 MiB save capacity, while the literal characters keep the
// request body under the 16 MiB body limit (each '<' is one request byte but
// six saved bytes).
const bigLtCount = 11_200_000

func oversizeEventBody(id, at string) string {
	return `{"events":[{"id":"` + id + `","service":"svc","severity":"info","message":"` +
		strings.Repeat("<", bigLtCount) + `","at":"` + at + `"}]}`
}

// TestServeOversizeBatchIs400AndStaysUsable is the end-to-end form of the
// reported bug: a legal, sub-16-MiB request that saves to over 64 MiB is a
// 400 (send less content), not a 503 (storage failed, restart). The rejected
// batch leaves no durable record, the server keeps accepting batches with no
// restart, and the rejected id is free on the same process and after a
// kill/recovery cycle.
func TestServeOversizeBatchIs400AndStaysUsable(t *testing.T) {
	dataDir := t.TempDir()
	p := startServer(t, dataDir)
	body := oversizeEventBody("huge", "2026-10-01T09:00:00Z")
	if len(body) >= 16<<20 {
		t.Fatalf("setup: request body %d exceeds the 16 MiB body limit", len(body))
	}

	status, raw := httpDo(t, http.MethodPost, "http://"+p.addr+"/events", body)
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
	if !strings.Contains(msg, "save capacity") && !strings.Contains(strings.ToLower(msg), "reduce") {
		t.Fatalf("error must point at the save capacity / sending less, got %q", msg)
	}
	if strings.Contains(strings.ToLower(msg), "restart") {
		t.Fatalf("oversize error must not demand a restart, got %q", msg)
	}

	// Nothing from the rejected batch is queryable.
	if got := getEvents(t, p, "?service=svc"); len(got) != 0 {
		t.Fatalf("rejected events must not be visible, got %d", len(got))
	}

	// The store is still usable with no restart: a normal batch commits.
	normal := `{"events":[{"id":"ok","service":"svc","severity":"info","message":"fine","at":"2026-10-01T09:01:00Z"}]}`
	if counts := postEvents(t, p, normal); counts["created"] != 1 {
		t.Fatalf("normal batch after rejection: %v", counts)
	}

	// The rejected id is reusable right away in a smaller batch.
	reuse := `{"events":[{"id":"huge","service":"svc","severity":"info","message":"tiny","at":"2026-10-01T09:02:00Z"}]}`
	if counts := postEvents(t, p, reuse); counts["created"] != 1 {
		t.Fatalf("rejected id must be reusable: %v", counts)
	}

	// Kill without graceful shutdown; on recovery only the two accepted
	// events may be present — the rejected batch must have left no frame.
	p.signal(t, syscall.SIGKILL)
	p.waitExit(t, -1)
	waitTCPPortClosed(t, p.addr)

	if onDisk, err := os.ReadFile(filepath.Join(dataDir, "events.log")); err != nil {
		t.Fatalf("read events log: %v", err)
	} else if strings.Count(string(onDisk), `"huge"`) != 1 {
		// One accepted frame now carries the small "huge" retry; the
		// oversize message never landed, so there must be no second hit.
		t.Fatalf("rejected batch left a durable record (huge hits=%d)", strings.Count(string(onDisk), `"huge"`))
	}

	p2 := startServer(t, dataDir)
	got := getEvents(t, p2, "?service=svc")
	if len(got) != 2 {
		t.Fatalf("after recovery want the 2 accepted events, got %d: %+v", len(got), got)
	}
	// Reusing the id again after restart is a replay, not a conflict.
	if counts := postEvents(t, p2, reuse); counts["created"] != 0 || counts["replayed"] != 1 {
		t.Fatalf("rejected-then-accepted id must replay after restart: %v", counts)
	}
	p2.signal(t, syscall.SIGTERM)
	p2.waitExit(t, 0)
}

func truncate(b []byte) string {
	const max = 200
	if len(b) <= max {
		return string(b)
	}
	return string(b[:max]) + "..."
}
