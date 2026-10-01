package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func postIncident(t *testing.T, p *serverProc, body string) (int, map[string]any) {
	t.Helper()
	status, raw := httpDo(t, http.MethodPost, "http://"+p.addr+"/incidents", body)
	var out map[string]any
	if len(bytes.TrimSpace(raw)) > 0 {
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatalf("decode %q: %v", raw, err)
		}
	}
	return status, out
}

func postIncidentAction(t *testing.T, p *serverProc, id, body string) (int, map[string]any) {
	t.Helper()
	status, raw := httpDo(t, http.MethodPost, "http://"+p.addr+"/incidents/"+id+"/actions", body)
	var out map[string]any
	if len(bytes.TrimSpace(raw)) > 0 {
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatalf("decode %q: %v", raw, err)
		}
	}
	return status, out
}

func getIncident(t *testing.T, p *serverProc, id string) (int, map[string]any) {
	t.Helper()
	status, raw := httpDo(t, http.MethodGet, "http://"+p.addr+"/incidents/"+id, "")
	var out map[string]any
	if len(bytes.TrimSpace(raw)) > 0 {
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatalf("decode %q: %v", raw, err)
		}
	}
	return status, out
}

func incNumber(v any) int {
	f, _ := v.(float64)
	return int(f)
}

func TestServeIncidentsPersistAcrossKillAndDedupeRetry(t *testing.T) {
	dataDir := t.TempDir()
	p := startServer(t, dataDir)
	postEvents(t, p, `{"events":[{"id":"e1","service":"gateway","severity":"critical","message":"spike","at":"2026-10-01T10:00:00Z"}]}`)

	status, created := postIncident(t, p, `{"id":"INC-1","title":" Outage ","service":"gateway","operator":"alice"}`)
	if status != http.StatusOK || created["status"] != "open" || incNumber(created["version"]) != 1 {
		t.Fatalf("create %d %v", status, created)
	}

	noteBody := `{"action_id":"n1","operator":"bob","expected_version":1,"action":"add_note","content":"looking"}`
	if status, note := postIncidentAction(t, p, "INC-1", noteBody); status != http.StatusOK || incNumber(note["version"]) != 2 {
		t.Fatalf("note %d %v", status, note)
	}
	linkBody := `{"action_id":"l1","operator":"bob","expected_version":2,"action":"link_event","event_id":"e1"}`
	if status, link := postIncidentAction(t, p, "INC-1", linkBody); status != http.StatusOK || incNumber(link["version"]) != 3 {
		t.Fatalf("link %d %v", status, link)
	}
	resolveBody := `{"action_id":"r1","operator":"bob","expected_version":3,"action":"resolve","reason":"fixed"}`
	if status, res := postIncidentAction(t, p, "INC-1", resolveBody); status != http.StatusOK || incNumber(res["version"]) != 4 {
		t.Fatalf("resolve %d %v", status, res)
	}

	// Hard kill; fsync had already confirmed every record.
	p.signal(t, syscall.SIGKILL)
	p.waitExit(t, -1)
	waitTCPPortClosed(t, p.addr)

	p2 := startServer(t, dataDir)

	// Full snapshot recovered, including links and ordered history.
	status, got := getIncident(t, p2, "INC-1")
	if status != http.StatusOK {
		t.Fatalf("get after restart %d", status)
	}
	if got["title"] != "Outage" || got["status"] != "resolved" || incNumber(got["version"]) != 4 {
		t.Fatalf("recovered head %v", got)
	}
	linked, _ := got["events"].([]any)
	if len(linked) != 1 || linked[0].(map[string]any)["id"] != "e1" {
		t.Fatalf("recovered links %v", got["events"])
	}
	history, _ := got["history"].([]any)
	if len(history) != 4 {
		t.Fatalf("recovered history len %d", len(history))
	}
	for i, want := range []string{"create", "add_note", "link_event", "resolve"} {
		if history[i].(map[string]any)["action"] != want {
			t.Fatalf("history[%d]=%v want %s", i, history[i], want)
		}
	}

	// Retries after restart return the first results and change nothing.
	if s, c := postIncident(t, p2, `{"id":"INC-1","title":"Outage","service":"gateway","operator":"alice"}`); s != http.StatusOK || incNumber(c["version"]) != 1 {
		t.Fatalf("creation retry after restart %d %v", s, c)
	}
	if s, n := postIncidentAction(t, p2, "INC-1", noteBody); s != http.StatusOK || incNumber(n["version"]) != 2 {
		t.Fatalf("note retry after restart %d %v", s, n)
	}
	if s, r := postIncidentAction(t, p2, "INC-1", resolveBody); s != http.StatusOK || incNumber(r["version"]) != 4 {
		t.Fatalf("resolve retry after restart %d %v", s, r)
	}
	if _, still := getIncident(t, p2, "INC-1"); incNumber(still["version"]) != 4 {
		t.Fatalf("retries must not advance version: %v", still["version"])
	}

	// Continuing to operate on the recovered incident works.
	reopenBody := `{"action_id":"o1","operator":"carol","expected_version":4,"action":"reopen","reason":"regression"}`
	if s, o := postIncidentAction(t, p2, "INC-1", reopenBody); s != http.StatusOK || incNumber(o["version"]) != 5 {
		t.Fatalf("reopen after restart %d %v", s, o)
	}

	p2.signal(t, syscall.SIGTERM)
	p2.waitExit(t, 0)
}

func TestServeIncidentsWorkOnLegacyDataDir(t *testing.T) {
	// An older data directory holds only events.log; incidents.log appears on
	// first use and the recovered events can be linked.
	dataDir := t.TempDir()
	payload := []byte(`[{"id":"e1","service":"gateway","severity":"critical","message":"m","at":"2026-10-01T10:00:00Z"}]`)
	frame := make([]byte, 8+len(payload))
	binary.BigEndian.PutUint32(frame[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(payload))
	copy(frame[8:], payload)
	if err := os.WriteFile(filepath.Join(dataDir, "events.log"), frame, 0o644); err != nil {
		t.Fatal(err)
	}

	p := startServer(t, dataDir)
	if status, _ := getIncident(t, p, "anything"); status != http.StatusNotFound {
		t.Fatalf("legacy dir has no incidents: %d", status)
	}
	if status, c := postIncident(t, p, `{"id":"INC-1","title":"t","service":"gateway","operator":"alice"}`); status != http.StatusOK || incNumber(c["version"]) != 1 {
		t.Fatalf("create on legacy dir %d %v", status, c)
	}
	if status, l := postIncidentAction(t, p, "INC-1",
		`{"action_id":"l1","operator":"alice","expected_version":1,"action":"link_event","event_id":"e1"}`); status != http.StatusOK || incNumber(l["version"]) != 2 {
		t.Fatalf("link recovered event %d %v", status, l)
	}

	p.signal(t, syscall.SIGTERM)
	p.waitExit(t, 0)
	waitTCPPortClosed(t, p.addr)

	// The freshly written incidents.log recovers on the next start too.
	p2 := startServer(t, dataDir)
	if status, got := getIncident(t, p2, "INC-1"); status != http.StatusOK || incNumber(got["version"]) != 2 {
		t.Fatalf("incident log created in legacy dir must persist %d %v", status, got)
	}
	p2.signal(t, syscall.SIGTERM)
	p2.waitExit(t, 0)
}

func TestServeIncidentTornTailTrimmed(t *testing.T) {
	dataDir := t.TempDir()
	first := startServer(t, dataDir)
	postIncident(t, first, `{"id":"INC-1","title":"t","service":"gateway","operator":"alice"}`)
	first.signal(t, syscall.SIGKILL)
	first.waitExit(t, -1)
	waitTCPPortClosed(t, first.addr)

	path := filepath.Join(dataDir, "incidents.log")
	existing, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Append a torn second frame: full header, truncated payload.
	payload := []byte(`{"kind":"create","at":"2026-10-01T12:00:00Z","creation":{"id":"INC-2","title":"t","service":"gateway","operator":"alice"}}`)
	torn := make([]byte, 8+4)
	binary.BigEndian.PutUint32(torn[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(torn[4:8], crc32.ChecksumIEEE(payload))
	copy(torn[8:], payload[:4])
	if err := os.WriteFile(path, append(existing, torn...), 0o644); err != nil {
		t.Fatal(err)
	}

	second := startServer(t, dataDir)
	if status, got := getIncident(t, second, "INC-1"); status != http.StatusOK || incNumber(got["version"]) != 1 {
		t.Fatalf("good record must survive torn tail: %d %v", status, got)
	}
	if status, _ := getIncident(t, second, "INC-2"); status != http.StatusNotFound {
		t.Fatalf("torn record must be discarded: %d", status)
	}
	// The trimmed log accepts new durable writes.
	if status, c := postIncident(t, second, `{"id":"INC-3","title":"t","service":"gateway","operator":"alice"}`); status != http.StatusOK || incNumber(c["version"]) != 1 {
		t.Fatalf("create after torn tail %d %v", status, c)
	}
	second.signal(t, syscall.SIGTERM)
	second.waitExit(t, 0)
}

func TestServeFailsOnCorruptIncidentLogWithoutMutation(t *testing.T) {
	dataDir := t.TempDir()
	first := startServer(t, dataDir)
	postIncident(t, first, `{"id":"INC-1","title":"t","service":"gateway","operator":"alice"}`)
	first.signal(t, syscall.SIGKILL)
	first.waitExit(t, -1)
	waitTCPPortClosed(t, first.addr)

	path := filepath.Join(dataDir, "incidents.log")
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	firstLen := 8 + int(binary.BigEndian.Uint32(original[0:4]))

	payload := []byte(`{"kind":"create","at":"2026-10-01T12:00:00Z","creation":{"id":"INC-2","title":"t","service":"gateway","operator":"alice"}}`)
	bad := make([]byte, 8+len(payload))
	binary.BigEndian.PutUint32(bad[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(bad[4:8], crc32.ChecksumIEEE(payload))
	copy(bad[8:], payload)
	bad[8] ^= 0xFF // complete middle frame, bad checksum
	corrupt := append(append([]byte{}, original[:firstLen]...), bad...)
	if err := os.WriteFile(path, corrupt, 0o644); err != nil {
		t.Fatal(err)
	}

	logs := &safeBuffer{}
	failing := exec.Command(binaryPath, "serve", "--addr", "127.0.0.1:0", "--data", dataDir)
	failing.Stdout = logs
	failing.Stderr = logs
	if err := failing.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- failing.Wait() }()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("corrupt incident log must prevent startup")
		}
		if ee, ok := err.(*exec.ExitError); !ok || ee.ExitCode() == 0 {
			t.Fatalf("corrupt incident log must exit non-zero: %v", err)
		}
	case <-ctx.Done():
		failing.Process.Kill()
		t.Fatal("server stayed up despite a corrupt incident log")
	}

	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, corrupt) {
		t.Fatal("failed startup must not modify the corrupt incident log")
	}
}
