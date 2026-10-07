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

// bigNoteCount is a run of '<' large enough that its saved encoding escapes
// past the 64 MiB save capacity, while the literal characters keep the
// request body under the 16 MiB body limit (each '<' is one request byte but
// six saved bytes).
const bigNoteCount = 11_200_000

func oversizeNoteBody(actionID string, ch string) string {
	return `{"action_id":"` + actionID + `","operator":"alice","expected_version":1,` +
		`"action":"add_note","content":"` + strings.Repeat(ch, bigNoteCount) + `"}`
}

func createIncidentE2E(t *testing.T, p *serverProc) {
	t.Helper()
	if status, out := postIncident(t, p,
		`{"id":"INC-1","title":"Outage","service":"gateway","operator":"alice"}`); status != http.StatusOK {
		t.Fatalf("create incident: %d %v", status, out)
	}
}

// TestServeOversizeNoteIs400AndStaysUsable is the end-to-end form of the
// reported bug: a legal, sub-16-MiB add_note request that saves to over
// 64 MiB once its content is JSON-escaped must be a 400 (send less
// content), not a 503 (storage failed, restart). The rejected note leaves no
// durable record and changes nothing about the incident, the server keeps
// accepting requests with no restart, the rejected action id is reusable at
// the original expected_version, and a kill/recovery cycle shows no trace of
// the rejected note.
func TestServeOversizeNoteIs400AndStaysUsable(t *testing.T) {
	dataDir := t.TempDir()
	p := startServer(t, dataDir)
	createIncidentE2E(t, p)

	body := oversizeNoteBody("huge-note", "<")
	if len(body) >= 16<<20 {
		t.Fatalf("setup: request body %d exceeds the 16 MiB body limit", len(body))
	}
	status, raw := httpDo(t, http.MethodPost, "http://"+p.addr+"/incidents/INC-1/actions", body)
	if status != http.StatusBadRequest {
		t.Fatalf("oversize note must be 400, got %d: %s", status, truncate(raw))
	}
	var errBody map[string]string
	if err := json.Unmarshal(raw, &errBody); err != nil {
		t.Fatalf("decode error body %q: %v", raw, err)
	}
	msg := errBody["error"]
	if msg == "" {
		t.Fatal("error must be non-empty")
	}
	if !strings.Contains(msg, "64 MiB") || !strings.Contains(msg, "save capacity") {
		t.Fatalf("error must name the 64 MiB save capacity, got %q", msg)
	}
	if !strings.Contains(strings.ToLower(msg), "reduce") {
		t.Fatalf("error must ask for less content, got %q", msg)
	}
	if strings.Contains(strings.ToLower(msg), "restart") {
		t.Fatalf("oversize error must not demand a restart, got %q", msg)
	}

	// The incident is byte-for-byte the creation snapshot: no note history,
	// version, status, people, or linked-event change.
	sstatus, got := getIncident(t, p, "INC-1")
	if sstatus != http.StatusOK {
		t.Fatalf("incident must stay queryable: %d", sstatus)
	}
	if incNumber(got["version"]) != 1 || got["status"] != "open" || got["owner"] != "alice" {
		t.Fatalf("incident head must be untouched: %v", got)
	}
	history, _ := got["history"].([]any)
	if len(history) != 1 || history[0].(map[string]any)["action"] != "create" {
		t.Fatalf("no note history may be appended: %v", history)
	}
	participants, _ := got["participants"].([]any)
	if len(participants) != 1 || participants[0] != "alice" {
		t.Fatalf("participants must be untouched: %v", participants)
	}
	linked, _ := got["events"].([]any)
	if len(linked) != 0 {
		t.Fatalf("linked events must be untouched: %v", linked)
	}

	// The rejected action id accepts a shortened note at the original
	// expected_version (the incident never moved), producing exactly one
	// history entry.
	small := `{"action_id":"huge-note","operator":"alice","expected_version":1,` +
		`"action":"add_note","content":"shortened note"}`
	if status, out := postIncidentAction(t, p, "INC-1", small); status != http.StatusOK || incNumber(out["version"]) != 2 {
		t.Fatalf("shortened retry must commit at version 2, got %d %v", status, out)
	}
	if status, got = getIncident(t, p, "INC-1"); status != http.StatusOK || incNumber(got["version"]) != 2 {
		t.Fatalf("incident must advance exactly once: %d %v", status, got)
	}
	history, _ = got["history"].([]any)
	if len(history) != 2 || history[1].(map[string]any)["content"] != "shortened note" {
		t.Fatalf("shortened note must be the single new history entry: %v", history)
	}

	// Incident storage stays usable with no restart: a second normal note and
	// a resolve commit.
	next := `{"action_id":"n2","operator":"bob","expected_version":2,"action":"add_note","content":"still works"}`
	if status, out := postIncidentAction(t, p, "INC-1", next); status != http.StatusOK || incNumber(out["version"]) != 3 {
		t.Fatalf("follow-up note after rejection: %d %v", status, out)
	}
	resolve := `{"action_id":"r1","operator":"alice","expected_version":3,"action":"resolve","reason":"fixed"}`
	if status, out := postIncidentAction(t, p, "INC-1", resolve); status != http.StatusOK || incNumber(out["version"]) != 4 {
		t.Fatalf("resolve after rejection: %d %v", status, out)
	}

	// Kill without graceful shutdown; on recovery the rejected note must have
	// left no frame, so the action id appears only in the accepted small note.
	p.signal(t, syscall.SIGKILL)
	p.waitExit(t, -1)
	waitTCPPortClosed(t, p.addr)

	onDisk, err := os.ReadFile(filepath.Join(dataDir, "incidents.log"))
	if err != nil {
		t.Fatalf("read incidents log: %v", err)
	}
	if strings.Count(string(onDisk), "huge-note") != 1 {
		t.Fatalf("rejected note must leave no frame, huge-note hits=%d", strings.Count(string(onDisk), "huge-note"))
	}
	if strings.Contains(string(onDisk), strings.Repeat("<", 1000)) {
		t.Fatal("the expanded '<' run must never have been written to the log")
	}

	p2 := startServer(t, dataDir)
	status, got = getIncident(t, p2, "INC-1")
	if status != http.StatusOK || got["status"] != "resolved" || incNumber(got["version"]) != 4 {
		t.Fatalf("recovered incident must be the resolved version 4: %d %v", status, got)
	}
	history, _ = got["history"].([]any)
	if len(history) != 4 {
		t.Fatalf("recovered history must hold create, shortened note, follow-up, resolve: %d entries", len(history))
	}

	// Re-submitting the accepted shortened note replays its first result and
	// adds nothing, even though the incident is resolved now.
	if status, out := postIncidentAction(t, p2, "INC-1", small); status != http.StatusOK || incNumber(out["version"]) != 2 {
		t.Fatalf("accepted note retry must replay version 2, got %d %v", status, out)
	}
	if status, got = getIncident(t, p2, "INC-1"); incNumber(got["version"]) != 4 {
		t.Fatalf("replay must not advance the incident: %v", got["version"])
	}

	p2.signal(t, syscall.SIGTERM)
	p2.waitExit(t, 0)
}

// TestServeOversizeNoteKeepsExistingErrors checks the precedence and
// idempotency rules through the real server: an oversized note is only a 400
// when the request would otherwise have succeeded.
func TestServeOversizeNoteKeepsExistingErrors(t *testing.T) {
	dataDir := t.TempDir()
	p := startServer(t, dataDir)
	createIncidentE2E(t, p)

	// Unknown incident: 404, not the capacity 400.
	body := oversizeNoteBody("huge-note", "&")
	if status, raw := httpDo(t, http.MethodPost, "http://"+p.addr+"/incidents/ghost/actions", body); status != http.StatusNotFound {
		t.Fatalf("unknown incident must be 404, got %d: %s", status, truncate(raw))
	}

	// Stale expected_version: 409 carrying the current version, ahead of the
	// capacity check.
	stale := strings.Replace(body, `"expected_version":1`, `"expected_version":99`, 1)
	status, raw := httpDo(t, http.MethodPost, "http://"+p.addr+"/incidents/INC-1/actions", stale)
	if status != http.StatusConflict {
		t.Fatalf("stale version must be 409, got %d: %s", status, truncate(raw))
	}
	var conflict map[string]any
	if err := json.Unmarshal(raw, &conflict); err != nil || incNumber(conflict["current_version"]) != 1 {
		t.Fatalf("stale-version conflict must report current_version 1: %s", raw)
	}

	// The valid oversize note is 400 and leaves the incident at version 1.
	if status, raw := httpDo(t, http.MethodPost, "http://"+p.addr+"/incidents/INC-1/actions", body); status != http.StatusBadRequest {
		t.Fatalf("oversize note must be 400, got %d: %s", status, truncate(raw))
	}

	// Resolve, then an oversized note is the ordinary state-machine 409.
	resolve := `{"action_id":"r1","operator":"alice","expected_version":1,"action":"resolve","reason":"done"}`
	if status, out := postIncidentAction(t, p, "INC-1", resolve); status != http.StatusOK {
		t.Fatalf("resolve: %d %v", status, out)
	}
	late := strings.Replace(body, `"huge-note"`, `"late-note"`, 1)
	late = strings.Replace(late, `"expected_version":1`, `"expected_version":2`, 1)
	if status, raw := httpDo(t, http.MethodPost, "http://"+p.addr+"/incidents/INC-1/actions", late); status != http.StatusConflict {
		t.Fatalf("note on a resolved incident must be 409, got %d: %s", status, truncate(raw))
	}

	// The rejected-but-now-resolved action id was never committed, so the
	// incident is exactly at version 2 (create + resolve).
	if status, got := getIncident(t, p, "INC-1"); status != http.StatusOK || incNumber(got["version"]) != 2 {
		t.Fatalf("only the resolve may have advanced the incident: %d %v", status, got)
	}

	p.signal(t, syscall.SIGTERM)
	p.waitExit(t, 0)
}
