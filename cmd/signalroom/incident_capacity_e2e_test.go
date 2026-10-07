package main

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
)

// bigNoteCount is a run of less-than characters large enough that the saved
// action record's JSON HTML escaping (one request byte becomes six saved
// bytes per character) pushes it past the 64 MiB save capacity, while the
// literal characters keep the request body under the 16 MiB body limit.
const bigNoteCount = 11_200_000

func bigNoteBody(actionID string, expected int) string {
	return `{"action_id":"` + actionID + `","operator":"bob","expected_version":` +
		strconv.Itoa(expected) + `,"action":"add_note","content":"` +
		strings.Repeat("<", bigNoteCount) + `"}`
}

// TestServeOversizeNoteIs400AndStaysUsable is the end-to-end form of the
// reported bug: a legal add_note whose body is under 16 MiB but whose saved
// JSON record exceeds 64 MiB is a 400 (send less content), not a 503
// (storage failed, restart). The rejection appends no history, version, or
// record, leaves status/people/links untouched, the rejected action_id stays
// usable with the same expected_version, and incident storage keeps serving
// without a restart or a durable trace.
func TestServeOversizeNoteIs400AndStaysUsable(t *testing.T) {
	dataDir := t.TempDir()
	p := startServer(t, dataDir)

	if status, created := postIncident(t, p,
		`{"id":"INC-1","title":"Outage","service":"gateway","operator":"alice"}`); status != http.StatusOK ||
		incNumber(created["version"]) != 1 {
		t.Fatalf("create %d %v", status, created)
	}

	oversize := bigNoteBody("big", 1)
	if len(oversize) >= 16<<20 {
		t.Fatalf("setup: request body %d exceeds the 16 MiB body limit", len(oversize))
	}

	status, raw := httpDo(t, http.MethodPost, "http://"+p.addr+"/incidents/INC-1/actions", oversize)
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

	// The incident detail is exactly the pre-submission snapshot.
	status, got := getIncident(t, p, "INC-1")
	if status != http.StatusOK {
		t.Fatalf("get after rejection %d", status)
	}
	if got["status"] != "open" || incNumber(got["version"]) != 1 {
		t.Fatalf("rejection changed status/version: %v", got)
	}
	history, _ := got["history"].([]any)
	if len(history) != 1 || history[0].(map[string]any)["action"] != "create" {
		t.Fatalf("rejection appended history: %v", got["history"])
	}
	people, _ := got["participants"].([]any)
	if len(people) != 1 || people[0] != "alice" || got["owner"] != "alice" {
		t.Fatalf("rejection changed people: %v owner=%v", people, got["owner"])
	}
	if linked, _ := got["events"].([]any); len(linked) != 0 {
		t.Fatalf("rejection changed linked events: %v", linked)
	}

	// No durable record of the rejected note, and no poisoning: a normal
	// note on a fresh action id commits without a restart.
	normal := `{"action_id":"other","operator":"bob","expected_version":1,"action":"add_note","content":"fine"}`
	if status, out := postIncidentAction(t, p, "INC-1", normal); status != http.StatusOK ||
		incNumber(out["version"]) != 2 {
		t.Fatalf("storage must stay usable: %d %v", status, out)
	}

	// The rejected action id works with the (now current) version for a
	// shortened note, adding exactly one more history entry.
	shortened := `{"action_id":"big","operator":"bob","expected_version":2,"action":"add_note","content":"shortened"}`
	if status, out := postIncidentAction(t, p, "INC-1", shortened); status != http.StatusOK ||
		incNumber(out["version"]) != 3 {
		t.Fatalf("rejected action id must accept a shortened note, got %d %v", status, out)
	}
	status, got = getIncident(t, p, "INC-1")
	history, _ = got["history"].([]any)
	if status != http.StatusOK || incNumber(got["version"]) != 3 || len(history) != 3 {
		t.Fatalf("shortened note must add exactly one entry: %d %v", status, got["history"])
	}
	if history[2].(map[string]any)["content"] != "shortened" {
		t.Fatalf("shortened note must be saved whole: %v", history[2])
	}

	// Kill; on recovery only the create and two small notes exist, and the
	// oversize content left no frame behind.
	p.signal(t, syscall.SIGKILL)
	p.waitExit(t, -1)
	waitTCPPortClosed(t, p.addr)

	onDisk, err := os.ReadFile(filepath.Join(dataDir, "incidents.log"))
	if err != nil {
		t.Fatalf("read incidents log: %v", err)
	}
	// The save format HTML-escapes a less-than character to a six-byte
	// sequence, so a durable run of the rejected note would show up as a long
	// stretch of that sequence.
	escapedLT := string([]byte{'\\', 'u', '0', '0', '3', 'c'})
	if strings.Count(string(onDisk), strings.Repeat(escapedLT, 1000)) != 0 {
		t.Fatal("rejected oversize note left a durable trace")
	}

	p2 := startServer(t, dataDir)
	status, got = getIncident(t, p2, "INC-1")
	if status != http.StatusOK || incNumber(got["version"]) != 3 {
		t.Fatalf("recovered incident version: %d %v", status, got)
	}
	history, _ = got["history"].([]any)
	if len(history) != 3 {
		t.Fatalf("recovered history len %d", len(history))
	}

	// Resolve, then retry the committed shortened note: it must keep
	// returning the first successful version (3) and add no history even
	// though the incident is now resolved.
	resolve := `{"action_id":"r1","operator":"bob","expected_version":3,"action":"resolve","reason":"fixed"}`
	if status, out := postIncidentAction(t, p2, "INC-1", resolve); status != http.StatusOK ||
		incNumber(out["version"]) != 4 {
		t.Fatalf("resolve %d %v", status, out)
	}
	if status, out := postIncidentAction(t, p2, "INC-1", shortened); status != http.StatusOK ||
		incNumber(out["version"]) != 3 {
		t.Fatalf("successful-note retry must return version 3 after resolve, got %d %v", status, out)
	}
	status, got = getIncident(t, p2, "INC-1")
	if status != http.StatusOK || got["status"] != "resolved" || incNumber(got["version"]) != 4 {
		t.Fatalf("retry must not change the resolved incident: %d %v", status, got)
	}
	if history, _ = got["history"].([]any); len(history) != 4 {
		t.Fatalf("retry appended history: %d", len(history))
	}

	// The same action id with changed content is still a conflict.
	changed := `{"action_id":"big","operator":"bob","expected_version":4,"action":"add_note","content":"different"}`
	if status, _ := postIncidentAction(t, p2, "INC-1", changed); status != http.StatusConflict {
		t.Fatalf("same action id with changed content must be 409, got %d", status)
	}

	p2.signal(t, syscall.SIGTERM)
	p2.waitExit(t, 0)
}

// TestServeOversizeNoteDoesNotPoisonIncidentStorage focuses on the
// availability guarantee: after the 400, every incident endpoint and later
// legal notes keep working in the same process, with no restart.
func TestServeOversizeNoteDoesNotPoisonIncidentStorage(t *testing.T) {
	dataDir := t.TempDir()
	p := startServer(t, dataDir)
	postIncident(t, p, `{"id":"INC-1","title":"t","service":"gateway","operator":"alice"}`)

	for i := 0; i < 2; i++ {
		if status, raw := httpDo(t, http.MethodPost, "http://"+p.addr+"/incidents/INC-1/actions",
			bigNoteBody("big"+strconv.Itoa(i), 1)); status != http.StatusBadRequest {
			t.Fatalf("oversize attempt %d must be 400, got %d: %s", i, status, truncate(raw))
		}
		// Repeated rejections keep the same pre-state.
		if status, got := getIncident(t, p, "INC-1"); status != http.StatusOK ||
			incNumber(got["version"]) != 1 {
			t.Fatalf("incident moved after rejection %d: %d %v", i, status, got)
		}
	}

	// A legal note still commits with the original expected_version.
	body := `{"action_id":"fine","operator":"bob","expected_version":1,"action":"add_note","content":"ok"}`
	if status, out := postIncidentAction(t, p, "INC-1", body); status != http.StatusOK ||
		incNumber(out["version"]) != 2 {
		t.Fatalf("legal note after repeated oversize rejections: %d %v", status, out)
	}

	p.signal(t, syscall.SIGTERM)
	p.waitExit(t, 0)
}
