package httpapi

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/cocosolocoder/signalroom/internal/incidents"
)

// bigNoteLen returns how many expanding characters an add_note record needs
// for its saved encoding to exceed the save capacity, measured against the
// exact save format the server writes. A literal '<' is one request-body byte
// but six saved bytes, so the request stays comfortably under the 16 MiB
// body limit even though it saves to over 64 MiB.
func bigNoteLen(t *testing.T, actionID string) int {
	t.Helper()
	at := time.Date(2026, 10, 1, 12, 0, 0, 1, time.UTC)
	probe := incidents.Request{
		IncidentID:      "INC-1",
		ActionID:        actionID,
		Operator:        "alice",
		ExpectedVersion: 1,
		Type:            incidents.ActionNote,
	}
	encode := func(n int) int {
		probe.Content = strings.Repeat("<", n)
		payload, err := incidents.MarshalRecord(incidents.NewActionRecord(probe, at))
		if err != nil {
			t.Fatalf("marshal record: %v", err)
		}
		return len(payload)
	}
	lo, hi := 1, maxBodyBytes
	for lo < hi {
		mid := (lo + hi) / 2
		if encode(mid) > incidents.MaxRecordPayloadBytes {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	if encode(lo) <= incidents.MaxRecordPayloadBytes {
		t.Fatal("test setup failed to find an oversize note length")
	}
	return lo
}

func oversizeNoteBody(actionID string, ch string, n int) string {
	return `{"action_id":"` + actionID + `","operator":"alice","expected_version":1,` +
		`"action":"add_note","content":"` + strings.Repeat(ch, n) + `"}`
}

// TestPostOversizeNoteIs400NotStorageFailure exercises the reported bug: a
// legal, sub-16-MiB add_note request whose saved content escapes past 64 MiB
// must be a 400 telling the client to send less, never a 503 demanding a
// restart. The rejected note is saved nowhere, the incident is untouched, and
// the rejected action id remains usable with the original expected_version.
func TestPostOversizeNoteIs400NotStorageFailure(t *testing.T) {
	server, store, _, reg := newIncidentServer(t)
	createIncident(t, server)
	// Each of the three HTML-escaped characters the save format expands must
	// be classified the same way. All rejections happen at version 1 because
	// none of them commit.
	for i, ch := range []string{"&", "<", ">"} {
		actionID := "big" + string(rune('a'+i))
		n := bigNoteLen(t, actionID)
		body := oversizeNoteBody(actionID, ch, n)
		if len(body) >= maxBodyBytes {
			t.Fatalf("test setup: request body %d must be under the %d body limit", len(body), maxBodyBytes)
		}
		status, out := postAction(t, server, "INC-1", body)
		if status != http.StatusBadRequest {
			t.Fatalf("oversize note with %q must be 400, got %d %v", ch, status, out)
		}
		msg, _ := out["error"].(string)
		if msg == "" {
			t.Fatal("error must be non-empty")
		}
		if !strings.Contains(msg, "64 MiB") {
			t.Fatalf("error must name the 64 MiB save capacity, got %q", msg)
		}
		if !strings.Contains(msg, "save capacity") || !strings.Contains(strings.ToLower(msg), "reduce") {
			t.Fatalf("error must explain the save capacity and ask for less content, got %q", msg)
		}
		if strings.Contains(strings.ToLower(msg), "restart") {
			t.Fatalf("oversize error must not suggest a restart, got %q", msg)
		}
	}

	// Nothing durable was written beyond the creation, the store was not
	// poisoned, and the incident is exactly as it was at creation.
	if len(store.records) != 1 {
		t.Fatalf("oversize notes must not reach storage, got %d records", len(store.records))
	}
	if store.Poisoned() {
		t.Fatal("oversize note must not poison storage")
	}
	status, got := doJSON(t, server, http.MethodGet, "/incidents/INC-1", "")
	if status != http.StatusOK {
		t.Fatalf("incident must stay queryable, got %d %v", status, got)
	}
	if number(got["version"]) != 1 {
		t.Fatalf("version must stay 1, got %v", got["version"])
	}
	history, _ := got["history"].([]any)
	if len(history) != 1 {
		t.Fatalf("no note history may be appended, got %d entries", len(history))
	}
	if got["status"] != "open" || got["owner"] != "alice" {
		t.Fatalf("status/owner must be untouched: %v", got)
	}
	participants, _ := got["participants"].([]any)
	if len(participants) != 1 || participants[0] != "alice" {
		t.Fatalf("participants must be untouched: %v", participants)
	}
	linked, _ := got["events"].([]any)
	if len(linked) != 0 {
		t.Fatalf("linked events must be untouched: %v", linked)
	}

	// The rejected action id succeeds with a shortened note and the original
	// expected_version (the incident never moved), adding exactly one
	// history entry.
	small := `{"action_id":"biga","operator":"alice","expected_version":1,` +
		`"action":"add_note","content":"much shorter"}`
	status, out := postAction(t, server, "INC-1", small)
	if status != http.StatusOK || number(out["version"]) != 2 {
		t.Fatalf("shortened retry must commit at version 2, got %d %v", status, out)
	}
	status, got = doJSON(t, server, http.MethodGet, "/incidents/INC-1", "")
	if status != http.StatusOK || number(got["version"]) != 2 {
		t.Fatalf("incident must advance to 2 once, got %d %v", status, got)
	}
	history, _ = got["history"].([]any)
	if len(history) != 2 || history[1].(map[string]any)["content"] != "much shorter" {
		t.Fatalf("shortened note must be the one new history entry: %v", history)
	}
	if len(store.records) != 2 {
		t.Fatalf("exactly one note record may persist, got %d", len(store.records))
	}

	// The registry agrees with storage: no oversized entry reached state.
	inc, err := reg.Get("INC-1")
	if err != nil {
		t.Fatal(err)
	}
	if inc.Version != 2 || len(inc.History) != 2 {
		t.Fatalf("in-memory state must match, got %+v", inc)
	}
}

// TestPostOversizeNoteAfterErrorsKeepsPrecedence ensures the existing errors
// still win: a note to a resolved incident is 409 and an unknown action shape
// is 400, even when the same content would exceed the save capacity.
func TestPostOversizeNoteAfterErrorsKeepsPrecedence(t *testing.T) {
	server, store, _, _ := newIncidentServer(t)
	createIncident(t, server)
	n := bigNoteLen(t, "late")

	// Resolve the incident first.
	resolve := `{"action_id":"r1","operator":"alice","expected_version":1,"action":"resolve","reason":"done"}`
	if status, out := postAction(t, server, "INC-1", resolve); status != http.StatusOK {
		t.Fatalf("resolve: %d %v", status, out)
	}

	// The oversized note must surface the state conflict, not the capacity.
	body := oversizeNoteBody("late", "<", n)
	status, out := postAction(t, server, "INC-1", body)
	if status != http.StatusConflict {
		t.Fatalf("resolved-state conflict must precede capacity, got %d %v", status, out)
	}
	if msg, _ := out["error"].(string); strings.Contains(msg, "save capacity") {
		t.Fatalf("conflict error must not be the capacity message, got %q", msg)
	}
	if len(store.records) != 2 { // create + resolve only
		t.Fatalf("rejected note must not persist, got %d records", len(store.records))
	}

	// A malformed request never reaches the capacity check.
	bad := strings.Replace(body, `"add_note"`, `"add_note",`, 1)
	if status, out := postAction(t, server, "INC-1", bad); status != http.StatusBadRequest {
		t.Fatalf("invalid JSON must stay 400, got %d %v", status, out)
	}

	// An unknown incident is 404 regardless of the note size.
	if status, out := doJSON(t, server, http.MethodPost, "/incidents/ghost/actions",
		oversizeNoteBody("late", "<", n)); status != http.StatusNotFound {
		t.Fatalf("unknown incident must be 404, got %d %v", status, out)
	}
}

// TestPostOversizeReplayNotMeasured shows that only a brand-new saved note
// counts toward the capacity: an oversized note recovered via the registry's
// startup Load path replays its first result over HTTP, even after the
// incident has been resolved, writing nothing.
func TestPostOversizeReplayNotMeasured(t *testing.T) {
	server, store, _, reg := newIncidentServer(t)
	createIncident(t, server)
	n := bigNoteLen(t, "big-old")
	at := time.Date(2026, 10, 1, 12, 0, 0, 1, time.UTC)
	big := incidents.Request{
		IncidentID: "INC-1", ActionID: "big-old", Operator: "alice",
		ExpectedVersion: 1, Type: incidents.ActionNote, Content: strings.Repeat("&", n),
	}
	if err := reg.Load(incidents.NewActionRecord(big, at)); err != nil {
		t.Fatalf("seed oversized note through recovery: %v", err)
	}

	// The live store never saw the seeded note; a resolve then advances to 3.
	resolve := `{"action_id":"r1","operator":"alice","expected_version":2,"action":"resolve","reason":"done"}`
	if status, out := postAction(t, server, "INC-1", resolve); status != http.StatusOK || number(out["version"]) != 3 {
		t.Fatalf("resolve: %d %v", status, out)
	}

	// An identical retry of the huge note returns the first result, saves
	// nothing, and is neither capacity-checked nor rejected for being late.
	body := oversizeNoteBody("big-old", "&", n)
	if len(body) >= maxBodyBytes {
		t.Fatalf("test setup: replay body %d exceeds the body limit", len(body))
	}
	status, out := postAction(t, server, "INC-1", body)
	if status != http.StatusOK || number(out["version"]) != 2 {
		t.Fatalf("identical oversized replay must return its first result, got %d %v", status, out)
	}
	status, got := doJSON(t, server, http.MethodGet, "/incidents/INC-1", "")
	history, _ := got["history"].([]any)
	if status != http.StatusOK || number(got["version"]) != 3 || len(history) != 3 {
		t.Fatalf("replay must add no history: %d %v", status, got)
	}
	if len(store.records) != 2 { // create + resolve; replay wrote nothing
		t.Fatalf("replay must not persist, got %d records", len(store.records))
	}

	// The same action id with changed content is still a conflict, size aside.
	changed := oversizeNoteBody("big-old", ">", n)
	if status, out = postAction(t, server, "INC-1", changed); status != http.StatusConflict {
		t.Fatalf("changed content must conflict, got %d %v", status, out)
	}
}

// TestPostOversizeNoteRealStorageFailureStill503 keeps the two failure modes
// distinct: a genuine write failure still poisons the store and reports 503
// with the restart guidance, even after an oversize rejection.
func TestPostOversizeNoteRealStorageFailureStill503(t *testing.T) {
	server, store, _, _ := newIncidentServer(t)
	createIncident(t, server)
	n := bigNoteLen(t, "big")

	if status, out := postAction(t, server, "INC-1", oversizeNoteBody("big", "<", n)); status != http.StatusBadRequest {
		t.Fatalf("oversize note must be 400, got %d %v", status, out)
	}

	// A subsequent genuine write failure is still 503 and poisons the store.
	store.failNext = true
	small := `{"action_id":"ok","operator":"alice","expected_version":1,"action":"add_note","content":"fine"}`
	status, out := postAction(t, server, "INC-1", small)
	if status != http.StatusServiceUnavailable {
		t.Fatalf("real write failure must be 503, got %d %v", status, out)
	}
	raw, _ := json.Marshal(out)
	if !strings.Contains(string(raw), "restart") {
		t.Fatalf("503 must keep the restart guidance, got %s", raw)
	}
	if !store.Poisoned() {
		t.Fatal("the real failure must poison the store")
	}
	status, _ = postAction(t, server, "INC-1",
		`{"action_id":"later","operator":"alice","expected_version":1,"action":"add_note","content":"x"}`)
	if status != http.StatusServiceUnavailable {
		t.Fatalf("poisoned store must keep returning 503, got %d", status)
	}
}
