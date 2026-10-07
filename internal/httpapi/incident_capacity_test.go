package httpapi

import (
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cocosolocoder/signalroom/internal/incidents"
)

// bigNoteAmps finds an ampersand count whose add_note body stays under the
// 16 MiB request limit while its saved record exceeds the 64 MiB save
// capacity: the save format HTML-escapes '&' to a six-byte sequence, so one
// request byte costs up to six saved bytes. The count is found against the
// real save format rather than guessed.
func bigNoteAmps(t *testing.T) (amps int, body string) {
	t.Helper()
	at := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	probe := incidents.Request{
		IncidentID: "INC-1", ActionID: "big", Operator: "bob",
		ExpectedVersion: 1, Type: incidents.ActionNote, Content: "",
	}
	empty, err := incidents.MarshalRecord(incidents.NewActionRecord(probe, at))
	if err != nil {
		t.Fatalf("marshal probe: %v", err)
	}
	wide := probe
	wide.Content = strings.Repeat("&", 1000)
	widePayload, err := incidents.MarshalRecord(incidents.NewActionRecord(wide, at))
	if err != nil {
		t.Fatalf("marshal wide probe: %v", err)
	}
	width := (len(widePayload) - len(empty)) / 1000
	if width <= 1 {
		t.Fatalf("test setup: escaping must expand each '&', got width %d", width)
	}
	amps = (incidents.MaxActionPayloadBytes-len(empty))/width + 1

	body = `{"action_id":"big","operator":"bob","expected_version":1,"action":"add_note","content":"` +
		strings.Repeat("&", amps) + `"}`
	if len(body) >= maxBodyBytes {
		t.Fatalf("test setup: request body %d exceeds the %d body limit", len(body), maxBodyBytes)
	}
	saved := probe
	saved.Content = strings.Repeat("&", amps)
	payload, err := incidents.MarshalRecord(incidents.NewActionRecord(saved, at))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if len(payload) <= incidents.MaxActionPayloadBytes {
		t.Fatal("test setup failed to find an oversize note length")
	}
	return amps, body
}

func oversizeNoteBody(actionID string, amps, expected int) string {
	return `{"action_id":"` + actionID + `","operator":"bob","expected_version":` +
		strconv.Itoa(expected) + `,"action":"add_note","content":"` + strings.Repeat("&", amps) + `"}`
}

// TestPostOversizeNoteIs400NotStorageFailure is the reported bug over HTTP:
// a legal, sub-16-MiB add_note whose JSON-escaped record exceeds 64 MiB must
// be a 400 telling the client to send less, never a 503 demanding a restart.
func TestPostOversizeNoteIs400NotStorageFailure(t *testing.T) {
	server, store, _, reg := newIncidentServer(t)
	createIncident(t, server)
	_, body := bigNoteAmps(t)

	status, out := postAction(t, server, "INC-1", body)
	if status != http.StatusBadRequest {
		t.Fatalf("oversize note must be 400, got %d: %v", status, out)
	}
	msg, _ := out["error"].(string)
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

	// Nothing durable was written and storage was not poisoned.
	if len(store.records) != 1 {
		t.Fatalf("only the creation record must exist, got %d", len(store.records))
	}
	if store.Poisoned() {
		t.Fatal("oversize note must not poison storage")
	}

	// The incident is byte-for-byte the pre-submission snapshot: no history
	// entry, no version bump, unchanged status, people, and links.
	inc, err := reg.Get("INC-1")
	if err != nil {
		t.Fatal(err)
	}
	if inc.Version != 1 || len(inc.History) != 1 {
		t.Fatalf("rejected note must not append history or bump version: %+v", inc)
	}
	if inc.Status != incidents.StatusOpen {
		t.Fatalf("status changed to %q", inc.Status)
	}
	if len(inc.Participants) != 1 || inc.Participants[0] != "alice" || inc.Owner != "alice" {
		t.Fatalf("people changed: participants=%v owner=%q", inc.Participants, inc.Owner)
	}
	if len(inc.Links) != 0 {
		t.Fatalf("links changed: %v", inc.Links)
	}

	// Queries keep working with no restart, and the rejected action id is
	// reusable right away with the same expected_version: ordinary success,
	// exactly one new history entry.
	if s, got := doJSON(t, server, http.MethodGet, "/incidents/INC-1", ""); s != http.StatusOK ||
		number(got["version"]) != 1 || len(got["history"].([]any)) != 1 {
		t.Fatalf("incident query after rejection: %d %v", s, got)
	}
	retry := `{"action_id":"big","operator":"bob","expected_version":1,"action":"add_note","content":"shortened"}`
	if s, got := postAction(t, server, "INC-1", retry); s != http.StatusOK || number(got["version"]) != 2 {
		t.Fatalf("rejected action id must succeed when shortened, got %d %v", s, got)
	}
	if s, got := doJSON(t, server, http.MethodGet, "/incidents/INC-1", ""); s != http.StatusOK ||
		number(got["version"]) != 2 || len(got["history"].([]any)) != 2 {
		t.Fatalf("retry must add exactly one history entry: %d %v", s, got)
	}
	if len(store.records) != 2 {
		t.Fatalf("creation plus one note record expected, got %d", len(store.records))
	}

	// Replaying the now-committed shortened note returns its first result.
	if s, got := postAction(t, server, "INC-1", retry); s != http.StatusOK || number(got["version"]) != 2 {
		t.Fatalf("identical retry must replay version 2, got %d %v", s, got)
	}
}

// TestPostNoteJustUnderCapacityIsSaved keeps the boundary honest from the
// other side: a large legal note whose record lands just under 64 MiB is
// accepted and saved whole. A timestamp-length margin keeps the check
// independent of the live server clock's RFC3339Nano spelling.
func TestPostNoteJustUnderCapacityIsSaved(t *testing.T) {
	server, _, _, _ := newIncidentServer(t)
	createIncident(t, server)
	at := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	probe := incidents.Request{
		IncidentID: "INC-1", ActionID: "near", Operator: "bob",
		ExpectedVersion: 1, Type: incidents.ActionNote, Content: "",
	}
	empty, _ := incidents.MarshalRecord(incidents.NewActionRecord(probe, at))
	wide := probe
	wide.Content = strings.Repeat("&", 1000)
	widePayload, _ := incidents.MarshalRecord(incidents.NewActionRecord(wide, at))
	width := (len(widePayload) - len(empty)) / 1000
	// Keep 4 KiB of slack so no plausible timestamp spelling flips the result.
	amps := (incidents.MaxActionPayloadBytes - len(empty) - 4096) / width
	body := oversizeNoteBody("near", amps, 1)
	if len(body) >= maxBodyBytes {
		t.Fatalf("test setup: body %d over the request limit", len(body))
	}
	status, out := postAction(t, server, "INC-1", body)
	if status != http.StatusOK || number(out["version"]) != 2 {
		t.Fatalf("just-under-capacity note must be saved, got %d %v", status, out)
	}
}

// TestPostOversizeNotePrecedenceAndReplay checks that the capacity judgment
// affects only a new legal note: stale versions and resolved incidents keep
// their original 409, unknown incidents stay 404, and a committed note's
// identical retry returns the first version even after resolution.
func TestPostOversizeNotePrecedenceAndReplay(t *testing.T) {
	server, _, _, reg := newIncidentServer(t)
	createIncident(t, server)
	amps, _ := bigNoteAmps(t)

	// Unknown incident: 404, not 400-capacity.
	if status, out := postAction(t, server, "ghost", oversizeNoteBody("big", amps, 1)); status != http.StatusNotFound {
		t.Fatalf("unknown incident must be 404, got %d %v", status, out)
	}
	// Stale version: 409 carrying current version.
	if status, out := postAction(t, server, "INC-1", oversizeNoteBody("big", amps, 7)); status != http.StatusConflict ||
		number(out["current_version"]) != 1 {
		t.Fatalf("stale version must be 409, got %d %v", status, out)
	}
	// Field error still wins.
	badFields := `{"action_id":"big","operator":"bob","expected_version":0,"action":"add_note","content":"` +
		strings.Repeat("&", amps) + `"}`
	if status, out := postAction(t, server, "INC-1", badFields); status != http.StatusBadRequest {
		t.Fatalf("field error must be 400, got %d %v", status, out)
	}

	// Commit a normal note, resolve, and confirm the resolved rule wins.
	normal := `{"action_id":"n1","operator":"bob","expected_version":1,"action":"add_note","content":"c"}`
	if _, out := postAction(t, server, "INC-1", normal); number(out["version"]) != 2 {
		t.Fatalf("seed note %v", out)
	}
	resolve := `{"action_id":"r1","operator":"bob","expected_version":2,"action":"resolve","reason":"fixed"}`
	if status, out := postAction(t, server, "INC-1", resolve); status != http.StatusOK || number(out["version"]) != 3 {
		t.Fatalf("resolve %d %v", status, out)
	}
	if status, _ := postAction(t, server, "INC-1", oversizeNoteBody("big", amps, 3)); status != http.StatusConflict {
		t.Fatalf("note on resolved incident must stay 409, got %d", status)
	}

	// The committed note's identical retry still returns version 2 even
	// though the incident is now resolved — replay never appends history.
	if status, out := postAction(t, server, "INC-1", normal); status != http.StatusOK || number(out["version"]) != 2 {
		t.Fatalf("committed-note retry after resolve must return version 2, got %d %v", status, out)
	}
	// Same action id with changed content is a conflict.
	changed := `{"action_id":"n1","operator":"bob","expected_version":3,"action":"add_note","content":"different"}`
	if status, _ := postAction(t, server, "INC-1", changed); status != http.StatusConflict {
		t.Fatalf("changed replay must be 409, got %d", status)
	}
	inc, _ := reg.Get("INC-1")
	if inc.Version != 3 || len(inc.History) != 3 {
		t.Fatalf("rejections and replays must not move the incident: %+v", inc)
	}
}

// TestPostOversizeNoteThenRealFailureStill503 keeps the failure modes
// distinct: a genuine write failure after an oversize rejection still poisons
// storage and reports 503 with restart guidance.
func TestPostOversizeNoteThenRealFailureStill503(t *testing.T) {
	server, store, _, _ := newIncidentServer(t)
	createIncident(t, server)
	amps, _ := bigNoteAmps(t)

	if status, out := postAction(t, server, "INC-1", oversizeNoteBody("big", amps, 1)); status != http.StatusBadRequest {
		t.Fatalf("oversize note must be 400, got %d %v", status, out)
	}

	store.failNext = true
	normal := `{"action_id":"ok","operator":"bob","expected_version":1,"action":"add_note","content":"ordinary"}`
	status, out := postAction(t, server, "INC-1", normal)
	if status != http.StatusServiceUnavailable {
		t.Fatalf("real write failure must be 503, got %d %v", status, out)
	}
	if msg, _ := out["error"].(string); !strings.Contains(msg, "restart") {
		t.Fatalf("503 must keep the restart guidance, got %q", msg)
	}
	if status, _ = doJSON(t, server, http.MethodGet, "/incidents/INC-1", ""); status != http.StatusServiceUnavailable {
		t.Fatalf("poisoned storage must keep returning 503, got %d", status)
	}
}

// TestPostOversizeNoteJSONShape checks the response decodes as a JSON object
// whose only required member is a non-empty error string.
func TestPostOversizeNoteJSONShape(t *testing.T) {
	server, _, _, _ := newIncidentServer(t)
	createIncident(t, server)
	amps, _ := bigNoteAmps(t)
	status, body := doJSON(t, server, http.MethodPost, "/incidents/INC-1/actions", oversizeNoteBody("big", amps, 1))
	if status != http.StatusBadRequest {
		t.Fatalf("want 400, got %d: %v", status, body)
	}
	msg, _ := body["error"].(string)
	if strings.TrimSpace(msg) == "" {
		t.Fatalf("error must be a non-empty string: %v", body)
	}
}
