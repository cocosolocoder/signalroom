package httpapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cocosolocoder/signalroom/internal/events"
	"github.com/cocosolocoder/signalroom/internal/incidents"
)

type fakeIncidentStorage struct {
	mu       sync.Mutex
	failNext bool
	poisoned bool
	records  []incidents.Record
}

func (f *fakeIncidentStorage) AppendRecord(record incidents.Record) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failNext {
		f.poisoned = true
		f.failNext = false
		return errBoom
	}
	f.records = append(f.records, record)
	return nil
}

func (f *fakeIncidentStorage) Poisoned() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.poisoned
}

var errBoom = errors.New("simulated incident write failure")

func newIncidentServer(t *testing.T) (*httptest.Server, *fakeIncidentStorage, *events.Timeline, *incidents.Registry) {
	t.Helper()
	tl := events.NewTimeline()
	eventStore := &fakeStorage{cursorKey: []byte("test-cursor-key-0123456789ab")}
	incStore := &fakeIncidentStorage{}
	reg := incidents.NewRegistry(tl, incStore.AppendRecord)
	server := httptest.NewServer(NewHandler(tl, eventStore, eventStore, WithIncidents(reg, incStore)))
	t.Cleanup(server.Close)
	return server, incStore, tl, reg
}

func parseEventTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		panic(err)
	}
	return t
}

// number reads a JSON number that decoded into float64.
func number(v any) int {
	f, _ := v.(float64)
	return int(f)
}

func ingestEvent(t *testing.T, tl *events.Timeline, id, service string) {
	t.Helper()
	err := tl.Load(events.Event{ID: id, Service: service, Severity: "critical", Message: "m", At: parseEventTime("2026-10-01T10:00:00Z")})
	if err != nil {
		t.Fatal(err)
	}
}

func doJSON(t *testing.T, server *httptest.Server, method, path, body string) (int, map[string]any) {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, server.URL+path, reader)
	if err != nil {
		t.Fatal(err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if len(bytes.TrimSpace(raw)) > 0 {
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatalf("decode %q: %v", raw, err)
		}
	}
	return resp.StatusCode, out
}

func createIncident(t *testing.T, server *httptest.Server) {
	t.Helper()
	status, body := doJSON(t, server, http.MethodPost, "/incidents",
		`{"id":"INC-1","title":" Outage ","service":"gateway","operator":"alice"}`)
	if status != http.StatusOK {
		t.Fatalf("create status %d: %v", status, body)
	}
	if body["id"] != "INC-1" || body["status"] != "open" || number(body["version"]) != 1 {
		t.Fatalf("create body %v", body)
	}
}

func postAction(t *testing.T, server *httptest.Server, id, body string) (int, map[string]any) {
	t.Helper()
	return doJSON(t, server, http.MethodPost, "/incidents/"+id+"/actions", body)
}

func TestHTTPCreateIncident(t *testing.T) {
	server, store, _, _ := newIncidentServer(t)
	createIncident(t, server)

	if len(store.records) != 1 {
		t.Fatalf("one durable record expected, got %d", len(store.records))
	}

	// Identical (trimmed) creation replays with the first result.
	status, body := doJSON(t, server, http.MethodPost, "/incidents",
		`{"id":"INC-1","title":"Outage","service":"gateway","operator":"alice"}`)
	if status != http.StatusOK || number(body["version"]) != 1 {
		t.Fatalf("replay %d %v", status, body)
	}
	if len(store.records) != 1 {
		t.Fatalf("replay must not persist again: %d", len(store.records))
	}

	// Different content conflicts.
	status, body = doJSON(t, server, http.MethodPost, "/incidents",
		`{"id":"INC-1","title":"Changed","service":"gateway","operator":"alice"}`)
	if status != http.StatusConflict || body["error"] == "" {
		t.Fatalf("conflict %d %v", status, body)
	}
}

func TestHTTPCreateIncidentValidation(t *testing.T) {
	server, _, _, _ := newIncidentServer(t)
	bad := []string{
		`{"id":" ","title":"t","service":"s","operator":"o"}`,
		`{"id":"i","title":"  ","service":"s","operator":"o"}`,
		`{"id":"i","title":"t","service":"","operator":"o"}`,
		`{"id":"i","title":"t","service":"s","operator":""}`,
		`{"id":"i","title":"t","service":"s","operator":9}`,
		`{"id":"i","title":"t","service":"s","operator":"o","extra":1}`,
		`{"id":"i","title":"t","service":"s"}`,
		`not json`,
		`["an array"]`,
	}
	for _, body := range bad {
		status, out := doJSON(t, server, http.MethodPost, "/incidents", body)
		if status != http.StatusBadRequest || out["error"] == "" {
			t.Fatalf("body %s: want 400 with error, got %d %v", body, status, out)
		}
	}
}

func TestHTTPActionLifecycleAndStatuses(t *testing.T) {
	server, store, tl, _ := newIncidentServer(t)
	ingestEvent(t, tl, "e1", "gateway")
	ingestEvent(t, tl, "e2", "checkout")
	createIncident(t, server)

	// Note.
	status, body := postAction(t, server, "INC-1",
		`{"action_id":"n1","operator":"bob","expected_version":1,"action":"add_note","content":" looking "}`)
	if status != http.StatusOK || number(body["version"]) != 2 {
		t.Fatalf("note %d %v", status, body)
	}

	// Link unknown event -> 404.
	status, body = postAction(t, server, "INC-1",
		`{"action_id":"l0","operator":"bob","expected_version":2,"action":"link_event","event_id":"missing"}`)
	if status != http.StatusNotFound || body["error"] == "" {
		t.Fatalf("missing event %d %v", status, body)
	}

	// Link other service -> 409.
	status, _ = postAction(t, server, "INC-1",
		`{"action_id":"l2","operator":"bob","expected_version":2,"action":"link_event","event_id":"e2"}`)
	if status != http.StatusConflict {
		t.Fatalf("wrong service status %d", status)
	}

	// Link good event.
	status, body = postAction(t, server, "INC-1",
		`{"action_id":"l1","operator":"bob","expected_version":2,"action":"link_event","event_id":"e1"}`)
	if status != http.StatusOK || number(body["version"]) != 3 {
		t.Fatalf("link %d %v", status, body)
	}

	// Link same event again -> 409.
	status, _ = postAction(t, server, "INC-1",
		`{"action_id":"l1b","operator":"bob","expected_version":3,"action":"link_event","event_id":"e1"}`)
	if status != http.StatusConflict {
		t.Fatalf("dup link status %d", status)
	}

	// Resolve without reason -> 400.
	status, _ = postAction(t, server, "INC-1",
		`{"action_id":"r0","operator":"bob","expected_version":3,"action":"resolve"}`)
	if status != http.StatusBadRequest {
		t.Fatalf("resolve missing reason status %d", status)
	}

	// Resolve.
	status, body = postAction(t, server, "INC-1",
		`{"action_id":"r1","operator":"bob","expected_version":3,"action":"resolve","reason":"fixed"}`)
	if status != http.StatusOK || number(body["version"]) != 4 {
		t.Fatalf("resolve %d %v", status, body)
	}

	// While resolved, note/link/resolve all 409.
	for _, b := range []string{
		`{"action_id":"x1","operator":"b","expected_version":4,"action":"add_note","content":"c"}`,
		`{"action_id":"x2","operator":"b","expected_version":4,"action":"link_event","event_id":"e1"}`,
		`{"action_id":"x3","operator":"b","expected_version":4,"action":"resolve","reason":"again"}`,
	} {
		if status, _ := postAction(t, server, "INC-1", b); status != http.StatusConflict {
			t.Fatalf("resolved action %s: want 409 got %d", b, status)
		}
	}

	// Reopen.
	status, body = postAction(t, server, "INC-1",
		`{"action_id":"o1","operator":"bob","expected_version":4,"action":"reopen","reason":"regression"}`)
	if status != http.StatusOK || number(body["version"]) != 5 {
		t.Fatalf("reopen %d %v", status, body)
	}

	if len(store.records) != 5 { // create, note, link, resolve, reopen
		t.Fatalf("durable records %d", len(store.records))
	}
}

func TestHTTPActionOnMissingIncident(t *testing.T) {
	server, _, _, _ := newIncidentServer(t)
	status, body := postAction(t, server, "ghost",
		`{"action_id":"a","operator":"b","expected_version":1,"action":"add_note","content":"c"}`)
	if status != http.StatusNotFound || body["error"] == "" {
		t.Fatalf("missing incident %d %v", status, body)
	}
	status, body = doJSON(t, server, http.MethodGet, "/incidents/ghost", "")
	if status != http.StatusNotFound {
		t.Fatalf("get missing %d", status)
	}
}

func TestHTTPActionBadRequest(t *testing.T) {
	server, _, _, _ := newIncidentServer(t)
	createIncident(t, server)
	bad := map[string]string{
		"missing action id":   `{"operator":"b","expected_version":1,"action":"add_note","content":"c"}`,
		"missing operator":    `{"action_id":"a","expected_version":1,"action":"add_note","content":"c"}`,
		"missing version":     `{"action_id":"a","operator":"b","action":"add_note","content":"c"}`,
		"version zero":        `{"action_id":"a","operator":"b","expected_version":0,"action":"add_note","content":"c"}`,
		"version fraction":    `{"action_id":"a","operator":"b","expected_version":1.5,"action":"add_note","content":"c"}`,
		"version string":      `{"action_id":"a","operator":"b","expected_version":"1","action":"add_note","content":"c"}`,
		"unknown action":      `{"action_id":"a","operator":"b","expected_version":1,"action":"frobnicate","content":"c"}`,
		"empty content":       `{"action_id":"a","operator":"b","expected_version":1,"action":"add_note","content":"  "}`,
		"unknown field":       `{"action_id":"a","operator":"b","expected_version":1,"action":"add_note","content":"c","bogus":1}`,
		"wrong payload field": `{"action_id":"a","operator":"b","expected_version":1,"action":"add_note","event_id":"e1"}`,
		"both payload fields": `{"action_id":"a","operator":"b","expected_version":1,"action":"resolve","reason":"r","content":"c"}`,
		"not json":            `{`,
	}
	for name, body := range bad {
		status, out := postAction(t, server, "INC-1", body)
		if status != http.StatusBadRequest || out["error"] == "" {
			t.Fatalf("%s: want 400, got %d %v", name, status, out)
		}
	}
}

func TestHTTPVersionConflictReportsCurrent(t *testing.T) {
	server, _, _, _ := newIncidentServer(t)
	createIncident(t, server)
	_, _ = postAction(t, server, "INC-1",
		`{"action_id":"n1","operator":"b","expected_version":1,"action":"add_note","content":"c"}`)

	status, body := postAction(t, server, "INC-1",
		`{"action_id":"n2","operator":"b","expected_version":1,"action":"add_note","content":"c"}`)
	if status != http.StatusConflict {
		t.Fatalf("status %d", status)
	}
	if number(body["current_version"]) != 2 {
		t.Fatalf("current_version %v", body["current_version"])
	}
}

func TestHTTPIdempotentReplayAcrossVersions(t *testing.T) {
	server, _, _, _ := newIncidentServer(t)
	createIncident(t, server)
	first := `{"action_id":"n1","operator":"bob","expected_version":1,"action":"add_note","content":"c"}`
	_, body := postAction(t, server, "INC-1", first)
	firstVersion := number(body["version"])

	// Advance with another action.
	_, _ = postAction(t, server, "INC-1",
		`{"action_id":"r1","operator":"bob","expected_version":2,"action":"resolve","reason":"fixed"}`)

	// Replaying the old request returns its first result despite stale version.
	status, replay := postAction(t, server, "INC-1", first)
	if status != http.StatusOK || number(replay["version"]) != firstVersion {
		t.Fatalf("replay %d %v (first version %d)", status, replay, firstVersion)
	}

	// Same action id with different content is a conflict.
	status, _ = postAction(t, server, "INC-1",
		`{"action_id":"n1","operator":"bob","expected_version":3,"action":"add_note","content":"changed"}`)
	if status != http.StatusConflict {
		t.Fatalf("changed replay status %d", status)
	}
}

func TestHTTPGetIncidentSnapshot(t *testing.T) {
	server, _, tl, _ := newIncidentServer(t)
	ingestEvent(t, tl, "e1", "gateway")
	createIncident(t, server)
	_, _ = postAction(t, server, "INC-1",
		`{"action_id":"l1","operator":"bob","expected_version":1,"action":"link_event","event_id":"e1"}`)
	_, _ = postAction(t, server, "INC-1",
		`{"action_id":"r1","operator":"bob","expected_version":2,"action":"resolve","reason":"fixed"}`)

	status, body := doJSON(t, server, http.MethodGet, "/incidents/INC-1", "")
	if status != http.StatusOK {
		t.Fatalf("get status %d", status)
	}
	if body["title"] != "Outage" || body["service"] != "gateway" || body["status"] != "resolved" || number(body["version"]) != 3 {
		t.Fatalf("incident head %v", body)
	}
	linked, ok := body["events"].([]any)
	if !ok || len(linked) != 1 {
		t.Fatalf("events %v", body["events"])
	}
	first := linked[0].(map[string]any)
	if first["id"] != "e1" || first["service"] != "gateway" || first["message"] != "m" {
		t.Fatalf("linked event content %v", first)
	}
	history, ok := body["history"].([]any)
	if !ok || len(history) != 3 {
		t.Fatalf("history %v", body["history"])
	}
	if history[0].(map[string]any)["action"] != "create" {
		t.Fatalf("first history entry %v", history[0])
	}
	if _, present := history[0].(map[string]any)["action_id"]; present {
		t.Fatalf("creation entry must omit action_id: %v", history[0])
	}
	last := history[2].(map[string]any)
	if last["action"] != "resolve" || number(last["version"]) != 3 || last["at"] == "" {
		t.Fatalf("last history entry %v", last)
	}
	if _, present := last["reason"]; present {
		t.Fatalf("history payload must be under content, not reason: %v", last)
	}
}

func TestHTTPIncidentDoesNotModifyEvents(t *testing.T) {
	server, _, tl, _ := newIncidentServer(t)
	ingestEvent(t, tl, "e1", "gateway")
	createIncident(t, server)
	_, _ = postAction(t, server, "INC-1",
		`{"action_id":"l1","operator":"bob","expected_version":1,"action":"link_event","event_id":"e1"}`)

	resp, err := http.Get(server.URL + "/events")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var got []map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0]["id"] != "e1" {
		t.Fatalf("event timeline must stay unchanged: %v", got)
	}
}

func TestHTTPIncidentStorageFailurePoisoned(t *testing.T) {
	server, store, _, _ := newIncidentServer(t)
	store.failNext = true
	status, body := doJSON(t, server, http.MethodPost, "/incidents",
		`{"id":"i","title":"t","service":"s","operator":"o"}`)
	if status != http.StatusServiceUnavailable || body["error"] == "" {
		t.Fatalf("failed write %d %v", status, body)
	}
	// Every later incident request stays 503 until restart.
	for _, try := range []func() (int, map[string]any){
		func() (int, map[string]any) {
			return doJSON(t, server, http.MethodPost, "/incidents", `{"id":"j","title":"t","service":"s","operator":"o"}`)
		},
		func() (int, map[string]any) {
			return doJSON(t, server, http.MethodGet, "/incidents/i", "")
		},
	} {
		if code, _ := try(); code != http.StatusServiceUnavailable {
			t.Fatalf("poisoned store: want 503 got %d", code)
		}
	}
	// Event endpoints are unaffected by an incident write failure.
	resp, err := http.Get(server.URL + "/events")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("events endpoint must stay healthy, got %d", resp.StatusCode)
	}
}

func TestHTTPIncidentMethodNotAllowed(t *testing.T) {
	server, _, _, _ := newIncidentServer(t)
	for _, tc := range []struct {
		method string
		path   string
	}{
		{http.MethodDelete, "/incidents"},
		{http.MethodPost, "/incidents/INC-1"},
		{http.MethodDelete, "/incidents/INC-1"},
		{http.MethodGet, "/incidents/INC-1/actions"},
	} {
		req, _ := http.NewRequest(tc.method, server.URL+tc.path, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Fatalf("%s %s: want 405 got %d", tc.method, tc.path, resp.StatusCode)
		}
	}
}

func TestHTTPConcurrentDuplicateActionCommitsOnce(t *testing.T) {
	server, store, _, _ := newIncidentServer(t)
	createIncident(t, server)

	const n = 24
	var wg sync.WaitGroup
	start := make(chan struct{})
	statuses := make([]int, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			req, _ := http.NewRequest(http.MethodPost, server.URL+"/incidents/INC-1/actions",
				strings.NewReader(`{"action_id":"dup","operator":"bob","expected_version":1,"action":"add_note","content":"same"}`))
			req.Header.Set("Content-Type", "application/json")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				statuses[i] = -1
				return
			}
			resp.Body.Close()
			statuses[i] = resp.StatusCode
		}(i)
	}
	close(start)
	wg.Wait()
	for i, code := range statuses {
		if code != http.StatusOK {
			t.Fatalf("duplicate request %d: want 200 replay, got %d", i, code)
		}
	}
	// create + exactly one action record.
	if len(store.records) != 2 {
		t.Fatalf("duplicate concurrent actions must persist once, got %d records", len(store.records))
	}
}

func TestHTTPGetIncidentIncludesParticipantsAndOwner(t *testing.T) {
	server, _, _, _ := newIncidentServer(t)
	createIncident(t, server)

	status, body := doJSON(t, server, http.MethodGet, "/incidents/INC-1", "")
	if status != http.StatusOK {
		t.Fatalf("get status %d", status)
	}
	participants, ok := body["participants"].([]any)
	if !ok || len(participants) != 1 || participants[0] != "alice" {
		t.Fatalf("participants %v", body["participants"])
	}
	if body["owner"] != "alice" {
		t.Fatalf("owner %v", body["owner"])
	}
}

func TestHTTPParticipantActions(t *testing.T) {
	server, store, _, _ := newIncidentServer(t)
	createIncident(t, server)

	// Add participant.
	status, body := postAction(t, server, "INC-1",
		`{"action_id":"a1","operator":"bob","expected_version":1,"action":"add_participant","participant":" zoe "}`)
	if status != http.StatusOK || number(body["version"]) != 2 {
		t.Fatalf("add_participant %d %v", status, body)
	}

	// Add duplicate -> 409 with current version.
	status, body = postAction(t, server, "INC-1",
		`{"action_id":"a2","operator":"bob","expected_version":2,"action":"add_participant","participant":"zoe"}`)
	if status != http.StatusConflict || number(body["current_version"]) != 2 {
		t.Fatalf("dup add %d %v", status, body)
	}

	// Assign owner to zoe.
	status, body = postAction(t, server, "INC-1",
		`{"action_id":"o1","operator":"bob","expected_version":2,"action":"assign_owner","participant":"zoe"}`)
	if status != http.StatusOK || number(body["version"]) != 3 {
		t.Fatalf("assign_owner %d %v", status, body)
	}

	// Assign to non-participant -> 409.
	status, body = postAction(t, server, "INC-1",
		`{"action_id":"o2","operator":"bob","expected_version":3,"action":"assign_owner","participant":"ghost"}`)
	if status != http.StatusConflict || number(body["current_version"]) != 3 {
		t.Fatalf("assign non-participant %d %v", status, body)
	}

	// Remove current owner -> 409.
	status, body = postAction(t, server, "INC-1",
		`{"action_id":"r1","operator":"bob","expected_version":3,"action":"remove_participant","participant":"zoe"}`)
	if status != http.StatusConflict || number(body["current_version"]) != 3 {
		t.Fatalf("remove owner %d %v", status, body)
	}

	// Remove non-existent -> 409.
	status, body = postAction(t, server, "INC-1",
		`{"action_id":"r2","operator":"bob","expected_version":3,"action":"remove_participant","participant":"ghost"}`)
	if status != http.StatusConflict || number(body["current_version"]) != 3 {
		t.Fatalf("remove missing %d %v", status, body)
	}

	// Remove alice (not owner) -> ok.
	status, body = postAction(t, server, "INC-1",
		`{"action_id":"r3","operator":"bob","expected_version":3,"action":"remove_participant","participant":"alice"}`)
	if status != http.StatusOK || number(body["version"]) != 4 {
		t.Fatalf("remove alice %d %v", status, body)
	}

	// Check GET reflects state.
	status, got := doJSON(t, server, http.MethodGet, "/incidents/INC-1", "")
	if status != http.StatusOK {
		t.Fatalf("get %d", status)
	}
	participants, _ := got["participants"].([]any)
	if len(participants) != 1 || participants[0] != "zoe" {
		t.Fatalf("participants %v", got["participants"])
	}
	if got["owner"] != "zoe" {
		t.Fatalf("owner %v", got["owner"])
	}

	// History content is normalized.
	history, _ := got["history"].([]any)
	last := history[len(history)-1].(map[string]any)
	if last["content"] != "alice" || last["action"] != "remove_participant" {
		t.Fatalf("history entry %v", last)
	}

	// create + 3 successful participant actions.
	if len(store.records) != 4 {
		t.Fatalf("records %d", len(store.records))
	}
}

func TestHTTPParticipantActionBadRequest(t *testing.T) {
	server, _, _, _ := newIncidentServer(t)
	createIncident(t, server)

	bad := map[string]string{
		"missing participant":   `{"action_id":"a","operator":"b","expected_version":1,"action":"add_participant"}`,
		"wrong type":            `{"action_id":"a","operator":"b","expected_version":1,"action":"add_participant","participant":5}`,
		"empty participant":     `{"action_id":"a","operator":"b","expected_version":1,"action":"add_participant","participant":"  "}`,
		"unknown field":         `{"action_id":"a","operator":"b","expected_version":1,"action":"add_participant","participant":"zoe","bogus":1}`,
		"extra payload content": `{"action_id":"a","operator":"b","expected_version":1,"action":"add_participant","participant":"zoe","content":"c"}`,
		"extra payload event":   `{"action_id":"a","operator":"b","expected_version":1,"action":"add_participant","participant":"zoe","event_id":"e1"}`,
		"extra payload reason":  `{"action_id":"a","operator":"b","expected_version":1,"action":"add_participant","participant":"zoe","reason":"r"}`,
		"note with participant": `{"action_id":"a","operator":"b","expected_version":1,"action":"add_note","participant":"zoe"}`,
	}
	for name, body := range bad {
		status, out := postAction(t, server, "INC-1", body)
		if status != http.StatusBadRequest || out["error"] == "" {
			t.Fatalf("%s: want 400, got %d %v", name, status, out)
		}
	}
}

func TestHTTPParticipantActionsRejectedOnResolved(t *testing.T) {
	server, _, _, _ := newIncidentServer(t)
	createIncident(t, server)
	_, _ = postAction(t, server, "INC-1",
		`{"action_id":"r1","operator":"bob","expected_version":1,"action":"resolve","reason":"fixed"}`)

	for _, body := range []string{
		`{"action_id":"a1","operator":"b","expected_version":2,"action":"add_participant","participant":"zoe"}`,
		`{"action_id":"a2","operator":"b","expected_version":2,"action":"remove_participant","participant":"alice"}`,
		`{"action_id":"a3","operator":"b","expected_version":2,"action":"assign_owner","participant":"alice"}`,
	} {
		status, out := postAction(t, server, "INC-1", body)
		if status != http.StatusConflict || out["error"] == "" {
			t.Fatalf("resolved %s: want 409, got %d %v", body, status, out)
		}
	}
}

func TestHTTPParticipantActionIdempotentReplay(t *testing.T) {
	server, _, _, _ := newIncidentServer(t)
	createIncident(t, server)
	first := `{"action_id":"a1","operator":"bob","expected_version":1,"action":"add_participant","participant":"zoe"}`
	_, body := postAction(t, server, "INC-1", first)
	firstVersion := number(body["version"])

	// Advance with another action.
	_, _ = postAction(t, server, "INC-1",
		`{"action_id":"a2","operator":"bob","expected_version":2,"action":"add_participant","participant":"bob"}`)

	// Replay returns first result.
	status, replay := postAction(t, server, "INC-1", first)
	if status != http.StatusOK || number(replay["version"]) != firstVersion {
		t.Fatalf("replay %d %v", status, replay)
	}

	// Same action id with different content -> 409.
	status, _ = postAction(t, server, "INC-1",
		`{"action_id":"a1","operator":"bob","expected_version":3,"action":"add_participant","participant":"changed"}`)
	if status != http.StatusConflict {
		t.Fatalf("changed replay %d", status)
	}
}
