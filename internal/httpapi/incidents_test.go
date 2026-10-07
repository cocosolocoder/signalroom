package httpapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
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

func TestHTTPUnlinkEventFlow(t *testing.T) {
	server, store, tl, _ := newIncidentServer(t)
	ingestEvent(t, tl, "e1", "gateway")
	ingestEvent(t, tl, "e2", "gateway")
	ingestEvent(t, tl, "e3", "gateway")
	createIncident(t, server)

	// Link all three events.
	for i, id := range []string{"e1", "e2", "e3"} {
		body := fmtAction(id, i+1, "l"+id, "link_event", "event_id")
		status, res := postAction(t, server, "INC-1", body)
		if status != http.StatusOK || number(res["version"]) != i+2 {
			t.Fatalf("link %s %d %v", id, status, res)
		}
	}

	// Unlink the middle event; whitespace around the id is trimmed.
	status, res := postAction(t, server, "INC-1",
		`{"action_id":"u2","operator":"carol","expected_version":4,"action":"unlink_event","event_id":"  e2  "}`)
	if status != http.StatusOK || number(res["version"]) != 5 {
		t.Fatalf("unlink e2 %d %v", status, res)
	}
	if res["incident_id"] != "INC-1" || res["action_id"] != "u2" {
		t.Fatalf("unlink response %v", res)
	}

	status, got := doJSON(t, server, http.MethodGet, "/incidents/INC-1", "")
	if status != http.StatusOK {
		t.Fatalf("get %d", status)
	}
	linked, _ := got["events"].([]any)
	if len(linked) != 2 ||
		linked[0].(map[string]any)["id"] != "e1" ||
		linked[1].(map[string]any)["id"] != "e3" {
		t.Fatalf("remaining links must keep order: %v", got["events"])
	}
	history, _ := got["history"].([]any)
	if len(history) != 5 {
		t.Fatalf("history len %d", len(history))
	}
	unlinkEntry := history[4].(map[string]any)
	if unlinkEntry["action"] != "unlink_event" || unlinkEntry["content"] != "e2" ||
		unlinkEntry["operator"] != "carol" || number(unlinkEntry["version"]) != 5 {
		t.Fatalf("unlink history entry %v", unlinkEntry)
	}
	// The original link entry remains with its own version.
	oldLink := history[2].(map[string]any)
	if oldLink["action"] != "link_event" || oldLink["content"] != "e2" {
		t.Fatalf("original link entry must remain: %v", oldLink)
	}

	// Remove the final remaining events one by one; the last removal yields
	// an empty events array.
	status, _ = postAction(t, server, "INC-1",
		`{"action_id":"u1","operator":"carol","expected_version":5,"action":"unlink_event","event_id":"e1"}`)
	if status != http.StatusOK {
		t.Fatalf("unlink e1 status %d", status)
	}
	status, _ = postAction(t, server, "INC-1",
		`{"action_id":"u3","operator":"carol","expected_version":6,"action":"unlink_event","event_id":"e3"}`)
	if status != http.StatusOK {
		t.Fatalf("unlink e3 status %d", status)
	}
	status, got = doJSON(t, server, http.MethodGet, "/incidents/INC-1", "")
	if status != http.StatusOK {
		t.Fatalf("get %d", status)
	}
	if events, ok := got["events"].([]any); !ok || len(events) != 0 {
		t.Fatalf("events must be an empty array: %v", got["events"])
	}
	if number(got["version"]) != 7 {
		t.Fatalf("version %v", got["version"])
	}
	// One durable record per accepted action: create, 3 links, 3 unlinks.
	if len(store.records) != 7 {
		t.Fatalf("durable records %d", len(store.records))
	}
}

func TestHTTPUnlinkValidation(t *testing.T) {
	server, _, tl, _ := newIncidentServer(t)
	ingestEvent(t, tl, "e1", "gateway")
	createIncident(t, server)
	_, _ = postAction(t, server, "INC-1",
		`{"action_id":"l1","operator":"b","expected_version":1,"action":"link_event","event_id":"e1"}`)

	bad := map[string]string{
		"missing event id":    `{"action_id":"u0","operator":"b","expected_version":2,"action":"unlink_event"}`,
		"null event id":       `{"action_id":"u0","operator":"b","expected_version":2,"action":"unlink_event","event_id":null}`,
		"blank event id":      `{"action_id":"u0","operator":"b","expected_version":2,"action":"unlink_event","event_id":"  "}`,
		"wrong payload field": `{"action_id":"u0","operator":"b","expected_version":2,"action":"unlink_event","content":"e1"}`,
		"extra payload field": `{"action_id":"u0","operator":"b","expected_version":2,"action":"unlink_event","event_id":"e1","reason":"x"}`,
		"unknown field":       `{"action_id":"u0","operator":"b","expected_version":2,"action":"unlink_event","event_id":"e1","bogus":1}`,
	}
	for name, body := range bad {
		status, out := postAction(t, server, "INC-1", body)
		if status != http.StatusBadRequest || out["error"] == "" {
			t.Fatalf("%s: want 400 with error, got %d %v", name, status, out)
		}
	}
}

func TestHTTPUnlinkConflicts(t *testing.T) {
	server, _, tl, _ := newIncidentServer(t)
	ingestEvent(t, tl, "e1", "gateway")
	ingestEvent(t, tl, "e2", "gateway")
	createIncident(t, server)
	_, _ = postAction(t, server, "INC-1",
		`{"action_id":"l1","operator":"b","expected_version":1,"action":"link_event","event_id":"e1"}`)

	// Not linked: an existing-but-unlinked event and an id that was never
	// ingested are both plain 409s carrying the current version.
	for _, target := range []string{"e2", "never-ingested"} {
		status, out := postAction(t, server, "INC-1",
			`{"action_id":"u-x","operator":"b","expected_version":2,"action":"unlink_event","event_id":"`+target+`"}`)
		if status != http.StatusConflict {
			t.Fatalf("unlink %s: want 409, got %d", target, status)
		}
		if number(out["current_version"]) != 2 || out["error"] == "" {
			t.Fatalf("unlink %s response %v", target, out)
		}
	}

	// Stale version conflict also carries the current version.
	status, out := postAction(t, server, "INC-1",
		`{"action_id":"u-s","operator":"b","expected_version":1,"action":"unlink_event","event_id":"e1"}`)
	if status != http.StatusConflict || number(out["current_version"]) != 2 {
		t.Fatalf("stale unlink %d %v", status, out)
	}

	// Resolve; unlink is refused there too, even for a currently linked event.
	if status, _ := postAction(t, server, "INC-1",
		`{"action_id":"r1","operator":"b","expected_version":2,"action":"resolve","reason":"done"}`); status != http.StatusOK {
		t.Fatalf("resolve status %d", status)
	}
	status, out = postAction(t, server, "INC-1",
		`{"action_id":"u-r","operator":"b","expected_version":3,"action":"unlink_event","event_id":"e1"}`)
	if status != http.StatusConflict || number(out["current_version"]) != 3 {
		t.Fatalf("resolved unlink %d %v", status, out)
	}

	// Every refusal leaves the link list and history alone.
	status, got := doJSON(t, server, http.MethodGet, "/incidents/INC-1", "")
	if status != http.StatusOK {
		t.Fatalf("get %d", status)
	}
	linked, _ := got["events"].([]any)
	if len(linked) != 1 || linked[0].(map[string]any)["id"] != "e1" {
		t.Fatalf("refused unlinks must keep the link: %v", got["events"])
	}
	history, _ := got["history"].([]any)
	if len(history) != 3 { // create, link, resolve
		t.Fatalf("refused unlinks must not add history: %d", len(history))
	}
}

func TestHTTPUnlinkMissingIncident(t *testing.T) {
	server, _, _, _ := newIncidentServer(t)
	status, body := postAction(t, server, "ghost",
		`{"action_id":"u1","operator":"b","expected_version":1,"action":"unlink_event","event_id":"e1"}`)
	if status != http.StatusNotFound || body["error"] == "" {
		t.Fatalf("missing incident %d %v", status, body)
	}
}

// TestHTTPUnlinkReplayAndRelink covers link -> unlink -> relink retries
// end to end: identical replays return first results without advancing, the
// stale link replay cannot restore the removed event, the stale unlink
// replay cannot remove a re-linked event, and the fresh relink lands last.
func TestHTTPUnlinkReplayAndRelink(t *testing.T) {
	server, _, tl, _ := newIncidentServer(t)
	ingestEvent(t, tl, "e1", "gateway")
	ingestEvent(t, tl, "e9", "gateway")
	createIncident(t, server)

	linkBody := `{"action_id":"l1","operator":"bob","expected_version":1,"action":"link_event","event_id":"e1"}`
	unlinkBody := `{"action_id":"u1","operator":"bob","expected_version":2,"action":"unlink_event","event_id":"e1"}`
	if status, body := postAction(t, server, "INC-1", linkBody); status != http.StatusOK || number(body["version"]) != 2 {
		t.Fatalf("link %d %v", status, body)
	}
	if status, body := postAction(t, server, "INC-1", unlinkBody); status != http.StatusOK || number(body["version"]) != 3 {
		t.Fatalf("unlink %d %v", status, body)
	}

	// Both replays return their original results and change nothing.
	if status, body := postAction(t, server, "INC-1", linkBody); status != http.StatusOK || number(body["version"]) != 2 {
		t.Fatalf("link replay %d %v", status, body)
	}
	if status, body := postAction(t, server, "INC-1", unlinkBody); status != http.StatusOK || number(body["version"]) != 3 {
		t.Fatalf("unlink replay %d %v", status, body)
	}
	status, got := doJSON(t, server, http.MethodGet, "/incidents/INC-1", "")
	if status != http.StatusOK {
		t.Fatalf("get status %d", status)
	}
	if number(got["version"]) != 3 {
		t.Fatalf("replays must not advance: %v", got["version"])
	}
	if events, _ := got["events"].([]any); len(events) != 0 {
		t.Fatalf("link replay must not re-add the event: %v", got["events"])
	}

	// Fresh link of e9, then relink e1; e1 must appear last.
	if status, _ := postAction(t, server, "INC-1",
		`{"action_id":"l9","operator":"bob","expected_version":3,"action":"link_event","event_id":"e9"}`); status != http.StatusOK {
		t.Fatalf("link e9 status %d", status)
	}
	if status, _ := postAction(t, server, "INC-1",
		`{"action_id":"l2","operator":"bob","expected_version":4,"action":"link_event","event_id":"e1"}`); status != http.StatusOK {
		t.Fatalf("relink e1 status %d", status)
	}
	status, got = doJSON(t, server, http.MethodGet, "/incidents/INC-1", "")
	linked, _ := got["events"].([]any)
	if len(linked) != 2 || linked[0].(map[string]any)["id"] != "e9" || linked[1].(map[string]any)["id"] != "e1" {
		t.Fatalf("relinked event must be last: %v", got["events"])
	}

	// The old unlink replay still reports version 3 and must not remove the
	// relink.
	if status, body := postAction(t, server, "INC-1", unlinkBody); status != http.StatusOK || number(body["version"]) != 3 {
		t.Fatalf("unlink replay after relink %d %v", status, body)
	}
	status, got = doJSON(t, server, http.MethodGet, "/incidents/INC-1", "")
	linked, _ = got["events"].([]any)
	if number(got["version"]) != 5 || len(linked) != 2 || linked[1].(map[string]any)["id"] != "e1" {
		t.Fatalf("stale unlink replay must not remove the relink: %v", got)
	}

	// Same action id with different content is a conflict for both actions.
	if status, _ := postAction(t, server, "INC-1",
		`{"action_id":"u1","operator":"bob","expected_version":5,"action":"unlink_event","event_id":"e9"}`); status != http.StatusConflict {
		t.Fatalf("changed unlink content status %d", status)
	}
	if status, _ := postAction(t, server, "INC-1",
		`{"action_id":"l1","operator":"bob","expected_version":5,"action":"link_event","event_id":"e9"}`); status != http.StatusConflict {
		t.Fatalf("changed link content status %d", status)
	}

	// History documents link, unlink, link e9, relink e1 in that order.
	history, _ := got["history"].([]any)
	want := []struct{ action, content string }{
		{"create", "Outage"},
		{"link_event", "e1"},
		{"unlink_event", "e1"},
		{"link_event", "e9"},
		{"link_event", "e1"},
	}
	if len(history) != len(want) {
		t.Fatalf("history len %d", len(history))
	}
	for i, w := range want {
		entry := history[i].(map[string]any)
		if entry["action"] != w.action || entry["content"] != w.content {
			t.Fatalf("history[%d]=%v want %+v", i, entry, w)
		}
	}
}

// TestHTTPUnlinkIsolatesRelationship verifies the unlink touches only this
// incident/event pair: the event stays in the timeline and stays linked to a
// second incident, while status, participants, and owner are unchanged.
func TestHTTPUnlinkIsolatesRelationship(t *testing.T) {
	server, _, tl, _ := newIncidentServer(t)
	ingestEvent(t, tl, "e1", "gateway")
	createIncident(t, server)
	if status, body := doJSON(t, server, http.MethodPost, "/incidents",
		`{"id":"INC-2","title":"Second","service":"gateway","operator":"alice"}`); status != http.StatusOK {
		t.Fatalf("create INC-2 %d %v", status, body)
	}
	if status, _ := postAction(t, server, "INC-1",
		`{"action_id":"l1","operator":"bob","expected_version":1,"action":"link_event","event_id":"e1"}`); status != http.StatusOK {
		t.Fatalf("link to INC-1 status %d", status)
	}
	if status, _ := postAction(t, server, "INC-2",
		`{"action_id":"l1","operator":"bob","expected_version":1,"action":"link_event","event_id":"e1"}`); status != http.StatusOK {
		t.Fatalf("link to INC-2 status %d", status)
	}

	if status, _ := postAction(t, server, "INC-1",
		`{"action_id":"u1","operator":"bob","expected_version":2,"action":"unlink_event","event_id":"e1"}`); status != http.StatusOK {
		t.Fatalf("unlink status %d", status)
	}

	// INC-1: link gone, everything else untouched.
	_, got1 := doJSON(t, server, http.MethodGet, "/incidents/INC-1", "")
	if events, _ := got1["events"].([]any); len(events) != 0 {
		t.Fatalf("INC-1 events %v", got1["events"])
	}
	if got1["status"] != "open" || got1["owner"] != "alice" {
		t.Fatalf("INC-1 status/owner moved: %v", got1)
	}
	if people, _ := got1["participants"].([]any); len(people) != 1 || people[0] != "alice" {
		t.Fatalf("INC-1 participants moved: %v", got1["participants"])
	}

	// INC-2 still links the event.
	_, got2 := doJSON(t, server, http.MethodGet, "/incidents/INC-2", "")
	if events, _ := got2["events"].([]any); len(events) != 1 || events[0].(map[string]any)["id"] != "e1" {
		t.Fatalf("INC-2 must keep the link: %v", got2["events"])
	}

	// The raw event is still queryable with its original content.
	resp, err := http.Get(server.URL + "/events")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var eventsList []map[string]any
	if err := json.Unmarshal(raw, &eventsList); err != nil {
		t.Fatal(err)
	}
	if len(eventsList) != 1 || eventsList[0]["id"] != "e1" || eventsList[0]["service"] != "gateway" {
		t.Fatalf("event timeline changed: %v", eventsList)
	}
}

// fmtAction builds an action body with one named payload member.
func fmtAction(eventID string, expectedVersion int, actionID, action, payloadField string) string {
	return `{"action_id":"` + actionID + `","operator":"bob","expected_version":` +
		strconv.Itoa(expectedVersion) + `,"action":"` + action + `","` + payloadField + `":"` + eventID + `"}`
}
