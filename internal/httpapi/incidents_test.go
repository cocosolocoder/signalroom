package httpapi

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func postIncident(t *testing.T, server *httptest.Server, body string) (int, map[string]any) {
	t.Helper()
	resp, err := http.Post(server.URL+"/incidents", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post incident: %v", err)
	}
	defer resp.Body.Close()
	return decodeIncidentBody(t, resp)
}

func postAction(t *testing.T, server *httptest.Server, id, body string) (int, map[string]any) {
	t.Helper()
	resp, err := http.Post(server.URL+"/incidents/"+id+"/actions", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post action: %v", err)
	}
	defer resp.Body.Close()
	return decodeIncidentBody(t, resp)
}

func getIncident(t *testing.T, server *httptest.Server, id string) (int, map[string]any) {
	t.Helper()
	resp, err := http.Get(server.URL + "/incidents/" + id)
	if err != nil {
		t.Fatalf("get incident: %v", err)
	}
	defer resp.Body.Close()
	return decodeIncidentBody(t, resp)
}

func decodeIncidentBody(t *testing.T, resp *http.Response) (int, map[string]any) {
	t.Helper()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	return resp.StatusCode, out
}

func TestIncidentCreateAndGet(t *testing.T) {
	server, _, _ := newTestServer(t)
	status, out := postIncident(t, server, `{"id":"INC-1","title":"  Error spike  ","service":"gateway","operator":"  alice  "}`)
	if status != http.StatusOK {
		t.Fatalf("create: %d %v", status, out)
	}
	if out["id"] != "INC-1" || out["status"] != "open" || out["version"].(float64) != 1 {
		t.Fatalf("create response: %v", out)
	}

	status, got := getIncident(t, server, "INC-1")
	if status != http.StatusOK {
		t.Fatalf("get: %d %v", status, got)
	}
	if got["title"] != "Error spike" || got["service"] != "gateway" || got["status"] != "open" ||
		got["version"].(float64) != 1 {
		t.Fatalf("get response: %v", got)
	}
	history, ok := got["history"].([]any)
	if !ok || len(history) != 1 {
		t.Fatalf("history: %v", got["history"])
	}
	entry := history[0].(map[string]any)
	if entry["action"] != "create" || entry["operator"] != "alice" || entry["version"].(float64) != 1 {
		t.Fatalf("create history entry: %v", entry)
	}
	if _, ok := entry["action_id"]; ok {
		t.Fatalf("create entry must omit action_id: %v", entry)
	}
}

func TestIncidentCreateReplayAndConflict(t *testing.T) {
	server, _, _ := newTestServer(t)
	postIncident(t, server, `{"id":"INC-1","title":"Title","service":"svc","operator":"alice"}`)

	// Replay with whitespace-padded but normalized-same content.
	status, out := postIncident(t, server, `{"id":"INC-1","title":"  Title  ","service":"  svc  ","operator":"  alice  "}`)
	if status != http.StatusOK {
		t.Fatalf("replay: %d %v", status, out)
	}
	if out["version"].(float64) != 1 {
		t.Fatalf("replay version: %v", out)
	}

	// Different title -> 409.
	status, out = postIncident(t, server, `{"id":"INC-1","title":"Different","service":"svc","operator":"alice"}`)
	if status != http.StatusConflict {
		t.Fatalf("conflict: %d %v", status, out)
	}
}

func TestIncidentCreateValidation(t *testing.T) {
	server, _, _ := newTestServer(t)
	for _, body := range []string{
		`{"id":"INC-1","title":"Title","service":"svc"}`,
		`{"id":"INC-1","title":"Title","service":"svc","operator":"alice","extra":1}`,
		`{"id":"INC-1","title":"Title","service":"svc","operator":123}`,
		`{"id":"INC-1","title":"  ","service":"svc","operator":"alice"}`,
	} {
		status, out := postIncident(t, server, body)
		if status != http.StatusBadRequest {
			t.Fatalf("body %s: expected 400, got %d %v", body, status, out)
		}
	}
}

func TestIncidentActions(t *testing.T) {
	server, _, _ := newTestServer(t)
	postIncident(t, server, `{"id":"INC-1","title":"Title","service":"svc","operator":"alice"}`)

	// Add note.
	status, out := postAction(t, server, "INC-1", `{"action":"add_note","action_id":"ACT-1","operator":"bob","expected_version":1,"content":"investigating"}`)
	if status != http.StatusOK {
		t.Fatalf("add note: %d %v", status, out)
	}
	if out["version"].(float64) != 2 || out["action_id"] != "ACT-1" {
		t.Fatalf("add note response: %v", out)
	}

	// Version mismatch -> 409 with current version.
	status, out = postAction(t, server, "INC-1", `{"action":"add_note","action_id":"ACT-2","operator":"bob","expected_version":1,"content":"more"}`)
	if status != http.StatusConflict {
		t.Fatalf("version mismatch: %d %v", status, out)
	}
	if out["current_version"].(float64) != 2 {
		t.Fatalf("current_version: %v", out)
	}

	// Resolve.
	status, out = postAction(t, server, "INC-1", `{"action":"resolve","action_id":"ACT-3","operator":"bob","expected_version":2,"reason":"fixed"}`)
	if status != http.StatusOK {
		t.Fatalf("resolve: %d %v", status, out)
	}
	if out["version"].(float64) != 3 {
		t.Fatalf("resolve version: %v", out)
	}

	// Resolved: add note not allowed -> 409.
	status, out = postAction(t, server, "INC-1", `{"action":"add_note","action_id":"ACT-4","operator":"bob","expected_version":3,"content":"note"}`)
	if status != http.StatusConflict {
		t.Fatalf("add note on resolved: %d %v", status, out)
	}

	// Reopen.
	status, out = postAction(t, server, "INC-1", `{"action":"reopen","action_id":"ACT-5","operator":"bob","expected_version":3,"reason":"regressed"}`)
	if status != http.StatusOK {
		t.Fatalf("reopen: %d %v", status, out)
	}
	if out["version"].(float64) != 4 {
		t.Fatalf("reopen version: %v", out)
	}

	// Check GET reflects everything.
	_, got := getIncident(t, server, "INC-1")
	if got["status"] != "open" || got["version"].(float64) != 4 {
		t.Fatalf("final state: %v", got)
	}
	history := got["history"].([]any)
	if len(history) != 4 {
		t.Fatalf("history length: %d", len(history))
	}
}

func TestIncidentActionIdempotentReplay(t *testing.T) {
	server, _, _ := newTestServer(t)
	postIncident(t, server, `{"id":"INC-1","title":"Title","service":"svc","operator":"alice"}`)
	postAction(t, server, "INC-1", `{"action":"add_note","action_id":"ACT-1","operator":"bob","expected_version":1,"content":"note"}`)
	postAction(t, server, "INC-1", `{"action":"resolve","action_id":"ACT-2","operator":"bob","expected_version":2,"reason":"fixed"}`)

	// Replay ACT-1: same normalized content, even though version is now 3.
	status, out := postAction(t, server, "INC-1", `{"action":"add_note","action_id":"ACT-1","operator":"bob","expected_version":1,"content":"  note  "}`)
	if status != http.StatusOK {
		t.Fatalf("replay: %d %v", status, out)
	}
	if out["version"].(float64) != 2 {
		t.Fatalf("replay must return first result version: %v", out)
	}

	// Different content -> 409.
	status, out = postAction(t, server, "INC-1", `{"action":"add_note","action_id":"ACT-1","operator":"bob","expected_version":1,"content":"different"}`)
	if status != http.StatusConflict {
		t.Fatalf("replay conflict: %d %v", status, out)
	}
}

func TestIncidentActionValidation(t *testing.T) {
	server, _, _ := newTestServer(t)
	postIncident(t, server, `{"id":"INC-1","title":"Title","service":"svc","operator":"alice"}`)
	for _, body := range []string{
		`{"action":"add_note","action_id":"ACT-1","operator":"bob","expected_version":1}`,
		`{"action":"add_note","action_id":"ACT-1","operator":"bob","expected_version":0,"content":"note"}`,
		`{"action":"add_note","action_id":"ACT-1","operator":"bob","expected_version":1,"content":"note","extra":1}`,
		`{"action":"bogus","action_id":"ACT-1","operator":"bob","expected_version":1,"content":"note"}`,
		`{"action":"resolve","action_id":"ACT-1","operator":"bob","expected_version":1}`,
		`{"action":"add_note","action_id":"ACT-1","operator":"bob","expected_version":"1","content":"note"}`,
	} {
		status, out := postAction(t, server, "INC-1", body)
		if status != http.StatusBadRequest {
			t.Fatalf("body %s: expected 400, got %d %v", body, status, out)
		}
	}
}

func TestIncidentNotFound(t *testing.T) {
	server, _, _ := newTestServer(t)
	status, out := getIncident(t, server, "MISSING")
	if status != http.StatusNotFound {
		t.Fatalf("get missing: %d %v", status, out)
	}
	status, out = postAction(t, server, "MISSING", `{"action":"add_note","action_id":"ACT-1","operator":"bob","expected_version":1,"content":"note"}`)
	if status != http.StatusNotFound {
		t.Fatalf("action missing: %d %v", status, out)
	}
}

func TestIncidentLinkEvent(t *testing.T) {
	server, _, _ := newTestServer(t)
	// Ingest an event first.
	post(t, server, `{"events":[{"id":"evt-1","service":"gateway","severity":"critical","message":"spike","at":"2026-10-01T09:00:00Z"}]}`)
	postIncident(t, server, `{"id":"INC-1","title":"Title","service":"gateway","operator":"alice"}`)

	// Link the event.
	status, out := postAction(t, server, "INC-1", `{"action":"link","action_id":"ACT-1","operator":"bob","expected_version":1,"event_id":"evt-1"}`)
	if status != http.StatusOK {
		t.Fatalf("link: %d %v", status, out)
	}
	if out["version"].(float64) != 2 {
		t.Fatalf("link version: %v", out)
	}

	// GET includes the full linked event.
	_, got := getIncident(t, server, "INC-1")
	links := got["links"].([]any)
	if len(links) != 1 {
		t.Fatalf("links: %v", got["links"])
	}
	event := links[0].(map[string]any)
	if event["id"] != "evt-1" || event["service"] != "gateway" || event["severity"] != "critical" {
		t.Fatalf("linked event: %v", event)
	}

	// Link again -> 409.
	status, out = postAction(t, server, "INC-1", `{"action":"link","action_id":"ACT-2","operator":"bob","expected_version":2,"event_id":"evt-1"}`)
	if status != http.StatusConflict {
		t.Fatalf("duplicate link: %d %v", status, out)
	}

	// Link missing event -> 404.
	status, out = postAction(t, server, "INC-1", `{"action":"link","action_id":"ACT-3","operator":"bob","expected_version":2,"event_id":"missing"}`)
	if status != http.StatusNotFound {
		t.Fatalf("missing event link: %d %v", status, out)
	}
}

func TestIncidentLinkWrongService(t *testing.T) {
	server, _, _ := newTestServer(t)
	post(t, server, `{"events":[{"id":"evt-1","service":"api","severity":"info","message":"ok","at":"2026-10-01T09:00:00Z"}]}`)
	postIncident(t, server, `{"id":"INC-1","title":"Title","service":"gateway","operator":"alice"}`)
	status, out := postAction(t, server, "INC-1", `{"action":"link","action_id":"ACT-1","operator":"bob","expected_version":1,"event_id":"evt-1"}`)
	if status != http.StatusConflict {
		t.Fatalf("wrong service link: %d %v", status, out)
	}
}

func TestIncidentRejectedActionDoesNotChangeState(t *testing.T) {
	server, _, _ := newTestServer(t)
	postIncident(t, server, `{"id":"INC-1","title":"Title","service":"svc","operator":"alice"}`)
	// Invalid action -> 400, incident unchanged.
	postAction(t, server, "INC-1", `{"action":"bogus","action_id":"ACT-1","operator":"bob","expected_version":1,"content":"note"}`)
	_, got := getIncident(t, server, "INC-1")
	if got["version"].(float64) != 1 || len(got["history"].([]any)) != 1 {
		t.Fatalf("rejected action changed state: %v", got)
	}
}
