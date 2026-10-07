package httpapi

import (
	"net/http"
	"strconv"
	"testing"
)

// linkAction and unlinkAction build action bodies for an event id.
func linkAction(actionID string, version int, eventID string) string {
	return `{"action_id":"` + actionID + `","operator":"bob","expected_version":` +
		strconv.Itoa(version) + `,"action":"link_event","event_id":"` + eventID + `"}`
}

func unlinkAction(actionID string, version int, eventID string) string {
	return `{"action_id":"` + actionID + `","operator":"bob","expected_version":` +
		strconv.Itoa(version) + `,"action":"unlink_event","event_id":"` + eventID + `"}`
}

// TestHTTPUnlinkEventLifecycle drives link -> unlink -> relink over HTTP and
// checks the detail view at each step: events keep relative order, removing
// the last link yields an empty JSON array, history appends an unlink entry
// keyed by the target event id while the original link entry stays, and a
// relink lands at the tail.
func TestHTTPUnlinkEventLifecycle(t *testing.T) {
	server, store, tl, _ := newIncidentServer(t)
	ingestEvent(t, tl, "e1", "gateway")
	ingestEvent(t, tl, "e2", "gateway")
	createIncident(t, server)

	if status, body := postAction(t, server, "INC-1", linkAction("l1", 1, "e1")); status != http.StatusOK || number(body["version"]) != 2 {
		t.Fatalf("link e1 %d %v", status, body)
	}
	if status, body := postAction(t, server, "INC-1", linkAction("l2", 2, "e2")); status != http.StatusOK || number(body["version"]) != 3 {
		t.Fatalf("link e2 %d %v", status, body)
	}

	// Surrounding whitespace is trimmed; version advances to 4.
	status, body := postAction(t, server, "INC-1",
		`{"action_id":"u1","operator":"bob","expected_version":3,"action":"unlink_event","event_id":" e1 "}`)
	if status != http.StatusOK || body["incident_id"] != "INC-1" || body["action_id"] != "u1" || number(body["version"]) != 4 {
		t.Fatalf("unlink %d %v", status, body)
	}

	status, got := doJSON(t, server, http.MethodGet, "/incidents/INC-1", "")
	if status != http.StatusOK {
		t.Fatalf("get %d", status)
	}
	events, _ := got["events"].([]any)
	if len(events) != 1 || events[0].(map[string]any)["id"] != "e2" {
		t.Fatalf("only e2 survives, in place: %v", got["events"])
	}
	history, _ := got["history"].([]any)
	if len(history) != 4 {
		t.Fatalf("history %v", got["history"])
	}
	unlinkEntry := history[3].(map[string]any)
	if unlinkEntry["action"] != "unlink_event" || unlinkEntry["action_id"] != "u1" ||
		unlinkEntry["content"] != "e1" || number(unlinkEntry["version"]) != 4 {
		t.Fatalf("unlink history entry %v", unlinkEntry)
	}
	if history[1].(map[string]any)["action"] != "link_event" ||
		history[1].(map[string]any)["content"] != "e1" {
		t.Fatalf("original link entry must be retained: %v", history[1])
	}

	// Removing the last link yields an empty JSON array, not null.
	if status, body := postAction(t, server, "INC-1", unlinkAction("u2", 4, "e2")); status != http.StatusOK || number(body["version"]) != 5 {
		t.Fatalf("unlink e2 %d %v", status, body)
	}
	status, got = doJSON(t, server, http.MethodGet, "/incidents/INC-1", "")
	if status != http.StatusOK {
		t.Fatalf("get %d", status)
	}
	if events, _ := got["events"].([]any); len(events) != 0 || got["events"] == nil {
		t.Fatalf("last removal must serialize as an empty array: %v", got["events"])
	}

	// Re-link e1: it appears at the tail of the now-empty list.
	if status, body := postAction(t, server, "INC-1", linkAction("l3", 5, "e1")); status != http.StatusOK || number(body["version"]) != 6 {
		t.Fatalf("relink e1 %d %v", status, body)
	}
	status, got = doJSON(t, server, http.MethodGet, "/incidents/INC-1", "")
	events, _ = got["events"].([]any)
	if len(events) != 1 || events[0].(map[string]any)["id"] != "e1" {
		t.Fatalf("relink lands at the tail: %v", got["events"])
	}
	history, _ = got["history"].([]any)
	wantActions := []string{"create", "link_event", "link_event", "unlink_event", "unlink_event", "link_event"}
	if len(history) != len(wantActions) {
		t.Fatalf("history len %d", len(history))
	}
	for i, want := range wantActions {
		if history[i].(map[string]any)["action"] != want {
			t.Fatalf("history[%d]=%v want %s", i, history[i], want)
		}
	}
	if len(store.records) != 6 { // create, 3 links, 2 unlinks
		t.Fatalf("durable records %d", len(store.records))
	}
}

// TestHTTPUnlinkStatuses covers every rejection branch: 400 for malformed
// shapes, 404 for a missing incident, 409 for not-linked (including an id
// that is not even an event), for a resolved incident, and for a stale
// version with current_version. A rejected request never alters the detail.
func TestHTTPUnlinkStatuses(t *testing.T) {
	server, _, tl, _ := newIncidentServer(t)
	ingestEvent(t, tl, "e1", "gateway")
	ingestEvent(t, tl, "eX", "checkout")
	createIncident(t, server)
	_, _ = postAction(t, server, "INC-1", linkAction("l1", 1, "e1"))

	bad := map[string]string{
		"missing event_id":    `{"action_id":"u","operator":"b","expected_version":2,"action":"unlink_event"}`,
		"blank event_id":      `{"action_id":"u","operator":"b","expected_version":2,"action":"unlink_event","event_id":"  "}`,
		"wrong payload field": `{"action_id":"u","operator":"b","expected_version":2,"action":"unlink_event","content":"e1"}`,
		"two payload fields":  `{"action_id":"u","operator":"b","expected_version":2,"action":"unlink_event","event_id":"e1","reason":"r"}`,
		"unknown extra field": `{"action_id":"u","operator":"b","expected_version":2,"action":"unlink_event","event_id":"e1","bogus":1}`,
		"event_id wrong type": `{"action_id":"u","operator":"b","expected_version":2,"action":"unlink_event","event_id":7}`,
	}
	for name, body := range bad {
		if status, out := postAction(t, server, "INC-1", body); status != http.StatusBadRequest || out["error"] == "" {
			t.Fatalf("%s: want 400 with error, got %d %v", name, status, out)
		}
	}

	// Unknown incident -> 404, even though the action shape is valid.
	if status, out := postAction(t, server, "ghost", unlinkAction("u", 1, "e1")); status != http.StatusNotFound || out["error"] == "" {
		t.Fatalf("missing incident %d %v", status, out)
	}

	// Not currently linked: another service's event and a nonexistent event
	// are both plain 409 conflicts carrying the current version, never 404.
	for name, body := range map[string]string{
		"linked nowhere here, event exists": unlinkAction("u2", 2, "eX"),
		"event does not exist":              unlinkAction("u3", 2, "ghost"),
	} {
		status, out := postAction(t, server, "INC-1", body)
		if status != http.StatusConflict || out["error"] == "" || number(out["current_version"]) != 2 {
			t.Fatalf("%s: want 409 at v2, got %d %v", name, status, out)
		}
	}

	// Stale version wins and reports the current version.
	status, out := postAction(t, server, "INC-1", unlinkAction("u4", 99, "ghost"))
	if status != http.StatusConflict || number(out["current_version"]) != 2 {
		t.Fatalf("stale version %d %v", status, out)
	}

	// Case is preserved: "E1" is not the linked "e1".
	if status, out := postAction(t, server, "INC-1", unlinkAction("u5", 2, "E1")); status != http.StatusConflict {
		t.Fatalf("case-sensitive id %d %v", status, out)
	}

	// Resolved incidents refuse new unlinks.
	_, _ = postAction(t, server, "INC-1",
		`{"action_id":"r1","operator":"bob","expected_version":2,"action":"resolve","reason":"done"}`)
	if status, out := postAction(t, server, "INC-1", unlinkAction("u6", 3, "e1")); status != http.StatusConflict || number(out["current_version"]) != 3 {
		t.Fatalf("resolved unlink %d %v", status, out)
	}

	// Nothing rejected so far changed the links or history length.
	status, got := doJSON(t, server, http.MethodGet, "/incidents/INC-1", "")
	events, _ := got["events"].([]any)
	if len(events) != 1 || events[0].(map[string]any)["id"] != "e1" {
		t.Fatalf("rejected unlinks must keep e1 linked: %v", got["events"])
	}
	history, _ := got["history"].([]any)
	if len(history) != 3 || number(got["version"]) != 3 {
		t.Fatalf("rejected unlinks must add no history: %v", got["history"])
	}
}

// TestHTTPUnlinkIdempotentAcrossRelink pins retry semantics end to end: a
// repeated successful unlink keeps returning its first version and does not
// remove the event after it is re-linked, while a retry of the original link
// cannot put a removed event back either; the same action id with different
// content is a conflict.
func TestHTTPUnlinkIdempotentAcrossRelink(t *testing.T) {
	server, _, tl, _ := newIncidentServer(t)
	ingestEvent(t, tl, "e1", "gateway")
	ingestEvent(t, tl, "e2", "gateway")
	createIncident(t, server)

	firstLink := linkAction("l1", 1, "e1")
	if status, _ := postAction(t, server, "INC-1", firstLink); status != http.StatusOK {
		t.Fatalf("link %d", status)
	}
	firstUnlink := unlinkAction("u1", 2, "e1")
	if status, body := postAction(t, server, "INC-1", firstUnlink); status != http.StatusOK || number(body["version"]) != 3 {
		t.Fatalf("unlink %d %v", status, body)
	}
	// While e1 is absent, the original link retry reports v2 but must not
	// put the removed event back.
	if status, body := postAction(t, server, "INC-1", firstLink); status != http.StatusOK || number(body["version"]) != 2 {
		t.Fatalf("link retry while absent %d %v", status, body)
	}
	_, absent := doJSON(t, server, http.MethodGet, "/incidents/INC-1", "")
	if evs, _ := absent["events"].([]any); len(evs) != 0 || number(absent["version"]) != 3 {
		t.Fatalf("link retry must not restore the removed event: %v", absent["events"])
	}
	// Advance: link e2, then re-link e1 at the tail.
	_, _ = postAction(t, server, "INC-1", linkAction("l2", 3, "e2"))
	_, _ = postAction(t, server, "INC-1", linkAction("l3", 4, "e1"))

	// The unlink retry reports its first result (v3) and leaves e1 linked.
	status, body := postAction(t, server, "INC-1", firstUnlink)
	if status != http.StatusOK || number(body["version"]) != 3 {
		t.Fatalf("unlink retry %d %v", status, body)
	}
	_, got := doJSON(t, server, http.MethodGet, "/incidents/INC-1", "")
	events, _ := got["events"].([]any)
	if len(events) != 2 || events[0].(map[string]any)["id"] != "e2" || events[1].(map[string]any)["id"] != "e1" {
		t.Fatalf("unlink retry must not remove the relink: %v", got["events"])
	}
	if number(got["version"]) != 5 {
		t.Fatalf("unlink retry must not advance version: %v", got["version"])
	}

	// The original link retry returns its first result (v2) and adds nothing.
	status, body = postAction(t, server, "INC-1", firstLink)
	if status != http.StatusOK || number(body["version"]) != 2 {
		t.Fatalf("link retry %d %v", status, body)
	}

	// Same action id, different content -> conflict.
	if status, out := postAction(t, server, "INC-1", unlinkAction("u1", 5, "e2")); status != http.StatusConflict || number(out["current_version"]) != 5 {
		t.Fatalf("changed unlink content %d %v", status, out)
	}
}

// TestHTTPUnlinkScopedToIncident verifies that one incident's unlink leaves
// the event and the other incident's link alone.
func TestHTTPUnlinkScopedToIncident(t *testing.T) {
	server, _, tl, _ := newIncidentServer(t)
	ingestEvent(t, tl, "e1", "gateway")
	createIncident(t, server)
	if status, _ := doJSON(t, server, http.MethodPost, "/incidents",
		`{"id":"INC-2","title":"Other","service":"gateway","operator":"carol"}`); status != http.StatusOK {
		t.Fatalf("create INC-2 %d", status)
	}
	_, _ = postAction(t, server, "INC-1", linkAction("la", 1, "e1"))
	_, _ = postAction(t, server, "INC-2", linkAction("lb", 1, "e1"))

	if status, body := postAction(t, server, "INC-1", unlinkAction("ua", 2, "e1")); status != http.StatusOK || number(body["version"]) != 3 {
		t.Fatalf("unlink from INC-1 %d %v", status, body)
	}

	_, one := doJSON(t, server, http.MethodGet, "/incidents/INC-1", "")
	if evs, _ := one["events"].([]any); len(evs) != 0 {
		t.Fatalf("INC-1 events %v", one["events"])
	}
	_, two := doJSON(t, server, http.MethodGet, "/incidents/INC-2", "")
	evs, _ := two["events"].([]any)
	if len(evs) != 1 || evs[0].(map[string]any)["id"] != "e1" {
		t.Fatalf("INC-2 must keep its link: %v", two["events"])
	}
	// The event is still queryable.
	resp, err := http.Get(server.URL + "/events")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("event query after unlink %d", resp.StatusCode)
	}
}
