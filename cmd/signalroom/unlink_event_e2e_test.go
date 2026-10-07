package main

import (
	"net/http"
	"syscall"
	"testing"
)

// TestServeUnlinkPersistsAcrossKill drives a link -> unlink -> relink
// sequence through the real server and a hard kill: confirmed unlink results
// survive a restart with the current link list and history in agreement,
// retries after restart replay the first results without changing anything,
// and the recovered incident keeps accepting actions.
func TestServeUnlinkPersistsAcrossKill(t *testing.T) {
	dataDir := t.TempDir()
	p := startServer(t, dataDir)
	postEvents(t, p, `{"events":[`+
		`{"id":"e1","service":"gateway","severity":"critical","message":"spike","at":"2026-10-01T10:00:00Z"},`+
		`{"id":"e2","service":"gateway","severity":"warning","message":"lag","at":"2026-10-01T10:05:00Z"}`+
		`]}`)

	postIncident(t, p, `{"id":"INC-1","title":"Outage","service":"gateway","operator":"alice"}`)
	linkE1 := `{"action_id":"l1","operator":"bob","expected_version":1,"action":"link_event","event_id":"e1"}`
	if status, res := postIncidentAction(t, p, "INC-1", linkE1); status != http.StatusOK || incNumber(res["version"]) != 2 {
		t.Fatalf("link e1 %d %v", status, res)
	}
	linkE2 := `{"action_id":"l2","operator":"bob","expected_version":2,"action":"link_event","event_id":"e2"}`
	if status, res := postIncidentAction(t, p, "INC-1", linkE2); status != http.StatusOK || incNumber(res["version"]) != 3 {
		t.Fatalf("link e2 %d %v", status, res)
	}
	unlinkE1 := `{"action_id":"u1","operator":"carol","expected_version":3,"action":"unlink_event","event_id":" e1 "}`
	if status, res := postIncidentAction(t, p, "INC-1", unlinkE1); status != http.StatusOK || incNumber(res["version"]) != 4 {
		t.Fatalf("unlink e1 %d %v", status, res)
	}

	// Detail reflects the removal: only e2 remains, history keeps the link
	// entry and appends the unlink entry with the event id as content.
	status, got := getIncident(t, p, "INC-1")
	if status != http.StatusOK {
		t.Fatalf("get status %d", status)
	}
	linked, _ := got["events"].([]any)
	if len(linked) != 1 || linked[0].(map[string]any)["id"] != "e2" {
		t.Fatalf("events after unlink %v", got["events"])
	}
	history, _ := got["history"].([]any)
	if len(history) != 4 {
		t.Fatalf("history len %d", len(history))
	}
	for i, want := range []string{"create", "link_event", "link_event", "unlink_event"} {
		if history[i].(map[string]any)["action"] != want {
			t.Fatalf("history[%d]=%v want %s", i, history[i], want)
		}
	}
	unlinkEntry := history[3].(map[string]any)
	if unlinkEntry["content"] != "e1" || unlinkEntry["operator"] != "carol" || incNumber(unlinkEntry["version"]) != 4 {
		t.Fatalf("unlink history entry %v", unlinkEntry)
	}

	// Hard kill; every confirmed record was fsynced.
	p.signal(t, syscall.SIGKILL)
	p.waitExit(t, -1)
	waitTCPPortClosed(t, p.addr)

	p2 := startServer(t, dataDir)
	status, got = getIncident(t, p2, "INC-1")
	if status != http.StatusOK {
		t.Fatalf("get after restart %d", status)
	}
	if incNumber(got["version"]) != 4 || got["status"] != "open" {
		t.Fatalf("recovered head %v", got)
	}
	linked, _ = got["events"].([]any)
	if len(linked) != 1 || linked[0].(map[string]any)["id"] != "e2" {
		t.Fatalf("recovered events %v", got["events"])
	}
	history, _ = got["history"].([]any)
	if len(history) != 4 || history[3].(map[string]any)["action"] != "unlink_event" {
		t.Fatalf("recovered history %v", got["history"])
	}

	// Retries after restart return the first results and change nothing. In
	// particular the original link retry cannot re-add e1.
	if s, res := postIncidentAction(t, p2, "INC-1", linkE1); s != http.StatusOK || incNumber(res["version"]) != 2 {
		t.Fatalf("link retry after restart %d %v", s, res)
	}
	if s, res := postIncidentAction(t, p2, "INC-1", unlinkE1); s != http.StatusOK || incNumber(res["version"]) != 4 {
		t.Fatalf("unlink retry after restart %d %v", s, res)
	}
	if _, still := getIncident(t, p2, "INC-1"); incNumber(still["version"]) != 4 {
		t.Fatalf("retries must not advance version: %v", still["version"])
	}
	if _, still := getIncident(t, p2, "INC-1"); len(still["events"].([]any)) != 1 {
		t.Fatalf("link retry must not re-add the event: %v", still["events"])
	}

	// A fresh relink of e1 succeeds and lands at the tail, and history then
	// shows link -> unlink -> relink.
	relink := `{"action_id":"l3","operator":"bob","expected_version":4,"action":"link_event","event_id":"e1"}`
	if s, res := postIncidentAction(t, p2, "INC-1", relink); s != http.StatusOK || incNumber(res["version"]) != 5 {
		t.Fatalf("relink after restart %d %v", s, res)
	}
	_, got = getIncident(t, p2, "INC-1")
	linked, _ = got["events"].([]any)
	if len(linked) != 2 || linked[0].(map[string]any)["id"] != "e2" || linked[1].(map[string]any)["id"] != "e1" {
		t.Fatalf("relinked event must appear last: %v", got["events"])
	}
	history, _ = got["history"].([]any)
	if len(history) != 5 {
		t.Fatalf("history after relink len %d", len(history))
	}
	last := history[4].(map[string]any)
	if last["action"] != "link_event" || last["content"] != "e1" || incNumber(last["version"]) != 5 {
		t.Fatalf("relink history entry %v", last)
	}

	// The raw event is still queryable after the whole sequence.
	events := getEvents(t, p2, "")
	if len(events) != 2 {
		t.Fatalf("both events must remain queryable, got %v", events)
	}

	p2.signal(t, syscall.SIGTERM)
	p2.waitExit(t, 0)
}
