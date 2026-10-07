package main

import (
	"net/http"
	"syscall"
	"testing"
)

// TestServeUnlinkEventRecoversAcrossKill exercises the full
// request -> durable record -> restart path for unlink_event, including the
// link -> unlink -> relink story. After each restart the current link list
// and the complete history (original link entries retained) must line up one
// for one with the successful actions, idempotent retries must keep
// returning their first result without re-removing a re-linked event, and
// the same event's link in another incident must survive untouched.
func TestServeUnlinkEventRecoversAcrossKill(t *testing.T) {
	dataDir := t.TempDir()
	p := startServer(t, dataDir)
	postEvents(t, p, `{"events":[`+
		`{"id":"e1","service":"gateway","severity":"critical","message":"one","at":"2026-10-01T10:00:00Z"},`+
		`{"id":"e2","service":"gateway","severity":"critical","message":"two","at":"2026-10-01T10:01:00Z"}]}`)

	if status, created := postIncident(t, p, `{"id":"INC-11","title":"Outage","service":"gateway","operator":"alice"}`); status != http.StatusOK || incNumber(created["version"]) != 1 {
		t.Fatalf("create %d %v", status, created)
	}
	if status, _ := postIncident(t, p, `{"id":"INC-12","title":"Other","service":"gateway","operator":"carol"}`); status != http.StatusOK {
		t.Fatalf("create INC-12 %d", status)
	}

	linkE1 := `{"action_id":"l1","operator":"bob","expected_version":1,"action":"link_event","event_id":"e1"}`
	linkE2 := `{"action_id":"l2","operator":"bob","expected_version":2,"action":"link_event","event_id":"e2"}`
	unlinkE1 := `{"action_id":"u1","operator":"bob","expected_version":3,"action":"unlink_event","event_id":"e1"}`
	if s, b := postIncidentAction(t, p, "INC-11", linkE1); s != http.StatusOK || incNumber(b["version"]) != 2 {
		t.Fatalf("link e1 %d %v", s, b)
	}
	if s, b := postIncidentAction(t, p, "INC-11", linkE2); s != http.StatusOK || incNumber(b["version"]) != 3 {
		t.Fatalf("link e2 %d %v", s, b)
	}
	if s, _ := postIncidentAction(t, p, "INC-12",
		`{"action_id":"lb","operator":"carol","expected_version":1,"action":"link_event","event_id":"e1"}`); s != http.StatusOK {
		t.Fatalf("link e1 to INC-12 %d", s)
	}
	if s, b := postIncidentAction(t, p, "INC-11", unlinkE1); s != http.StatusOK || incNumber(b["version"]) != 4 {
		t.Fatalf("unlink e1 %d %v", s, b)
	}

	// Pre-kill detail: e2 alone remains, history has create + 2 links + unlink.
	status, before := getIncident(t, p, "INC-11")
	if status != http.StatusOK {
		t.Fatalf("get before kill %d", status)
	}
	evs, _ := before["events"].([]any)
	if len(evs) != 1 || evs[0].(map[string]any)["id"] != "e2" {
		t.Fatalf("events before kill %v", before["events"])
	}
	hist, _ := before["history"].([]any)
	last := hist[3].(map[string]any)
	if last["action"] != "unlink_event" || last["action_id"] != "u1" ||
		last["content"] != "e1" || incNumber(last["version"]) != 4 || last["at"] == "" {
		t.Fatalf("unlink history entry before kill %v", last)
	}
	savedAt := last["at"].(string)
	if hist[1].(map[string]any)["action"] != "link_event" || hist[1].(map[string]any)["content"] != "e1" {
		t.Fatalf("original link entry retained before kill %v", hist[1])
	}

	p.signal(t, syscall.SIGKILL)
	p.waitExit(t, -1)
	waitTCPPortClosed(t, p.addr)

	p2 := startServer(t, dataDir)
	status, got := getIncident(t, p2, "INC-11")
	if status != http.StatusOK {
		t.Fatalf("get after restart %d", status)
	}
	if got["status"] != "open" || incNumber(got["version"]) != 4 {
		t.Fatalf("recovered head %v", got)
	}
	evs, _ = got["events"].([]any)
	if len(evs) != 1 || evs[0].(map[string]any)["id"] != "e2" {
		t.Fatalf("recovered events %v", got["events"])
	}
	history, _ := got["history"].([]any)
	wantActions := []struct {
		actionID any
		action   string
		content  string
		version  int
	}{
		{nil, "create", "Outage", 1},
		{"l1", "link_event", "e1", 2},
		{"l2", "link_event", "e2", 3},
		{"u1", "unlink_event", "e1", 4},
	}
	if len(history) != len(wantActions) {
		t.Fatalf("recovered history len %d", len(history))
	}
	for i, w := range wantActions {
		e := history[i].(map[string]any)
		if e["action_id"] != w.actionID || e["action"] != w.action ||
			e["content"] != w.content || incNumber(e["version"]) != w.version || e["at"] == "" {
			t.Fatalf("history[%d] %v want %+v", i, e, w)
		}
	}
	if history[3].(map[string]any)["at"] != savedAt {
		t.Fatalf("unlink timestamp changed across restart: %v vs %s", history[3], savedAt)
	}

	// The other incident's link to the same event is untouched.
	_, other := getIncident(t, p2, "INC-12")
	otherEvs, _ := other["events"].([]any)
	if len(otherEvs) != 1 || otherEvs[0].(map[string]any)["id"] != "e1" {
		t.Fatalf("INC-12 link must survive: %v", other["events"])
	}

	// Idempotent unlink retry after restart: first result (v4), no change.
	if s, b := postIncidentAction(t, p2, "INC-11", unlinkE1); s != http.StatusOK || incNumber(b["version"]) != 4 {
		t.Fatalf("unlink retry after restart %d %v", s, b)
	}
	// While e1 is absent, the original link retry reports its first result
	// (v2) but must not restore the removed link.
	if s, b := postIncidentAction(t, p2, "INC-11", linkE1); s != http.StatusOK || incNumber(b["version"]) != 2 {
		t.Fatalf("link retry while e1 absent %d %v", s, b)
	}
	if _, cur := getIncident(t, p2, "INC-11"); incNumber(cur["version"]) != 4 {
		t.Fatalf("link retry must not advance version: %v", cur["version"])
	}

	// Re-link e1 with a fresh action: it lands at the tail after e2.
	relinkE1 := `{"action_id":"l3","operator":"bob","expected_version":4,"action":"link_event","event_id":"e1"}`
	if s, b := postIncidentAction(t, p2, "INC-11", relinkE1); s != http.StatusOK || incNumber(b["version"]) != 5 {
		t.Fatalf("relink e1 %d %v", s, b)
	}
	_, relinked := getIncident(t, p2, "INC-11")
	evs, _ = relinked["events"].([]any)
	if len(evs) != 2 || evs[0].(map[string]any)["id"] != "e2" || evs[1].(map[string]any)["id"] != "e1" {
		t.Fatalf("relink must land at the tail %v", relinked["events"])
	}
	// Even after the relink, the old unlink retry still reports v4 and must
	// not remove e1 a second time.
	if s, b := postIncidentAction(t, p2, "INC-11", unlinkE1); s != http.StatusOK || incNumber(b["version"]) != 4 {
		t.Fatalf("unlink retry after relink %d %v", s, b)
	}
	// And the original link retry cannot alter anything either (returns v2).
	if s, b := postIncidentAction(t, p2, "INC-11", linkE1); s != http.StatusOK || incNumber(b["version"]) != 2 {
		t.Fatalf("original link retry %d %v", s, b)
	}
	_, still := getIncident(t, p2, "INC-11")
	evs, _ = still["events"].([]any)
	if incNumber(still["version"]) != 5 || len(evs) != 2 {
		t.Fatalf("retries must not change state %v", still["events"])
	}

	// A second restart keeps the complete link -> unlink -> relink story.
	p2.signal(t, syscall.SIGTERM)
	p2.waitExit(t, 0)

	p3 := startServer(t, dataDir)
	status, final := getIncident(t, p3, "INC-11")
	if status != http.StatusOK || incNumber(final["version"]) != 5 {
		t.Fatalf("final get %d %v", status, final)
	}
	evs, _ = final["events"].([]any)
	if len(evs) != 2 || evs[0].(map[string]any)["id"] != "e2" || evs[1].(map[string]any)["id"] != "e1" {
		t.Fatalf("final events %v", final["events"])
	}
	history, _ = final["history"].([]any)
	wantSeq := []string{"create", "link_event", "link_event", "unlink_event", "link_event"}
	if len(history) != len(wantSeq) {
		t.Fatalf("final history len %d", len(history))
	}
	for i, want := range wantSeq {
		if history[i].(map[string]any)["action"] != want {
			t.Fatalf("history[%d]=%v want %s", i, history[i], want)
		}
	}

	// A not-linked unlink is 409 with the current version on recovered state.
	if s, b := postIncidentAction(t, p3, "INC-11",
		`{"action_id":"u9","operator":"bob","expected_version":5,"action":"unlink_event","event_id":"ghost"}`); s != http.StatusConflict || incNumber(b["current_version"]) != 5 {
		t.Fatalf("unlink ghost after restart %d %v", s, b)
	}

	p3.signal(t, syscall.SIGTERM)
	p3.waitExit(t, 0)
}
