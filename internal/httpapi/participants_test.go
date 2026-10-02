package httpapi

import (
	"net/http"
	"testing"
)

func TestHTTPIncidentParticipantsSnapshot(t *testing.T) {
	server, _, _, _ := newIncidentServer(t)
	createIncident(t, server)

	status, body := doJSON(t, server, http.MethodGet, "/incidents/INC-1", "")
	if status != http.StatusOK {
		t.Fatalf("get %d", status)
	}
	participants, _ := body["participants"].([]any)
	if len(participants) != 1 || participants[0] != "alice" {
		t.Fatalf("initial participants %v", body["participants"])
	}
	if body["owner"] != "alice" {
		t.Fatalf("initial owner %v", body["owner"])
	}

	// Names are trimmed; the listing is sorted by name with case kept.
	for _, b := range []string{
		`{"action_id":"p1","operator":"alice","expected_version":1,"action":"add_participant","participant":" Carol "}`,
		`{"action_id":"p2","operator":"alice","expected_version":2,"action":"add_participant","participant":"bob"}`,
	} {
		if s, r := postAction(t, server, "INC-1", b); s != http.StatusOK {
			t.Fatalf("add %d %v", s, r)
		}
	}
	_, body = doJSON(t, server, http.MethodGet, "/incidents/INC-1", "")
	participants, _ = body["participants"].([]any)
	if len(participants) != 3 || participants[0] != "Carol" || participants[1] != "alice" || participants[2] != "bob" {
		t.Fatalf("sorted participants %v", participants)
	}

	// Hand ownership over, then remove a non-owner participant.
	if s, r := postAction(t, server, "INC-1",
		`{"action_id":"o1","operator":"alice","expected_version":3,"action":"assign_owner","participant":"bob"}`); s != http.StatusOK || number(r["version"]) != 4 {
		t.Fatalf("assign %d %v", s, r)
	}
	if s, r := postAction(t, server, "INC-1",
		`{"action_id":"p3","operator":"bob","expected_version":4,"action":"remove_participant","participant":"Carol"}`); s != http.StatusOK || number(r["version"]) != 5 {
		t.Fatalf("remove %d %v", s, r)
	}
	status, body = doJSON(t, server, http.MethodGet, "/incidents/INC-1", "")
	participants, _ = body["participants"].([]any)
	if len(participants) != 2 || participants[0] != "alice" || participants[1] != "bob" {
		t.Fatalf("membership after remove %v", participants)
	}
	if body["owner"] != "bob" {
		t.Fatalf("owner %v", body["owner"])
	}

	// The personnel changes appear in history with normalized content.
	history, _ := body["history"].([]any)
	last := history[len(history)-1].(map[string]any)
	if last["action"] != "remove_participant" || last["content"] != "Carol" || number(last["version"]) != 5 {
		t.Fatalf("last history entry %v", last)
	}
}

func TestHTTPParticipantActionConflicts(t *testing.T) {
	server, _, _, _ := newIncidentServer(t)
	createIncident(t, server)
	if s, _ := postAction(t, server, "INC-1",
		`{"action_id":"p1","operator":"alice","expected_version":1,"action":"add_participant","participant":"carol"}`); s != http.StatusOK {
		t.Fatalf("add carol %d", s)
	}

	conflicts := map[string]string{
		"duplicate add":        `{"action_id":"c1","operator":"alice","expected_version":2,"action":"add_participant","participant":"carol"}`,
		"remove missing":       `{"action_id":"c2","operator":"alice","expected_version":2,"action":"remove_participant","participant":"zoe"}`,
		"remove owner":         `{"action_id":"c3","operator":"alice","expected_version":2,"action":"remove_participant","participant":"alice"}`,
		"assign non-member":    `{"action_id":"c4","operator":"alice","expected_version":2,"action":"assign_owner","participant":"zoe"}`,
		"assign current owner": `{"action_id":"c5","operator":"alice","expected_version":2,"action":"assign_owner","participant":"alice"}`,
	}
	for name, body := range conflicts {
		s, out := postAction(t, server, "INC-1", body)
		if s != http.StatusConflict || number(out["current_version"]) != 2 {
			t.Fatalf("%s: want 409 at version 2, got %d %v", name, s, out)
		}
	}

	// Personnel actions on a resolved incident conflict; reopen keeps people.
	if s, _ := postAction(t, server, "INC-1",
		`{"action_id":"r1","operator":"alice","expected_version":2,"action":"resolve","reason":"done"}`); s != http.StatusOK {
		t.Fatalf("resolve %d", s)
	}
	s, out := postAction(t, server, "INC-1",
		`{"action_id":"c6","operator":"alice","expected_version":3,"action":"add_participant","participant":"zoe"}`)
	if s != http.StatusConflict || number(out["current_version"]) != 3 {
		t.Fatalf("add on resolved: %d %v", s, out)
	}
	if s, _ := postAction(t, server, "INC-1",
		`{"action_id":"o1","operator":"carol","expected_version":3,"action":"reopen","reason":"again"}`); s != http.StatusOK {
		t.Fatalf("reopen %d", s)
	}
	_, body := doJSON(t, server, http.MethodGet, "/incidents/INC-1", "")
	if body["owner"] != "alice" {
		t.Fatalf("owner after reopen %v", body["owner"])
	}
	participants, _ := body["participants"].([]any)
	if len(participants) != 2 {
		t.Fatalf("participants survive resolve/reopen: %v", participants)
	}
}

func TestHTTPParticipantActionBadRequests(t *testing.T) {
	server, _, _, _ := newIncidentServer(t)
	createIncident(t, server)
	bad := map[string]string{
		"missing participant":   `{"action_id":"a","operator":"b","expected_version":1,"action":"add_participant"}`,
		"null participant":      `{"action_id":"a","operator":"b","expected_version":1,"action":"add_participant","participant":null}`,
		"numeric participant":   `{"action_id":"a","operator":"b","expected_version":1,"action":"add_participant","participant":7}`,
		"blank participant":     `{"action_id":"a","operator":"b","expected_version":1,"action":"add_participant","participant":"  "}`,
		"remove wrong payload":  `{"action_id":"a","operator":"b","expected_version":1,"action":"remove_participant","content":"x"}`,
		"assign extra payload":  `{"action_id":"a","operator":"b","expected_version":1,"action":"assign_owner","participant":"x","reason":"y"}`,
		"note with participant": `{"action_id":"a","operator":"b","expected_version":1,"action":"add_note","participant":"x"}`,
		"unknown field":         `{"action_id":"a","operator":"b","expected_version":1,"action":"add_participant","participant":"x","bogus":1}`,
	}
	for name, body := range bad {
		s, out := postAction(t, server, "INC-1", body)
		if s != http.StatusBadRequest || out["error"] == "" {
			t.Fatalf("%s: want 400, got %d %v", name, s, out)
		}
	}

	// Unknown incident is 404 even for the new actions.
	if s, _ := postAction(t, server, "ghost",
		`{"action_id":"a","operator":"b","expected_version":1,"action":"add_participant","participant":"x"}`); s != http.StatusNotFound {
		t.Fatalf("unknown incident status %d", s)
	}
}

func TestHTTPParticipantActionReplay(t *testing.T) {
	server, _, _, _ := newIncidentServer(t)
	createIncident(t, server)
	first := `{"action_id":"p1","operator":"alice","expected_version":1,"action":"add_participant","participant":"carol"}`
	if s, r := postAction(t, server, "INC-1", first); s != http.StatusOK || number(r["version"]) != 2 {
		t.Fatalf("first add %d %v", s, r)
	}
	// Advance the incident and change the people.
	if s, _ := postAction(t, server, "INC-1",
		`{"action_id":"o1","operator":"alice","expected_version":2,"action":"assign_owner","participant":"carol"}`); s != http.StatusOK {
		t.Fatalf("assign %d", s)
	}
	if s, _ := postAction(t, server, "INC-1",
		`{"action_id":"p2","operator":"carol","expected_version":3,"action":"remove_participant","participant":"alice"}`); s != http.StatusOK {
		t.Fatalf("remove alice %d", s)
	}
	// Replaying the original (now stale, and targeting the removed owner
	// side) returns its first result and changes nothing.
	s, replay := postAction(t, server, "INC-1", first)
	if s != http.StatusOK || number(replay["version"]) != 2 {
		t.Fatalf("replay %d %v", s, replay)
	}
	_, body := doJSON(t, server, http.MethodGet, "/incidents/INC-1", "")
	if number(body["version"]) != 4 {
		t.Fatalf("version after replay %v", body["version"])
	}
	if body["owner"] != "carol" {
		t.Fatalf("owner after replay %v", body["owner"])
	}
	participants, _ := body["participants"].([]any)
	if len(participants) != 1 || participants[0] != "carol" {
		t.Fatalf("participants after replay %v", participants)
	}

	// Same action id with a different target conflicts.
	if s, _ := postAction(t, server, "INC-1",
		`{"action_id":"p1","operator":"alice","expected_version":4,"action":"add_participant","participant":"dave"}`); s != http.StatusConflict {
		t.Fatalf("changed replay status %d", s)
	}
}
