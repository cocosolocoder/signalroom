package main

import (
	"net/http"
	"syscall"
	"testing"
)

func TestServeIncidentParticipantsPersistAcrossKill(t *testing.T) {
	dataDir := t.TempDir()
	p := startServer(t, dataDir)

	if status, created := postIncident(t, p, `{"id":"INC-9","title":"Outage","service":"gateway","operator":" Alice "}`); status != http.StatusOK || incNumber(created["version"]) != 1 {
		t.Fatalf("create %d %v", status, created)
	}

	addBody := `{"action_id":"p1","operator":"alice","expected_version":1,"action":"add_participant","participant":" Carol "}`
	if status, add := postIncidentAction(t, p, "INC-9", addBody); status != http.StatusOK || incNumber(add["version"]) != 2 {
		t.Fatalf("add participant %d %v", status, add)
	}
	assignBody := `{"action_id":"o1","operator":"alice","expected_version":2,"action":"assign_owner","participant":"Carol"}`
	if status, assign := postIncidentAction(t, p, "INC-9", assignBody); status != http.StatusOK || incNumber(assign["version"]) != 3 {
		t.Fatalf("assign owner %d %v", status, assign)
	}
	noteBody := `{"action_id":"n1","operator":"mallory","expected_version":3,"action":"add_note","content":"watching"}`
	if status, note := postIncidentAction(t, p, "INC-9", noteBody); status != http.StatusOK || incNumber(note["version"]) != 4 {
		t.Fatalf("outsider note %d %v", status, note)
	}

	// Hard kill; confirmed records must survive with their people.
	p.signal(t, syscall.SIGKILL)
	p.waitExit(t, -1)
	waitTCPPortClosed(t, p.addr)

	p2 := startServer(t, dataDir)
	status, got := getIncident(t, p2, "INC-9")
	if status != http.StatusOK {
		t.Fatalf("get after restart %d", status)
	}
	participants, _ := got["participants"].([]any)
	if len(participants) != 2 || participants[0] != "Alice" || participants[1] != "Carol" {
		t.Fatalf("participants recovered %v", got["participants"])
	}
	if got["owner"] != "Carol" {
		t.Fatalf("owner recovered %v", got["owner"])
	}
	if incNumber(got["version"]) != 4 {
		t.Fatalf("version recovered %v", got["version"])
	}

	// Idempotent retries after restart return the first results, normalized
	// whitespace and stale expected_version included, and change nothing.
	if s, r := postIncidentAction(t, p2, "INC-9", addBody); s != http.StatusOK || incNumber(r["version"]) != 2 {
		t.Fatalf("add retry %d %v", s, r)
	}
	if s, r := postIncidentAction(t, p2, "INC-9", assignBody); s != http.StatusOK || incNumber(r["version"]) != 3 {
		t.Fatalf("assign retry %d %v", s, r)
	}
	if _, still := getIncident(t, p2, "INC-9"); incNumber(still["version"]) != 4 {
		t.Fatalf("retries must not advance version: %v", still["version"])
	}

	// The recovered incident keeps accepting personnel actions.
	removeBody := `{"action_id":"p2","operator":"carol","expected_version":4,"action":"remove_participant","participant":"Alice"}`
	if s, r := postIncidentAction(t, p2, "INC-9", removeBody); s != http.StatusOK || incNumber(r["version"]) != 5 {
		t.Fatalf("remove after restart %d %v", s, r)
	}
	_, got = getIncident(t, p2, "INC-9")
	participants, _ = got["participants"].([]any)
	if len(participants) != 1 || participants[0] != "Carol" || got["owner"] != "Carol" {
		t.Fatalf("state after remove %v", got)
	}

	p2.signal(t, syscall.SIGTERM)
	p2.waitExit(t, 0)
}
