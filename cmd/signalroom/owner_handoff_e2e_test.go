package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/cocosolocoder/signalroom/internal/incidents"
)

// ownerFrame encodes one complete, checksummed incidents.log frame exactly as
// a live server would have written it. Frames built this way decode and
// validate cleanly, so a startup rejection can only come from the handover
// rule replayed over the recovered state — never from a missing field, a
// version gap, a reused action id, or a torn tail.
func ownerFrame(t *testing.T, record incidents.Record) []byte {
	t.Helper()
	payload, err := incidents.MarshalRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	frame := make([]byte, 8+len(payload))
	binary.BigEndian.PutUint32(frame[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(payload))
	copy(frame[8:], payload)
	return frame
}

func appendOwnerFrames(t *testing.T, path string, records ...incidents.Record) []byte {
	t.Helper()
	existing, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	content := append([]byte{}, existing...)
	for _, record := range records {
		content = append(content, ownerFrame(t, record)...)
	}
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatal(err)
	}
	return content
}

// startExpectingFailure launches serve and requires a non-zero exit with no
// readiness announcement, surfacing the server output on failure.
func startExpectingFailure(t *testing.T, dataDir string) string {
	t.Helper()
	logs := &safeBuffer{}
	proc := exec.Command(binaryPath, "serve", "--addr", "127.0.0.1:0", "--data", dataDir)
	proc.Stdout = logs
	proc.Stderr = logs
	if err := proc.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- proc.Wait() }()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatalf("server started despite an illegal record; output:\n%s", logs.String())
		}
		if ee, ok := err.(*exec.ExitError); !ok || ee.ExitCode() == 0 {
			t.Fatalf("server must exit non-zero: %v; output:\n%s", err, logs.String())
		}
	case <-ctx.Done():
		proc.Process.Kill()
		t.Fatalf("server stayed up; output:\n%s", logs.String())
	}
	if strings.Contains(logs.String(), "listening on") {
		t.Fatalf("failed startup must not announce readiness:\n%s", logs.String())
	}
	return logs.String()
}

func TestServeOwnerHandoverCaseSensitiveAcrossRestart(t *testing.T) {
	dataDir := t.TempDir()
	p := startServer(t, dataDir)

	if status, created := postIncident(t, p, `{"id":"INC-H","title":"Outage","service":"gateway","operator":"alice"}`); status != http.StatusOK || incNumber(created["version"]) != 1 {
		t.Fatalf("create %d %v", status, created)
	}
	addBob := `{"action_id":"p1","operator":"alice","expected_version":1,"action":"add_participant","participant":"Bob"}`
	if status, res := postIncidentAction(t, p, "INC-H", addBob); status != http.StatusOK || incNumber(res["version"]) != 2 {
		t.Fatalf("add Bob %d %v", status, res)
	}
	// "bob" differs from "Bob" only in case: a different person who can
	// join independently.
	addbob := `{"action_id":"p2","operator":"alice","expected_version":2,"action":"add_participant","participant":"bob"}`
	if status, res := postIncidentAction(t, p, "INC-H", addbob); status != http.StatusOK || incNumber(res["version"]) != 3 {
		t.Fatalf("add bob %d %v", status, res)
	}
	assignBody := `{"action_id":"o1","operator":"alice","expected_version":3,"action":"assign_owner","participant":"Bob"}`
	if status, res := postIncidentAction(t, p, "INC-H", assignBody); status != http.StatusOK || incNumber(res["version"]) != 4 {
		t.Fatalf("assign Bob %d %v", status, res)
	}

	// The request entry rejects handovers the handover rule forbids: a
	// never-joined target and the current owner both 409 carrying the
	// current version, without changing anything.
	for name, body := range map[string]string{
		"non-member": `{"action_id":"x1","operator":"alice","expected_version":4,"action":"assign_owner","participant":"zoe"}`,
		"self":       `{"action_id":"x2","operator":"alice","expected_version":4,"action":"assign_owner","participant":"Bob"}`,
		// While only "Bob" is the owner and "bob" merely a participant, the
		// case-folded name is not a valid owner-equality excuse either.
	} {
		status, raw := postIncidentAction(t, p, "INC-H", body)
		if status != http.StatusConflict || incNumber(raw["current_version"]) != 4 {
			t.Fatalf("%s handover must be 409 at current version: %d %v", name, status, raw)
		}
	}

	p.signal(t, syscall.SIGKILL)
	p.waitExit(t, -1)
	waitTCPPortClosed(t, p.addr)

	p2 := startServer(t, dataDir)
	status, got := getIncident(t, p2, "INC-H")
	if status != http.StatusOK {
		t.Fatalf("get after restart %d", status)
	}
	if got["owner"] != "Bob" {
		t.Fatalf("owner must recover with exact case: %v", got["owner"])
	}
	participants, _ := got["participants"].([]any)
	if len(participants) != 3 || participants[0] != "Bob" || participants[1] != "alice" || participants[2] != "bob" {
		t.Fatalf("participants recover sorted, case kept, both cased members present: %v", participants)
	}
	if incNumber(got["version"]) != 4 || got["status"] != "open" {
		t.Fatalf("version/status recover: %v", got)
	}
	history, _ := got["history"].([]any)
	if len(history) != 4 {
		t.Fatalf("history len %d", len(history))
	}
	handover, _ := history[3].(map[string]any)
	if handover["action_id"] != "o1" || handover["operator"] != "alice" ||
		handover["action"] != "assign_owner" || handover["content"] != "Bob" ||
		incNumber(handover["version"]) != 4 {
		t.Fatalf("handover history entry must keep saved fields: %v", handover)
	}
	if at, _ := handover["at"].(string); at == "" {
		t.Fatalf("handover history entry must keep its saved time: %v", handover["at"])
	}

	// The no-op handover is still rejected after recovery.
	if status, raw := postIncidentAction(t, p2, "INC-H",
		`{"action_id":"x2","operator":"alice","expected_version":4,"action":"assign_owner","participant":"Bob"}`); status != http.StatusConflict {
		t.Fatalf("self handover after restart must stay rejected: %d %v", status, raw)
	}
	p2.signal(t, syscall.SIGTERM)
	p2.waitExit(t, 0)
}

func TestServeRejectsIllegalHandoverRecordOnRecovery(t *testing.T) {
	base := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	create := incidents.NewCreationRecord(incidents.Creation{
		ID: "INC-R", Title: "Outage", Service: "gateway", Operator: "alice",
	}, base)
	add := func(id string, ev int, name string, at time.Time) incidents.Record {
		return incidents.NewActionRecord(incidents.Request{
			IncidentID: "INC-R", ActionID: id, Operator: "alice",
			ExpectedVersion: ev, Type: incidents.ActionAddParticipant, Content: name,
		}, at)
	}
	assign := func(id string, ev int, target string, at time.Time) incidents.Record {
		return incidents.NewActionRecord(incidents.Request{
			IncidentID: "INC-R", ActionID: id, Operator: "alice",
			ExpectedVersion: ev, Type: incidents.ActionAssignOwner, Content: target,
		}, at)
	}
	note := func(id string, ev int, at time.Time) incidents.Record {
		return incidents.NewActionRecord(incidents.Request{
			IncidentID: "INC-R", ActionID: id, Operator: "mallory",
			ExpectedVersion: ev, Type: incidents.ActionNote, Content: "still watching",
		}, at)
	}

	cases := []struct {
		name       string
		prefix     []incidents.Record // genuine server-written records
		bad        incidents.Record
		suffix     []incidents.Record // further legal records after the bad one
		wantInLog  string
		wantTarget string
	}{
		{
			name:       "non-participant target at tail",
			prefix:     []incidents.Record{create, add("p1", 1, "Carol", base.Add(time.Second))},
			bad:        assign("bad-nonmember-tail", 2, "zoe", base.Add(2*time.Second)),
			wantInLog:  "non-participant",
			wantTarget: "zoe",
		},
		{
			name:       "non-participant target before later legal records",
			prefix:     []incidents.Record{create, add("p1", 1, "Carol", base.Add(time.Second))},
			bad:        assign("bad-nonmember-middle", 2, "zoe", base.Add(2*time.Second)),
			suffix:     []incidents.Record{note("n1", 3, base.Add(3*time.Second))},
			wantInLog:  "non-participant",
			wantTarget: "zoe",
		},
		{
			name:       "current owner target at tail",
			prefix:     []incidents.Record{create},
			bad:        assign("bad-self-tail", 1, "alice", base.Add(time.Second)),
			wantInLog:  "handed over to itself",
			wantTarget: "alice",
		},
		{
			name: "current owner target before later legal records",
			prefix: []incidents.Record{
				create,
				add("p1", 1, "Bob", base.Add(time.Second)),
				assign("o1", 2, "Bob", base.Add(2*time.Second)),
			},
			bad:        assign("bad-self-middle", 3, "Bob", base.Add(3*time.Second)),
			suffix:     []incidents.Record{note("n2", 4, base.Add(4*time.Second))},
			wantInLog:  "handed over to itself",
			wantTarget: "Bob",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dataDir := t.TempDir()

			// Write the genuine prefix by running a live server and
			// killing it after fsync, so the good records are real
			// confirmed frames rather than test-built payloads.
			first := startServer(t, dataDir)
			if status, _ := postIncident(t, first, `{"id":"INC-R","title":"Outage","service":"gateway","operator":"alice"}`); status != http.StatusOK {
				t.Fatalf("create %d", status)
			}
			first.signal(t, syscall.SIGKILL)
			first.waitExit(t, -1)
			waitTCPPortClosed(t, first.addr)

			// Sanity: the on-disk prefix contains exactly the genuine
			// records the test case's expected versions assume.
			if len(tc.prefix) < 1 {
				t.Fatal("prefix must include the creation")
			}
			_ = appendOwnerFrames(t, filepath.Join(dataDir, "incidents.log"), tc.prefix[1:]...)

			// The bad handover record is itself a complete legal frame:
			// unique action id, timestamp, expected_version in lockstep.
			craftedFrames := []incidents.Record{tc.bad}
			craftedFrames = append(craftedFrames, tc.suffix...)
			crafted := appendOwnerFrames(t, filepath.Join(dataDir, "incidents.log"), craftedFrames...)

			output := startExpectingFailure(t, dataDir)
			if !strings.Contains(output, tc.wantInLog) || !strings.Contains(output, fmt.Sprintf("%q", tc.wantTarget)) {
				t.Fatalf("error must explain why %q is an illegal handover (%s); output:\n%s",
					tc.wantTarget, tc.wantInLog, output)
			}

			// The complete illegal frame must not be treated as an
			// incomplete tail and trimmed, and nothing else may change:
			// no auto-joined target, no default owner, no partial replay.
			after, err := os.ReadFile(filepath.Join(dataDir, "incidents.log"))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(after, crafted) {
				t.Fatal("failed startup must leave incidents.log byte-for-byte unchanged")
			}

			// A second restart fails identically rather than limping up
			// after having "handled" the record once.
			again := startExpectingFailure(t, dataDir)
			if !strings.Contains(again, tc.wantInLog) {
				t.Fatalf("second restart must keep rejecting the record; output:\n%s", again)
			}
		})
	}
}
