package main

import (
	"bytes"
	"context"
	"encoding/binary"
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

// TestServeOwnerHandoverRecoversAcrossKill exercises the full
// request -> durable record -> restart path for a legal handover. The
// recovered detail view must show the last owner, an unchanged membership
// sorted by name, versions and history matching the successful actions one
// for one, and the saved operator/target/timestamp on the handover entry.
// Names differing only in case stay distinct end to end.
func TestServeOwnerHandoverRecoversAcrossKill(t *testing.T) {
	dataDir := t.TempDir()
	p := startServer(t, dataDir)

	if status, created := postIncident(t, p, `{"id":"INC-7","title":"Outage","service":"gateway","operator":"Alice"}`); status != http.StatusOK || incNumber(created["version"]) != 1 {
		t.Fatalf("create %d %v", status, created)
	}
	addLower := `{"action_id":"p1","operator":"alice","expected_version":1,"action":"add_participant","participant":"bob"}`
	if status, add := postIncidentAction(t, p, "INC-7", addLower); status != http.StatusOK || incNumber(add["version"]) != 2 {
		t.Fatalf("add bob %d %v", status, add)
	}
	addUpper := `{"action_id":"p2","operator":"alice","expected_version":2,"action":"add_participant","participant":"Bob"}`
	if status, add := postIncidentAction(t, p, "INC-7", addUpper); status != http.StatusOK || incNumber(add["version"]) != 3 {
		t.Fatalf("add Bob %d %v", status, add)
	}
	assignUpper := `{"action_id":"o1","operator":"alice","expected_version":3,"action":"assign_owner","participant":"Bob"}`
	if status, assign := postIncidentAction(t, p, "INC-7", assignUpper); status != http.StatusOK || incNumber(assign["version"]) != 4 {
		t.Fatalf("assign Bob %d %v", status, assign)
	}

	status, before := getIncident(t, p, "INC-7")
	if status != http.StatusOK {
		t.Fatalf("get before restart %d", status)
	}
	members, _ := before["participants"].([]any)
	if len(members) != 3 || members[0] != "Alice" || members[1] != "Bob" || members[2] != "bob" {
		t.Fatalf("participants sorted, case kept: %v", before["participants"])
	}
	if before["owner"] != "Bob" || before["status"] != "open" || incNumber(before["version"]) != 4 {
		t.Fatalf("head before restart: %v", before)
	}
	histBefore, _ := before["history"].([]any)
	lastBefore := histBefore[3].(map[string]any)
	if lastBefore["action"] != "assign_owner" || lastBefore["action_id"] != "o1" ||
		lastBefore["operator"] != "alice" || lastBefore["content"] != "Bob" ||
		incNumber(lastBefore["version"]) != 4 || lastBefore["at"] == nil || lastBefore["at"] == "" {
		t.Fatalf("handover history entry before restart: %v", lastBefore)
	}
	savedAt := lastBefore["at"].(string)

	// Hard kill; confirmed records are the only source on the next start.
	p.signal(t, syscall.SIGKILL)
	p.waitExit(t, -1)
	waitTCPPortClosed(t, p.addr)

	p2 := startServer(t, dataDir)
	status, got := getIncident(t, p2, "INC-7")
	if status != http.StatusOK {
		t.Fatalf("get after restart %d", status)
	}
	if got["status"] != "open" || incNumber(got["version"]) != 4 {
		t.Fatalf("recovered status/version: %v", got)
	}
	if got["owner"] != "Bob" {
		t.Fatalf("recovered owner %v", got["owner"])
	}
	recovered, _ := got["participants"].([]any)
	if len(recovered) != 3 || recovered[0] != "Alice" || recovered[1] != "Bob" || recovered[2] != "bob" {
		t.Fatalf("handover must leave both case variants participating: %v", got["participants"])
	}

	history, _ := got["history"].([]any)
	if len(history) != 4 {
		t.Fatalf("history must line up 1:1 with the four successful actions, got %d", len(history))
	}
	wantActions := []struct {
		actionID any
		operator string
		action   string
		content  string
		version  int
	}{
		{nil, "Alice", "create", "Outage", 1},
		{"p1", "alice", "add_participant", "bob", 2},
		{"p2", "alice", "add_participant", "Bob", 3},
		{"o1", "alice", "assign_owner", "Bob", 4},
	}
	for i, w := range wantActions {
		entry := history[i].(map[string]any)
		if entry["action_id"] != w.actionID || entry["operator"] != w.operator ||
			entry["action"] != w.action || entry["content"] != w.content ||
			incNumber(entry["version"]) != w.version {
			t.Fatalf("history[%d] %v want %+v", i, entry, w)
		}
		if entry["at"] == nil || entry["at"] == "" {
			t.Fatalf("history[%d] missing saved timestamp", i)
		}
	}
	// The handover entry keeps the exact server timestamp captured before the
	// kill, not a recovery-time value.
	if history[3].(map[string]any)["at"] != savedAt {
		t.Fatalf("handover timestamp changed across restart: %v vs %s", history[3], savedAt)
	}

	// Idempotent retries after restart return first results; a handover retry
	// must not be counted as another successful action.
	if s, r := postIncidentAction(t, p2, "INC-7", addLower); s != http.StatusOK || incNumber(r["version"]) != 2 {
		t.Fatalf("add retry %d %v", s, r)
	}
	if s, r := postIncidentAction(t, p2, "INC-7", assignUpper); s != http.StatusOK || incNumber(r["version"]) != 4 {
		t.Fatalf("assign retry %d %v", s, r)
	}
	if _, still := getIncident(t, p2, "INC-7"); incNumber(still["version"]) != 4 {
		t.Fatalf("retries must not advance version: %v", still["version"])
	}

	p2.signal(t, syscall.SIGTERM)
	p2.waitExit(t, 0)
}

// TestServeFailsOnIllegalHandoverRecordWithoutMutation covers the regression
// where the request entry refuses a handover but recovery accepts the same
// kind of record. Each injected record is a complete, CRC-valid frame with
// legal fields, a timestamp, a sequential expected_version, and a fresh
// action_id, so the failure must come from the handover rule: it must abort
// startup (non-zero exit, no readiness line), name why the target is illegal,
// and leave every saved byte — including the complete offending frame — in
// place rather than trimming it as a torn tail or auto-joining/substituting.
func TestServeFailsOnIllegalHandoverRecordWithoutMutation(t *testing.T) {
	stamp := func(sec int) time.Time {
		return time.Date(2026, 10, 1, 12, 0, 0, sec, time.UTC)
	}
	frame := func(t *testing.T, record incidents.Record) []byte {
		t.Helper()
		payload, err := incidents.MarshalRecord(record)
		if err != nil {
			t.Fatal(err)
		}
		out := make([]byte, 8+len(payload))
		binary.BigEndian.PutUint32(out[0:4], uint32(len(payload)))
		binary.BigEndian.PutUint32(out[4:8], crc32.ChecksumIEEE(payload))
		copy(out[8:], payload)
		return out
	}
	appendFrames := func(t *testing.T, path string, records ...incidents.Record) []byte {
		t.Helper()
		existing, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		content := append([]byte{}, existing...)
		for _, record := range records {
			content = append(content, frame(t, record)...)
		}
		if err := os.WriteFile(path, content, 0o644); err != nil {
			t.Fatal(err)
		}
		return content
	}

	t.Run("handover to a never-joined participant at the tail", func(t *testing.T) {
		dataDir := t.TempDir()
		first := startServer(t, dataDir)
		postIncident(t, first, `{"id":"INC-8","title":"t","service":"gateway","operator":"Alice"}`)
		if s, _ := postIncidentAction(t, first, "INC-8",
			`{"action_id":"p1","operator":"alice","expected_version":1,"action":"add_participant","participant":"bob"}`); s != http.StatusOK {
			t.Fatalf("add bob status %d", s)
		}
		first.signal(t, syscall.SIGKILL)
		first.waitExit(t, -1)
		waitTCPPortClosed(t, first.addr)

		// Complete, well-formed record: fresh action id, sequential version,
		// legal in every respect except that carol never joined.
		bad := incidents.NewActionRecord(incidents.Request{
			IncidentID: "INC-8", ActionID: "o1", Operator: "alice",
			ExpectedVersion: 2, Type: incidents.ActionAssignOwner, Content: "carol",
		}, stamp(1))
		path := filepath.Join(dataDir, "incidents.log")
		wantBytes := appendFrames(t, path, bad)

		expectServeFailsOnIncidentLog(t, dataDir, wantBytes, path,
			"recover incidents", "non-participant", `"carol"`)

		// A later start still fails the same way: the complete bad frame was
		// neither discarded as a torn tail nor replaced by a default owner.
		expectServeFailsOnIncidentLog(t, dataDir, wantBytes, path,
			"recover incidents", "non-participant", `"carol"`)
	})

	t.Run("self-handover ahead of another legal record", func(t *testing.T) {
		dataDir := t.TempDir()
		first := startServer(t, dataDir)
		postIncident(t, first, `{"id":"INC-8","title":"t","service":"gateway","operator":"Alice"}`)
		if s, _ := postIncidentAction(t, first, "INC-8",
			`{"action_id":"p1","operator":"alice","expected_version":1,"action":"add_participant","participant":"bob"}`); s != http.StatusOK {
			t.Fatalf("add bob status %d", s)
		}
		if s, _ := postIncidentAction(t, first, "INC-8",
			`{"action_id":"o0","operator":"alice","expected_version":2,"action":"assign_owner","participant":"bob"}`); s != http.StatusOK {
			t.Fatalf("assign bob status %d", s)
		}
		first.signal(t, syscall.SIGKILL)
		first.waitExit(t, -1)
		waitTCPPortClosed(t, first.addr)

		path := filepath.Join(dataDir, "incidents.log")
		// Both frames are complete and sequentially numbered as if the
		// no-op handover had committed; recovery must abort at the handover
		// rather than skip it and apply the following note.
		bad := incidents.NewActionRecord(incidents.Request{
			IncidentID: "INC-8", ActionID: "o1", Operator: "alice",
			ExpectedVersion: 3, Type: incidents.ActionAssignOwner, Content: "bob",
		}, stamp(2))
		follow := incidents.NewActionRecord(incidents.Request{
			IncidentID: "INC-8", ActionID: "n1", Operator: "alice",
			ExpectedVersion: 4, Type: incidents.ActionNote, Content: "after the bad handover",
		}, stamp(3))
		wantBytes := appendFrames(t, path, bad, follow)

		expectServeFailsOnIncidentLog(t, dataDir, wantBytes, path,
			"recover incidents", "itself", `"bob"`)
	})
}

// TestServeIllegalHandoverBeforeTornTailKeepsEveryByte covers the recovery
// ordering regression: a complete illegal handover record followed by an
// unfinished trailing write. Startup must reject the handover and leave the
// log byte-for-byte intact — the torn tail must not be trimmed before the
// failure, and no complete record may be rewritten or dropped. Both torn
// shapes are covered: a partial frame header, and a full header whose
// payload never completely landed.
func TestServeIllegalHandoverBeforeTornTailKeepsEveryByte(t *testing.T) {
	stamp := func(sec int) time.Time {
		return time.Date(2026, 10, 1, 12, 0, 0, sec, time.UTC)
	}
	frame := func(t *testing.T, record incidents.Record) []byte {
		t.Helper()
		payload, err := incidents.MarshalRecord(record)
		if err != nil {
			t.Fatal(err)
		}
		out := make([]byte, 8+len(payload))
		binary.BigEndian.PutUint32(out[0:4], uint32(len(payload)))
		binary.BigEndian.PutUint32(out[4:8], crc32.ChecksumIEEE(payload))
		copy(out[8:], payload)
		return out
	}
	// seed writes one legal incident through the live server and hard kills
	// it, returning the incident log path and its confirmed bytes.
	seed := func(t *testing.T, assignBob bool) (string, string) {
		t.Helper()
		dataDir := t.TempDir()
		first := startServer(t, dataDir)
		postIncident(t, first, `{"id":"INC-9","title":"t","service":"gateway","operator":"Alice"}`)
		if assignBob {
			if s, _ := postIncidentAction(t, first, "INC-9",
				`{"action_id":"p1","operator":"alice","expected_version":1,"action":"add_participant","participant":"bob"}`); s != http.StatusOK {
				t.Fatalf("add bob status %d", s)
			}
		}
		first.signal(t, syscall.SIGKILL)
		first.waitExit(t, -1)
		waitTCPPortClosed(t, first.addr)
		return dataDir, filepath.Join(dataDir, "incidents.log")
	}

	t.Run("non-participant handover ahead of a partial header", func(t *testing.T) {
		dataDir, path := seed(t, true)

		bad := incidents.NewActionRecord(incidents.Request{
			IncidentID: "INC-9", ActionID: "o1", Operator: "alice",
			ExpectedVersion: 2, Type: incidents.ActionAssignOwner, Content: "carol",
		}, stamp(1))
		existing, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		// The tail is only the first three bytes of the next frame's header.
		torn := frame(t, incidents.NewActionRecord(incidents.Request{
			IncidentID: "INC-9", ActionID: "n1", Operator: "alice",
			ExpectedVersion: 3, Type: incidents.ActionNote, Content: "never finished",
		}, stamp(2)))[:3]
		wantBytes := append(append(append([]byte{}, existing...), frame(t, bad)...), torn...)
		if err := os.WriteFile(path, wantBytes, 0o644); err != nil {
			t.Fatal(err)
		}

		// The error must name the handover rule and target, not the torn tail.
		expectServeFailsOnIncidentLog(t, dataDir, wantBytes, path,
			"recover incidents", "non-participant", `"carol"`)
		// A retry fails identically: nothing was trimmed or rewritten.
		expectServeFailsOnIncidentLog(t, dataDir, wantBytes, path,
			"recover incidents", "non-participant", `"carol"`)
	})

	t.Run("self handover ahead of a header-complete torn payload", func(t *testing.T) {
		dataDir, path := seed(t, false)

		bad := incidents.NewActionRecord(incidents.Request{
			IncidentID: "INC-9", ActionID: "o1", Operator: "alice",
			ExpectedVersion: 1, Type: incidents.ActionAssignOwner, Content: "Alice",
		}, stamp(1))
		existing, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		// The tail has a complete header but only half of its payload.
		tornFrame := frame(t, incidents.NewActionRecord(incidents.Request{
			IncidentID: "INC-9", ActionID: "n1", Operator: "alice",
			ExpectedVersion: 2, Type: incidents.ActionNote, Content: "never finished",
		}, stamp(2)))
		torn := tornFrame[:8+(len(tornFrame)-8)/2]
		wantBytes := append(append(append([]byte{}, existing...), frame(t, bad)...), torn...)
		if err := os.WriteFile(path, wantBytes, 0o644); err != nil {
			t.Fatal(err)
		}

		expectServeFailsOnIncidentLog(t, dataDir, wantBytes, path,
			"recover incidents", "itself", `"Alice"`)
	})
}

// TestServeLegalIncidentHistoryWithTornTailStarts confirms the torn-tail
// rule still applies when every complete record is legal: startup trims only
// the unfinished trailing write, and the incident recovers to the last
// complete record's state — owner, participants, version, and history
// unchanged, with the tail neither applied nor fabricated into history.
func TestServeLegalIncidentHistoryWithTornTailStarts(t *testing.T) {
	dataDir := t.TempDir()
	first := startServer(t, dataDir)
	postIncident(t, first, `{"id":"INC-10","title":"t","service":"gateway","operator":"Alice"}`)
	if s, _ := postIncidentAction(t, first, "INC-10",
		`{"action_id":"p1","operator":"alice","expected_version":1,"action":"add_participant","participant":"bob"}`); s != http.StatusOK {
		t.Fatalf("add bob status %d", s)
	}
	if s, _ := postIncidentAction(t, first, "INC-10",
		`{"action_id":"o1","operator":"alice","expected_version":2,"action":"assign_owner","participant":"bob"}`); s != http.StatusOK {
		t.Fatalf("assign bob status %d", s)
	}
	first.signal(t, syscall.SIGKILL)
	first.waitExit(t, -1)
	waitTCPPortClosed(t, first.addr)

	path := filepath.Join(dataDir, "incidents.log")
	complete, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// A torn tail: a full header plus only part of an assign_owner payload.
	tornRecord := incidents.NewActionRecord(incidents.Request{
		IncidentID: "INC-10", ActionID: "o2", Operator: "alice",
		ExpectedVersion: 3, Type: incidents.ActionAssignOwner, Content: "Alice",
	}, time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC))
	tornPayload, err := incidents.MarshalRecord(tornRecord)
	if err != nil {
		t.Fatal(err)
	}
	torn := make([]byte, 8+6)
	binary.BigEndian.PutUint32(torn[0:4], uint32(len(tornPayload)))
	binary.BigEndian.PutUint32(torn[4:8], crc32.ChecksumIEEE(tornPayload))
	copy(torn[8:], tornPayload[:6])
	if err := os.WriteFile(path, append(append([]byte{}, complete...), torn...), 0o644); err != nil {
		t.Fatal(err)
	}

	second := startServer(t, dataDir)
	status, got := getIncident(t, second, "INC-10")
	if status != http.StatusOK {
		t.Fatalf("get after torn-tail restart %d", status)
	}
	if got["owner"] != "bob" || got["status"] != "open" || incNumber(got["version"]) != 3 {
		t.Fatalf("recovered head must match the last complete record: %v", got)
	}
	members, _ := got["participants"].([]any)
	if len(members) != 2 || members[0] != "Alice" || members[1] != "bob" {
		t.Fatalf("recovered participants: %v", got["participants"])
	}
	history, _ := got["history"].([]any)
	if len(history) != 3 {
		t.Fatalf("the torn tail must not become a history entry: %v", got["history"])
	}
	last := history[2].(map[string]any)
	if last["action"] != "assign_owner" || last["action_id"] != "o1" || incNumber(last["version"]) != 3 {
		t.Fatalf("last history entry: %v", last)
	}

	// Only the torn bytes were dropped; every complete record is untouched.
	trimmed, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(trimmed, complete) {
		t.Fatal("startup must trim exactly the torn tail and keep every complete record")
	}

	// The action the torn write hinted at never happened: Alice is not the
	// owner, and a fresh handover to Alice commits as version 4.
	if s, r := postIncidentAction(t, second, "INC-10",
		`{"action_id":"o2","operator":"alice","expected_version":3,"action":"assign_owner","participant":"Alice"}`); s != http.StatusOK || incNumber(r["version"]) != 4 {
		t.Fatalf("handover after trim %d %v", s, r)
	}
	second.signal(t, syscall.SIGTERM)
	second.waitExit(t, 0)
}

// expectServeFailsOnIncidentLog runs serve against dataDir, requires a
// non-zero exit without a readiness line, checks that the startup error
// contains every want fragment, and verifies the incident log file is
// byte-for-byte unchanged.
func expectServeFailsOnIncidentLog(t *testing.T, dataDir string, wantBytes []byte, path string, want ...string) {
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
			t.Fatal("an illegal handover record must prevent startup")
		}
		if ee, ok := err.(*exec.ExitError); !ok || ee.ExitCode() == 0 {
			t.Fatalf("startup must exit non-zero: %v", err)
		}
	case <-ctx.Done():
		proc.Process.Kill()
		t.Fatalf("server stayed up despite an illegal handover record; output:\n%s", logs.String())
	}

	out := logs.String()
	for _, fragment := range want {
		if !strings.Contains(out, fragment) {
			t.Fatalf("startup error must contain %q:\n%s", fragment, out)
		}
	}
	if strings.Contains(out, "listening on") {
		t.Fatalf("failed startup must not announce readiness:\n%s", out)
	}

	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, wantBytes) {
		t.Fatal("failed startup must not modify the incident log, complete offending frame included")
	}
}
