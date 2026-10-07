package logfile

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cocosolocoder/signalroom/internal/incidents"
)

// recordFrame encodes one incident record as a complete on-disk frame.
func recordFrame(t *testing.T, record incidents.Record) []byte {
	t.Helper()
	payload, err := incidents.MarshalRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	frame := make([]byte, headerSize+len(payload))
	putFrameHeader(frame, payload)
	copy(frame[headerSize:], payload)
	return frame
}

// writeIncidentFrames builds incidents.log from raw frames without a running
// server, so tests can splice complete records together with torn tails.
func writeIncidentFrames(t *testing.T, dir string, frames ...[]byte) []byte {
	t.Helper()
	var content []byte
	for _, frame := range frames {
		content = append(content, frame...)
	}
	if err := os.WriteFile(filepath.Join(dir, incidentLogName), content, 0o644); err != nil {
		t.Fatal(err)
	}
	return content
}

func recoveryStamp(sec int) time.Time {
	return time.Date(2026, 10, 1, 12, 0, 0, sec, time.UTC)
}

// TestIncidentLogIllegalHistoryKeepsTornTail is the core regression: when a
// complete, well-formed record breaks a handover rule and an unfinished write
// still trails it, startup must fail on the handover (not the tail) and must
// not truncate first. Every original byte survives — the offending record,
// the complete records around it (including another incident's), and the
// torn tail in either of its two shapes.
func TestIncidentLogIllegalHistoryKeepsTornTail(t *testing.T) {
	creation := incidents.NewCreationRecord(incidents.Creation{
		ID: "INC-1", Title: "t", Service: "gateway", Operator: "alice",
	}, recoveryStamp(0))
	addBob := incidents.NewActionRecord(incidents.Request{
		IncidentID: "INC-1", ActionID: "p1", Operator: "alice",
		ExpectedVersion: 1, Type: incidents.ActionAddParticipant, Content: "bob",
	}, recoveryStamp(1))

	// Complete frame that is only legal if the rejected handover had
	// committed: a later same-named join must not legitimize the earlier
	// handover, and this frame stays on disk either way.
	addCarol := incidents.NewActionRecord(incidents.Request{
		IncidentID: "INC-1", ActionID: "p2", Operator: "alice",
		ExpectedVersion: 3, Type: incidents.ActionAddParticipant, Content: "carol",
	}, recoveryStamp(3))
	noteAfter := incidents.NewActionRecord(incidents.Request{
		IncidentID: "INC-1", ActionID: "n1", Operator: "alice",
		ExpectedVersion: 4, Type: incidents.ActionNote, Content: "after the bad handover",
	}, recoveryStamp(4))
	// A legal record belonging to a different incident must survive too.
	otherCreation := incidents.NewCreationRecord(incidents.Creation{
		ID: "INC-2", Title: "t", Service: "gateway", Operator: "dora",
	}, recoveryStamp(5))

	tornPayload, err := incidents.MarshalRecord(incidents.NewActionRecord(incidents.Request{
		IncidentID: "INC-1", ActionID: "p9", Operator: "alice",
		ExpectedVersion: 5, Type: incidents.ActionAddParticipant, Content: "erin",
	}, recoveryStamp(6)))
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name       string
		prefix     []incidents.Record
		bad        incidents.Record
		follow     []incidents.Record
		wantErr    string
		wantTarget string
	}{
		{
			name:   "handover to a never-joined target before the target joins later",
			prefix: []incidents.Record{creation, addBob},
			bad: incidents.NewActionRecord(incidents.Request{
				IncidentID: "INC-1", ActionID: "o1", Operator: "alice",
				ExpectedVersion: 2, Type: incidents.ActionAssignOwner, Content: "carol",
			}, recoveryStamp(2)),
			follow:     []incidents.Record{addCarol, noteAfter, otherCreation},
			wantErr:    "non-participant",
			wantTarget: `"carol"`,
		},
		{
			name: "handover to the current owner",
			prefix: []incidents.Record{
				creation, addBob,
				incidents.NewActionRecord(incidents.Request{
					IncidentID: "INC-1", ActionID: "o0", Operator: "alice",
					ExpectedVersion: 2, Type: incidents.ActionAssignOwner, Content: "bob",
				}, recoveryStamp(2)),
			},
			bad: incidents.NewActionRecord(incidents.Request{
				IncidentID: "INC-1", ActionID: "o1", Operator: "alice",
				ExpectedVersion: 3, Type: incidents.ActionAssignOwner, Content: "bob",
			}, recoveryStamp(3)),
			follow: []incidents.Record{
				incidents.NewActionRecord(incidents.Request{
					IncidentID: "INC-1", ActionID: "n1", Operator: "alice",
					ExpectedVersion: 4, Type: incidents.ActionNote, Content: "after the bad handover",
				}, recoveryStamp(4)),
				otherCreation,
			},
			wantErr:    "itself",
			wantTarget: `"bob"`,
		},
	}

	tails := []struct {
		name string
		tail []byte
	}{
		// Only a fragment of the header landed: not even eight bytes.
		{"partial header", []byte{0x00, 0x00, 0x01}},
		// A full header whose payload never fully landed.
		{"header with partial payload", func() []byte {
			torn := make([]byte, headerSize+4)
			putFrameHeader(torn, tornPayload)
			copy(torn[headerSize:], tornPayload[:4])
			return torn
		}()},
	}

	for _, tc := range cases {
		for _, tail := range tails {
			t.Run(tc.name+"/"+tail.name, func(t *testing.T) {
				dir := t.TempDir()
				eventLog, _, err := Open(dir)
				if err != nil {
					t.Fatalf("open dir: %v", err)
				}
				defer eventLog.Close()

				var frames [][]byte
				for _, record := range tc.prefix {
					frames = append(frames, recordFrame(t, record))
				}
				frames = append(frames, recordFrame(t, tc.bad))
				for _, record := range tc.follow {
					frames = append(frames, recordFrame(t, record))
				}
				frames = append(frames, tail.tail)
				wantBytes := writeIncidentFrames(t, dir, frames...)

				path := filepath.Join(dir, incidentLogName)
				_, _, err = OpenIncidentLog(dir, nil)
				if err == nil {
					t.Fatal("an illegal handover ahead of a torn tail must fail startup")
				}
				if !strings.Contains(err.Error(), tc.wantErr) ||
					!strings.Contains(err.Error(), tc.wantTarget) {
					t.Fatalf("error must name the handover rule and target, got %v", err)
				}
				if strings.Contains(err.Error(), "trailing") ||
					strings.Contains(err.Error(), "truncate") {
					t.Fatalf("failure must not be reported as the torn tail: %v", err)
				}
				after, rerr := os.ReadFile(path)
				if rerr != nil {
					t.Fatal(rerr)
				}
				if len(after) != len(wantBytes) || string(after) != string(wantBytes) {
					t.Fatal("failed recovery must keep the full original log, torn tail included")
				}

				// A second start fails identically: nothing was discarded or
				// "repaired" (no auto-join, no substitute owner, no skip).
				_, _, err = OpenIncidentLog(dir, nil)
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) ||
					!strings.Contains(err.Error(), tc.wantTarget) {
					t.Fatalf("second startup must reject the same handover: %v", err)
				}
				again, rerr := os.ReadFile(path)
				if rerr != nil {
					t.Fatal(rerr)
				}
				if len(again) != len(wantBytes) || string(again) != string(wantBytes) {
					t.Fatal("the repeated failed recovery must not modify the log")
				}
			})
		}
	}
}

// TestIncidentLogLegalHistoryTornTailStillTrimmed pins the unchanged
// recovery rule for a consistent history: exactly one incomplete trailing
// frame is dropped, complete records rebuild to the last committed state,
// and later writes land at the trimmed end.
func TestIncidentLogLegalHistoryTornTailStillTrimmed(t *testing.T) {
	dir := t.TempDir()
	creation := incidents.NewCreationRecord(incidents.Creation{
		ID: "INC-1", Title: "t", Service: "gateway", Operator: "alice",
	}, recoveryStamp(0))
	addBob := incidents.NewActionRecord(incidents.Request{
		IncidentID: "INC-1", ActionID: "p1", Operator: "alice",
		ExpectedVersion: 1, Type: incidents.ActionAddParticipant, Content: "bob",
	}, recoveryStamp(1))
	assignBob := incidents.NewActionRecord(incidents.Request{
		IncidentID: "INC-1", ActionID: "o1", Operator: "alice",
		ExpectedVersion: 2, Type: incidents.ActionAssignOwner, Content: "bob",
	}, recoveryStamp(2))
	// The unfinished write would have added carol; it must not become a
	// participant or an owner/history change.
	tornPayload, err := incidents.MarshalRecord(incidents.NewActionRecord(incidents.Request{
		IncidentID: "INC-1", ActionID: "p2", Operator: "alice",
		ExpectedVersion: 3, Type: incidents.ActionAddParticipant, Content: "carol",
	}, recoveryStamp(3)))
	if err != nil {
		t.Fatal(err)
	}

	eventLog, _, err := Open(dir)
	if err != nil {
		t.Fatalf("open dir: %v", err)
	}
	defer eventLog.Close()

	completeLen := len(recordFrame(t, creation)) + len(recordFrame(t, addBob)) + len(recordFrame(t, assignBob))
	torn := make([]byte, headerSize+4)
	putFrameHeader(torn, tornPayload)
	copy(torn[headerSize:], tornPayload[:4])
	wantBefore := completeLen + len(torn)
	onDisk := writeIncidentFrames(t, dir,
		recordFrame(t, creation), recordFrame(t, addBob), recordFrame(t, assignBob), torn)
	if len(onDisk) != wantBefore {
		t.Fatalf("setup length %d want %d", len(onDisk), wantBefore)
	}

	incLog, records, err := OpenIncidentLog(dir, nil)
	if err != nil {
		t.Fatalf("a legal history with a torn tail must recover: %v", err)
	}
	if len(records) != 3 {
		t.Fatalf("only the three complete records recover, got %d", len(records))
	}
	path := filepath.Join(dir, incidentLogName)
	trimmed, rerr := os.ReadFile(path)
	if rerr != nil {
		t.Fatal(rerr)
	}
	if len(trimmed) != completeLen {
		t.Fatalf("only the torn tail may be dropped: size %d want %d", len(trimmed), completeLen)
	}

	// A fresh append lands exactly where the trimmed tail began.
	next := incidents.NewActionRecord(incidents.Request{
		IncidentID: "INC-1", ActionID: "p3", Operator: "alice",
		ExpectedVersion: 3, Type: incidents.ActionNote, Content: "clean",
	}, recoveryStamp(4))
	if err := incLog.AppendRecord(next); err != nil {
		t.Fatalf("append after trim: %v", err)
	}
	if err := incLog.Close(); err != nil {
		t.Fatal(err)
	}

	incLog2, recovered, err := OpenIncidentLog(dir, nil)
	defer incLog2.Close()
	if err != nil {
		t.Fatal(err)
	}
	if len(recovered) != 4 || recovered[3].Action.Request.ActionID != "p3" {
		t.Fatalf("post-trim write must recover as the fourth record: %+v", recovered)
	}
}
