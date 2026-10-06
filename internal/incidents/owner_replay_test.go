package incidents

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

// These tests pin the recovery-side judgment of an owner handover
// (assign_owner). The live request path and the startup replay path route
// through the same checkPersonnelAction, so a regression that lets replay
// accept a handover the request entry would reject — handing the incident to
// a non-participant or "handing it over" to the current owner — must fail
// here.

func handoverTimes(n int) []time.Time {
	times := make([]time.Time, n)
	base := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	for i := range times {
		times[i] = base.Add(time.Duration(i+1) * time.Second)
	}
	return times
}

func TestReplayOwnerHandoverRebuildsPeopleState(t *testing.T) {
	times := handoverTimes(4)
	records := []Record{
		NewCreationRecord(Creation{ID: "I1", Title: "T", Service: "gateway", Operator: "alice"}, times[0]),
		NewActionRecord(Request{IncidentID: "I1", ActionID: "p1", Operator: "alice",
			ExpectedVersion: 1, Type: ActionAddParticipant, Content: "Bob"}, times[1]),
		NewActionRecord(Request{IncidentID: "I1", ActionID: "p2", Operator: "alice",
			ExpectedVersion: 2, Type: ActionAddParticipant, Content: "carol"}, times[2]),
		NewActionRecord(Request{IncidentID: "I1", ActionID: "o1", Operator: "alice",
			ExpectedVersion: 3, Type: ActionAssignOwner, Content: "Bob"}, times[3]),
	}

	reg := NewRegistry(&fakeEvents{}, nil)
	for _, record := range records {
		if err := reg.Load(record); err != nil {
			t.Fatalf("load: %v", err)
		}
	}

	inc, err := reg.Get("I1")
	if err != nil {
		t.Fatal(err)
	}
	// The incident is still open and shows the last handover's owner.
	if inc.Status != StatusOpen {
		t.Fatalf("handover must not resolve the incident: %s", inc.Status)
	}
	if inc.Owner != "Bob" {
		t.Fatalf("recovered owner %q want Bob", inc.Owner)
	}
	// A handover never changes membership; the snapshot lists the original
	// members, duplicates removed, sorted lexicographically with case kept.
	if !reflect.DeepEqual(inc.Participants, []string{"Bob", "alice", "carol"}) {
		t.Fatalf("participants after recovered handover %+v", inc.Participants)
	}
	// Versions and history correspond one-to-one with the successful records.
	if inc.Version != 4 {
		t.Fatalf("recovered version %d want 4", inc.Version)
	}
	if len(inc.History) != 4 {
		t.Fatalf("recovered history len %d want 4", len(inc.History))
	}
	for i, entry := range inc.History {
		if entry.Version != i+1 {
			t.Fatalf("history[%d] version %d", i, entry.Version)
		}
		if !entry.At.Equal(times[i]) {
			t.Fatalf("history[%d] at %s want %s", i, entry.At, times[i])
		}
	}
	last := inc.History[3]
	// The handover entry keeps the operator, normalized target name, and
	// server time saved at commit time — nothing is re-derived on recovery.
	if last.ActionID != "o1" || last.Operator != "alice" ||
		last.Action != ActionAssignOwner || last.Content != "Bob" {
		t.Fatalf("handover history entry %+v", last)
	}
}

func TestReplayHandoverNamesAreCaseSensitive(t *testing.T) {
	times := handoverTimes(4)

	t.Run("lowercase name is not the joined member", func(t *testing.T) {
		records := []Record{
			NewCreationRecord(Creation{ID: "I2", Title: "T", Service: "gateway", Operator: "alice"}, times[0]),
			NewActionRecord(Request{IncidentID: "I2", ActionID: "p1", Operator: "alice",
				ExpectedVersion: 1, Type: ActionAddParticipant, Content: "Bob"}, times[1]),
		}
		reg := NewRegistry(&fakeEvents{}, nil)
		for _, record := range records {
			if err := reg.Load(record); err != nil {
				t.Fatalf("load: %v", err)
			}
		}
		// Only "Bob" participates; "bob" is a different, never-joined person.
		bad := NewActionRecord(Request{IncidentID: "I2", ActionID: "x1", Operator: "alice",
			ExpectedVersion: 2, Type: ActionAssignOwner, Content: "bob"}, times[2])
		if err := reg.Load(bad); err == nil {
			t.Fatal("handover to \"bob\" while only \"Bob\" joined must fail recovery")
		} else if !IsValidationError(err) || !strings.Contains(err.Error(), "non-participant") {
			t.Fatalf("want a non-participant handover error, got %v", err)
		}
		inc, _ := reg.Get("I2")
		if inc.Owner != "alice" || !reflect.DeepEqual(inc.Participants, []string{"Bob", "alice"}) {
			t.Fatalf("rejected replay must not change people: owner %q participants %+v",
				inc.Owner, inc.Participants)
		}
	})

	t.Run("both names joined keep distinct identities", func(t *testing.T) {
		records := []Record{
			NewCreationRecord(Creation{ID: "I3", Title: "T", Service: "gateway", Operator: "alice"}, times[0]),
			NewActionRecord(Request{IncidentID: "I3", ActionID: "p1", Operator: "alice",
				ExpectedVersion: 1, Type: ActionAddParticipant, Content: "Bob"}, times[1]),
			NewActionRecord(Request{IncidentID: "I3", ActionID: "p2", Operator: "alice",
				ExpectedVersion: 2, Type: ActionAddParticipant, Content: "bob"}, times[2]),
			NewActionRecord(Request{IncidentID: "I3", ActionID: "o1", Operator: "alice",
				ExpectedVersion: 3, Type: ActionAssignOwner, Content: "Bob"}, times[3]),
		}
		reg := NewRegistry(&fakeEvents{}, nil)
		for _, record := range records {
			if err := reg.Load(record); err != nil {
				t.Fatalf("load: %v", err)
			}
		}
		inc, _ := reg.Get("I3")
		// The owner keeps the exact handed-over spelling; the other
		// differently-cased person is still a participant.
		if inc.Owner != "Bob" {
			t.Fatalf("owner must preserve exact case %q", inc.Owner)
		}
		if !reflect.DeepEqual(inc.Participants, []string{"Bob", "alice", "bob"}) {
			t.Fatalf("both cased names must remain participants %+v", inc.Participants)
		}
	})
}

func TestReplayRejectsIllegalHandoverRecord(t *testing.T) {
	times := handoverTimes(6)
	creation := NewCreationRecord(Creation{ID: "INC-1", Title: "T", Service: "gateway", Operator: "alice"}, times[0])
	add := func(id string, ev int, name string, at time.Time) Record {
		return NewActionRecord(Request{IncidentID: "INC-1", ActionID: id, Operator: "alice",
			ExpectedVersion: ev, Type: ActionAddParticipant, Content: name}, at)
	}
	note := func(id string, ev int, at time.Time) Record {
		return NewActionRecord(Request{IncidentID: "INC-1", ActionID: id, Operator: "mallory",
			ExpectedVersion: ev, Type: ActionNote, Content: "looking"}, at)
	}
	assign := func(id string, ev int, target string, at time.Time) Record {
		return NewActionRecord(Request{IncidentID: "INC-1", ActionID: id, Operator: "alice",
			ExpectedVersion: ev, Type: ActionAssignOwner, Content: target}, at)
	}

	cases := []struct {
		name           string
		prefix         []Record
		bad            Record
		suffix         []Record // well-formed records that would be legal after the bad one
		wantMsg        string
		wantTarget     string
		wantVersion    int
		wantOwner      string
		wantMembers    []string
		wantHistoryLen int
	}{
		{
			name:           "non-participant target at tail",
			prefix:         []Record{creation, add("p1", 1, "carol", times[1])},
			bad:            assign("bad-nonmember-tail", 2, "zoe", times[2]),
			wantMsg:        "non-participant",
			wantTarget:     "zoe",
			wantVersion:    2,
			wantOwner:      "alice",
			wantMembers:    []string{"alice", "carol"},
			wantHistoryLen: 2,
		},
		{
			name:           "non-participant target before later legal records",
			prefix:         []Record{creation, note("n1", 1, times[1])},
			bad:            assign("bad-nonmember-middle", 2, "zoe", times[2]),
			suffix:         []Record{note("n2", 3, times[3])},
			wantMsg:        "non-participant",
			wantTarget:     "zoe",
			wantVersion:    2,
			wantOwner:      "alice",
			wantMembers:    []string{"alice"},
			wantHistoryLen: 2,
		},
		{
			name:           "current owner target at tail",
			prefix:         []Record{creation},
			bad:            assign("bad-self-tail", 1, "alice", times[1]),
			wantMsg:        "handed over to itself",
			wantTarget:     "alice",
			wantVersion:    1,
			wantOwner:      "alice",
			wantMembers:    []string{"alice"},
			wantHistoryLen: 1,
		},
		{
			name: "current owner target before later legal records",
			prefix: []Record{
				creation,
				add("p1", 1, "Bob", times[1]),
				assign("o1", 2, "Bob", times[2]),
			},
			bad:            assign("bad-self-middle", 3, "Bob", times[3]),
			suffix:         []Record{note("n2", 4, times[4])},
			wantMsg:        "handed over to itself",
			wantTarget:     "Bob",
			wantVersion:    3,
			wantOwner:      "Bob",
			wantMembers:    []string{"Bob", "alice"},
			wantHistoryLen: 3,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// The offending record is itself a complete, legal frame: it
			// round-trips the strict codec, carries a timestamp, its
			// expected_version is exactly the current version (no gap), and
			// its action id has never appeared. The rejection must come
			// from the handover rule alone.
			data, err := MarshalRecord(tc.bad)
			if err != nil {
				t.Fatalf("marshal bad record: %v", err)
			}
			decoded, err := DecodeRecord(data)
			if err != nil {
				t.Fatalf("the bad record must be a legal record: %v", err)
			}
			badReq := decoded.Action.Request

			reg := NewRegistry(&fakeEvents{}, nil)
			for _, record := range tc.prefix {
				if err := reg.Load(record); err != nil {
					t.Fatalf("load prefix: %v", err)
				}
			}
			state := reg.incidents["INC-1"]
			if badReq.ExpectedVersion != state.version {
				t.Fatalf("bad record expected_version %d must follow the current version %d",
					badReq.ExpectedVersion, state.version)
			}
			if _, used := state.actions[badReq.ActionID]; used {
				t.Fatalf("bad record action id %q must be unused", badReq.ActionID)
			}

			// Parity with the request entry: the same normalized request is
			// a live 409-style conflict and leaves the incident untouched.
			if _, err := reg.Apply(badReq); !IsConflictError(err) || CurrentVersionOf(err) != state.version {
				t.Fatalf("live request must reject the handover with the current version, got %v", err)
			}
			if inc, _ := reg.Get("INC-1"); inc.Version != tc.wantVersion {
				t.Fatalf("rejected live request must not advance the version: %d", inc.Version)
			}

			// Recovery rejects the very same record instead of accepting it.
			err = reg.Load(tc.bad)
			if err == nil {
				t.Fatal("replay must reject the illegal handover")
			}
			if !IsValidationError(err) {
				t.Fatalf("replay rejection must be a log validation failure, got %T %v", err, err)
			}
			if !strings.Contains(err.Error(), tc.wantMsg) || !strings.Contains(err.Error(), tc.wantTarget) {
				t.Fatalf("error must explain why %q is an illegal handover target, got %v",
					tc.wantTarget, err)
			}

			// Replaying the whole log — illegal record followed by more
			// legal records — still fails at the handover, never skips it
			// and continues startup.
			fresh := NewRegistry(&fakeEvents{}, nil)
			var replayErr error
			for _, record := range append(append(append([]Record{}, tc.prefix...), tc.bad), tc.suffix...) {
				if replayErr = fresh.Load(record); replayErr != nil {
					break
				}
			}
			if replayErr == nil || !strings.Contains(replayErr.Error(), tc.wantMsg) {
				t.Fatalf("whole-log recovery must fail on the handover rule, got %v", replayErr)
			}

			// Content already saved before and after the rejected frame is
			// unchanged: no auto-join for the target, no default owner, no
			// history entry or version from the failed record.
			inc, _ := reg.Get("INC-1")
			if inc.Version != tc.wantVersion || len(inc.History) != tc.wantHistoryLen {
				t.Fatalf("failed replay must leave version/history unchanged: version %d history %d",
					inc.Version, len(inc.History))
			}
			if inc.Owner != tc.wantOwner {
				t.Fatalf("failed replay must not replace the owner: %q", inc.Owner)
			}
			if !reflect.DeepEqual(inc.Participants, tc.wantMembers) {
				t.Fatalf("failed replay must not add the target: %+v", inc.Participants)
			}
			for _, entry := range inc.History {
				if entry.ActionID == badReq.ActionID {
					t.Fatal("the rejected handover must not be committed to history")
				}
			}
		})
	}
}
