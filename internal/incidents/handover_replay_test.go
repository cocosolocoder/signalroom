package incidents

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

// TestAssignOwnerReplayRebuildsHandover pins the recovery of a legal owner
// handover: the saved records are the only source after a restart, so the
// rebuilt owner, membership, versions, history, operators, target names, and
// timestamps must match the successful actions one for one.
func TestAssignOwnerReplayRebuildsHandover(t *testing.T) {
	times := seededTimes(4)
	records := []Record{
		NewCreationRecord(Creation{ID: "I1", Title: "T", Service: "gateway", Operator: "Alice"}, times[0]),
		NewActionRecord(Request{IncidentID: "I1", ActionID: "p1", Operator: "alice",
			ExpectedVersion: 1, Type: ActionAddParticipant, Content: "bob"}, times[1]),
		NewActionRecord(Request{IncidentID: "I1", ActionID: "p2", Operator: "alice",
			ExpectedVersion: 2, Type: ActionAddParticipant, Content: "Carol"}, times[2]),
		NewActionRecord(Request{IncidentID: "I1", ActionID: "o1", Operator: "bob",
			ExpectedVersion: 3, Type: ActionAssignOwner, Content: "Carol"}, times[3]),
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
	if inc.Status != StatusOpen {
		t.Fatalf("incident must stay open: %q", inc.Status)
	}
	// The last handover names the owner; the membership is untouched and is
	// listed lexicographically with the original case preserved.
	if inc.Owner != "Carol" {
		t.Fatalf("rebuilt owner %q", inc.Owner)
	}
	if !reflect.DeepEqual(inc.Participants, []string{"Alice", "Carol", "bob"}) {
		t.Fatalf("rebuilt participants %+v", inc.Participants)
	}
	if inc.Version != 4 {
		t.Fatalf("rebuilt version %d", inc.Version)
	}

	want := []struct {
		actionID string
		operator string
		action   string
		content  string
		version  int
		at       time.Time
	}{
		{"", "Alice", ActionCreate, "T", 1, times[0]},
		{"p1", "alice", ActionAddParticipant, "bob", 2, times[1]},
		{"p2", "alice", ActionAddParticipant, "Carol", 3, times[2]},
		{"o1", "bob", ActionAssignOwner, "Carol", 4, times[3]},
	}
	if len(inc.History) != len(want) {
		t.Fatalf("history len %d want %d", len(inc.History), len(want))
	}
	for i, w := range want {
		got := inc.History[i]
		if got.ActionID != w.actionID || got.Operator != w.operator || got.Action != w.action ||
			got.Content != w.content || got.Version != w.version || !got.At.Equal(w.at) {
			t.Fatalf("history[%d] %+v want %+v", i, got, w)
		}
	}

	// A retry of the saved handover returns its first result (version 4) and
	// counts as no new action: version and history stay put.
	res, err := reg.Apply(Request{IncidentID: "I1", ActionID: "o1", Operator: "bob",
		ExpectedVersion: 3, Type: ActionAssignOwner, Content: "Carol"})
	if err != nil {
		t.Fatalf("retry after replay: %v", err)
	}
	if res != (ActionResult{IncidentID: "I1", ActionID: "o1", Version: 4}) {
		t.Fatalf("retry result %+v", res)
	}
	again, _ := reg.Get("I1")
	if again.Version != 4 || len(again.History) != 4 || again.Owner != "Carol" {
		t.Fatalf("retry must not count as another action: %+v", again)
	}
}

// TestAssignOwnerReplayIsCaseSensitive guards case-sensitive handover
// targets during recovery: with only "Bob" in the list, "bob" is a stranger;
// once both have joined, handing to one keeps that exact name as owner and
// leaves the other's participation untouched.
func TestAssignOwnerReplayIsCaseSensitive(t *testing.T) {
	t0 := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	at := func(sec int) time.Time { return t0.Add(time.Duration(sec) * time.Second) }
	creation := NewCreationRecord(Creation{ID: "I1", Title: "T", Service: "gateway", Operator: "Alice"}, at(0))

	t.Run("lowercase name is not the joined participant", func(t *testing.T) {
		addBob := NewActionRecord(Request{IncidentID: "I1", ActionID: "p1", Operator: "alice",
			ExpectedVersion: 1, Type: ActionAddParticipant, Content: "Bob"}, at(1))
		// Fresh action id, sequential version, every field legal: the only
		// thing wrong is the handover target.
		handToBob := NewActionRecord(Request{IncidentID: "I1", ActionID: "o1", Operator: "alice",
			ExpectedVersion: 2, Type: ActionAssignOwner, Content: "bob"}, at(2))
		if err := assertRecordWellFormed(handToBob); err != nil {
			t.Fatal(err)
		}

		reg := NewRegistry(&fakeEvents{}, nil)
		if err := reg.Load(creation); err != nil {
			t.Fatal(err)
		}
		if err := reg.Load(addBob); err != nil {
			t.Fatal(err)
		}
		err := reg.Load(handToBob)
		if err == nil || !strings.Contains(err.Error(), "non-participant") ||
			!strings.Contains(err.Error(), `"bob"`) {
			t.Fatalf("handover to a merely case-similar name must fail the membership rule, got %v", err)
		}

		inc, _ := reg.Get("I1")
		if inc.Owner != "Alice" {
			t.Fatalf("owner must not move on a rejected handover: %q", inc.Owner)
		}
		if !reflect.DeepEqual(inc.Participants, []string{"Alice", "Bob"}) {
			t.Fatalf("membership must not gain the target: %+v", inc.Participants)
		}
		if inc.Version != 2 || len(inc.History) != 2 {
			t.Fatalf("rejected record must not become a version/history entry: %+v", inc)
		}
	})

	t.Run("both names joined are distinct handover targets", func(t *testing.T) {
		records := []Record{
			creation,
			NewActionRecord(Request{IncidentID: "I1", ActionID: "p1", Operator: "alice",
				ExpectedVersion: 1, Type: ActionAddParticipant, Content: "bob"}, at(1)),
			NewActionRecord(Request{IncidentID: "I1", ActionID: "p2", Operator: "alice",
				ExpectedVersion: 2, Type: ActionAddParticipant, Content: "Bob"}, at(2)),
			NewActionRecord(Request{IncidentID: "I1", ActionID: "o1", Operator: "alice",
				ExpectedVersion: 3, Type: ActionAssignOwner, Content: "bob"}, at(3)),
			NewActionRecord(Request{IncidentID: "I1", ActionID: "o2", Operator: "alice",
				ExpectedVersion: 4, Type: ActionAssignOwner, Content: "Bob"}, at(4)),
		}
		reg := NewRegistry(&fakeEvents{}, nil)
		for _, record := range records {
			if err := reg.Load(record); err != nil {
				t.Fatalf("load: %v", err)
			}
		}
		inc, _ := reg.Get("I1")
		// The last handover preserves the capitalized target verbatim.
		if inc.Owner != "Bob" {
			t.Fatalf("owner %q want Bob", inc.Owner)
		}
		if !reflect.DeepEqual(inc.Participants, []string{"Alice", "Bob", "bob"}) {
			t.Fatalf("both case variants remain participants: %+v", inc.Participants)
		}
		if inc.Version != 5 || len(inc.History) != 5 {
			t.Fatalf("version/history %+v", inc)
		}
	})
}

// TestReplayRejectsIllegalOwnerHandover is the core regression for
// "accepted at recovery but refused at the request entry". Both illegal
// handovers are otherwise flawless records (legal fields and timestamp,
// sequential expected_version, a fresh action id), so a rejection must come
// from the handover rule itself — whether the bad record sits at the tail or
// ahead of other, independently legal records. A failed Load leaves the
// already-saved content unchanged: no auto-join for the target, no default
// owner substituted, and the following records are never applied.
func TestReplayRejectsIllegalOwnerHandover(t *testing.T) {
	t0 := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	at := func(sec int) time.Time { return t0.Add(time.Duration(sec) * time.Second) }

	cases := []struct {
		name         string
		prefix       []Record
		bad          Record
		follow       Record
		wantVersion  int
		wantOwner    string
		wantMembers  []string
		wantErrPart  string
		wantTarget   string
		liveCurrentV int
	}{
		{
			name: "target never joined",
			prefix: []Record{
				NewCreationRecord(Creation{ID: "I1", Title: "T", Service: "gateway", Operator: "Alice"}, at(0)),
				NewActionRecord(Request{IncidentID: "I1", ActionID: "p1", Operator: "alice",
					ExpectedVersion: 1, Type: ActionAddParticipant, Content: "bob"}, at(1)),
			},
			bad: NewActionRecord(Request{IncidentID: "I1", ActionID: "o1", Operator: "alice",
				ExpectedVersion: 2, Type: ActionAssignOwner, Content: "carol"}, at(2)),
			// The following note was (wrongly, in a consistent world) written
			// against the version the bad handover would reach, so a recovery
			// path that accepts the handover would accept this too and start
			// successfully: the test then fails because startup did not abort.
			follow: NewActionRecord(Request{IncidentID: "I1", ActionID: "n1", Operator: "alice",
				ExpectedVersion: 3, Type: ActionNote, Content: "after the bad handover"}, at(3)),
			wantVersion:  2,
			wantOwner:    "Alice",
			wantMembers:  []string{"Alice", "bob"},
			wantErrPart:  "non-participant",
			wantTarget:   `"carol"`,
			liveCurrentV: 2,
		},
		{
			name: "target is the current owner",
			prefix: []Record{
				NewCreationRecord(Creation{ID: "I1", Title: "T", Service: "gateway", Operator: "Alice"}, at(0)),
				NewActionRecord(Request{IncidentID: "I1", ActionID: "p1", Operator: "alice",
					ExpectedVersion: 1, Type: ActionAddParticipant, Content: "bob"}, at(1)),
				NewActionRecord(Request{IncidentID: "I1", ActionID: "o0", Operator: "alice",
					ExpectedVersion: 2, Type: ActionAssignOwner, Content: "bob"}, at(2)),
			},
			bad: NewActionRecord(Request{IncidentID: "I1", ActionID: "o1", Operator: "alice",
				ExpectedVersion: 3, Type: ActionAssignOwner, Content: "bob"}, at(3)),
			// Same rationale as above: sequential with the bad handover's
			// would-be version, so accepting the no-op handover lets startup
			// proceed past it.
			follow: NewActionRecord(Request{IncidentID: "I1", ActionID: "n1", Operator: "alice",
				ExpectedVersion: 4, Type: ActionNote, Content: "after the bad handover"}, at(4)),
			wantVersion:  3,
			wantOwner:    "bob",
			wantMembers:  []string{"Alice", "bob"},
			wantErrPart:  "itself",
			wantTarget:   `"bob"`,
			liveCurrentV: 3,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			badReq := tc.bad.Action.Request

			// The bad record and the trailing record must themselves be
			// complete, well-formed records: failure is the handover rule, not
			// a missing field, a timestamp problem, or JSON corruption.
			if err := assertRecordWellFormed(tc.bad); err != nil {
				t.Fatalf("bad record: %v", err)
			}
			if err := assertRecordWellFormed(tc.follow); err != nil {
				t.Fatalf("following record: %v", err)
			}
			if _, err := NormalizeRequest(badReq); err != nil {
				t.Fatalf("bad request normalizes fine on its own: %v", err)
			}
			if badReq.ExpectedVersion != tc.wantVersion {
				t.Fatalf("expected_version %d must be the current version %d",
					badReq.ExpectedVersion, tc.wantVersion)
			}

			// Parity: the live request entry rejects the same normalized
			// request as a 409-style conflict at the same version.
			live, _, _ := newTestRegistry(&fakeEvents{})
			if _, err := live.Create(
				Creation{ID: "I1", Title: "T", Service: "gateway", Operator: "Alice"}); err != nil {
				t.Fatal(err)
			}
			for _, record := range tc.prefix[1:] {
				if _, err := live.Apply(record.Action.Request); err != nil {
					t.Fatalf("live prefix: %v", err)
				}
			}
			_, liveErr := live.Apply(badReq)
			if !IsConflictError(liveErr) || CurrentVersionOf(liveErr) != tc.liveCurrentV {
				t.Fatalf("live request path must reject the handover: %v", liveErr)
			}
			liveInc, _ := live.Get("I1")
			if liveInc.Version != tc.wantVersion || liveInc.Owner != tc.wantOwner {
				t.Fatalf("rejected live request must leave state put: %+v", liveInc)
			}

			for _, withFollow := range []bool{false, true} {
				placement := "at the tail"
				if withFollow {
					placement = "ahead of other records"
				}
				t.Run(placement, func(t *testing.T) {
					// Recovery walks the log in order and stops at the first
					// contradiction, exactly like runServe's replay loop.
					sequence := append(append([]Record{}, tc.prefix...), tc.bad)
					if withFollow {
						sequence = append(sequence, tc.follow)
					}
					reg := NewRegistry(&fakeEvents{}, nil)
					var err error
					failedAt := -1
					for i, record := range sequence {
						if err = reg.Load(record); err != nil {
							failedAt = i
							break
						}
					}
					if failedAt != len(tc.prefix) {
						t.Fatalf("recovery must fail on the bad record (index %d), failed at %d: %v",
							len(tc.prefix), failedAt, err)
					}
					if !strings.Contains(err.Error(), tc.wantErrPart) ||
						!strings.Contains(err.Error(), tc.wantTarget) {
						t.Fatalf("error must explain why the target %s is illegal: %v",
							tc.wantTarget, err)
					}
					if strings.Contains(err.Error(), "expected version") {
						t.Fatalf("failure must not be a version jump: %v", err)
					}
					if strings.Contains(err.Error(), "duplicate") {
						t.Fatalf("failure must not be a reused action id: %v", err)
					}

					inc, _ := reg.Get("I1")
					if inc.Owner != tc.wantOwner {
						t.Fatalf("owner %q want %q (no default substitution)", inc.Owner, tc.wantOwner)
					}
					if !reflect.DeepEqual(inc.Participants, tc.wantMembers) {
						t.Fatalf("membership changed around a failed handover: %+v", inc.Participants)
					}
					if inc.Version != tc.wantVersion || len(inc.History) != tc.wantVersion {
						t.Fatalf("state must stay at the prefix: %+v", inc)
					}
					for _, entry := range inc.History {
						if entry.Action == ActionNote && entry.Content == "after the bad handover" {
							t.Fatal("records following the bad handover must not be applied")
						}
						if entry.ActionID == "o1" && entry.Action == ActionAssignOwner {
							t.Fatal("the rejected handover must not appear in history")
						}
					}
				})
			}
		})
	}
}

// assertRecordWellFormed verifies that a record survives the durable
// round-trip: strict JSON decode, re-validation, and an equal round-trip. A
// record used in a rule-failure test must pass all of these so the rejection
// under test is attributable to the handover rules alone.
func assertRecordWellFormed(record Record) error {
	data, err := MarshalRecord(record)
	if err != nil {
		return err
	}
	back, err := DecodeRecord(data)
	if err != nil {
		return err
	}
	if back.Kind != RecordAction || back.Action.Request != record.Action.Request || !back.At.Equal(record.At) {
		return &ValidationError{Reason: "record round-trip mismatch"}
	}
	return nil
}
