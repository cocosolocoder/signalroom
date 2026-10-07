package incidents

import (
	"strings"
	"testing"
	"time"
)

// TestUnlinkReplayRebuildsState pins recovery through a
// link -> unlink -> relink history: saved records are the only source after
// a restart, so the rebuilt link list, versions, history entries, operators,
// target ids, and timestamps must match the committed actions one for one,
// and retries of the saved requests replay their original results instead of
// changing state.
func TestUnlinkReplayRebuildsState(t *testing.T) {
	lookup := &fakeEvents{services: map[string]string{"e1": "gateway", "e2": "gateway"}}
	times := seededTimes(7)
	records := []Record{
		NewCreationRecord(Creation{ID: "I1", Title: "T", Service: "gateway", Operator: "alice"}, times[0]),
		NewActionRecord(Request{IncidentID: "I1", ActionID: "l1", Operator: "bob",
			ExpectedVersion: 1, Type: ActionLinkEvent, Content: "e1"}, times[1]),
		NewActionRecord(Request{IncidentID: "I1", ActionID: "l2", Operator: "bob",
			ExpectedVersion: 2, Type: ActionLinkEvent, Content: "e2"}, times[2]),
		NewActionRecord(Request{IncidentID: "I1", ActionID: "u1", Operator: "carol",
			ExpectedVersion: 3, Type: ActionUnlinkEvent, Content: "e1"}, times[3]),
		NewActionRecord(Request{IncidentID: "I1", ActionID: "l3", Operator: "bob",
			ExpectedVersion: 4, Type: ActionLinkEvent, Content: "e1"}, times[4]),
		NewActionRecord(Request{IncidentID: "I1", ActionID: "u2", Operator: "carol",
			ExpectedVersion: 5, Type: ActionUnlinkEvent, Content: "e1"}, times[5]),
	}

	reg := NewRegistry(lookup, nil)
	for i, record := range records {
		if err := reg.Load(record); err != nil {
			t.Fatalf("load record %d: %v", i, err)
		}
	}

	inc, err := reg.Get("I1")
	if err != nil {
		t.Fatal(err)
	}
	// e1 was linked twice and unlinked twice; e2 is still linked.
	if len(inc.Links) != 1 || inc.Links[0] != "e2" {
		t.Fatalf("rebuilt links %+v", inc.Links)
	}
	if inc.Version != 6 || inc.Status != StatusOpen {
		t.Fatalf("rebuilt head %+v", inc)
	}

	wantActions := []string{
		ActionCreate, ActionLinkEvent, ActionLinkEvent, ActionUnlinkEvent,
		ActionLinkEvent, ActionUnlinkEvent,
	}
	if len(inc.History) != len(wantActions) {
		t.Fatalf("history len %d", len(inc.History))
	}
	for i, want := range wantActions {
		got := inc.History[i]
		if got.Action != want || got.Version != i+1 || !got.At.Equal(times[i]) {
			t.Fatalf("history[%d] %+v want action %s version %d at %s",
				i, got, want, i+1, times[i])
		}
	}
	if inc.History[3].ActionID != "u1" || inc.History[3].Content != "e1" || inc.History[3].Operator != "carol" {
		t.Fatalf("unlink history entry %+v", inc.History[3])
	}

	// Post-recovery retries reproduce the first results without advancing.
	res, err := reg.Apply(Request{IncidentID: "I1", ActionID: "u1", Operator: "carol",
		ExpectedVersion: 3, Type: ActionUnlinkEvent, Content: "e1"})
	if err != nil {
		t.Fatalf("unlink retry after replay: %v", err)
	}
	if res != (ActionResult{IncidentID: "I1", ActionID: "u1", Version: 4}) {
		t.Fatalf("unlink retry result %+v", res)
	}
	res, err = reg.Apply(Request{IncidentID: "I1", ActionID: "l3", Operator: "bob",
		ExpectedVersion: 4, Type: ActionLinkEvent, Content: "e1"})
	if err != nil {
		t.Fatalf("relink retry after replay: %v", err)
	}
	if res != (ActionResult{IncidentID: "I1", ActionID: "l3", Version: 5}) {
		t.Fatalf("relink retry result %+v", res)
	}
	again, _ := reg.Get("I1")
	if again.Version != 6 || len(again.History) != 6 {
		t.Fatalf("retries must not advance the recovered incident: %+v", again)
	}
	if len(again.Links) != 1 || again.Links[0] != "e2" {
		t.Fatalf("retries must not move links: %+v", again.Links)
	}
}

// TestReplayRejectsIllegalUnlink is the recovery-side guard: an unlink
// record that contradicts the rebuilt state (incident resolved, or no
// current link to the target) aborts startup and leaves the already-loaded
// state untouched, even when a trailing record would fit the version the
// bad unlink would have reached.
func TestReplayRejectsIllegalUnlink(t *testing.T) {
	t0 := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	at := func(sec int) time.Time { return t0.Add(time.Duration(sec) * time.Second) }
	lookup := &fakeEvents{services: map[string]string{"e1": "gateway"}}

	cases := []struct {
		name        string
		prefix      []Record
		bad         Record
		follow      Record
		wantVersion int
		wantLinks   []string
		wantStatus  string
		wantErrPart string
	}{
		{
			name: "unlink target never linked",
			prefix: []Record{
				NewCreationRecord(Creation{ID: "I1", Title: "T", Service: "gateway", Operator: "alice"}, at(0)),
			},
			bad: NewActionRecord(Request{IncidentID: "I1", ActionID: "u1", Operator: "bob",
				ExpectedVersion: 1, Type: ActionUnlinkEvent, Content: "ghost-event"}, at(1)),
			// A note written against the version the bad unlink would reach; an
			// accepting replay would happily apply it too.
			follow: NewActionRecord(Request{IncidentID: "I1", ActionID: "n1", Operator: "bob",
				ExpectedVersion: 2, Type: ActionNote, Content: "after the bad unlink"}, at(2)),
			wantVersion: 1,
			wantLinks:   []string{},
			wantStatus:  StatusOpen,
			wantErrPart: "without a link",
		},
		{
			name: "unlink an already-unlinked event",
			prefix: []Record{
				NewCreationRecord(Creation{ID: "I1", Title: "T", Service: "gateway", Operator: "alice"}, at(0)),
				NewActionRecord(Request{IncidentID: "I1", ActionID: "l1", Operator: "bob",
					ExpectedVersion: 1, Type: ActionLinkEvent, Content: "e1"}, at(1)),
				NewActionRecord(Request{IncidentID: "I1", ActionID: "u1", Operator: "bob",
					ExpectedVersion: 2, Type: ActionUnlinkEvent, Content: "e1"}, at(2)),
			},
			bad: NewActionRecord(Request{IncidentID: "I1", ActionID: "u2", Operator: "bob",
				ExpectedVersion: 3, Type: ActionUnlinkEvent, Content: "e1"}, at(3)),
			follow: NewActionRecord(Request{IncidentID: "I1", ActionID: "n1", Operator: "bob",
				ExpectedVersion: 4, Type: ActionNote, Content: "after the bad unlink"}, at(4)),
			wantVersion: 3,
			wantLinks:   []string{},
			wantStatus:  StatusOpen,
			wantErrPart: "without a link",
		},
		{
			name: "unlink while resolved",
			prefix: []Record{
				NewCreationRecord(Creation{ID: "I1", Title: "T", Service: "gateway", Operator: "alice"}, at(0)),
				NewActionRecord(Request{IncidentID: "I1", ActionID: "l1", Operator: "bob",
					ExpectedVersion: 1, Type: ActionLinkEvent, Content: "e1"}, at(1)),
				NewActionRecord(Request{IncidentID: "I1", ActionID: "r1", Operator: "bob",
					ExpectedVersion: 2, Type: ActionResolve, Content: "done"}, at(2)),
			},
			bad: NewActionRecord(Request{IncidentID: "I1", ActionID: "u1", Operator: "bob",
				ExpectedVersion: 3, Type: ActionUnlinkEvent, Content: "e1"}, at(3)),
			follow: NewActionRecord(Request{IncidentID: "I1", ActionID: "n1", Operator: "bob",
				ExpectedVersion: 4, Type: ActionNote, Content: "after the bad unlink"}, at(4)),
			wantVersion: 3,
			wantLinks:   []string{"e1"},
			wantStatus:  StatusResolved,
			wantErrPart: "resolved incident",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// The bad record must be flawless apart from the unlink rule.
			if err := assertRecordWellFormed(tc.bad); err != nil {
				t.Fatalf("bad record: %v", err)
			}
			if err := assertRecordWellFormed(tc.follow); err != nil {
				t.Fatalf("following record: %v", err)
			}

			// Parity: the live request path rejects the same request.
			live := NewRegistry(lookup, nil)
			for _, record := range tc.prefix {
				switch record.Kind {
				case RecordCreation:
					if _, err := live.Create(*record.Creation); err != nil {
						t.Fatalf("live prefix creation: %v", err)
					}
				case RecordAction:
					if _, err := live.Apply(record.Action.Request); err != nil {
						t.Fatalf("live prefix action: %v", err)
					}
				}
			}
			_, liveErr := live.Apply(tc.bad.Action.Request)
			if !IsConflictError(liveErr) || CurrentVersionOf(liveErr) != tc.wantVersion {
				t.Fatalf("live path must reject the unlink at version %d: %v", tc.wantVersion, liveErr)
			}

			for _, withFollow := range []bool{false, true} {
				placement := "at the tail"
				if withFollow {
					placement = "ahead of another record"
				}
				t.Run(placement, func(t *testing.T) {
					sequence := append(append([]Record{}, tc.prefix...), tc.bad)
					if withFollow {
						sequence = append(sequence, tc.follow)
					}
					reg := NewRegistry(lookup, nil)
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
					if !strings.Contains(err.Error(), tc.wantErrPart) {
						t.Fatalf("error %q must mention %q", err.Error(), tc.wantErrPart)
					}

					inc, _ := reg.Get("I1")
					if inc.Version != tc.wantVersion || inc.Status != tc.wantStatus {
						t.Fatalf("state must stay at the prefix: %+v", inc)
					}
					if len(inc.Links) != len(tc.wantLinks) {
						t.Fatalf("links %+v want %+v", inc.Links, tc.wantLinks)
					}
					for i, want := range tc.wantLinks {
						if inc.Links[i] != want {
							t.Fatalf("links %+v want %+v", inc.Links, tc.wantLinks)
						}
					}
					if len(inc.History) != tc.wantVersion {
						t.Fatalf("rejected record and follower must not become history: %+v", inc.History)
					}
				})
			}
		})
	}
}
