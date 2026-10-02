package incidents

import (
	"reflect"
	"testing"
	"time"
)

func TestCreateSeedsOperatorAsSoleParticipantAndOwner(t *testing.T) {
	reg, _, _ := newTestRegistry(&fakeEvents{})
	in := validCreation()
	in.Operator = " Alice "
	if _, err := reg.Create(in); err != nil {
		t.Fatal(err)
	}
	inc, err := reg.Get("INC-1")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(inc.Participants, []string{"Alice"}) {
		t.Fatalf("participants %+v", inc.Participants)
	}
	if inc.Owner != "Alice" {
		t.Fatalf("owner %q", inc.Owner)
	}
}

func TestParticipantLifecycle(t *testing.T) {
	reg, commit, _ := newTestRegistry(&fakeEvents{})
	if _, err := reg.Create(validCreation()); err != nil {
		t.Fatal(err)
	}

	add := func(actionID string, version int, name string) {
		t.Helper()
		res, err := reg.Apply(Request{IncidentID: "INC-1", ActionID: actionID, Operator: "alice",
			ExpectedVersion: version, Type: ActionAddParticipant, Content: name})
		if err != nil {
			t.Fatalf("add %q: %v", name, err)
		}
		if res.Version != version+1 {
			t.Fatalf("add %q result version %d", name, res.Version)
		}
	}

	// Whitespace is trimmed, interior case kept; listing is sorted by name.
	add("p1", 1, " Carol ")
	add("p2", 2, "bob")
	inc, _ := reg.Get("INC-1")
	if !reflect.DeepEqual(inc.Participants, []string{"Alice", "Carol", "bob"}) {
		t.Fatalf("participants sorted, case kept: %+v", inc.Participants)
	}

	// Duplicate add is a 409-style conflict carrying the current version.
	_, err := reg.Apply(Request{IncidentID: "INC-1", ActionID: "p3", Operator: "alice",
		ExpectedVersion: 3, Type: ActionAddParticipant, Content: "bob"})
	if !IsConflictError(err) || CurrentVersionOf(err) != 3 {
		t.Fatalf("duplicate add: %v", err)
	}

	// Names differing only in case are distinct people.
	add("p4", 3, "Bob")
	inc, _ = reg.Get("INC-1")
	if !reflect.DeepEqual(inc.Participants, []string{"Alice", "Bob", "Carol", "bob"}) {
		t.Fatalf("case-sensitive names: %+v", inc.Participants)
	}

	// Hand ownership to a participant.
	res, err := reg.Apply(Request{IncidentID: "INC-1", ActionID: "o1", Operator: "alice",
		ExpectedVersion: 4, Type: ActionAssignOwner, Content: "Carol"})
	if err != nil {
		t.Fatalf("assign: %v", err)
	}
	if res.Version != 5 {
		t.Fatalf("assign version %d", res.Version)
	}
	inc, _ = reg.Get("INC-1")
	if inc.Owner != "Carol" {
		t.Fatalf("owner after handover %q", inc.Owner)
	}
	// Handing over leaves the membership unchanged.
	if !reflect.DeepEqual(inc.Participants, []string{"Alice", "Bob", "Carol", "bob"}) {
		t.Fatalf("membership after handover: %+v", inc.Participants)
	}

	// The current owner cannot be removed; a non-owner participant can.
	_, err = reg.Apply(Request{IncidentID: "INC-1", ActionID: "p5", Operator: "alice",
		ExpectedVersion: 5, Type: ActionRemoveParticipant, Content: "Carol"})
	if !IsConflictError(err) || CurrentVersionOf(err) != 5 {
		t.Fatalf("remove owner: %v", err)
	}
	if _, err := reg.Apply(Request{IncidentID: "INC-1", ActionID: "p6", Operator: "alice",
		ExpectedVersion: 5, Type: ActionRemoveParticipant, Content: "stranger"}); !IsConflictError(err) {
		t.Fatalf("remove non-member: %v", err)
	}
	if _, err := reg.Apply(Request{IncidentID: "INC-1", ActionID: "p7", Operator: "alice",
		ExpectedVersion: 5, Type: ActionAssignOwner, Content: "stranger"}); !IsConflictError(err) {
		t.Fatalf("assign non-member: %v", err)
	}
	if _, err := reg.Apply(Request{IncidentID: "INC-1", ActionID: "p8", Operator: "alice",
		ExpectedVersion: 5, Type: ActionAssignOwner, Content: "Carol"}); !IsConflictError(err) {
		t.Fatalf("assign current owner: %v", err)
	}

	if _, err := reg.Apply(Request{IncidentID: "INC-1", ActionID: "p9", Operator: "alice",
		ExpectedVersion: 5, Type: ActionRemoveParticipant, Content: "bob"}); err != nil {
		t.Fatalf("remove member: %v", err)
	}
	inc, _ = reg.Get("INC-1")
	if inc.Owner != "Carol" {
		t.Fatalf("owner must survive removals: %q", inc.Owner)
	}
	if !reflect.DeepEqual(inc.Participants, []string{"Alice", "Bob", "Carol"}) {
		t.Fatalf("membership after remove: %+v", inc.Participants)
	}

	// Rejected actions leave version/history untouched: version moved 1->6
	// through p1, p2, p4, o1, p9 only.
	if inc.Version != 6 {
		t.Fatalf("version after accepted actions %d", inc.Version)
	}
	if len(inc.History) != 6 {
		t.Fatalf("history after accepted actions %d", len(inc.History))
	}
	if commit.count() != 6 {
		t.Fatalf("durable records %d", commit.count())
	}
}

func TestPersonnelActionsOnlyWhileOpen(t *testing.T) {
	reg, _, _ := newTestRegistry(&fakeEvents{})
	_, _ = reg.Create(validCreation())
	_, _ = reg.Apply(Request{IncidentID: "INC-1", ActionID: "r1", Operator: "alice",
		ExpectedVersion: 1, Type: ActionResolve, Content: "done"})

	for _, tc := range []struct {
		name   string
		action string
	}{
		{"add", ActionAddParticipant},
		{"remove", ActionRemoveParticipant},
		{"assign", ActionAssignOwner},
	} {
		_, err := reg.Apply(Request{IncidentID: "INC-1", ActionID: "x-" + tc.name, Operator: "alice",
			ExpectedVersion: 2, Type: tc.action, Content: "zoe"})
		if !IsConflictError(err) || CurrentVersionOf(err) != 2 {
			t.Fatalf("%s on resolved: %v", tc.name, err)
		}
	}

	// Reopen keeps the people, and a personnel action works again.
	if _, err := reg.Apply(Request{IncidentID: "INC-1", ActionID: "o1", Operator: "alice",
		ExpectedVersion: 2, Type: ActionReopen, Content: "regression"}); err != nil {
		t.Fatal(err)
	}
	inc, _ := reg.Get("INC-1")
	if inc.Owner != "Alice" || !reflect.DeepEqual(inc.Participants, []string{"Alice"}) {
		t.Fatalf("resolve/reopen must keep people: %+v", inc)
	}
	if _, err := reg.Apply(Request{IncidentID: "INC-1", ActionID: "p1", Operator: "alice",
		ExpectedVersion: 3, Type: ActionAddParticipant, Content: "zoe"}); err != nil {
		t.Fatalf("add after reopen: %v", err)
	}
}

func TestPersonnelActionValidation(t *testing.T) {
	reg, _, _ := newTestRegistry(&fakeEvents{})
	_, _ = reg.Create(validCreation())

	base := Request{IncidentID: "INC-1", ActionID: "a", Operator: "alice",
		ExpectedVersion: 1, Type: ActionAddParticipant}
	for name, mutate := range map[string]func(Request) Request{
		"blank participant": func(r Request) Request { r.Content = "  "; return r },
		"remove blank":      func(r Request) Request { r.Type = ActionRemoveParticipant; r.Content = "\t"; return r },
		"assign blank":      func(r Request) Request { r.Type = ActionAssignOwner; r.Content = ""; return r },
	} {
		if _, err := reg.Apply(mutate(base)); !IsValidationError(err) {
			t.Fatalf("%s: want validation, got %v", name, err)
		}
	}
}

func TestNoteOperatorsDoNotJoinParticipants(t *testing.T) {
	reg, _, _ := newTestRegistry(&fakeEvents{})
	_, _ = reg.Create(validCreation())
	if _, err := reg.Apply(Request{IncidentID: "INC-1", ActionID: "n1", Operator: "mallory",
		ExpectedVersion: 1, Type: ActionNote, Content: "hi"}); err != nil {
		t.Fatal(err)
	}
	inc, _ := reg.Get("INC-1")
	if !reflect.DeepEqual(inc.Participants, []string{"Alice"}) || inc.Owner != "Alice" {
		t.Fatalf("note operator must not join: %+v", inc)
	}
}

func TestPersonnelActionReplayAcrossChanges(t *testing.T) {
	reg, _, _ := newTestRegistry(&fakeEvents{})
	_, _ = reg.Create(validCreation())

	// Add Carol, add Eve, hand ownership to Eve, remove Carol, resolve.
	first := Request{IncidentID: "INC-1", ActionID: "p1", Operator: "alice",
		ExpectedVersion: 1, Type: ActionAddParticipant, Content: "Carol"}
	res, err := reg.Apply(first)
	if err != nil {
		t.Fatal(err)
	}
	must := func(req Request) {
		t.Helper()
		if _, err := reg.Apply(req); err != nil {
			t.Fatal(err)
		}
	}
	must(Request{IncidentID: "INC-1", ActionID: "p2", Operator: "alice",
		ExpectedVersion: 2, Type: ActionAddParticipant, Content: "Eve"})
	must(Request{IncidentID: "INC-1", ActionID: "o1", Operator: "alice",
		ExpectedVersion: 3, Type: ActionAssignOwner, Content: "Eve"})
	must(Request{IncidentID: "INC-1", ActionID: "p3", Operator: "eve",
		ExpectedVersion: 4, Type: ActionRemoveParticipant, Content: "Carol"})
	must(Request{IncidentID: "INC-1", ActionID: "r1", Operator: "eve",
		ExpectedVersion: 5, Type: ActionResolve, Content: "done"})

	// Even with the incident resolved and Carol long removed, retrying the
	// original add returns its first result and restores nothing: Carol stays
	// absent, Eve stays owner, and the version does not move.
	replay, err := reg.Apply(first)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if replay != res {
		t.Fatalf("replay %+v want %+v", replay, res)
	}
	inc, _ := reg.Get("INC-1")
	if inc.Version != 6 {
		t.Fatalf("replay must not advance version: %d", inc.Version)
	}
	if !reflect.DeepEqual(inc.Participants, []string{"Alice", "Eve"}) {
		t.Fatalf("replay must not restore membership: %+v", inc.Participants)
	}
	if inc.Owner != "Eve" {
		t.Fatalf("replay must not touch owner: %q", inc.Owner)
	}

	// Same action id with changed content is a conflict even after resolution.
	_, err = reg.Apply(Request{IncidentID: "INC-1", ActionID: "p1", Operator: "alice",
		ExpectedVersion: 6, Type: ActionAddParticipant, Content: "Dave"})
	if !IsConflictError(err) {
		t.Fatalf("changed replay: %v", err)
	}
}

func TestPersonnelReplayRebuildsState(t *testing.T) {
	times := seededTimes(7)
	records := []Record{
		NewCreationRecord(Creation{ID: "I1", Title: "T", Service: "gateway", Operator: "alice"}, times[0]),
		NewActionRecord(Request{IncidentID: "I1", ActionID: "p1", Operator: "alice", ExpectedVersion: 1, Type: ActionAddParticipant, Content: "bob"}, times[1]),
		NewActionRecord(Request{IncidentID: "I1", ActionID: "p2", Operator: "alice", ExpectedVersion: 2, Type: ActionAddParticipant, Content: "carol"}, times[2]),
		NewActionRecord(Request{IncidentID: "I1", ActionID: "o1", Operator: "alice", ExpectedVersion: 3, Type: ActionAssignOwner, Content: "bob"}, times[3]),
		NewActionRecord(Request{IncidentID: "I1", ActionID: "r1", Operator: "bob", ExpectedVersion: 4, Type: ActionResolve, Content: "done"}, times[4]),
		NewActionRecord(Request{IncidentID: "I1", ActionID: "o2", Operator: "bob", ExpectedVersion: 5, Type: ActionReopen, Content: "again"}, times[5]),
		NewActionRecord(Request{IncidentID: "I1", ActionID: "p3", Operator: "bob", ExpectedVersion: 6, Type: ActionRemoveParticipant, Content: "carol"}, times[6]),
	}

	reg := NewRegistry(&fakeEvents{}, nil)
	for _, record := range records {
		if err := reg.Load(record); err != nil {
			t.Fatalf("load: %v", err)
		}
	}
	inc, _ := reg.Get("I1")
	if inc.Owner != "bob" {
		t.Fatalf("rebuilt owner %q", inc.Owner)
	}
	if !reflect.DeepEqual(inc.Participants, []string{"alice", "bob"}) {
		t.Fatalf("rebuilt participants %+v", inc.Participants)
	}
	if inc.Version != 7 {
		t.Fatalf("rebuilt version %d", inc.Version)
	}

	// History content carries the normalized target names.
	last := inc.History[len(inc.History)-1]
	if last.Action != ActionRemoveParticipant || last.Content != "carol" {
		t.Fatalf("last history entry %+v", last)
	}
}

// seededTimes returns n strictly increasing UTC instants for replay fixtures.
func seededTimes(n int) []time.Time {
	times := make([]time.Time, n)
	base := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	for i := range times {
		times[i] = base.Add(time.Duration(i+1) * time.Second)
	}
	return times
}
