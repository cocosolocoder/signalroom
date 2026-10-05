package incidents

import (
	"errors"
	"testing"
	"time"
)

// TestLinkConflictPrecedence pins the order in which the link path reports
// problems: a stale expected_version is a conflict even when the event is
// also missing; a resolved incident conflicts on its state even when the
// event would otherwise 404.
func TestLinkConflictPrecedence(t *testing.T) {
	lookup := &fakeEvents{services: map[string]string{"e1": "gateway"}}
	reg, _, _ := newTestRegistry(lookup)
	_, _ = reg.Create(validCreation())
	_, _ = reg.Apply(Request{IncidentID: "INC-1", ActionID: "r1", Operator: "bob",
		ExpectedVersion: 1, Type: ActionResolve, Content: "fixed"})

	// Stale version plus an event that does not exist: the version conflict
	// wins and must still carry the current version.
	_, err := reg.Apply(Request{IncidentID: "INC-1", ActionID: "l1", Operator: "bob",
		ExpectedVersion: 1, Type: ActionLinkEvent, Content: "missing"})
	var conflict *ConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("stale version + missing event: want conflict, got %v", err)
	}
	if conflict.CurrentVersion != 2 {
		t.Fatalf("current version reported as %d, want 2", conflict.CurrentVersion)
	}

	// Current version on a resolved incident plus a missing event: the state
	// conflict wins over the 404 the event would earn on its own.
	_, err = reg.Apply(Request{IncidentID: "INC-1", ActionID: "l2", Operator: "bob",
		ExpectedVersion: 2, Type: ActionLinkEvent, Content: "missing"})
	if !IsConflictError(err) || CurrentVersionOf(err) != 2 {
		t.Fatalf("resolved + missing event: want state conflict, got %v", err)
	}

	// Neither rejection may have moved the incident.
	inc, _ := reg.Get("INC-1")
	if inc.Version != 2 || inc.Status != StatusResolved || len(inc.Links) != 0 || len(inc.History) != 2 {
		t.Fatalf("rejected links must leave state untouched: %+v", inc)
	}
}

// TestLinkIdempotentReplayAcrossResolution retries a successful link with
// its original, now-stale content after the incident resolved: it returns
// the first result and adds nothing, while the same action id with changed
// content conflicts.
func TestLinkIdempotentReplayAcrossResolution(t *testing.T) {
	lookup := &fakeEvents{services: map[string]string{"e1": "gateway"}}
	reg, commit, _ := newTestRegistry(lookup)
	_, _ = reg.Create(validCreation())
	first := Request{IncidentID: "INC-1", ActionID: "l1", Operator: "bob",
		ExpectedVersion: 1, Type: ActionLinkEvent, Content: "e1"}
	res, err := reg.Apply(first)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reg.Apply(Request{IncidentID: "INC-1", ActionID: "r1", Operator: "bob",
		ExpectedVersion: 2, Type: ActionResolve, Content: "fixed"}); err != nil {
		t.Fatal(err)
	}

	replay, err := reg.Apply(first)
	if err != nil {
		t.Fatalf("replay across resolution: %v", err)
	}
	if replay != res {
		t.Fatalf("replay %+v want first result %+v", replay, res)
	}
	inc, _ := reg.Get("INC-1")
	if inc.Version != 3 || len(inc.Links) != 1 || len(inc.History) != 3 {
		t.Fatalf("replay must add nothing: %+v", inc)
	}
	if commit.count() != 3 { // create + link + resolve
		t.Fatalf("commit count %d", commit.count())
	}

	// Same action id, changed event id: still a conflict after resolution.
	_, err = reg.Apply(Request{IncidentID: "INC-1", ActionID: "l1", Operator: "bob",
		ExpectedVersion: 3, Type: ActionLinkEvent, Content: "other"})
	if !IsConflictError(err) {
		t.Fatalf("changed link content: want conflict, got %v", err)
	}
}

// TestReplayRejectsEveryLinkViolation feeds complete, individually
// well-formed link records that nonetheless break a rule against the
// already-loaded state; startup recovery must reject each one.
func TestReplayRejectsEveryLinkViolation(t *testing.T) {
	lookup := &fakeEvents{services: map[string]string{"e1": "gateway", "other": "checkout"}}
	t0 := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	creation := NewCreationRecord(Creation{ID: "I1", Title: "T", Service: "gateway", Operator: "alice"}, t0)
	link := func(actionID string, expected int, event string, at time.Time) Record {
		return NewActionRecord(Request{IncidentID: "I1", ActionID: actionID, Operator: "bob",
			ExpectedVersion: expected, Type: ActionLinkEvent, Content: event}, at)
	}

	t.Run("link on resolved incident", func(t *testing.T) {
		reg := NewRegistry(lookup, nil)
		for _, rec := range []Record{
			creation,
			NewActionRecord(Request{IncidentID: "I1", ActionID: "r1", Operator: "bob",
				ExpectedVersion: 1, Type: ActionResolve, Content: "fixed"}, t0.Add(time.Second)),
			link("l1", 2, "e1", t0.Add(2*time.Second)),
		} {
			if err := reg.Load(rec); err != nil {
				if rec.Kind == RecordAction && rec.Action.Request.ActionID == "l1" {
					return
				}
				t.Fatalf("unexpected load error: %v", err)
			}
		}
		t.Fatal("link on a resolved incident must fail recovery")
	})

	t.Run("link twice", func(t *testing.T) {
		reg := NewRegistry(lookup, nil)
		if err := reg.Load(creation); err != nil {
			t.Fatal(err)
		}
		if err := reg.Load(link("l1", 1, "e1", t0.Add(time.Second))); err != nil {
			t.Fatal(err)
		}
		if err := reg.Load(link("l2", 2, "e1", t0.Add(2*time.Second))); err == nil {
			t.Fatal("duplicating an existing link must fail recovery")
		}
		// State after the failed Load is exactly the first committed link.
		inc, _ := reg.Get("I1")
		if inc.Version != 2 || len(inc.Links) != 1 || inc.Links[0] != "e1" {
			t.Fatalf("failed replay must leave state untouched: %+v", inc)
		}
	})

	t.Run("missing event", func(t *testing.T) {
		reg := NewRegistry(lookup, nil)
		if err := reg.Load(creation); err != nil {
			t.Fatal(err)
		}
		if err := reg.Load(link("l1", 1, "gone", t0.Add(time.Second))); err == nil {
			t.Fatal("link to a vanished event must fail recovery")
		}
	})

	t.Run("wrong service", func(t *testing.T) {
		reg := NewRegistry(lookup, nil)
		if err := reg.Load(creation); err != nil {
			t.Fatal(err)
		}
		if err := reg.Load(link("l1", 1, "other", t0.Add(time.Second))); err == nil {
			t.Fatal("link to another service must fail recovery")
		}
	})

	t.Run("no event source at all", func(t *testing.T) {
		reg := NewRegistry(nil, nil)
		if err := reg.Load(creation); err != nil {
			t.Fatal(err)
		}
		if err := reg.Load(link("l1", 1, "e1", t0.Add(time.Second))); err == nil {
			t.Fatal("link without an event source must fail recovery")
		}
	})
}
