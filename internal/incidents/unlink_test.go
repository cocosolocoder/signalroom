package incidents

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

// linkReq builds a link_event request for id at an expected version.
func linkReq(actionID string, version int, eventID string) Request {
	return Request{IncidentID: "INC-1", ActionID: actionID, Operator: "bob",
		ExpectedVersion: version, Type: ActionLinkEvent, Content: eventID}
}

// unlinkReq builds an unlink_event request for id at an expected version.
func unlinkReq(actionID string, version int, eventID string) Request {
	return Request{IncidentID: "INC-1", ActionID: actionID, Operator: "bob",
		ExpectedVersion: version, Type: ActionUnlinkEvent, Content: eventID}
}

// TestUnlinkRemovesOneLinkAndKeepsOrder is the core happy path: version
// advances once, only the named event leaves the list and the survivors keep
// their relative order, the original link history entry stays and an unlink
// entry is appended with the target event id as its content, and people and
// status are untouched.
func TestUnlinkRemovesOneLinkAndKeepsOrder(t *testing.T) {
	lookup := &fakeEvents{services: map[string]string{
		"e1": "gateway", "e2": "gateway", "e3": "gateway",
	}}
	reg, commit, _ := newTestRegistry(lookup)
	_, _ = reg.Create(validCreation())

	for i, req := range []Request{
		linkReq("l1", 1, "e1"),
		linkReq("l2", 2, "e2"),
		linkReq("l3", 3, "e3"),
	} {
		if _, err := reg.Apply(req); err != nil {
			t.Fatalf("link %d: %v", i, err)
		}
	}

	res, err := reg.Apply(unlinkReq("u1", 4, " e2 "))
	if err != nil {
		t.Fatalf("unlink: %v", err)
	}
	if res != (ActionResult{IncidentID: "INC-1", ActionID: "u1", Version: 5}) {
		t.Fatalf("unlink result %+v", res)
	}

	inc, _ := reg.Get("INC-1")
	if inc.Version != 5 {
		t.Fatalf("version %d", inc.Version)
	}
	if !reflect.DeepEqual(inc.Links, []string{"e1", "e3"}) {
		t.Fatalf("survivors keep order: %+v", inc.Links)
	}
	if inc.Status != StatusOpen {
		t.Fatalf("status changed: %q", inc.Status)
	}
	if inc.Owner != "Alice" || !reflect.DeepEqual(inc.Participants, []string{"Alice"}) {
		t.Fatalf("people must not change: owner=%q participants=%+v", inc.Owner, inc.Participants)
	}
	if len(inc.History) != 5 {
		t.Fatalf("history len %d", len(inc.History))
	}
	last := inc.History[4]
	if last.Action != ActionUnlinkEvent || last.ActionID != "u1" ||
		last.Operator != "bob" || last.Content != "e2" || last.Version != 5 {
		t.Fatalf("unlink history entry %+v", last)
	}
	// The original link record remains in history, ahead of the unlink.
	if inc.History[2].Action != ActionLinkEvent || inc.History[2].Content != "e2" {
		t.Fatalf("original link entry must be retained: %+v", inc.History[2])
	}
	if commit.count() != 5 { // create + 3 links + unlink
		t.Fatalf("durable records %d", commit.count())
	}
}

// TestUnlinkLastEventReturnsEmptyList covers the tail removal: the detail
// links become an empty list rather than nil, and later links start a fresh
// list.
func TestUnlinkLastEventReturnsEmptyList(t *testing.T) {
	lookup := &fakeEvents{services: map[string]string{"e1": "gateway", "e2": "gateway"}}
	reg, _, _ := newTestRegistry(lookup)
	_, _ = reg.Create(validCreation())
	_, _ = reg.Apply(linkReq("l1", 1, "e1"))
	if _, err := reg.Apply(unlinkReq("u1", 2, "e1")); err != nil {
		t.Fatalf("unlink: %v", err)
	}
	inc, _ := reg.Get("INC-1")
	if inc.Links == nil || len(inc.Links) != 0 {
		t.Fatalf("last removal yields an empty list: %+v", inc.Links)
	}

	// A new link after the list emptied works and is the sole member.
	if _, err := reg.Apply(linkReq("l2", 3, "e2")); err != nil {
		t.Fatalf("relink after emptying: %v", err)
	}
	inc, _ = reg.Get("INC-1")
	if !reflect.DeepEqual(inc.Links, []string{"e2"}) {
		t.Fatalf("fresh link after unlink: %+v", inc.Links)
	}
}

// TestUnlinkNotLinkedConflicts covers the 409 rules: an id that is simply
// not linked, an id that does not even exist as an event, and an id already
// removed. None consult the event store, and every rejection carries the
// current version and changes nothing.
func TestUnlinkNotLinkedConflicts(t *testing.T) {
	lookup := &fakeEvents{services: map[string]string{"e1": "gateway", "eX": "checkout"}}

	cases := []struct {
		name      string
		seed      func(*Registry)
		req       Request
		current   int
		wantLinks []string
	}{
		{
			name:      "never linked, event exists in another service",
			seed:      func(r *Registry) {},
			req:       unlinkReq("u2", 2, "eX"),
			current:   2,
			wantLinks: []string{"e1"},
		},
		{
			name:      "never linked, event does not exist",
			seed:      func(r *Registry) {},
			req:       unlinkReq("u3", 2, "ghost"),
			current:   2,
			wantLinks: []string{"e1"},
		},
		{
			name: "already unlinked",
			seed: func(r *Registry) {
				if _, err := r.Apply(unlinkReq("u1", 2, "e1")); err != nil {
					t.Fatalf("seed unlink: %v", err)
				}
			},
			req:       unlinkReq("u4", 3, "e1"),
			current:   3,
			wantLinks: []string{},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reg, commit, _ := newTestRegistry(lookup)
			_, _ = reg.Create(validCreation())
			_, _ = reg.Apply(linkReq("l1", 1, "e1"))
			tc.seed(reg)

			_, err := reg.Apply(tc.req)
			var conflict *ConflictError
			if !errors.As(err, &conflict) {
				t.Fatalf("want conflict, got %v", err)
			}
			if IsNotFoundError(err) {
				t.Fatalf("an unlinked id must be 409 even when the event is missing: %v", err)
			}
			if conflict.CurrentVersion != tc.current {
				t.Fatalf("current version reported as %d, want %d", conflict.CurrentVersion, tc.current)
			}
			inc, _ := reg.Get("INC-1")
			if inc.Version != tc.current || len(inc.History) != tc.current {
				t.Fatalf("rejected unlink must change nothing: %+v", inc)
			}
			if !reflect.DeepEqual(inc.Links, tc.wantLinks) {
				t.Fatalf("links %+v want %+v", inc.Links, tc.wantLinks)
			}
			if commit.count() != tc.current {
				t.Fatalf("rejected unlink must not persist, count=%d", commit.count())
			}
		})
	}
}

// TestUnlinkOnResolvedIncidentRejected mirrors the resolved gate: only
// reopen is accepted while resolved.
func TestUnlinkOnResolvedIncidentRejected(t *testing.T) {
	lookup := &fakeEvents{services: map[string]string{"e1": "gateway"}}
	reg, commit, _ := newTestRegistry(lookup)
	_, _ = reg.Create(validCreation())
	_, _ = reg.Apply(linkReq("l1", 1, "e1"))
	_, _ = reg.Apply(Request{IncidentID: "INC-1", ActionID: "r1", Operator: "bob",
		ExpectedVersion: 2, Type: ActionResolve, Content: "done"})

	_, err := reg.Apply(unlinkReq("u1", 3, "e1"))
	if !IsConflictError(err) || CurrentVersionOf(err) != 3 {
		t.Fatalf("unlink on resolved incident: want 409 at v3, got %v", err)
	}
	inc, _ := reg.Get("INC-1")
	if inc.Status != StatusResolved || inc.Version != 3 ||
		!reflect.DeepEqual(inc.Links, []string{"e1"}) || len(inc.History) != 3 {
		t.Fatalf("rejected unlink must leave everything put: %+v", inc)
	}
	if commit.count() != 3 {
		t.Fatalf("rejected unlink must not persist, count=%d", commit.count())
	}

	// Reopen, and the unlink is now accepted against the advanced version.
	_, _ = reg.Apply(Request{IncidentID: "INC-1", ActionID: "o1", Operator: "bob",
		ExpectedVersion: 3, Type: ActionReopen, Content: "regression"})
	if _, err := reg.Apply(unlinkReq("u2", 4, "e1")); err != nil {
		t.Fatalf("unlink after reopen: %v", err)
	}
	inc, _ = reg.Get("INC-1")
	if inc.Version != 5 || len(inc.Links) != 0 {
		t.Fatalf("unlink after reopen: %+v", inc)
	}
}

// TestUnlinkVersionConflictAndMissingIncident pins the generic entry rules
// for the new action: incident absence is 404, a stale expected_version is
// 409 with the current version and wins before the link-membership check.
func TestUnlinkVersionConflictAndMissingIncident(t *testing.T) {
	lookup := &fakeEvents{services: map[string]string{"e1": "gateway"}}
	reg, commit, _ := newTestRegistry(lookup)
	_, _ = reg.Create(validCreation())
	_, _ = reg.Apply(linkReq("l1", 1, "e1"))

	_, err := reg.Apply(Request{IncidentID: "ghost", ActionID: "u0", Operator: "bob",
		ExpectedVersion: 1, Type: ActionUnlinkEvent, Content: "ghost"})
	if !IsNotFoundError(err) {
		t.Fatalf("missing incident: want 404, got %v", err)
	}

	_, err = reg.Apply(unlinkReq("u9", 99, "ghost"))
	if !IsConflictError(err) || CurrentVersionOf(err) != 2 {
		t.Fatalf("stale version wins over not-linked: %v", err)
	}
	inc, _ := reg.Get("INC-1")
	if inc.Version != 2 || len(inc.Links) != 1 || len(inc.History) != 2 {
		t.Fatalf("rejected unlink must change nothing: %+v", inc)
	}
	if commit.count() != 2 {
		t.Fatalf("rejected unlink must not persist, count=%d", commit.count())
	}
}

// TestUnlinkValidation mirrors link normalization: surrounding whitespace is
// trimmed while interior case is kept, and a blank id is a validation error.
func TestUnlinkValidation(t *testing.T) {
	reg, _, _ := newTestRegistry(&fakeEvents{})
	_, _ = reg.Create(validCreation())
	base := unlinkReq("u1", 1, "e1")
	for name, mutate := range map[string]func(Request) Request{
		"blank id":       func(r Request) Request { r.Content = "  "; return r },
		"empty action":   func(r Request) Request { r.ActionID = " "; return r },
		"zero version":   func(r Request) Request { r.ExpectedVersion = 0; return r },
		"empty operator": func(r Request) Request { r.Operator = ""; return r },
	} {
		if _, err := reg.Apply(mutate(base)); !IsValidationError(err) {
			t.Fatalf("%s: want validation, got %v", name, err)
		}
	}

	// Interior case is kept: "E1" is a different id and is therefore reported
	// as not linked rather than removing "e1".
	lookup := &fakeEvents{services: map[string]string{"e1": "gateway"}}
	reg2, _, _ := newTestRegistry(lookup)
	_, _ = reg2.Create(validCreation())
	_, _ = reg2.Apply(linkReq("l1", 1, "e1"))
	_, err := reg2.Apply(unlinkReq("u1", 2, "E1"))
	if !IsConflictError(err) {
		t.Fatalf("case must be preserved: want not-linked conflict, got %v", err)
	}
}

// TestUnlinkIdempotentAcrossRelink is the crucial idempotency regression: the
// successful unlink retry keeps returning its first result even though the
// event has since been linked again, and a retry of the original link cannot
// put a removed event back. The relink appends at the end of the list.
func TestUnlinkIdempotentAcrossRelink(t *testing.T) {
	lookup := &fakeEvents{services: map[string]string{"e1": "gateway", "e2": "gateway"}}
	reg, commit, _ := newTestRegistry(lookup)
	_, _ = reg.Create(validCreation())
	firstLink := linkReq("l1", 1, "e1")
	_, _ = reg.Apply(firstLink)
	firstUnlink := unlinkReq("u1", 2, "e1")
	unlinkRes, err := reg.Apply(firstUnlink)
	if err != nil {
		t.Fatalf("unlink: %v", err)
	}
	if unlinkRes != (ActionResult{IncidentID: "INC-1", ActionID: "u1", Version: 3}) {
		t.Fatalf("unlink result %+v", unlinkRes)
	}

	// While e1 is absent, replaying the original link returns its first
	// result (version 2) but must not put the unlinked event back.
	staleLinkReplay, err := reg.Apply(firstLink)
	if err != nil {
		t.Fatalf("link retry while absent: %v", err)
	}
	if staleLinkReplay != (ActionResult{IncidentID: "INC-1", ActionID: "l1", Version: 2}) {
		t.Fatalf("link retry while absent %+v", staleLinkReplay)
	}
	mid, _ := reg.Get("INC-1")
	if mid.Version != 3 || len(mid.Links) != 0 || len(mid.History) != 3 {
		t.Fatalf("link retry must not restore the removed event: %+v", mid)
	}

	// Link a second event, then re-link e1: e1 must land at the tail.
	_, _ = reg.Apply(linkReq("l2", 3, "e2"))
	_, _ = reg.Apply(linkReq("l3", 4, "e1"))

	// The unlink retry returns its first result (version 3) and must not
	// remove the re-linked e1.
	replay, err := reg.Apply(firstUnlink)
	if err != nil {
		t.Fatalf("unlink retry: %v", err)
	}
	if replay != unlinkRes {
		t.Fatalf("unlink retry %+v want %+v", replay, unlinkRes)
	}
	inc, _ := reg.Get("INC-1")
	if inc.Version != 5 || !reflect.DeepEqual(inc.Links, []string{"e2", "e1"}) {
		t.Fatalf("unlink retry must not remove the relink: %+v", inc)
	}
	if len(inc.History) != 5 {
		t.Fatalf("unlink retry must add no history: %+v", inc.History)
	}

	// The original link retry likewise returns its first result (version 2)
	// and adds nothing now that e1 happens to be linked again.
	linkReplay, err := reg.Apply(firstLink)
	if err != nil {
		t.Fatalf("link retry: %v", err)
	}
	if linkReplay != (ActionResult{IncidentID: "INC-1", ActionID: "l1", Version: 2}) {
		t.Fatalf("link retry %+v", linkReplay)
	}
	inc, _ = reg.Get("INC-1")
	if inc.Version != 5 || !reflect.DeepEqual(inc.Links, []string{"e2", "e1"}) {
		t.Fatalf("link retry must not mutate: %+v", inc)
	}

	// The whole link -> unlink -> relink story is visible in history.
	wantActions := []string{ActionCreate, ActionLinkEvent, ActionUnlinkEvent, ActionLinkEvent, ActionLinkEvent}
	for i, want := range wantActions {
		if inc.History[i].Action != want {
			t.Fatalf("history[%d]=%s want %s; full: %+v", i, inc.History[i].Action, want, inc.History)
		}
	}
	if commit.count() != 5 {
		t.Fatalf("retries must not persist, count=%d", commit.count())
	}

	// The same action id with different content stays a conflict.
	_, err = reg.Apply(unlinkReq("u1", 5, "e2"))
	if !IsConflictError(err) || CurrentVersionOf(err) != 5 {
		t.Fatalf("changed unlink content: want conflict at v5, got %v", err)
	}
}

// TestUnlinkIsScopedToOneIncident verifies that removing a link from one
// incident leaves the event queryable and the same event's link in another
// incident intact, with no status/people side effects.
func TestUnlinkIsScopedToOneIncident(t *testing.T) {
	lookup := &fakeEvents{services: map[string]string{"e1": "gateway"}}
	reg, _, _ := newTestRegistry(lookup)
	_, _ = reg.Create(Creation{ID: "A", Title: "t", Service: "gateway", Operator: "alice"})
	_, _ = reg.Create(Creation{ID: "B", Title: "t", Service: "gateway", Operator: "carol"})
	_, _ = reg.Apply(Request{IncidentID: "A", ActionID: "la", Operator: "alice",
		ExpectedVersion: 1, Type: ActionLinkEvent, Content: "e1"})
	_, _ = reg.Apply(Request{IncidentID: "B", ActionID: "lb", Operator: "carol",
		ExpectedVersion: 1, Type: ActionLinkEvent, Content: "e1"})

	if _, err := reg.Apply(Request{IncidentID: "A", ActionID: "ua", Operator: "alice",
		ExpectedVersion: 2, Type: ActionUnlinkEvent, Content: "e1"}); err != nil {
		t.Fatalf("unlink from A: %v", err)
	}

	a, _ := reg.Get("A")
	b, _ := reg.Get("B")
	if len(a.Links) != 0 {
		t.Fatalf("incident A links: %+v", a.Links)
	}
	if !reflect.DeepEqual(b.Links, []string{"e1"}) {
		t.Fatalf("incident B must keep its link: %+v", b.Links)
	}
	if service, ok := lookup.LookupEvent("e1"); !ok || service != "gateway" {
		t.Fatalf("event must remain queryable: service=%q ok=%v", service, ok)
	}
	if a.Owner != "alice" || b.Owner != "carol" || a.Status != StatusOpen || b.Status != StatusOpen {
		t.Fatalf("unlink must not change owner/status: %+v %+v", a, b)
	}

	// e1 can be linked to A again and lands at the tail of an empty list.
	if _, err := reg.Apply(Request{IncidentID: "A", ActionID: "la2", Operator: "alice",
		ExpectedVersion: 3, Type: ActionLinkEvent, Content: "e1"}); err != nil {
		t.Fatalf("relink to A: %v", err)
	}
	a, _ = reg.Get("A")
	if !reflect.DeepEqual(a.Links, []string{"e1"}) {
		t.Fatalf("relink lands at tail: %+v", a.Links)
	}
}

// TestUnlinkReplayRebuildsState pins restart recovery of a link -> unlink ->
// relink sequence: links, versions, history contents and saved timestamps
// must rebuild one for one, and a retried unlink still returns its first
// result after recovery.
func TestUnlinkReplayRebuildsState(t *testing.T) {
	lookup := &fakeEvents{services: map[string]string{"e1": "gateway", "e2": "gateway"}}
	times := seededTimes(6)
	records := []Record{
		NewCreationRecord(Creation{ID: "I1", Title: "T", Service: "gateway", Operator: "alice"}, times[0]),
		NewActionRecord(linkReqAt("l1", 1, "e1", "I1"), times[1]),
		NewActionRecord(unlinkReqAt("u1", 2, "e1", "I1"), times[2]),
		NewActionRecord(linkReqAt("l2", 3, "e2", "I1"), times[3]),
		NewActionRecord(linkReqAt("l3", 4, "e1", "I1"), times[4]),
		NewActionRecord(unlinkReqAt("u2", 5, "e2", "I1"), times[5]),
	}
	reg := NewRegistry(lookup, nil)
	for _, record := range records {
		if err := reg.Load(record); err != nil {
			t.Fatalf("load: %v", err)
		}
	}
	inc, _ := reg.Get("I1")
	if inc.Version != 6 || !reflect.DeepEqual(inc.Links, []string{"e1"}) {
		t.Fatalf("rebuilt links/version: %+v", inc)
	}
	want := []struct {
		actionID string
		action   string
		content  string
		version  int
	}{
		{"", ActionCreate, "T", 1},
		{"l1", ActionLinkEvent, "e1", 2},
		{"u1", ActionUnlinkEvent, "e1", 3},
		{"l2", ActionLinkEvent, "e2", 4},
		{"l3", ActionLinkEvent, "e1", 5},
		{"u2", ActionUnlinkEvent, "e2", 6},
	}
	if len(inc.History) != len(want) {
		t.Fatalf("history len %d", len(inc.History))
	}
	for i, w := range want {
		got := inc.History[i]
		if got.ActionID != w.actionID || got.Action != w.action ||
			got.Content != w.content || got.Version != w.version || !got.At.Equal(times[i]) {
			t.Fatalf("history[%d] %+v want %+v at %s", i, got, w, times[i])
		}
	}

	// A retry of a recovered unlink replays its first result, even though e1
	// is currently linked again: it must not be removed a second time.
	res, err := reg.Apply(unlinkReqAt("u1", 2, "e1", "I1"))
	if err != nil {
		t.Fatalf("unlink retry after replay: %v", err)
	}
	if res != (ActionResult{IncidentID: "I1", ActionID: "u1", Version: 3}) {
		t.Fatalf("unlink retry result %+v", res)
	}
	again, _ := reg.Get("I1")
	if again.Version != 6 || !reflect.DeepEqual(again.Links, []string{"e1"}) || len(again.History) != 6 {
		t.Fatalf("retry must not count as another action: %+v", again)
	}
}

// TestReplayRejectsIllegalUnlink guards recovery parity for the unlink
// rules: unlinking while resolved or unlinking an id that is not currently
// linked must abort startup just like the live path refuses the request.
func TestReplayRejectsIllegalUnlink(t *testing.T) {
	lookup := &fakeEvents{services: map[string]string{"e1": "gateway"}}
	t0 := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	creation := NewCreationRecord(Creation{ID: "I1", Title: "T", Service: "gateway", Operator: "alice"}, t0)

	t.Run("unlink an id that is not linked", func(t *testing.T) {
		bad := NewActionRecord(unlinkReqAt("u1", 1, "ghost", "I1"), t0.Add(time.Second))
		if err := assertRecordWellFormed(bad); err != nil {
			t.Fatal(err)
		}
		reg := NewRegistry(lookup, nil)
		if err := reg.Load(creation); err != nil {
			t.Fatal(err)
		}
		err := reg.Load(bad)
		if err == nil || err.Error() == "" {
			t.Fatalf("unlinking a non-linked id must fail recovery, got %v", err)
		}
		inc, _ := reg.Get("I1")
		if inc.Version != 1 || len(inc.Links) != 0 || len(inc.History) != 1 {
			t.Fatalf("rejected record must not be applied: %+v", inc)
		}

		// Live parity.
		live, _, _ := newTestRegistry(lookup)
		_, _ = live.Create(Creation{ID: "I1", Title: "T", Service: "gateway", Operator: "alice"})
		_, liveErr := live.Apply(unlinkReqAt("u1", 1, "ghost", "I1"))
		if !IsConflictError(liveErr) {
			t.Fatalf("live path must reject the same unlink: %v", liveErr)
		}
	})

	t.Run("unlink an already-unlinked id after relink of another", func(t *testing.T) {
		link := NewActionRecord(linkReqAt("l1", 1, "e1", "I1"), t0.Add(time.Second))
		unlink := NewActionRecord(unlinkReqAt("u1", 2, "e1", "I1"), t0.Add(2*time.Second))
		// Sequential (expected_version 3 is current), fresh action id, but e1
		// is no longer linked.
		again := NewActionRecord(unlinkReqAt("u2", 3, "e1", "I1"), t0.Add(3*time.Second))
		reg := NewRegistry(lookup, nil)
		for _, rec := range []Record{creation, link, unlink} {
			if err := reg.Load(rec); err != nil {
				t.Fatal(err)
			}
		}
		if err := reg.Load(again); err == nil {
			t.Fatal("unlinking a removed id twice must fail recovery")
		}
		inc, _ := reg.Get("I1")
		if inc.Version != 3 || len(inc.Links) != 0 || len(inc.History) != 3 {
			t.Fatalf("rejected second unlink must not apply: %+v", inc)
		}
	})

	t.Run("unlink while resolved", func(t *testing.T) {
		link := NewActionRecord(linkReqAt("l1", 1, "e1", "I1"), t0.Add(time.Second))
		resolve := NewActionRecord(Request{IncidentID: "I1", ActionID: "r1", Operator: "alice",
			ExpectedVersion: 2, Type: ActionResolve, Content: "done"}, t0.Add(2*time.Second))
		bad := NewActionRecord(unlinkReqAt("u1", 3, "e1", "I1"), t0.Add(3*time.Second))
		if err := assertRecordWellFormed(bad); err != nil {
			t.Fatal(err)
		}
		reg := NewRegistry(lookup, nil)
		for _, rec := range []Record{creation, link, resolve} {
			if err := reg.Load(rec); err != nil {
				t.Fatal(err)
			}
		}
		err := reg.Load(bad)
		if err == nil || err.Error() == "" {
			t.Fatalf("unlink on a resolved incident must fail recovery, got %v", err)
		}
		inc, _ := reg.Get("I1")
		if inc.Status != StatusResolved || inc.Version != 3 || !reflect.DeepEqual(inc.Links, []string{"e1"}) {
			t.Fatalf("rejected unlink must leave resolved state put: %+v", inc)
		}
	})
}

// linkReqAt builds a link request for an arbitrary incident id.
func linkReqAt(actionID string, version int, eventID, incidentID string) Request {
	r := linkReq(actionID, version, eventID)
	r.IncidentID = incidentID
	return r
}

// unlinkReqAt builds an unlink request for an arbitrary incident id.
func unlinkReqAt(actionID string, version int, eventID, incidentID string) Request {
	r := unlinkReq(actionID, version, eventID)
	r.IncidentID = incidentID
	return r
}
