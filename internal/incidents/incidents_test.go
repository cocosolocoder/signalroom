package incidents

import (
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"
)

// fakeEvents is an in-memory EventLookup.
type fakeEvents struct {
	services map[string]string
}

func (f *fakeEvents) LookupEvent(id string) (string, bool) {
	service, ok := f.services[id]
	return service, ok
}

// fakeCommit counts durable writes and can be made to fail.
type fakeCommit struct {
	mu       sync.Mutex
	records  []Record
	failOnce bool
	failed   bool
}

func (c *fakeCommit) commit(record Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failOnce && !c.failed {
		c.failed = true
		return errors.New("disk full")
	}
	c.records = append(c.records, record)
	return nil
}

func (c *fakeCommit) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.records)
}

func newTestRegistry(events EventLookup) (*Registry, *fakeCommit, *tickingClock) {
	commit := &fakeCommit{}
	clock := newTickingClock()
	reg := NewRegistry(events, commit.commit, WithClock(clock.now))
	return reg, commit, clock
}

// tickingClock hands out strictly increasing instants even when the wall
// clock would not, which also models monotonic wall time across commits.
type tickingClock struct {
	mu sync.Mutex
	t  time.Time
}

func newTickingClock() *tickingClock {
	return &tickingClock{t: time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)}
}

func (c *tickingClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(time.Nanosecond)
	return c.t
}

func validCreation() Creation {
	return Creation{ID: "INC-1", Title: " Outage ", Service: "gateway", Operator: " Alice "}
}

func TestCreateNormalizesReplaysAndConflicts(t *testing.T) {
	reg, commit, _ := newTestRegistry(&fakeEvents{})

	res, err := reg.Create(validCreation())
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if res != (CreateResult{ID: "INC-1", Status: StatusOpen, Version: 1}) {
		t.Fatalf("unexpected create result %+v", res)
	}

	got, err := reg.Get("INC-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Title != "Outage" || got.Service != "gateway" {
		t.Fatalf("whitespace must be trimmed: %+v", got)
	}
	if got.History[0].Operator != "Alice" {
		t.Fatalf("operator case/space: %+v", got.History[0])
	}

	// Identical creation after trimming replays the first result.
	replay, err := reg.Create(Creation{ID: "INC-1", Title: "Outage", Service: "gateway", Operator: "Alice"})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if replay != res {
		t.Fatalf("replay must return first result %+v, got %+v", res, replay)
	}
	if commit.count() != 1 {
		t.Fatalf("replay must not persist again, count=%d", commit.count())
	}

	// Different content for the same id conflicts.
	_, err = reg.Create(Creation{ID: "INC-1", Title: "Changed", Service: "gateway", Operator: "Alice"})
	if !IsConflictError(err) {
		t.Fatalf("want conflict, got %v", err)
	}
	if commit.count() != 1 {
		t.Fatal("conflict must not persist")
	}
}

func TestCreateValidation(t *testing.T) {
	reg, _, _ := newTestRegistry(&fakeEvents{})
	for field, mutate := range map[string]func(Creation) Creation{
		"id":       func(c Creation) Creation { c.ID = "  "; return c },
		"title":    func(c Creation) Creation { c.Title = ""; return c },
		"service":  func(c Creation) Creation { c.Service = "\t"; return c },
		"operator": func(c Creation) Creation { c.Operator = ""; return c },
	} {
		if _, err := reg.Create(mutate(validCreation())); !IsValidationError(err) {
			t.Fatalf("field %s: want validation error, got %v", field, err)
		}
	}
}

func TestCreateKeepsInteriorCase(t *testing.T) {
	reg, _, _ := newTestRegistry(&fakeEvents{})
	in := validCreation()
	in.Title = "Payments API Down"
	if _, err := reg.Create(in); err != nil {
		t.Fatal(err)
	}
	got, _ := reg.Get("INC-1")
	if got.Title != "Payments API Down" {
		t.Fatalf("interior case changed: %q", got.Title)
	}
}

func TestActionLifecycle(t *testing.T) {
	lookup := &fakeEvents{services: map[string]string{"e1": "gateway", "e2": "checkout"}}
	reg, commit, _ := newTestRegistry(lookup)
	if _, err := reg.Create(validCreation()); err != nil {
		t.Fatal(err)
	}

	note := Request{IncidentID: "INC-1", ActionID: "n1", Operator: "bob", ExpectedVersion: 1, Type: ActionNote, Content: " looking "}
	res, err := reg.Apply(note)
	if err != nil {
		t.Fatalf("note: %v", err)
	}
	if res != (ActionResult{IncidentID: "INC-1", ActionID: "n1", Version: 2}) {
		t.Fatalf("note result %+v", res)
	}

	// Link a same-service event.
	link := Request{IncidentID: "INC-1", ActionID: "l1", Operator: "bob", ExpectedVersion: 2, Type: ActionLinkEvent, Content: "e1"}
	res, err = reg.Apply(link)
	if err != nil {
		t.Fatalf("link: %v", err)
	}
	if res.Version != 3 {
		t.Fatalf("link version %d", res.Version)
	}

	inc, _ := reg.Get("INC-1")
	if len(inc.Links) != 1 || inc.Links[0] != "e1" {
		t.Fatalf("links %+v", inc.Links)
	}
	if commit.count() != 3 { // create + note + link
		t.Fatalf("commit count %d", commit.count())
	}

	// Unknown event -> not found.
	_, err = reg.Apply(Request{IncidentID: "INC-1", ActionID: "l2", Operator: "bob", ExpectedVersion: 3, Type: ActionLinkEvent, Content: "missing"})
	if !IsNotFoundError(err) {
		t.Fatalf("missing event: want not-found, got %v", err)
	}

	// Different service -> conflict.
	_, err = reg.Apply(Request{IncidentID: "INC-1", ActionID: "l3", Operator: "bob", ExpectedVersion: 3, Type: ActionLinkEvent, Content: "e2"})
	if !IsConflictError(err) {
		t.Fatalf("wrong service: want conflict, got %v", err)
	}

	// Already linked -> conflict.
	_, err = reg.Apply(Request{IncidentID: "INC-1", ActionID: "l4", Operator: "bob", ExpectedVersion: 3, Type: ActionLinkEvent, Content: "e1"})
	if !IsConflictError(err) {
		t.Fatalf("dup link: want conflict, got %v", err)
	}

	// Resolve, after which only reopen is allowed.
	_, err = reg.Apply(Request{IncidentID: "INC-1", ActionID: "r1", Operator: "bob", ExpectedVersion: 3, Type: ActionResolve, Content: "fixed"})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	inc, _ = reg.Get("INC-1")
	if inc.Status != StatusResolved || inc.Version != 4 {
		t.Fatalf("post-resolve state %+v", inc)
	}
	for _, bad := range []Request{
		{IncidentID: "INC-1", ActionID: "x1", Operator: "b", ExpectedVersion: 4, Type: ActionNote, Content: "c"},
		{IncidentID: "INC-1", ActionID: "x2", Operator: "b", ExpectedVersion: 4, Type: ActionLinkEvent, Content: "e1"},
		{IncidentID: "INC-1", ActionID: "x3", Operator: "b", ExpectedVersion: 4, Type: ActionResolve, Content: "again"},
	} {
		if _, err := reg.Apply(bad); !IsConflictError(err) {
			t.Fatalf("resolved+%s: want conflict, got %v", bad.Type, err)
		}
	}

	// Reopen, then a note works again.
	_, err = reg.Apply(Request{IncidentID: "INC-1", ActionID: "o1", Operator: "bob", ExpectedVersion: 4, Type: ActionReopen, Content: "regression"})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	inc, _ = reg.Get("INC-1")
	if inc.Status != StatusOpen || inc.Version != 5 {
		t.Fatalf("post-reopen state %+v", inc)
	}
}

func TestActionVersionConflictReportsCurrent(t *testing.T) {
	lookup := &fakeEvents{services: map[string]string{"e1": "gateway"}}
	reg, _, _ := newTestRegistry(lookup)
	_, _ = reg.Create(validCreation())
	_, _ = reg.Apply(Request{IncidentID: "INC-1", ActionID: "n1", Operator: "b", ExpectedVersion: 1, Type: ActionNote, Content: "c"})

	_, err := reg.Apply(Request{IncidentID: "INC-1", ActionID: "n2", Operator: "b", ExpectedVersion: 1, Type: ActionNote, Content: "c"})
	var conflict *ConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("want conflict, got %v", err)
	}
	if conflict.CurrentVersion != 2 {
		t.Fatalf("current version reported as %d", conflict.CurrentVersion)
	}
}

func TestActionIdempotentReplayAcrossVersions(t *testing.T) {
	lookup := &fakeEvents{services: map[string]string{"e1": "gateway"}}
	reg, commit, _ := newTestRegistry(lookup)
	_, _ = reg.Create(validCreation())
	first := Request{IncidentID: "INC-1", ActionID: "n1", Operator: "bob", ExpectedVersion: 1, Type: ActionNote, Content: "c"}
	res, err := reg.Apply(first)
	if err != nil {
		t.Fatal(err)
	}
	// Advance the incident with a different action.
	_, _ = reg.Apply(Request{IncidentID: "INC-1", ActionID: "l1", Operator: "bob", ExpectedVersion: 2, Type: ActionLinkEvent, Content: "e1"})

	// Replaying the first request with its now-stale expected_version still
	// returns its first result and adds nothing.
	replay, err := reg.Apply(first)
	if err != nil {
		t.Fatalf("replay across versions: %v", err)
	}
	if replay != res {
		t.Fatalf("replay result %+v, want %+v", replay, res)
	}
	inc, _ := reg.Get("INC-1")
	if inc.Version != 3 || len(inc.History) != 3 {
		t.Fatalf("replay must not add a record: %+v", inc)
	}
	if commit.count() != 3 {
		t.Fatalf("commit count %d", commit.count())
	}

	// Same action id with different content conflicts.
	_, err = reg.Apply(Request{IncidentID: "INC-1", ActionID: "n1", Operator: "bob", ExpectedVersion: 3, Type: ActionNote, Content: "different"})
	if !IsConflictError(err) {
		t.Fatalf("changed action content: want conflict, got %v", err)
	}
}

func TestActionValidation(t *testing.T) {
	reg, _, _ := newTestRegistry(&fakeEvents{})
	_, _ = reg.Create(validCreation())

	base := Request{IncidentID: "INC-1", ActionID: "a", Operator: "b", ExpectedVersion: 1, Type: ActionNote, Content: "c"}
	cases := map[string]Request{
		"empty action id":   func() Request { r := base; r.ActionID = " "; return r }(),
		"empty operator":    func() Request { r := base; r.Operator = ""; return r }(),
		"zero version":      func() Request { r := base; r.ExpectedVersion = 0; return r }(),
		"negative version":  func() Request { r := base; r.ExpectedVersion = -2; return r }(),
		"unknown action":    func() Request { r := base; r.Type = "explode"; return r }(),
		"empty content":     func() Request { r := base; r.Content = "  "; return r }(),
		"resolve no reason": func() Request { r := base; r.Type = ActionResolve; r.Content = ""; return r }(),
	}
	for name, req := range cases {
		if _, err := reg.Apply(req); !IsValidationError(err) {
			t.Fatalf("%s: want validation, got %v", name, err)
		}
	}

	_, err := reg.Apply(Request{IncidentID: "missing", ActionID: "a", Operator: "b", ExpectedVersion: 1, Type: ActionNote, Content: "c"})
	if !IsNotFoundError(err) {
		t.Fatalf("unknown incident: want not-found, got %v", err)
	}
}

func TestCommitFailureRejectsWithoutChange(t *testing.T) {
	reg, commit, _ := newTestRegistry(&fakeEvents{})
	commit.failOnce = true
	_, err := reg.Create(validCreation())
	if err == nil {
		t.Fatal("expected commit failure")
	}
	if _, getErr := reg.Get("INC-1"); getErr == nil {
		t.Fatal("failed creation must not be visible")
	}
	// A later valid request still works once storage recovers at the domain
	// level (the durable layer is what stays poisoned until restart).
	commit.failOnce = false
	if _, err := reg.Create(validCreation()); err != nil {
		t.Fatalf("retry after commit failure: %v", err)
	}
}

func TestHistoryOrderAndTimestamps(t *testing.T) {
	lookup := &fakeEvents{services: map[string]string{"e1": "gateway"}}
	reg, _, _ := newTestRegistry(lookup)
	_, _ = reg.Create(validCreation())
	_, _ = reg.Apply(Request{IncidentID: "INC-1", ActionID: "n1", Operator: "b", ExpectedVersion: 1, Type: ActionNote, Content: "c"})
	_, _ = reg.Apply(Request{IncidentID: "INC-1", ActionID: "r1", Operator: "b", ExpectedVersion: 2, Type: ActionResolve, Content: "why"})
	_, _ = reg.Apply(Request{IncidentID: "INC-1", ActionID: "o1", Operator: "b", ExpectedVersion: 3, Type: ActionReopen, Content: "again"})

	inc, _ := reg.Get("INC-1")
	wantActions := []string{ActionCreate, ActionNote, ActionResolve, ActionReopen}
	if len(inc.History) != len(wantActions) {
		t.Fatalf("history len %d", len(inc.History))
	}
	var prev time.Time
	for i, entry := range inc.History {
		if entry.Action != wantActions[i] {
			t.Fatalf("history[%d]=%s want %s", i, entry.Action, wantActions[i])
		}
		if entry.Version != i+1 {
			t.Fatalf("history[%d] version %d", i, entry.Version)
		}
		if i > 0 && !entry.At.After(prev) {
			t.Fatalf("history times must strictly advance: %s !> %s", entry.At, prev)
		}
		if entry.At.Location() != time.UTC {
			t.Fatalf("history time must be UTC: %v", entry.At.Location())
		}
		prev = entry.At
	}
	if inc.History[0].ActionID != "" {
		t.Fatal("creation entry carries no action id")
	}
	if inc.History[1].ActionID != "n1" {
		t.Fatalf("action entry action id %q", inc.History[1].ActionID)
	}
}

func TestSameEventLinksMultipleIncidents(t *testing.T) {
	lookup := &fakeEvents{services: map[string]string{"e1": "gateway"}}
	reg, _, _ := newTestRegistry(lookup)
	_, _ = reg.Create(Creation{ID: "A", Title: "t", Service: "gateway", Operator: "o"})
	_, _ = reg.Create(Creation{ID: "B", Title: "t", Service: "gateway", Operator: "o"})
	for _, id := range []string{"A", "B"} {
		if _, err := reg.Apply(Request{IncidentID: id, ActionID: "l-" + id, Operator: "o", ExpectedVersion: 1, Type: ActionLinkEvent, Content: "e1"}); err != nil {
			t.Fatalf("link e1 to %s: %v", id, err)
		}
	}
	a, _ := reg.Get("A")
	b, _ := reg.Get("B")
	if len(a.Links) != 1 || len(b.Links) != 1 {
		t.Fatalf("event must link to both incidents: %+v %+v", a.Links, b.Links)
	}
}

func TestConcurrentSameVersionOnlyOneSucceeds(t *testing.T) {
	reg, commit, _ := newTestRegistry(&fakeEvents{})
	_, _ = reg.Create(validCreation())

	// Distinct action ids all racing at version 1: exactly one commits.
	const n = 32
	var wg sync.WaitGroup
	var successes int64
	var mu sync.Mutex
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			req := Request{IncidentID: "INC-1", ActionID: "race" + strconv.Itoa(i),
				Operator: "o", ExpectedVersion: 1, Type: ActionNote, Content: "c"}
			if _, err := reg.Apply(req); err == nil {
				mu.Lock()
				successes++
				mu.Unlock()
			}
		}(i)
	}
	close(start)
	wg.Wait()
	if successes != 1 {
		t.Fatalf("exactly one action at version 1 may succeed, got %d", successes)
	}
	inc, _ := reg.Get("INC-1")
	if inc.Version != 2 || len(inc.History) != 2 {
		t.Fatalf("exactly one action may land at a version: %+v", inc)
	}
	if commit.count() != 2 {
		t.Fatalf("exactly one action record may persist, got %d", commit.count())
	}
}

func TestConcurrentDuplicateRequestCommitsOnce(t *testing.T) {
	reg, commit, _ := newTestRegistry(&fakeEvents{})
	_, _ = reg.Create(validCreation())

	// Identical requests racing at the same version all report success, but
	// only one record may ever be produced.
	const n = 32
	var wg sync.WaitGroup
	var ok, bad int64
	var mu sync.Mutex
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			req := Request{IncidentID: "INC-1", ActionID: "dup",
				Operator: "o", ExpectedVersion: 1, Type: ActionNote, Content: "same"}
			res, err := reg.Apply(req)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil && res.Version == 2:
				ok++
			default:
				bad++
			}
		}()
	}
	close(start)
	wg.Wait()
	if ok != n || bad != 0 {
		t.Fatalf("every duplicate must return the first result, ok=%d bad=%d", ok, bad)
	}
	inc, _ := reg.Get("INC-1")
	if inc.Version != 2 || len(inc.History) != 2 {
		t.Fatalf("duplicate concurrent requests must produce one record: %+v", inc)
	}
	if commit.count() != 2 {
		t.Fatalf("one create + one action record expected, got %d", commit.count())
	}
}

func TestConcurrentDistinctActionsSerialize(t *testing.T) {
	reg, _, _ := newTestRegistry(&fakeEvents{})
	_, _ = reg.Create(validCreation())

	const n = 20
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			// Retry on stale versions until the unique action lands.
			for attempt := 0; attempt < n+5; attempt++ {
				inc, _ := reg.Get("INC-1")
				req := Request{IncidentID: "INC-1", ActionID: "n" + strconv.Itoa(i),
					Operator: "o", ExpectedVersion: inc.Version, Type: ActionNote, Content: "c"}
				if _, err := reg.Apply(req); err == nil {
					return
				} else if !IsConflictError(err) {
					t.Errorf("unexpected err: %v", err)
					return
				}
			}
			t.Errorf("action %d never committed", i)
		}(i)
	}
	close(start)
	wg.Wait()
	inc, _ := reg.Get("INC-1")
	if inc.Version != n+1 {
		t.Fatalf("all %d distinct actions should commit, version=%d", n, inc.Version)
	}
}

// A stale expected_version must be reported before the missing event is even
// looked up: when both a version conflict and a missing event apply, the
// conflict wins and still carries the current version.
func TestLinkVersionConflictPrecedesMissingEvent(t *testing.T) {
	reg, commit, _ := newTestRegistry(&fakeEvents{}) // resolves no events
	_, _ = reg.Create(validCreation())

	_, err := reg.Apply(Request{IncidentID: "INC-1", ActionID: "l0", Operator: "b",
		ExpectedVersion: 99, Type: ActionLinkEvent, Content: "ghost"})
	if !IsConflictError(err) || CurrentVersionOf(err) != 1 {
		t.Fatalf("stale version must win over the missing event, got %v", err)
	}
	if IsNotFoundError(err) {
		t.Fatalf("missing event must not be reported first: %v", err)
	}
	inc, _ := reg.Get("INC-1")
	if inc.Version != 1 || len(inc.Links) != 0 || len(inc.History) != 1 {
		t.Fatalf("rejected link must change nothing: %+v", inc)
	}
	if commit.count() != 1 {
		t.Fatalf("rejected link must not persist, count=%d", commit.count())
	}
}

// Linking into a resolved incident is a state conflict even when the event
// does not exist: the open/resolved gate precedes event existence.
func TestLinkResolvedStatusPrecedesMissingEvent(t *testing.T) {
	lookup := &fakeEvents{} // the linked event id never resolves
	reg, commit, _ := newTestRegistry(lookup)
	_, _ = reg.Create(validCreation())
	if _, err := reg.Apply(Request{IncidentID: "INC-1", ActionID: "r1", Operator: "b",
		ExpectedVersion: 1, Type: ActionResolve, Content: "done"}); err != nil {
		t.Fatal(err)
	}

	// expected_version matches (2), so only the resolved state and the
	// missing event are in play; the state conflict must be reported.
	_, err := reg.Apply(Request{IncidentID: "INC-1", ActionID: "l1", Operator: "b",
		ExpectedVersion: 2, Type: ActionLinkEvent, Content: "ghost"})
	if !IsConflictError(err) || CurrentVersionOf(err) != 2 {
		t.Fatalf("resolved status must win over the missing event, got %v", err)
	}
	if IsNotFoundError(err) {
		t.Fatalf("missing event must not be reported on a resolved incident: %v", err)
	}
	inc, _ := reg.Get("INC-1")
	if inc.Status != StatusResolved || inc.Version != 2 || len(inc.Links) != 0 {
		t.Fatalf("rejected link must change nothing: %+v", inc)
	}
	if commit.count() != 2 { // create + resolve only
		t.Fatalf("rejected link must not persist, count=%d", commit.count())
	}
}

// A committed link replays idempotently even after the incident has moved on
// and resolved: the first result comes back verbatim and nothing is added.
func TestLinkIdempotentReplayAcrossResolution(t *testing.T) {
	lookup := &fakeEvents{services: map[string]string{"e1": "gateway", "e2": "gateway"}}
	reg, commit, _ := newTestRegistry(lookup)
	_, _ = reg.Create(validCreation())

	first := Request{IncidentID: "INC-1", ActionID: "l1", Operator: "bob",
		ExpectedVersion: 1, Type: ActionLinkEvent, Content: "e1"}
	res, err := reg.Apply(first)
	if err != nil {
		t.Fatal(err)
	}
	if res != (ActionResult{IncidentID: "INC-1", ActionID: "l1", Version: 2}) {
		t.Fatalf("first link result %+v", res)
	}
	if _, err := reg.Apply(Request{IncidentID: "INC-1", ActionID: "r1", Operator: "bob",
		ExpectedVersion: 2, Type: ActionResolve, Content: "done"}); err != nil {
		t.Fatal(err)
	}

	// Same action id and same normalized content, now stale and resolved:
	// still the first result, still no extra link or history entry.
	replay, err := reg.Apply(first)
	if err != nil {
		t.Fatalf("replay after resolution: %v", err)
	}
	if replay != res {
		t.Fatalf("replay %+v want %+v", replay, res)
	}
	inc, _ := reg.Get("INC-1")
	if inc.Version != 3 || len(inc.History) != 3 || len(inc.Links) != 1 {
		t.Fatalf("replay must add nothing: %+v", inc)
	}
	if inc.Links[0] != "e1" {
		t.Fatalf("links must be untouched: %+v", inc.Links)
	}
	if commit.count() != 3 { // create + link + resolve
		t.Fatalf("replay must not persist, count=%d", commit.count())
	}

	// The same action id with changed content stays a conflict.
	_, err = reg.Apply(Request{IncidentID: "INC-1", ActionID: "l1", Operator: "bob",
		ExpectedVersion: 3, Type: ActionLinkEvent, Content: "e2"})
	if !IsConflictError(err) {
		t.Fatalf("changed link content: want conflict, got %v", err)
	}
	if got := CurrentVersionOf(err); got != 3 {
		t.Fatalf("changed-content conflict reports version %d, want 3", got)
	}
}

// TestUnlinkEventRemovesLinkAndAppendsHistory covers the happy path: a
// successful unlink bumps the version by one, removes just the target event
// while preserving the others' relative order, appends a history entry whose
// content is the target event id, and leaves the original link entry in
// place.
func TestUnlinkEventRemovesLinkAndAppendsHistory(t *testing.T) {
	lookup := &fakeEvents{services: map[string]string{"e1": "gateway", "e2": "gateway", "e3": "gateway"}}
	reg, commit, _ := newTestRegistry(lookup)
	_, _ = reg.Create(validCreation())
	for i, link := range []struct{ id, event string }{
		{"l1", "e1"}, {"l2", "e2"}, {"l3", "e3"},
	} {
		res, err := reg.Apply(Request{IncidentID: "INC-1", ActionID: link.id, Operator: "bob",
			ExpectedVersion: i + 1, Type: ActionLinkEvent, Content: link.event})
		if err != nil {
			t.Fatalf("link %s: %v", link.event, err)
		}
		if res.Version != i+2 {
			t.Fatalf("link %s version %d", link.event, res.Version)
		}
	}

	// Unlink the middle event: surrounding links keep their relative order.
	res, err := reg.Apply(Request{IncidentID: "INC-1", ActionID: "u1", Operator: "carol",
		ExpectedVersion: 4, Type: ActionUnlinkEvent, Content: "  e2  "})
	if err != nil {
		t.Fatalf("unlink: %v", err)
	}
	if res != (ActionResult{IncidentID: "INC-1", ActionID: "u1", Version: 5}) {
		t.Fatalf("unlink result %+v", res)
	}
	inc, _ := reg.Get("INC-1")
	if len(inc.Links) != 2 || inc.Links[0] != "e1" || inc.Links[1] != "e3" {
		t.Fatalf("middle removal must preserve order: %+v", inc.Links)
	}

	// Unlinking the remaining first event keeps the last one.
	res, err = reg.Apply(Request{IncidentID: "INC-1", ActionID: "u2", Operator: "carol",
		ExpectedVersion: 5, Type: ActionUnlinkEvent, Content: "e1"})
	if err != nil {
		t.Fatalf("unlink e1: %v", err)
	}
	if res.Version != 6 {
		t.Fatalf("unlink version %d", res.Version)
	}
	inc, _ = reg.Get("INC-1")
	if len(inc.Links) != 1 || inc.Links[0] != "e3" {
		t.Fatalf("links after second unlink %+v", inc.Links)
	}

	// Removing the final link leaves an empty, non-nil list.
	_, err = reg.Apply(Request{IncidentID: "INC-1", ActionID: "u3", Operator: "carol",
		ExpectedVersion: 6, Type: ActionUnlinkEvent, Content: "e3"})
	if err != nil {
		t.Fatalf("unlink e3: %v", err)
	}
	inc, _ = reg.Get("INC-1")
	if inc.Links == nil || len(inc.Links) != 0 {
		t.Fatalf("last removal must leave an empty list: %+v", inc.Links)
	}
	if inc.Version != 7 {
		t.Fatalf("version %d", inc.Version)
	}

	// Status, owner, and participants never move on an unlink.
	if inc.Status != StatusOpen || inc.Owner != "Alice" {
		t.Fatalf("unlink changed status/owner: %+v", inc)
	}
	if len(inc.Participants) != 1 || inc.Participants[0] != "Alice" {
		t.Fatalf("unlink must not touch participants: %+v", inc.Participants)
	}

	// History is create, three links, three unlinks in commit order; the
	// unlink entries carry the normalized event id as content and the old
	// link entries remain.
	wantActions := []string{ActionCreate, ActionLinkEvent, ActionLinkEvent, ActionLinkEvent,
		ActionUnlinkEvent, ActionUnlinkEvent, ActionUnlinkEvent}
	if len(inc.History) != len(wantActions) {
		t.Fatalf("history len %d", len(inc.History))
	}
	for i, want := range wantActions {
		if inc.History[i].Action != want {
			t.Fatalf("history[%d]=%s want %s", i, inc.History[i].Action, want)
		}
		if i > 0 && inc.History[i].Version != i+1 {
			t.Fatalf("history[%d] version %d", i, inc.History[i].Version)
		}
	}
	if inc.History[4].Content != "e2" || inc.History[4].Operator != "carol" {
		t.Fatalf("unlink history entry %+v", inc.History[4])
	}
	if inc.History[2].Action != ActionLinkEvent || inc.History[2].Content != "e2" {
		t.Fatalf("original link record must remain: %+v", inc.History[2])
	}
	if commit.count() != 7 {
		t.Fatalf("one record per accepted action, count=%d", commit.count())
	}
}

// TestUnlinkValidation rejects blank or missing ids with the event_id field
// name, exactly like link_event.
func TestUnlinkValidation(t *testing.T) {
	lookup := &fakeEvents{services: map[string]string{"e1": "gateway"}}
	reg, _, _ := newTestRegistry(lookup)
	_, _ = reg.Create(validCreation())
	_, _ = reg.Apply(Request{IncidentID: "INC-1", ActionID: "l1", Operator: "b",
		ExpectedVersion: 1, Type: ActionLinkEvent, Content: "e1"})

	for _, content := range []string{"", "   ", "\t\n"} {
		_, err := reg.Apply(Request{IncidentID: "INC-1", ActionID: "u0", Operator: "b",
			ExpectedVersion: 2, Type: ActionUnlinkEvent, Content: content})
		if !IsValidationError(err) {
			t.Fatalf("blank event id %q: want validation, got %v", content, err)
		}
	}
}

// TestUnlinkNotLinkedConflict: a target absent from the current link list is
// a 409 carrying the current version — including an id that no event ever
// had, which is treated as simply not linked rather than 404.
func TestUnlinkNotLinkedConflict(t *testing.T) {
	lookup := &fakeEvents{services: map[string]string{"e1": "gateway", "e2": "gateway"}}
	reg, commit, _ := newTestRegistry(lookup)
	_, _ = reg.Create(validCreation())
	_, _ = reg.Apply(Request{IncidentID: "INC-1", ActionID: "l1", Operator: "b",
		ExpectedVersion: 1, Type: ActionLinkEvent, Content: "e1"})

	for _, target := range []string{"e2", "never-ingested", "e1-other"} {
		_, err := reg.Apply(Request{IncidentID: "INC-1", ActionID: "u-x-" + target, Operator: "b",
			ExpectedVersion: 2, Type: ActionUnlinkEvent, Content: target})
		if !IsConflictError(err) {
			t.Fatalf("unlink %s: want conflict, got %v", target, err)
		}
		if CurrentVersionOf(err) != 2 {
			t.Fatalf("unlink %s conflict must carry current version 2, got %d", target, CurrentVersionOf(err))
		}
	}
	inc, _ := reg.Get("INC-1")
	if inc.Version != 2 || len(inc.Links) != 1 || inc.Links[0] != "e1" || len(inc.History) != 2 {
		t.Fatalf("rejected unlinks must change nothing: %+v", inc)
	}
	if commit.count() != 2 {
		t.Fatalf("rejected unlinks must not persist, count=%d", commit.count())
	}
}

// TestUnlinkResolvedRejected: resolved incidents accept no unlink actions.
func TestUnlinkResolvedRejected(t *testing.T) {
	lookup := &fakeEvents{services: map[string]string{"e1": "gateway"}}
	reg, commit, _ := newTestRegistry(lookup)
	_, _ = reg.Create(validCreation())
	_, _ = reg.Apply(Request{IncidentID: "INC-1", ActionID: "l1", Operator: "b",
		ExpectedVersion: 1, Type: ActionLinkEvent, Content: "e1"})
	_, _ = reg.Apply(Request{IncidentID: "INC-1", ActionID: "r1", Operator: "b",
		ExpectedVersion: 2, Type: ActionResolve, Content: "done"})

	_, err := reg.Apply(Request{IncidentID: "INC-1", ActionID: "u1", Operator: "b",
		ExpectedVersion: 3, Type: ActionUnlinkEvent, Content: "e1"})
	if !IsConflictError(err) || CurrentVersionOf(err) != 3 {
		t.Fatalf("resolved unlink: want conflict at version 3, got %v", err)
	}
	inc, _ := reg.Get("INC-1")
	if inc.Status != StatusResolved || len(inc.Links) != 1 || len(inc.History) != 3 {
		t.Fatalf("rejected unlink must change nothing: %+v", inc)
	}
	if commit.count() != 3 {
		t.Fatalf("rejected unlink must not persist, count=%d", commit.count())
	}

	// After reopening, the same event can finally be unlinked.
	_, err = reg.Apply(Request{IncidentID: "INC-1", ActionID: "o1", Operator: "b",
		ExpectedVersion: 3, Type: ActionReopen, Content: "regression"})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	res, err := reg.Apply(Request{IncidentID: "INC-1", ActionID: "u1", Operator: "b",
		ExpectedVersion: 4, Type: ActionUnlinkEvent, Content: "e1"})
	if err != nil {
		t.Fatalf("unlink after reopen: %v", err)
	}
	if res.Version != 5 {
		t.Fatalf("unlink version %d", res.Version)
	}
}

// TestUnlinkUnknownIncidentAndStaleVersion keep the common action rules.
func TestUnlinkUnknownIncidentAndStaleVersion(t *testing.T) {
	lookup := &fakeEvents{services: map[string]string{"e1": "gateway"}}
	reg, _, _ := newTestRegistry(lookup)
	_, _ = reg.Create(validCreation())
	_, _ = reg.Apply(Request{IncidentID: "INC-1", ActionID: "l1", Operator: "b",
		ExpectedVersion: 1, Type: ActionLinkEvent, Content: "e1"})

	_, err := reg.Apply(Request{IncidentID: "ghost", ActionID: "u0", Operator: "b",
		ExpectedVersion: 1, Type: ActionUnlinkEvent, Content: "e1"})
	if !IsNotFoundError(err) {
		t.Fatalf("unknown incident: want not-found, got %v", err)
	}

	_, err = reg.Apply(Request{IncidentID: "INC-1", ActionID: "u0", Operator: "b",
		ExpectedVersion: 99, Type: ActionUnlinkEvent, Content: "e1"})
	if !IsConflictError(err) || CurrentVersionOf(err) != 2 {
		t.Fatalf("stale version must win and report 2, got %v", err)
	}
}

// TestUnlinkIdempotentReplayAndRelink pins the retry semantics across a full
// link -> unlink -> relink cycle:
//   - replaying the successful unlink returns its first result forever, even
//     after the event is linked again, and never unlinks a second time;
//   - replaying the original link never puts an unlinked event back;
//   - a fresh relink lands at the end of the current link list;
//   - history shows link, unlink, then relink.
func TestUnlinkIdempotentReplayAndRelink(t *testing.T) {
	lookup := &fakeEvents{services: map[string]string{"e1": "gateway", "e9": "gateway"}}
	reg, commit, _ := newTestRegistry(lookup)
	_, _ = reg.Create(validCreation())

	linkReq := Request{IncidentID: "INC-1", ActionID: "l1", Operator: "bob",
		ExpectedVersion: 1, Type: ActionLinkEvent, Content: "e1"}
	linkRes, err := reg.Apply(linkReq)
	if err != nil {
		t.Fatal(err)
	}
	unlinkReq := Request{IncidentID: "INC-1", ActionID: "u1", Operator: "bob",
		ExpectedVersion: 2, Type: ActionUnlinkEvent, Content: "e1"}
	unlinkRes, err := reg.Apply(unlinkReq)
	if err != nil {
		t.Fatal(err)
	}
	if unlinkRes != (ActionResult{IncidentID: "INC-1", ActionID: "u1", Version: 3}) {
		t.Fatalf("unlink result %+v", unlinkRes)
	}
	inc, _ := reg.Get("INC-1")
	if len(inc.Links) != 0 {
		t.Fatalf("links after unlink %+v", inc.Links)
	}

	// The old link request cannot restore the removed event.
	replay, err := reg.Apply(linkReq)
	if err != nil {
		t.Fatalf("link replay: %v", err)
	}
	if replay != linkRes {
		t.Fatalf("link replay %+v want %+v", replay, linkRes)
	}
	inc, _ = reg.Get("INC-1")
	if len(inc.Links) != 0 || inc.Version != 3 {
		t.Fatalf("link replay must not re-add the event: %+v", inc)
	}

	// The old unlink request also replays its first result.
	replay, err = reg.Apply(unlinkReq)
	if err != nil {
		t.Fatalf("unlink replay: %v", err)
	}
	if replay != unlinkRes {
		t.Fatalf("unlink replay %+v want %+v", replay, unlinkRes)
	}
	inc, _ = reg.Get("INC-1")
	if inc.Version != 3 || len(inc.History) != 3 {
		t.Fatalf("unlink replay must add nothing: %+v", inc)
	}

	// A fresh link of another event followed by re-linking e1 puts e1 at the
	// tail of the current list.
	if _, err := reg.Apply(Request{IncidentID: "INC-1", ActionID: "l9", Operator: "bob",
		ExpectedVersion: 3, Type: ActionLinkEvent, Content: "e9"}); err != nil {
		t.Fatalf("link e9: %v", err)
	}
	if _, err := reg.Apply(Request{IncidentID: "INC-1", ActionID: "l2", Operator: "bob",
		ExpectedVersion: 4, Type: ActionLinkEvent, Content: "e1"}); err != nil {
		t.Fatalf("relink e1: %v", err)
	}
	inc, _ = reg.Get("INC-1")
	if len(inc.Links) != 2 || inc.Links[0] != "e9" || inc.Links[1] != "e1" {
		t.Fatalf("relinked event must appear last: %+v", inc.Links)
	}

	// Even now that e1 is linked again, replaying the first unlink is still a
	// harmless replay of its first result and cannot unlink it again.
	replay, err = reg.Apply(unlinkReq)
	if err != nil {
		t.Fatalf("unlink replay after relink: %v", err)
	}
	if replay.Version != 3 {
		t.Fatalf("unlink replay must keep reporting version 3, got %d", replay.Version)
	}
	inc, _ = reg.Get("INC-1")
	if inc.Version != 5 || len(inc.Links) != 2 || inc.Links[1] != "e1" {
		t.Fatalf("stale unlink replay must not remove the relink: %+v", inc)
	}

	// Same action id with different content stays a conflict either way.
	_, err = reg.Apply(Request{IncidentID: "INC-1", ActionID: "u1", Operator: "bob",
		ExpectedVersion: 5, Type: ActionUnlinkEvent, Content: "e9"})
	if !IsConflictError(err) || CurrentVersionOf(err) != 5 {
		t.Fatalf("changed unlink content: want conflict at 5, got %v", err)
	}
	_, err = reg.Apply(Request{IncidentID: "INC-1", ActionID: "l1", Operator: "bob",
		ExpectedVersion: 5, Type: ActionLinkEvent, Content: "e9"})
	if !IsConflictError(err) {
		t.Fatalf("changed link content: want conflict, got %v", err)
	}

	// History tells the whole story in order: link, unlink, link e9, relink.
	want := []struct {
		action  string
		content string
		version int
	}{
		{ActionCreate, "Outage", 1},
		{ActionLinkEvent, "e1", 2},
		{ActionUnlinkEvent, "e1", 3},
		{ActionLinkEvent, "e9", 4},
		{ActionLinkEvent, "e1", 5},
	}
	if len(inc.History) != len(want) {
		t.Fatalf("history len %d", len(inc.History))
	}
	for i, w := range want {
		got := inc.History[i]
		if got.Action != w.action || got.Content != w.content || got.Version != w.version {
			t.Fatalf("history[%d]=%+v want %+v", i, got, w)
		}
	}
	if commit.count() != 5 {
		t.Fatalf("retries never persist, count=%d", commit.count())
	}
}

// TestUnlinkAffectsOnlyThisIncidentAndEvent: the event stays queryable and
// stays linked to the other incident; only the one relationship is removed.
func TestUnlinkAffectsOnlyThisIncidentAndEvent(t *testing.T) {
	lookup := &fakeEvents{services: map[string]string{"e1": "gateway"}}
	reg, _, _ := newTestRegistry(lookup)
	_, _ = reg.Create(Creation{ID: "A", Title: "t", Service: "gateway", Operator: "o"})
	_, _ = reg.Create(Creation{ID: "B", Title: "t", Service: "gateway", Operator: "o"})
	_, _ = reg.Apply(Request{IncidentID: "A", ActionID: "la", Operator: "o",
		ExpectedVersion: 1, Type: ActionLinkEvent, Content: "e1"})
	_, _ = reg.Apply(Request{IncidentID: "B", ActionID: "lb", Operator: "o",
		ExpectedVersion: 1, Type: ActionLinkEvent, Content: "e1"})

	if _, err := reg.Apply(Request{IncidentID: "A", ActionID: "ua", Operator: "o",
		ExpectedVersion: 2, Type: ActionUnlinkEvent, Content: "e1"}); err != nil {
		t.Fatalf("unlink from A: %v", err)
	}
	a, _ := reg.Get("A")
	b, _ := reg.Get("B")
	if len(a.Links) != 0 {
		t.Fatalf("incident A must have no links: %+v", a.Links)
	}
	if len(b.Links) != 1 || b.Links[0] != "e1" {
		t.Fatalf("incident B must keep the link: %+v", b.Links)
	}
	// The event itself is untouched and can be linked again to A.
	if _, err := reg.Apply(Request{IncidentID: "A", ActionID: "la2", Operator: "o",
		ExpectedVersion: 3, Type: ActionLinkEvent, Content: "e1"}); err != nil {
		t.Fatalf("relink to A after unlink: %v", err)
	}
	if service, ok := lookup.LookupEvent("e1"); !ok || service != "gateway" {
		t.Fatalf("event must still resolve: %q %v", service, ok)
	}
}
