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
