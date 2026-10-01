package incidents

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/cocosolocoder/signalroom/internal/events"
)

// fakeEvents is an in-memory EventLookup for link tests.
type fakeEvents struct {
	byID map[string]events.Event
}

func newFakeEvents() *fakeEvents {
	return &fakeEvents{byID: make(map[string]events.Event)}
}

func (f *fakeEvents) add(id, service string) {
	f.byID[id] = events.Event{ID: id, Service: service, Severity: "info", Message: "msg", At: time.Now()}
}

func (f *fakeEvents) EventByID(id string) (events.Event, bool) {
	event, ok := f.byID[id]
	return event, ok
}

func newTestStore(t *testing.T) *Store {
	t.Helper()
	return NewStore(nil, newFakeEvents())
}

// fakeLogger is a RecordLogger that can simulate a write failure.
type fakeLogger struct {
	mu       sync.Mutex
	failNext bool
	poisoned bool
	records  []record
}

func (f *fakeLogger) Append(rec record) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.poisoned {
		return ErrPoisoned
	}
	if f.failNext {
		f.poisoned = true
		f.failNext = false
		return errors.New("simulated write failure")
	}
	f.records = append(f.records, rec)
	return nil
}

func (f *fakeLogger) Poisoned() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.poisoned
}

func TestCreateIncident(t *testing.T) {
	s := newTestStore(t)
	inc, err := s.Create("INC-1", "  Error rate spike  ", "gateway", "  alice  ")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if inc.ID != "INC-1" || inc.Title != "Error rate spike" || inc.Service != "gateway" ||
		inc.Status != StatusOpen || inc.Version != 1 {
		t.Fatalf("unexpected incident: %+v", inc)
	}
	if len(inc.History) != 1 || inc.History[0].Action != ActionCreate ||
		inc.History[0].Operator != "alice" || inc.History[0].Version != 1 {
		t.Fatalf("unexpected history: %+v", inc.History)
	}
}

func TestCreateReplayReturnsSameIncident(t *testing.T) {
	s := newTestStore(t)
	first, err := s.Create("INC-1", "Title", "svc", "alice")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	// Resubmit with surrounding whitespace; normalized content matches.
	second, err := s.Create("INC-1", "  Title  ", "  svc  ", "  alice  ")
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if second.Version != first.Version || second.Status != first.Status {
		t.Fatalf("replay differs: %+v vs %+v", second, first)
	}
}

func TestCreateConflictOnDifferentContent(t *testing.T) {
	s := newTestStore(t)
	if _, err := s.Create("INC-1", "Title", "svc", "alice"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := s.Create("INC-1", "Different", "svc", "alice"); !IsConflictError(err) {
		t.Fatalf("expected conflict, got %v", err)
	}
	if _, err := s.Create("INC-1", "Title", "other", "alice"); !IsConflictError(err) {
		t.Fatalf("expected conflict on service, got %v", err)
	}
	if _, err := s.Create("INC-1", "Title", "svc", "bob"); !IsConflictError(err) {
		t.Fatalf("expected conflict on operator, got %v", err)
	}
}

func TestCreateValidationErrors(t *testing.T) {
	s := newTestStore(t)
	for _, tc := range []struct {
		id, title, svc, op string
	}{
		{"", "Title", "svc", "alice"},
		{"INC-1", "  ", "svc", "alice"},
		{"INC-1", "Title", "", "alice"},
		{"INC-1", "Title", "svc", "  "},
	} {
		if _, err := s.Create(tc.id, tc.title, tc.svc, tc.op); !IsValidationError(err) {
			t.Fatalf("expected validation error for %+v, got %v", tc, err)
		}
	}
}

func TestAddNote(t *testing.T) {
	s := newTestStore(t)
	if _, err := s.Create("INC-1", "Title", "svc", "alice"); err != nil {
		t.Fatalf("create: %v", err)
	}
	result, err := s.ApplyAction("INC-1", "ACT-1", ActionAddNote, "bob", 1, "  investigating  ", "", "")
	if err != nil {
		t.Fatalf("add note: %v", err)
	}
	if result.Entry.Version != 2 || result.Entry.Content != "investigating" ||
		result.Entry.Operator != "bob" || result.Entry.ExpectedVersion != 1 {
		t.Fatalf("unexpected entry: %+v", result.Entry)
	}
	if result.Incident.Version != 2 || len(result.Incident.History) != 2 {
		t.Fatalf("incident not updated: %+v", result.Incident)
	}
}

func TestVersionConflict(t *testing.T) {
	s := newTestStore(t)
	if _, err := s.Create("INC-1", "Title", "svc", "alice"); err != nil {
		t.Fatalf("create: %v", err)
	}
	_, err := s.ApplyAction("INC-1", "ACT-1", ActionAddNote, "bob", 99, "note", "", "")
	var verr *VersionError
	if !errors.As(err, &verr) {
		t.Fatalf("expected version error, got %v", err)
	}
	if verr.CurrentVersion != 1 {
		t.Fatalf("current version: %d", verr.CurrentVersion)
	}
}

func TestActionIdempotentReplay(t *testing.T) {
	s := newTestStore(t)
	if _, err := s.Create("INC-1", "Title", "svc", "alice"); err != nil {
		t.Fatalf("create: %v", err)
	}
	first, err := s.ApplyAction("INC-1", "ACT-1", ActionAddNote, "bob", 1, "note", "", "")
	if err != nil {
		t.Fatalf("add note: %v", err)
	}
	// Move the incident forward.
	if _, err := s.ApplyAction("INC-1", "ACT-2", ActionResolve, "bob", 2, "", "fixed", ""); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	// Replay the first action: same normalized content, same expected_version.
	replay, err := s.ApplyAction("INC-1", "ACT-1", ActionAddNote, "bob", 1, "  note  ", "", "")
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if !replay.Replay {
		t.Fatal("expected replay flag")
	}
	if replay.Entry.Version != first.Entry.Version {
		t.Fatalf("replay version: %d vs %d", replay.Entry.Version, first.Entry.Version)
	}
	if replay.Incident.Version != 3 {
		t.Fatalf("replay must not increment version: %d", replay.Incident.Version)
	}
	if len(replay.Incident.History) != 3 {
		t.Fatalf("replay must not append history: %d", len(replay.Incident.History))
	}
}

func TestActionIdempotentConflict(t *testing.T) {
	s := newTestStore(t)
	if _, err := s.Create("INC-1", "Title", "svc", "alice"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := s.ApplyAction("INC-1", "ACT-1", ActionAddNote, "bob", 1, "note", "", ""); err != nil {
		t.Fatalf("add note: %v", err)
	}
	// Different content -> conflict.
	if _, err := s.ApplyAction("INC-1", "ACT-1", ActionAddNote, "bob", 1, "different", "", ""); !IsConflictError(err) {
		t.Fatalf("expected conflict, got %v", err)
	}
	// Different expected_version -> conflict.
	if _, err := s.ApplyAction("INC-1", "ACT-1", ActionAddNote, "bob", 2, "note", "", ""); !IsConflictError(err) {
		t.Fatalf("expected conflict on version, got %v", err)
	}
	// Different operator -> conflict.
	if _, err := s.ApplyAction("INC-1", "ACT-1", ActionAddNote, "carol", 1, "note", "", ""); !IsConflictError(err) {
		t.Fatalf("expected conflict on operator, got %v", err)
	}
}

func TestResolveAndReopen(t *testing.T) {
	s := newTestStore(t)
	if _, err := s.Create("INC-1", "Title", "svc", "alice"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := s.ApplyAction("INC-1", "ACT-1", ActionResolve, "bob", 1, "", "fixed", ""); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	inc, _ := s.Get("INC-1")
	if inc.Status != StatusResolved {
		t.Fatalf("status: %s", inc.Status)
	}
	// Resolved: only reopen allowed.
	if _, err := s.ApplyAction("INC-1", "ACT-2", ActionAddNote, "bob", 2, "note", "", ""); !IsConflictError(err) {
		t.Fatalf("add note on resolved should conflict, got %v", err)
	}
	if _, err := s.ApplyAction("INC-1", "ACT-2", ActionReopen, "bob", 2, "", "regressed", ""); err != nil {
		t.Fatalf("reopen: %v", err)
	}
	inc, _ = s.Get("INC-1")
	if inc.Status != StatusOpen || inc.Version != 3 {
		t.Fatalf("after reopen: %+v", inc)
	}
}

func TestResolveRequiresReason(t *testing.T) {
	s := newTestStore(t)
	if _, err := s.Create("INC-1", "Title", "svc", "alice"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := s.ApplyAction("INC-1", "ACT-1", ActionResolve, "bob", 1, "", "  ", ""); !IsValidationError(err) {
		t.Fatalf("resolve without reason should be validation error, got %v", err)
	}
	if _, err := s.ApplyAction("INC-1", "ACT-1", ActionReopen, "bob", 1, "", "", ""); !IsValidationError(err) {
		t.Fatalf("reopen without reason should be validation error, got %v", err)
	}
}

func TestLinkEvent(t *testing.T) {
	fe := newFakeEvents()
	fe.add("evt-1", "gateway")
	fe.add("evt-2", "api")
	s := NewStore(nil, fe)
	if _, err := s.Create("INC-1", "Title", "gateway", "alice"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := s.ApplyAction("INC-1", "ACT-1", ActionLink, "bob", 1, "", "", "evt-1"); err != nil {
		t.Fatalf("link: %v", err)
	}
	inc, _ := s.Get("INC-1")
	if len(inc.Links) != 1 || inc.Links[0] != "evt-1" {
		t.Fatalf("links: %+v", inc.Links)
	}
	// Same event can be linked to another incident.
	if _, err := s.Create("INC-2", "Other", "gateway", "alice"); err != nil {
		t.Fatalf("create 2: %v", err)
	}
	if _, err := s.ApplyAction("INC-2", "ACT-1", ActionLink, "bob", 1, "", "", "evt-1"); err != nil {
		t.Fatalf("link to second incident: %v", err)
	}
}

func TestLinkEventErrors(t *testing.T) {
	fe := newFakeEvents()
	fe.add("evt-1", "gateway")
	fe.add("evt-2", "api")
	s := NewStore(nil, fe)
	if _, err := s.Create("INC-1", "Title", "gateway", "alice"); err != nil {
		t.Fatalf("create: %v", err)
	}
	// Event not found -> 404.
	if _, err := s.ApplyAction("INC-1", "ACT-1", ActionLink, "bob", 1, "", "", "missing"); !IsNotFoundError(err) {
		t.Fatalf("expected not found, got %v", err)
	}
	// Different service -> 409.
	if _, err := s.ApplyAction("INC-1", "ACT-1", ActionLink, "bob", 1, "", "", "evt-2"); !IsConflictError(err) {
		t.Fatalf("expected service conflict, got %v", err)
	}
	// Already linked -> 409.
	if _, err := s.ApplyAction("INC-1", "ACT-1", ActionLink, "bob", 1, "", "", "evt-1"); err != nil {
		t.Fatalf("link: %v", err)
	}
	if _, err := s.ApplyAction("INC-1", "ACT-2", ActionLink, "bob", 2, "", "", "evt-1"); !IsConflictError(err) {
		t.Fatalf("expected already-linked conflict, got %v", err)
	}
}

func TestUnknownAction(t *testing.T) {
	s := newTestStore(t)
	if _, err := s.Create("INC-1", "Title", "svc", "alice"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := s.ApplyAction("INC-1", "ACT-1", "bogus", "bob", 1, "", "", ""); !IsValidationError(err) {
		t.Fatalf("unknown action should be validation error, got %v", err)
	}
}

func TestIncidentNotFound(t *testing.T) {
	s := newTestStore(t)
	if _, err := s.Get("MISSING"); !IsNotFoundError(err) {
		t.Fatalf("expected not found, got %v", err)
	}
	if _, err := s.ApplyAction("MISSING", "ACT-1", ActionAddNote, "bob", 1, "note", "", ""); !IsNotFoundError(err) {
		t.Fatalf("expected not found, got %v", err)
	}
}

func TestExpectedVersionMustBePositive(t *testing.T) {
	s := newTestStore(t)
	if _, err := s.Create("INC-1", "Title", "svc", "alice"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := s.ApplyAction("INC-1", "ACT-1", ActionAddNote, "bob", 0, "note", "", ""); !IsValidationError(err) {
		t.Fatalf("expected validation error, got %v", err)
	}
	if _, err := s.ApplyAction("INC-1", "ACT-1", ActionAddNote, "bob", -1, "note", "", ""); !IsValidationError(err) {
		t.Fatalf("expected validation error, got %v", err)
	}
}

func TestGetReturnsConsistentSnapshot(t *testing.T) {
	s := newTestStore(t)
	if _, err := s.Create("INC-1", "Title", "svc", "alice"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := s.ApplyAction("INC-1", "ACT-1", ActionAddNote, "bob", 1, "note", "", ""); err != nil {
		t.Fatalf("add note: %v", err)
	}
	inc, err := s.Get("INC-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	// Mutate the store; the returned snapshot must be unaffected.
	if _, err := s.ApplyAction("INC-1", "ACT-2", ActionAddNote, "bob", 2, "more", "", ""); err != nil {
		t.Fatalf("add note 2: %v", err)
	}
	if inc.Version != 2 || len(inc.History) != 2 {
		t.Fatalf("snapshot mutated: version=%d history=%d", inc.Version, len(inc.History))
	}
}

func TestWriteFailurePoisonsStore(t *testing.T) {
	logger := &fakeLogger{}
	s := NewStore(logger, newFakeEvents())
	if _, err := s.Create("INC-1", "Title", "svc", "alice"); err != nil {
		t.Fatalf("create: %v", err)
	}
	logger.failNext = true
	if _, err := s.ApplyAction("INC-1", "ACT-1", ActionAddNote, "bob", 1, "note", "", ""); err == nil {
		t.Fatal("expected write failure")
	}
	if !s.Poisoned() {
		t.Fatal("store should be poisoned after write failure")
	}
	// Subsequent requests fail with ErrPoisoned.
	if _, err := s.Get("INC-1"); !errors.Is(err, ErrPoisoned) {
		t.Fatalf("get after poison: %v", err)
	}
	if _, err := s.ApplyAction("INC-1", "ACT-2", ActionAddNote, "bob", 1, "note", "", ""); !errors.Is(err, ErrPoisoned) {
		t.Fatalf("action after poison: %v", err)
	}
	// The failed action must not have changed the incident. Check the
	// in-memory state directly since Get refuses after poison.
	s.mu.Lock()
	inc := s.incidents["INC-1"]
	s.mu.Unlock()
	if inc.Version != 1 || len(inc.History) != 1 {
		t.Fatalf("failed action changed state: version=%d history=%d", inc.Version, len(inc.History))
	}
}
