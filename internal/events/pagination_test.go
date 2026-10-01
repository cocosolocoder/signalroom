package events

import (
	"errors"
	"testing"
	"time"
)

func TestSnapshotFreezesCommittedSet(t *testing.T) {
	tl := NewTimeline()
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	batch := []Event{
		mkEvent("c", "svc", "info", "m", base.Add(2*time.Minute)),
		mkEvent("a", "svc", "info", "m", base),
		mkEvent("b", "svc", "info", "m", base.Add(time.Minute)),
	}
	if _, err := tl.Ingest(batch, nil); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	snapshot := tl.SnapshotRead(PreparedQuery{})
	if got := snapshot.Count(); got != 3 {
		t.Fatalf("count=%d", got)
	}
	page, next, ok := snapshot.Page(0, 2)
	if !ok || ids(page) != "a,b" || next != 2 {
		t.Fatalf("first page: %s %d %v", ids(page), next, ok)
	}
	page, next, ok = snapshot.Page(2, 2)
	if !ok || ids(page) != "c" || next != 3 {
		t.Fatalf("second page: %s %d %v", ids(page), next, ok)
	}

	// Later commits never alter a frozen snapshot, even with an earlier time
	// or an identical timestamp.
	later := []Event{
		mkEvent("zzz-early", "svc", "info", "m", base.Add(-time.Hour)),
		mkEvent("a2", "svc", "info", "m", base),
	}
	if _, err := tl.Ingest(later, nil); err != nil {
		t.Fatalf("ingest later: %v", err)
	}
	page, _, ok = snapshot.Page(0, 10)
	if !ok || ids(page) != "a,b,c" {
		t.Fatalf("snapshot admitted later commits: %s", ids(page))
	}
	if got := snapshot.IDs(); len(got) != 3 || got[0] != "a" {
		t.Fatalf("snapshot ids: %v", got)
	}
}

func TestSnapshotRespectsFilters(t *testing.T) {
	tl := NewTimeline()
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	batch := []Event{
		{ID: "a", Service: "gw", Severity: "critical", Message: "m", At: base, Labels: map[string]string{"env": "prod"}},
		{ID: "b", Service: "api", Severity: "info", Message: "m", At: base.Add(time.Minute), Labels: map[string]string{"env": "dev"}},
	}
	if _, err := tl.Ingest(batch, nil); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	query := PrepareQuery(Query{
		Service:  " gw ",
		Severity: "CRITICAL",
		Since:    base,
		Until:    base,
		Labels:   map[string]string{"env": "prod"},
	})
	snapshot := tl.SnapshotRead(query)
	if snapshot.Count() != 1 || snapshot.events[0].ID != "a" {
		t.Fatalf("filtered snapshot: %+v", snapshot.events)
	}
}

func TestResumePageReplaysFrozenOrderOnly(t *testing.T) {
	tl := NewTimeline()
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	batch := []Event{
		mkEvent("a", "svc", "info", "m", base),
		mkEvent("b", "svc", "info", "m", base.Add(time.Minute)),
		mkEvent("c", "svc", "info", "m", base.Add(2*time.Minute)),
	}
	if _, err := tl.Ingest(batch, nil); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	// Frozen order "c,a" (arbitrary cursor order) must be honored regardless
	// of the live timeline's time ordering.
	page, next, err := tl.ResumePage([]string{"c", "a"}, 0, 1)
	if err != nil || ids(page) != "c" || next != 1 {
		t.Fatalf("resume first: %s %d %v", ids(page), next, err)
	}
	page, next, err = tl.ResumePage([]string{"c", "a"}, 1, 10)
	if err != nil || ids(page) != "a" || next != 2 {
		t.Fatalf("resume second: %s %d %v", ids(page), next, err)
	}

	// Newly ingested ids are invisible to a cursor that does not name them.
	if _, err := tl.Ingest([]Event{mkEvent("new", "svc", "info", "m", base.Add(-time.Hour))}, nil); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	page, _, err = tl.ResumePage([]string{"c", "a"}, 0, 10)
	if err != nil || ids(page) != "c,a" {
		t.Fatalf("resume after ingest: %s %v", ids(page), err)
	}

	// Invalid offsets and limits.
	for _, params := range [][2]int{{-1, 1}, {3, 1}, {0, 0}, {0, -1}} {
		if _, _, err := tl.ResumePage([]string{"c", "a"}, params[0], params[1]); !errors.Is(err, ErrCursorInvalid) {
			t.Fatalf("params %v want ErrCursorInvalid, got %v", params, err)
		}
	}
	// An unknown id is the foreign-directory signature.
	if _, _, err := tl.ResumePage([]string{"ghost"}, 0, 1); !errors.Is(err, ErrCursorEventMissing) {
		t.Fatalf("unknown id: got %v", err)
	}
	// Resume at the very end returns an empty page and the same offset.
	page, next, err = tl.ResumePage([]string{"c", "a"}, 2, 1)
	if err != nil || len(page) != 0 || next != 2 {
		t.Fatalf("end resume: %+v %d %v", page, next, err)
	}
}

func TestPreparedQueryEquality(t *testing.T) {
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	otherZone := base.In(time.FixedZone("CST", 8*60*60))

	a := PreparedQuery{
		Service: "gw", Severity: "critical",
		Since: base, Until: base.Add(time.Hour),
		Labels: map[string]string{"b": "2", "a": "1"},
	}
	b := PreparedQuery{
		Service: "gw", Severity: "critical",
		Since: otherZone, Until: otherZone.Add(time.Hour),
		Labels: map[string]string{"a": "1", "b": "2"},
	}
	if !a.EqualTo(b) {
		t.Fatal("zone, label-order variants must be equal")
	}
	if !a.EqualTo(a) {
		t.Fatal("self equality")
	}
	c := a
	c.Labels = map[string]string{"a": "1"}
	if a.EqualTo(c) {
		t.Fatal("dropped label must differ")
	}
	c = a
	c.Severity = "info"
	if a.EqualTo(c) {
		t.Fatal("severity change must differ")
	}
	var nilLabels PreparedQuery
	emptyLabels := PreparedQuery{Labels: map[string]string{}}
	if !nilLabels.EqualTo(emptyLabels) {
		t.Fatal("nil and empty label sets must be equal")
	}
}
