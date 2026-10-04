package events

import (
	"testing"
	"time"
)

// TestQueryExplicitZeroBound verifies that an explicit boundary at the zero
// instant is applied like any other instant: it must not be confused with an
// unset bound. Bounds are inclusive and compared by absolute instant.
func TestQueryExplicitZeroBound(t *testing.T) {
	tl := NewTimeline()
	epoch := time.Date(2026, time.October, 1, 9, 0, 0, 0, time.UTC)
	batch := []Event{
		mkEvent("at", "s", "info", "m", epoch),
		mkEvent("after", "s", "info", "m", epoch.Add(time.Nanosecond)),
	}
	if _, err := tl.Ingest(batch, nil); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	zero := time.Time{}

	// until at the zero instant excludes every event after it; it is not
	// treated as "no upper bound".
	if got := tl.Query(Query{Until: &zero}); len(got) != 0 {
		t.Fatalf("zero until must exclude all later events, got %s", ids(got))
	}

	// since at the zero instant keeps all normal events (none precede it).
	if got := tl.Query(Query{Since: &zero}); ids(got) != "at,after" {
		t.Fatalf("zero since must include all events, got %s", ids(got))
	}

	// Equal endpoints are allowed and inclusive.
	at := epoch
	if got := tl.Query(Query{Since: &at, Until: &at}); ids(got) != "at" {
		t.Fatalf("since == until must keep the boundary event, got %s", ids(got))
	}

	// One nanosecond past the boundary excludes the event.
	justBefore := epoch.Add(-time.Nanosecond)
	if got := tl.Query(Query{Until: &justBefore}); len(got) != 0 {
		t.Fatalf("event one nanosecond after until must be excluded, got %s", ids(got))
	}
	justAfter := epoch.Add(time.Nanosecond)
	if got := tl.Query(Query{Since: &justAfter}); ids(got) != "after" {
		t.Fatalf("event one nanosecond before since must be excluded, got %s", ids(got))
	}
}

// TestQueryZeroBoundTimeZonesAgree checks that equivalent time zone spellings
// of the same boundary instant filter identically.
func TestQueryZeroBoundTimeZonesAgree(t *testing.T) {
	tl := NewTimeline()
	at := time.Date(2026, time.October, 1, 9, 0, 0, 0, time.UTC)
	if _, err := tl.Ingest([]Event{mkEvent("e1", "s", "info", "m", at)}, nil); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	offset := time.Date(2026, time.October, 1, 17, 0, 0, 0, time.FixedZone("UTC+8", 8*60*60))
	queries := []Query{
		{Until: &at},
		{Until: &offset},
	}
	var first []string
	for i, q := range queries {
		got := ids(tl.Query(q))
		if i == 0 {
			first = []string{got}
			continue
		}
		if got != first[0] {
			t.Fatalf("equivalent timezone spellings differ: %q vs %q", got, first[0])
		}
	}
}

// TestSnapshotIDsExplicitZeroBound mirrors the zero-bound rule for the
// paginated snapshot path.
func TestSnapshotIDsExplicitZeroBound(t *testing.T) {
	tl := NewTimeline()
	epoch := time.Date(2026, time.October, 1, 9, 0, 0, 0, time.UTC)
	if _, err := tl.Ingest([]Event{
		mkEvent("e1", "s", "info", "m", epoch),
		mkEvent("e2", "s", "info", "m", epoch.Add(time.Nanosecond)),
	}, nil); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	zero := time.Time{}
	if got := tl.SnapshotIDs(Query{Until: &zero}); len(got) != 0 {
		t.Fatalf("zero until snapshot must be empty, got %v", got)
	}
	if got := tl.SnapshotIDs(Query{Since: &zero}); len(got) != 2 {
		t.Fatalf("zero since snapshot must contain all events, got %v", got)
	}
}
