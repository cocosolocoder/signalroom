package metrics

import (
	"testing"
	"time"
)

// mustParseInstant parses an RFC3339Nano timestamp at package init, letting
// the regression tests use constants far outside the UnixNano int64 range.
func mustParseInstant(raw string) time.Time {
	at, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		panic("parse " + raw + ": " + err.Error())
	}
	return at
}

// distant samples whose UnixNano values collide through int64 overflow:
// both reduce to 946684800000000000 even though the instants are centuries
// apart. They belong to one series (gateway/requests, labels env=prod).
var (
	distantEarly = mustParseInstant("2000-01-01T00:00:00Z")
	distantLate  = mustParseInstant("2584-07-20T23:34:33.709551616Z")
)

// mustParseAt is the test-scoped form of mustParseInstant.
func mustParseAt(t *testing.T, raw string) time.Time {
	t.Helper()
	at, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		t.Fatalf("parse %q: %v", raw, err)
	}
	return at
}

func distantBatch(earlyID, lateID string) []Sample {
	labels := map[string]string{"env": "prod"}
	return []Sample{
		{ID: earlyID, Service: "gateway", Name: "requests", At: distantEarly, Value: 100, Labels: labels},
		{ID: lateID, Service: "gateway", Name: "requests", At: distantLate, Value: 130, Labels: labels},
	}
}

// TestIngestDistantInstantsSameSeries covers the original defect: two
// samples of one series at instants centuries apart must be two distinct
// points, even when their UnixNano values coincide through int64 overflow.
func TestIngestDistantInstantsSameSeries(t *testing.T) {
	cases := []struct {
		name  string
		batch []Sample
	}{
		{"chronological", distantBatch("a", "b")},
		{"reversed", []Sample{distantBatch("a", "b")[1], distantBatch("a", "b")[0]}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := NewStore()
			res, err := s.Ingest(tc.batch, noopCommit)
			if err != nil {
				t.Fatalf("distant points must not clash: %v", err)
			}
			if res.Created != 2 || res.Replayed != 0 {
				t.Fatalf("result: %+v", res)
			}
			assertDistantWindows(t, s)
		})
	}
}

// TestIngestDistantInstantsSeparateBatches must give the same stored result
// as one batch.
func TestIngestDistantInstantsSeparateBatches(t *testing.T) {
	s := NewStore()
	for _, sample := range distantBatch("a", "b") {
		res, err := s.Ingest([]Sample{sample}, noopCommit)
		if err != nil {
			t.Fatalf("separate ingest: %v", err)
		}
		if res.Created != 1 {
			t.Fatalf("separate ingest result: %+v", res)
		}
	}
	assertDistantWindows(t, s)
}

// assertDistantWindows queries a one-minute, one-step window starting at each
// sample time: each window holds exactly that sample; the first sample
// overall has no predecessor (delta 0) and the late window rises by 30 over
// the nearest earlier sample.
func assertDistantWindows(t *testing.T, s *Store) {
	t.Helper()
	window := func(start time.Time) []AggregateSegment {
		got, err := s.Aggregate(AggregateQuery{
			Name:    "requests",
			Service: "gateway",
			Labels:  map[string]string{"env": "prod"},
			Since:   start,
			Until:   start.Add(time.Minute),
			Step:    time.Minute,
		})
		if err != nil {
			t.Fatalf("aggregate from %v: %v", start, err)
		}
		if len(got) != 1 {
			t.Fatalf("from %v: want one series, got %d", start, len(got))
		}
		if len(got[0].Segments) != 1 {
			t.Fatalf("from %v: want one segment, got %d", start, len(got[0].Segments))
		}
		return got[0].Segments
	}

	early := window(distantEarly)[0]
	if early.Count != 1 || early.Delta != 0 {
		t.Fatalf("early window: count=%d delta=%v, want 1/0", early.Count, early.Delta)
	}
	late := window(distantLate)[0]
	if late.Count != 1 || late.Delta != 30 {
		t.Fatalf("late window: count=%d delta=%v, want 1/30", late.Count, late.Delta)
	}
}

// TestIngestSameInstantDifferentOffsetsStillConflicts checks that real
// conflicts survive the key change, including the in-batch and
// new-vs-stored cases and distinct timezone spellings of one instant.
func TestIngestSameInstantDifferentOffsetsStillConflicts(t *testing.T) {
	z := mustParseAt(t, "2584-07-20T23:34:33.709551616Z")
	offset := mustParseAt(t, "2584-07-21T07:34:33.709551616+08:00")
	if !z.Equal(offset) {
		t.Fatal("test setup: offsets must name the same absolute instant")
	}
	labels := map[string]string{"env": "prod"}

	// Two new samples in one batch.
	if _, err := NewStore().Ingest([]Sample{
		{ID: "a", Service: "gateway", Name: "requests", At: z, Value: 1, Labels: labels},
		{ID: "b", Service: "gateway", Name: "requests", At: offset, Value: 2, Labels: labels},
	}, noopCommit); !IsConflictError(err) {
		t.Fatalf("in-batch same-instant clash must conflict, got %v", err)
	}

	// New sample against an already-stored one.
	s := NewStore()
	if _, err := s.Ingest([]Sample{
		{ID: "a", Service: "gateway", Name: "requests", At: z, Value: 1, Labels: labels},
	}, noopCommit); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Ingest([]Sample{
		{ID: "b", Service: "gateway", Name: "requests", At: offset, Value: 2, Labels: labels},
	}, noopCommit); !IsConflictError(err) {
		t.Fatalf("stored same-instant clash must conflict, got %v", err)
	}
}

// TestIngestOneNanosecondApartAreDistinct guards the precision requirement:
// the key change must not collapse adjacent nanoseconds.
func TestIngestOneNanosecondApartAreDistinct(t *testing.T) {
	t1 := mustParseAt(t, "2584-07-20T23:34:33.709551615Z")
	t2 := mustParseAt(t, "2584-07-20T23:34:33.709551616Z")
	res, err := NewStore().Ingest([]Sample{
		{ID: "a", Service: "gateway", Name: "requests", At: t1, Value: 1},
		{ID: "b", Service: "gateway", Name: "requests", At: t2, Value: 2},
	}, noopCommit)
	if err != nil {
		t.Fatalf("instants one nanosecond apart are distinct points: %v", err)
	}
	if res.Created != 2 {
		t.Fatalf("result: %+v", res)
	}
}

// TestIngestDistantClashRejectsWholeBatch ensures a conflict between two new
// far-future samples leaves neither behind.
func TestIngestDistantClashRejectsWholeBatch(t *testing.T) {
	s := NewStore()
	z := distantLate
	offset := mustParseAt(t, "2584-07-21T07:34:33.709551616+08:00")
	labels := map[string]string{"env": "prod"}
	bad := []Sample{
		{ID: "a", Service: "gateway", Name: "requests", At: z, Value: 1, Labels: labels},
		{ID: "b", Service: "gateway", Name: "requests", At: offset, Value: 2, Labels: labels},
	}
	if _, err := s.Ingest(bad, noopCommit); !IsConflictError(err) {
		t.Fatalf("same instant must conflict, got %v", err)
	}
	// Neither id became visible: the window around the instant is empty and
	// a fresh id can still claim the point.
	got, err := s.Aggregate(AggregateQuery{
		Name:  "requests",
		Since: z.Add(-time.Minute),
		Until: z.Add(time.Minute),
		Step:  time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("rejected batch must leave nothing behind, got %+v", got)
	}
	if _, err := s.Ingest([]Sample{
		{ID: "c", Service: "gateway", Name: "requests", At: z, Value: 1, Labels: labels},
	}, noopCommit); err != nil {
		t.Fatalf("the instant must still be free after rejection: %v", err)
	}
}

// TestIngestDistantInstantsDifferentLabelsAreDifferentSeries checks that two
// complete label sets never block each other at one instant.
func TestIngestDistantInstantsDifferentLabelsAreDifferentSeries(t *testing.T) {
	z := distantLate
	batch := []Sample{
		{ID: "a", Service: "gateway", Name: "requests", At: z, Value: 1, Labels: map[string]string{"env": "prod"}},
		{ID: "b", Service: "gateway", Name: "requests", At: z, Value: 1, Labels: map[string]string{"env": "staging"}},
	}
	res, err := NewStore().Ingest(batch, noopCommit)
	if err != nil {
		t.Fatalf("distinct label sets are distinct series: %v", err)
	}
	if res.Created != 2 {
		t.Fatalf("result: %+v", res)
	}
}
