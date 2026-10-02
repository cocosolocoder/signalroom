package metrics

import (
	"errors"
	"testing"
	"time"
)

func mustIngest(t *testing.T, s *Store, batch []Sample) {
	t.Helper()
	if _, err := s.Ingest(batch, noopCommit); err != nil {
		t.Fatalf("ingest: %v", err)
	}
}

func q(since, until int64, stepSec int64, name string) AggregateQuery {
	return AggregateQuery{
		Name:  name,
		Since: at(since),
		Until: at(until),
		Step:  time.Duration(stepSec) * time.Second,
	}
}

func TestAggregateDeltasResetsAndPredecessor(t *testing.T) {
	s := NewStore()
	// Predecessor before the window at value 100. Then a rise, a reset
	// (fallback), and another rise; the sample exactly at the window end is
	// excluded.
	mustIngest(t, s, []Sample{
		{ID: "p", Service: "gw", Name: "req", At: at(9), Value: 100},
		{ID: "a", Service: "gw", Name: "req", At: at(10), Value: 105}, // +5 seg0
		{ID: "b", Service: "gw", Name: "req", At: at(13), Value: 3},   // reset -> +3 seg1
		{ID: "c", Service: "gw", Name: "req", At: at(16), Value: 4},   // +1 seg1
		{ID: "end", Service: "gw", Name: "req", At: at(20), Value: 999},
	})

	got, err := s.Aggregate(q(10, 20, 5, "req"))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("want one series, got %d", len(got))
	}
	segments := got[0].Segments
	if len(segments) != 2 {
		t.Fatalf("want two segments, got %d", len(segments))
	}
	want := []struct {
		count        int
		delta        float64
		since, until int64
	}{
		{2, 8, 10, 15}, // a@10 rises +5, b@13 resets to +3
		{1, 1, 15, 20}, // c@16 rises +1
	}
	for i, w := range want {
		if segments[i].Count != w.count || segments[i].Delta != w.delta {
			t.Fatalf("seg %d: got count=%d delta=%v, want %d/%v", i, segments[i].Count, segments[i].Delta, w.count, w.delta)
		}
		if !segments[i].Start.Equal(at(w.since)) || !segments[i].End.Equal(at(w.until)) {
			t.Fatalf("seg %d boundaries: %v..%v", i, segments[i].Start, segments[i].End)
		}
	}
}

func TestAggregateFirstSampleHasNoPredecessor(t *testing.T) {
	s := NewStore()
	mustIngest(t, s, []Sample{
		{ID: "a", Service: "gw", Name: "req", At: at(10), Value: 50},
		{ID: "b", Service: "gw", Name: "req", At: at(11), Value: 60},
	})
	got, err := s.Aggregate(q(10, 20, 10, "req"))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("want one series, got %d", len(got))
	}
	seg := got[0].Segments[0]
	if seg.Count != 2 {
		t.Fatalf("count=%d", seg.Count)
	}
	// First sample contributes nothing; only 60-50=10 lands in the segment.
	if seg.Delta != 10 {
		t.Fatalf("delta=%v, want 10", seg.Delta)
	}
}

func TestAggregateEmptySegmentsAreZero(t *testing.T) {
	s := NewStore()
	mustIngest(t, s, []Sample{
		{ID: "p", Service: "gw", Name: "req", At: at(-1), Value: 0},
		{ID: "a", Service: "gw", Name: "req", At: at(1), Value: 5},
	})
	got, err := s.Aggregate(q(0, 10, 2, "req"))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || len(got[0].Segments) != 5 {
		t.Fatalf("unexpected shape: %+v", got)
	}
	if got[0].Segments[0].Delta != 5 || got[0].Segments[0].Count != 1 {
		t.Fatalf("first segment: %+v", got[0].Segments[0])
	}
	for i := 1; i < 5; i++ {
		if got[0].Segments[i].Count != 0 || got[0].Segments[i].Delta != 0 {
			t.Fatalf("empty segment %d must be zero: %+v", i, got[0].Segments[i])
		}
	}
}

func TestAggregateRemainderSegment(t *testing.T) {
	s := NewStore()
	mustIngest(t, s, []Sample{
		{ID: "a", Service: "gw", Name: "req", At: at(10), Value: 1},
		{ID: "b", Service: "gw", Name: "req", At: at(12), Value: 2},
	})
	// 10..13 with step 5 -> segments [10,15) clipped to [10,13).
	got, err := s.Aggregate(q(10, 13, 5, "req"))
	if err != nil {
		t.Fatal(err)
	}
	if len(got[0].Segments) != 1 {
		t.Fatalf("want single remainder segment, got %d", len(got[0].Segments))
	}
	if !got[0].Segments[0].End.Equal(at(13)) {
		t.Fatalf("remainder must end at window end: %v", got[0].Segments[0].End)
	}
}

func TestAggregateFiltersAndOrdering(t *testing.T) {
	s := NewStore()
	mustIngest(t, s, []Sample{
		{ID: "1", Service: "b", Name: "req", At: at(10), Value: 1, Labels: map[string]string{"env": "prod"}},
		{ID: "2", Service: "a", Name: "req", At: at(10), Value: 1},
		{ID: "3", Service: "a", Name: "req", At: at(10), Value: 1, Labels: map[string]string{"env": "prod"}},
		{ID: "4", Service: "a", Name: "other", At: at(10), Value: 1},
	})

	// Exact name; services sorted, label set ordering canonical.
	got, err := s.Aggregate(q(10, 20, 10, "req"))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("want three series, got %d: %+v", len(got), got)
	}
	if got[0].Service != "a" || got[0].Labels != nil {
		t.Fatalf("series order 0: %+v", got[0])
	}
	if got[1].Service != "a" || got[1].Labels["env"] != "prod" {
		t.Fatalf("series order 1: %+v", got[1])
	}
	if got[2].Service != "b" {
		t.Fatalf("series order 2: %+v", got[2])
	}

	// Service filter.
	got, _ = s.Aggregate(func() AggregateQuery {
		q := q(10, 20, 10, "req")
		q.Service = "a"
		return q
	}())
	if len(got) != 2 {
		t.Fatalf("service filter: %d", len(got))
	}

	// Label filter follows event query rules (case-sensitive).
	got, _ = s.Aggregate(func() AggregateQuery {
		q := q(10, 20, 10, "req")
		q.Labels = map[string]string{"env": "prod"}
		return q
	}())
	if len(got) != 2 {
		t.Fatalf("label filter: %d", len(got))
	}
}

func TestAggregateLateArrivalDoesNotChangeResult(t *testing.T) {
	mk := func(order []int64) float64 {
		s := NewStore()
		for _, sec := range order {
			mustIngest(t, s, []Sample{{
				ID: "i" + string(rune('a'+sec)), Service: "gw", Name: "req",
				At: at(int64(sec)), Value: float64(sec),
			}})
		}
		got, err := s.Aggregate(q(0, 20, 10, "req"))
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 {
			t.Fatalf("series: %d", len(got))
		}
		var total float64
		for _, seg := range got[0].Segments {
			total += seg.Delta
		}
		return total
	}
	forward := mk([]int64{1, 2, 3})
	backward := mk([]int64{3, 2, 1})
	shuffled := mk([]int64{2, 1, 3})
	if forward != backward || forward != shuffled || forward != 2 {
		t.Fatalf("ordering changed totals: %v %v %v (want 2)", forward, backward, shuffled)
	}
}

func TestAggregateNoMatchingSeriesReturnsEmpty(t *testing.T) {
	s := NewStore()
	mustIngest(t, s, []Sample{{ID: "a", Service: "gw", Name: "req", At: at(10), Value: 1}})
	got, err := s.Aggregate(q(10, 20, 10, "absent"))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("want empty result, got %+v", got)
	}
	// Samples present but all outside the window: no series returned.
	got, _ = s.Aggregate(q(20, 30, 10, "req"))
	if len(got) != 0 {
		t.Fatalf("out-of-window samples must not produce a series: %+v", got)
	}
}

func TestAggregateQueryValidation(t *testing.T) {
	base := q(0, 100, 10, "req")
	cases := []func(AggregateQuery) AggregateQuery{
		func(q AggregateQuery) AggregateQuery { q.Step = 0; return q },
		func(q AggregateQuery) AggregateQuery { q.Until = q.Since; return q },
		func(q AggregateQuery) AggregateQuery { q.Until = at(-1); return q },
	}
	for i, mutate := range cases {
		if _, err := NewStore().Aggregate(mutate(base)); err == nil {
			t.Fatalf("case %d must fail", i)
		}
	}

	// More than 10000 segments.
	big := q(0, 10001, 1, "req")
	if _, err := NewStore().Aggregate(big); !errors.Is(err, ErrTooManySegments) {
		t.Fatalf("segment limit: %v", err)
	}
}

func TestAggregateNonFiniteDeltaFailsWholeQuery(t *testing.T) {
	s := NewStore()
	// Two near-ceiling rises in one segment (a reset between them makes the
	// second rise start again from a small value) sum past the float ceiling:
	// the whole query must fail rather than return an infinite delta.
	mustIngest(t, s, []Sample{
		{ID: "p", Service: "gw", Name: "req", At: at(9), Value: 0},
		{ID: "a", Service: "gw", Name: "req", At: at(10), Value: 1.7e308},
		{ID: "b", Service: "gw", Name: "req", At: at(11), Value: 1},
		{ID: "c", Service: "gw", Name: "req", At: at(12), Value: 1.7e308},
	})
	_, err := s.Aggregate(q(10, 20, 10, "req"))
	if !errors.Is(err, ErrDeltaNotFinite) {
		t.Fatalf("non-finite delta must fail the query, got %v", err)
	}
}

func TestAggregateNonFiniteGrandTotalAcrossSeries(t *testing.T) {
	s := NewStore()
	// Two distinct series each contribute a near-ceiling delta; each series
	// stays finite but their grand total overflows, which must still fail.
	mustIngest(t, s, []Sample{
		{ID: "p1", Service: "gw", Name: "req", At: at(9), Value: 0, Labels: map[string]string{"k": "a"}},
		{ID: "a1", Service: "gw", Name: "req", At: at(10), Value: 1.7e308, Labels: map[string]string{"k": "a"}},
		{ID: "p2", Service: "gw", Name: "req", At: at(9), Value: 0, Labels: map[string]string{"k": "b"}},
		{ID: "a2", Service: "gw", Name: "req", At: at(10), Value: 1.7e308, Labels: map[string]string{"k": "b"}},
	})
	_, err := s.Aggregate(q(10, 20, 10, "req"))
	if !errors.Is(err, ErrDeltaNotFinite) {
		t.Fatalf("non-finite grand total must fail the query, got %v", err)
	}
}

func TestAggregateSeriesSegmentLimit(t *testing.T) {
	s := NewStore()
	// 11 series over a window needing 10000 segments -> 110000 > 100000.
	for i := 0; i < 11; i++ {
		mustIngest(t, s, []Sample{{
			ID:      "s" + string(rune('a'+i)),
			Service: "gw",
			Name:    "req",
			At:      at(0),
			Value:   1,
			Labels:  map[string]string{"k": string(rune('a' + i))},
		}})
	}
	_, err := s.Aggregate(q(0, 10000, 1, "req"))
	if !errors.Is(err, ErrTooManySeriesSegments) {
		t.Fatalf("series-segment limit: %v", err)
	}
}
