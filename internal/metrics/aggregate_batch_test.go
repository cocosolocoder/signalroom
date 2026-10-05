package metrics

import (
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"
)

// Regression coverage for the relationship between whole-batch metric
// ingestion (POST /metrics) and cumulative aggregation
// (GET /metrics/aggregate): every aggregate response must be computed from
// one committed snapshot. The returned series set, every segment's sample
// count, and every segment delta must all belong to the same committed
// dataset — never a mix where one series already counts a batch while another
// series still shows its pre-batch result.
//
// The discriminating scenario: one metric name "requests" in service "svc"
// with three label series. Two series (labels a=1 and a=2) already exist;
// one batch supplements both, creates a third previously absent series
// (a=3), and carries late historical samples — including samples strictly
// before the query start, which rewrite the predecessor used by the first
// in-window sample (a=1's predecessor moves; a=2's first sample gains one it
// did not have). The window spans three one-minute segments, and a sample per
// existing series sits exactly at the query end (both excluded). The batch
// body is deliberately shuffled so predecessor/delta correctness can only
// come from the samples' own timestamps, not their batch order.
//
// Window [10:00:00, 10:03:00), step 60s. Cumulative readings (series keyed
// by label a), all monotonic within a series:
//
//	a=1: 09:57=90 (late), 09:58=100 (seed), 09:59=199 (late),
//	     10:00:30=200 (seed), 10:01:30=201, 10:03:00=999 (excluded)
//	a=2: 09:58=20 (late), 09:59=180 (late), 10:00:30=200 (seed),
//	     10:02:30=223, 10:03:00=999 (excluded)
//	a=3: 09:59=5 (late), 10:01:30=50, 10:02:30=60
//
// Before the batch a=1/a=2 each hold one in-window point. The pre-batch
// total delta is 100 and the post-batch total delta is also 100
// (a=1:2, a=2:43, a=3:55), so a check that only sums every series' deltas
// cannot distinguish the two states; the per-series, per-segment vectors do.

func aggScenarioAnchor() time.Time {
	return time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
}

func aggScenarioQuery() AggregateQuery {
	t0 := aggScenarioAnchor()
	return AggregateQuery{
		Name:  "requests",
		Since: t0,
		Until: t0.Add(3 * time.Minute),
		Step:  time.Minute,
	}
}

// scenarioSeedSamples are the pre-existing readings: one in-window point per
// existing series, plus a predecessor for series a=1. Series a=2's in-window
// point is its first sample overall (no predecessor yet).
func scenarioSeedSamples() []Sample {
	t0 := aggScenarioAnchor()
	return []Sample{
		{ID: "seed-pre-1", Service: "svc", Name: "requests", At: t0.Add(-2 * time.Minute), Value: 100, Labels: map[string]string{"a": "1"}},
		{ID: "seed-a1", Service: "svc", Name: "requests", At: t0.Add(30 * time.Second), Value: 200, Labels: map[string]string{"a": "1"}},
		{ID: "seed-a2", Service: "svc", Name: "requests", At: t0.Add(30 * time.Second), Value: 200, Labels: map[string]string{"a": "2"}},
	}
}

// scenarioBatchSamples is the whole batch under test, shuffled on purpose:
// points at the query end, the brand-new series, late pre-window
// predecessors (one of them earlier than the stored predecessor), in-window
// supplements, and more late history. Ordering must never affect the result.
func scenarioBatchSamples() []Sample {
	t0 := aggScenarioAnchor()
	return []Sample{
		{ID: "new-end-1", Service: "svc", Name: "requests", At: t0.Add(3 * time.Minute), Value: 999, Labels: map[string]string{"a": "1"}},
		{ID: "new-a3-1", Service: "svc", Name: "requests", At: t0.Add(90 * time.Second), Value: 50, Labels: map[string]string{"a": "3"}},
		{ID: "new-nearpre-1", Service: "svc", Name: "requests", At: t0.Add(-1 * time.Minute), Value: 199, Labels: map[string]string{"a": "1"}},
		{ID: "new-a1-2", Service: "svc", Name: "requests", At: t0.Add(90 * time.Second), Value: 201, Labels: map[string]string{"a": "1"}},
		{ID: "new-farpre-2", Service: "svc", Name: "requests", At: t0.Add(-2 * time.Minute), Value: 20, Labels: map[string]string{"a": "2"}},
		{ID: "new-a2-1", Service: "svc", Name: "requests", At: t0.Add(150 * time.Second), Value: 223, Labels: map[string]string{"a": "2"}},
		{ID: "new-farpre-1", Service: "svc", Name: "requests", At: t0.Add(-3 * time.Minute), Value: 90, Labels: map[string]string{"a": "1"}},
		{ID: "new-nearpre-2", Service: "svc", Name: "requests", At: t0.Add(-1 * time.Minute), Value: 180, Labels: map[string]string{"a": "2"}},
		{ID: "new-a3-0", Service: "svc", Name: "requests", At: t0.Add(-1 * time.Minute), Value: 5, Labels: map[string]string{"a": "3"}},
		{ID: "new-end-2", Service: "svc", Name: "requests", At: t0.Add(3 * time.Minute), Value: 999, Labels: map[string]string{"a": "2"}},
		{ID: "new-a3-2", Service: "svc", Name: "requests", At: t0.Add(150 * time.Second), Value: 60, Labels: map[string]string{"a": "3"}},
	}
}

// scenarioConflictBatchSamples are the valid batch samples plus one sample
// with a brand-new id that claims an already stored (series, instant) point:
// series a=1 at 10:00:30 already belongs to seed-a1. The conflict must
// reject the whole submission before anything is persisted.
func scenarioConflictBatchSamples() []Sample {
	conflict := append([]Sample(nil), scenarioBatchSamples()...)
	t0 := aggScenarioAnchor()
	conflict = append(conflict, Sample{
		ID:      "clash-a1",
		Service: "svc",
		Name:    "requests",
		At:      t0.Add(30 * time.Second),
		Value:   201,
		Labels:  map[string]string{"a": "1"},
	})
	return conflict
}

// Expected segment vectors (three one-minute segments in window order), one
// entry per present series. The pre- and post-batch grand totals both equal
// 100 even though every per-series/per-segment value moves, which is exactly
// why the snapshot check must compare the whole structured response.
var (
	beforeCounts = map[string][]int{
		"1": {1, 0, 0},
		"2": {1, 0, 0},
	}
	beforeDeltas = map[string][]float64{
		"1": {100, 0, 0}, // 200 - predecessor 100
		"2": {0, 0, 0},   // first sample overall: no predecessor
	}
	afterCounts = map[string][]int{
		"1": {1, 1, 0},
		"2": {1, 0, 1},
		"3": {0, 1, 1},
	}
	afterDeltas = map[string][]float64{
		"1": {1, 1, 0},   // (200-199), (201-200); end sample excluded
		"2": {20, 0, 23}, // (200-180), (223-200); end sample excluded
		"3": {0, 45, 10}, // first in-window 50 - predecessor 5, then 60-50
	}
)

func assertScenarioResult(t *testing.T, got []AggregateSeries, wantSeries []string,
	wantCounts map[string][]int, wantDeltas map[string][]float64) {
	t.Helper()
	if len(got) != len(wantSeries) {
		t.Fatalf("series set = %d (%v), want %v", len(got), seriesNames(got), wantSeries)
	}
	t0 := aggScenarioAnchor()
	for i, name := range wantSeries {
		series := got[i]
		if series.Service != "svc" || series.Labels["a"] != name {
			t.Fatalf("series %d = service=%q labels=%v, want a=%q (ordered a=1,a=2,a=3)",
				i, series.Service, series.Labels, name)
		}
		if len(series.Segments) != 3 {
			t.Fatalf("series a=%s segments = %d, want 3", name, len(series.Segments))
		}
		for j, seg := range series.Segments {
			wantStart := t0.Add(time.Duration(j) * time.Minute)
			wantEnd := wantStart.Add(time.Minute)
			if !seg.Start.Equal(wantStart) || !seg.End.Equal(wantEnd) {
				t.Fatalf("series a=%s seg %d bounds = %s..%s, want %s..%s",
					name, j, seg.Start, seg.End, wantStart, wantEnd)
			}
			if seg.Count != wantCounts[name][j] {
				t.Fatalf("series a=%s seg %d count = %d, want %d (whole result: %s)",
					name, j, seg.Count, wantCounts[name][j], summarizeAggregate(got))
			}
			if seg.Delta != wantDeltas[name][j] {
				t.Fatalf("series a=%s seg %d delta = %v, want %v (whole result: %s)",
					name, j, seg.Delta, wantDeltas[name][j], summarizeAggregate(got))
			}
		}
	}
}

func seriesNames(got []AggregateSeries) []string {
	names := make([]string, 0, len(got))
	for _, s := range got {
		names = append(names, s.Labels["a"])
	}
	return names
}

// summarizeAggregate renders counts/deltas per series so a splice failure
// shows every series and segment, not just the first mismatch.
func summarizeAggregate(got []AggregateSeries) string {
	out := ""
	for _, s := range got {
		out += "a=" + s.Labels["a"] + "{"
		for i, seg := range s.Segments {
			if i > 0 {
				out += " "
			}
			out += "c=" + strconv.Itoa(seg.Count) + ",d=" + strconv.FormatFloat(seg.Delta, 'g', -1, 64)
		}
		out += "} "
	}
	return out
}

func TestAggregateScenarioBeforeAndAfterWholeBatch(t *testing.T) {
	s := NewStore()
	query := aggScenarioQuery()
	if _, err := s.Ingest(scenarioSeedSamples(), nil); err != nil {
		t.Fatalf("seed: %v", err)
	}

	before, err := s.Aggregate(query)
	if err != nil {
		t.Fatalf("aggregate before: %v", err)
	}
	assertScenarioResult(t, before, []string{"1", "2"}, beforeCounts, beforeDeltas)

	result, err := s.Ingest(scenarioBatchSamples(), noopCommit)
	if err != nil {
		t.Fatalf("ingest batch: %v", err)
	}
	if result.Created != 11 || result.Replayed != 0 {
		t.Fatalf("batch result = %+v, want created=11", result)
	}

	// Once the successful ingestion has returned, the same query must show
	// the complete post-submission dataset across every series and segment.
	after, err := s.Aggregate(query)
	if err != nil {
		t.Fatalf("aggregate after: %v", err)
	}
	assertScenarioResult(t, after, []string{"1", "2", "3"}, afterCounts, afterDeltas)

	// Timestamps, not batch order, determine predecessor readings: replaying
	// the identical (shuffled) batch stores nothing and changes nothing.
	replay, err := s.Ingest(scenarioBatchSamples(), noopCommit)
	if err != nil {
		t.Fatalf("replay batch: %v", err)
	}
	if replay.Created != 0 || replay.Replayed != 11 {
		t.Fatalf("replay result = %+v, want replayed=11", replay)
	}
	again, err := s.Aggregate(query)
	if err != nil {
		t.Fatalf("aggregate after replay: %v", err)
	}
	if !reflect.DeepEqual(again, after) {
		t.Fatalf("replay changed the aggregate:\nbefore=%s\nafter =%s", summarizeAggregate(after), summarizeAggregate(again))
	}
}

// ensureSlowAggregate grows the store with irrelevant series until one
// aggregate call takes long enough to overlap deterministically with a
// concurrent ingestion. The filler series use another service, so the
// scenario query never includes them; they only widen the read scan.
func ensureSlowAggregate(t *testing.T, s *Store, query AggregateQuery) time.Duration {
	t.Helper()
	base := aggScenarioAnchor().AddDate(0, -1, 0)
	scan := func() time.Duration {
		start := time.Now()
		if _, err := s.Aggregate(query); err != nil {
			t.Fatalf("warmup aggregate: %v", err)
		}
		return time.Since(start)
	}
	total := 0
	for round := 0; round < 6; round++ {
		const chunk = 40000
		fillers := make([]Sample, 0, chunk)
		for i := range chunk {
			id := strconv.Itoa(total + i)
			fillers = append(fillers, Sample{
				ID:      "filler-" + id,
				Service: "filler-svc",
				Name:    "requests",
				At:      base.Add(time.Duration(total+i) * time.Nanosecond),
				Value:   float64(total + i),
				Labels:  map[string]string{"i": id},
			})
		}
		if _, err := s.Ingest(fillers, noopCommit); err != nil {
			t.Fatalf("seed fillers: %v", err)
		}
		total += chunk
		var slowest time.Duration
		for range 3 {
			if d := scan(); d > slowest {
				slowest = d
			}
		}
		if slowest >= 5*time.Millisecond {
			return slowest
		}
	}
	return scan()
}

// TestAggregateOverlappingBatchCommitIsOneSnapshot overlaps aggregate reads
// with a whole-batch submission held inside its durable commit:
//
//   - An aggregate already scanning when the submission takes the store write
//     lock finishes with the complete pre-submission state.
//   - Every aggregate whose read lock is queued while the commit hook is held
//     waits (that wait is not an error) and returns the complete
//     post-submission state.
//
// Either way a single response may reflect only the full batch or none of it:
// no partial series set, no segment whose count/delta comes from the other
// state — even though the deltas summed across every series and segment
// happen to coincide before and after (100 in both), the per-series and
// per-segment values still must not be spliced.
func TestAggregateOverlappingBatchCommitIsOneSnapshot(t *testing.T) {
	s := NewStore()
	query := aggScenarioQuery()
	if _, err := s.Ingest(scenarioSeedSamples(), nil); err != nil {
		t.Fatalf("seed: %v", err)
	}

	before, err := s.Aggregate(query)
	if err != nil {
		t.Fatalf("aggregate before: %v", err)
	}
	assertScenarioResult(t, before, []string{"1", "2"}, beforeCounts, beforeDeltas)
	scanTime := ensureSlowAggregate(t, s, query)

	earlyDone := make(chan []AggregateSeries, 1)
	go func() {
		got, err := s.Aggregate(query)
		if err != nil {
			t.Errorf("early aggregate: %v", err)
			return
		}
		earlyDone <- got
	}()
	// Start the submission while the slow aggregate is (almost certainly)
	// mid-scan; the assertions below hold for either scheduling outcome.
	time.Sleep(scanTime / 2)

	entered := make(chan struct{})
	release := make(chan struct{})
	ingestDone := make(chan struct{})
	go func() {
		defer close(ingestDone)
		result, err := s.Ingest(scenarioBatchSamples(), func(persisted []Sample) error {
			close(entered)
			<-release
			return nil
		})
		if err != nil {
			t.Errorf("blocked ingest: %v", err)
			return
		}
		if result.Created != 11 {
			t.Errorf("batch created=%d, want 11", result.Created)
		}
	}()
	<-entered

	// The commit hook holds the write lock now; these aggregates queue behind
	// it, wait for the commit, and scan only afterwards.
	const readers = 24
	observed := make(chan []AggregateSeries, readers)
	var wg sync.WaitGroup
	for range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := s.Aggregate(query)
			if err != nil {
				t.Errorf("queued aggregate: %v", err)
				return
			}
			observed <- got
		}()
	}
	// Give the reads time to pile onto the pending write lock; the guarantee
	// does not depend on this delay, it only widens coverage.
	time.Sleep(20 * time.Millisecond)
	close(release)
	<-ingestDone
	wg.Wait()
	close(observed)

	after, err := s.Aggregate(query)
	if err != nil {
		t.Fatalf("aggregate after: %v", err)
	}
	assertScenarioResult(t, after, []string{"1", "2", "3"}, afterCounts, afterDeltas)
	if reflect.DeepEqual(before, after) {
		t.Fatal("pre- and post-batch aggregates must differ")
	}

	count := 0
	for got := range observed {
		count++
		if !reflect.DeepEqual(got, after) {
			t.Fatalf("queued aggregate must show the whole post-batch state:\n%v\nwant:\n%v",
				summarizeAggregate(got), summarizeAggregate(after))
		}
	}
	if count != readers {
		t.Fatalf("collected %d queued aggregates, want %d", count, readers)
	}

	// The aggregate in flight when the submission started must be exactly one
	// committed dataset: fully pre-batch or fully post-batch.
	early := <-earlyDone
	pre := reflect.DeepEqual(early, before)
	post := reflect.DeepEqual(early, after)
	if !pre && !post {
		t.Fatalf("overlapping aggregate must be the complete pre- or post-batch state, got a splice:\n%v",
			summarizeAggregate(early))
	}
}

// TestAggregateUnchangedAfterConflictBatch covers the 409 contract for
// aggregation: one sample with a different id claiming a stored series at the
// same absolute instant rejects the whole batch, so none of its other valid
// samples may appear and every segment stays at its pre-submission result.
func TestAggregateUnchangedAfterConflictBatch(t *testing.T) {
	s := NewStore()
	query := aggScenarioQuery()
	if _, err := s.Ingest(scenarioSeedSamples(), nil); err != nil {
		t.Fatalf("seed: %v", err)
	}

	before, err := s.Aggregate(query)
	if err != nil {
		t.Fatalf("aggregate before: %v", err)
	}

	if _, err := s.Ingest(scenarioConflictBatchSamples(), noopCommit); !IsConflictError(err) {
		t.Fatalf("series/time clash must be a conflict, got %v", err)
	}
	after, err := s.Aggregate(query)
	if err != nil {
		t.Fatalf("aggregate after conflict: %v", err)
	}
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("409 batch must leave the aggregate untouched:\nbefore=%s\nafter =%s",
			summarizeAggregate(before), summarizeAggregate(after))
	}

	// A repeated id inside one otherwise-valid batch is rejected wholesale
	// too; append a duplicate of one of the batch's own samples.
	dup := append([]Sample(nil), scenarioBatchSamples()...)
	dup = append(dup, scenarioBatchSamples()[0])
	if _, err := s.Ingest(dup, noopCommit); !IsConflictError(err) {
		t.Fatalf("repeated id within batch must be a conflict, got %v", err)
	}
	again, err := s.Aggregate(query)
	if err != nil {
		t.Fatalf("aggregate after duplicate batch: %v", err)
	}
	if !reflect.DeepEqual(again, before) {
		t.Fatalf("duplicate-id batch must leave the aggregate untouched: %s", summarizeAggregate(again))
	}

	// The untouched dataset still moves to the full post state once a valid
	// batch commits, proving the equality checks above were discriminating.
	if _, err := s.Ingest(scenarioBatchSamples(), noopCommit); err != nil {
		t.Fatalf("valid batch after conflicts: %v", err)
	}
	committed, err := s.Aggregate(query)
	if err != nil {
		t.Fatalf("aggregate after valid batch: %v", err)
	}
	assertScenarioResult(t, committed, []string{"1", "2", "3"}, afterCounts, afterDeltas)
}
