package metrics

import (
	"errors"
	"math"
	"strconv"
	"sync"
	"testing"
	"time"
)

func at(sec int64) time.Time {
	return time.Unix(sec, 0).UTC()
}

func noopCommit(persisted []Sample) error { return nil }

func TestNormalizeTrimsAndRequiresFields(t *testing.T) {
	s, err := Normalize(Sample{
		ID: "  m1  ", Service: " gw ", Name: "requests",
		At: at(10), Value: 1, Labels: map[string]string{" env ": " prod "},
	})
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if s.ID != "m1" || s.Service != "gw" {
		t.Fatalf("trimming failed: %+v", s)
	}
	if s.Labels["env"] != "prod" {
		t.Fatalf("labels not normalized: %+v", s.Labels)
	}

	bad := []Sample{
		{ID: " ", Service: "gw", Name: "n", At: at(1), Value: 1},
		{ID: "m", Service: " ", Name: "n", At: at(1), Value: 1},
		{ID: "m", Service: "gw", Name: " ", At: at(1), Value: 1},
		{ID: "m", Service: "gw", Name: "n", Value: 1}, // zero time
		{ID: "m", Service: "gw", Name: "n", At: at(1), Value: -1},
		{ID: "m", Service: "gw", Name: "n", At: at(1), Value: math.NaN()},
		{ID: "m", Service: "gw", Name: "n", At: at(1), Value: math.Inf(1)},
	}
	for i, sample := range bad {
		if _, err := Normalize(sample); err == nil {
			t.Fatalf("bad sample %d (%+v) must fail normalization", i, sample)
		} else if !IsValidationError(err) {
			t.Fatalf("bad sample %d: want validation error, got %T", i, err)
		}
	}
}

func TestIngestRetriesAndConflicts(t *testing.T) {
	s := NewStore()
	batch := []Sample{
		{ID: "a", Service: "gw", Name: "requests", At: at(10), Value: 1},
		{ID: "b", Service: "gw", Name: "requests", At: at(20), Value: 2},
	}
	res, err := s.Ingest(batch, noopCommit)
	if err != nil {
		t.Fatal(err)
	}
	if res.Created != 2 || res.Replayed != 0 {
		t.Fatalf("initial ingest: %+v", res)
	}

	// Identical retry: 1 vs 1.0 and an equivalent timezone both count equal.
	tz := time.Unix(10, 0).In(time.FixedZone("CST", 8*3600))
	retry := []Sample{
		{ID: "a", Service: "gw", Name: "requests", At: tz, Value: 1.0},
		{ID: "b", Service: "gw", Name: "requests", At: at(20), Value: 2},
	}
	res, err = s.Ingest(retry, noopCommit)
	if err != nil {
		t.Fatalf("identical retry: %v", err)
	}
	if res.Created != 0 || res.Replayed != 2 {
		t.Fatalf("retry result: %+v", res)
	}

	// Different content under the same id is a conflict.
	if _, err := s.Ingest([]Sample{
		{ID: "a", Service: "gw", Name: "requests", At: at(10), Value: 9},
	}, noopCommit); !IsConflictError(err) {
		t.Fatalf("changed content must conflict, got %v", err)
	}

	// A repeated id within one batch is a conflict.
	if _, err := s.Ingest([]Sample{
		{ID: "c", Service: "gw", Name: "requests", At: at(30), Value: 3},
		{ID: "c", Service: "gw", Name: "requests", At: at(40), Value: 4},
	}, noopCommit); !IsConflictError(err) {
		t.Fatalf("in-batch duplicate id must conflict, got %v", err)
	}
}

func TestIngestSeriesTimeClashAcrossIDs(t *testing.T) {
	s := NewStore()
	first := []Sample{
		{ID: "a", Service: "gw", Name: "requests", At: at(10), Value: 1, Labels: map[string]string{"env": "prod"}},
	}
	if _, err := s.Ingest(first, noopCommit); err != nil {
		t.Fatal(err)
	}

	// A different id at the same instant for the same service/name/labels
	// conflicts even though the id itself is new.
	if _, err := s.Ingest([]Sample{
		{ID: "b", Service: "gw", Name: "requests", At: at(10), Value: 5, Labels: map[string]string{"env": "prod"}},
	}, noopCommit); !IsConflictError(err) {
		t.Fatalf("series/time clash must conflict, got %v", err)
	}

	// A different label set is a different series and is accepted.
	if _, err := s.Ingest([]Sample{
		{ID: "b", Service: "gw", Name: "requests", At: at(10), Value: 5, Labels: map[string]string{"env": "staging"}},
	}, noopCommit); err != nil {
		t.Fatalf("different series should be accepted: %v", err)
	}

	// Same instant in one batch across two ids on one series conflicts.
	if _, err := s.Ingest([]Sample{
		{ID: "x", Service: "gw", Name: "errors", At: at(10), Value: 1},
		{ID: "y", Service: "gw", Name: "errors", At: at(10), Value: 2},
	}, noopCommit); !IsConflictError(err) {
		t.Fatalf("in-batch series clash must conflict, got %v", err)
	}
}

func TestLabelsWithPunctuationDoNotAliasSeries(t *testing.T) {
	s := NewStore()
	// Two label sets whose naive "k=v;" rendering would coincide must remain
	// distinct series and both be accepted at the same instant.
	batch := []Sample{
		{ID: "a", Service: "gw", Name: "req", At: at(10), Value: 1, Labels: map[string]string{"a": "b", "c": "d"}},
		{ID: "b", Service: "gw", Name: "req", At: at(10), Value: 1, Labels: map[string]string{"a": "b;c=d"}},
	}
	if _, err := s.Ingest(batch, noopCommit); err != nil {
		t.Fatalf("punctuation label sets must be distinct series: %v", err)
	}
}

func TestIngestValidationPrecedesConflict(t *testing.T) {
	s := NewStore()
	if _, err := s.Ingest([]Sample{
		{ID: "a", Service: "gw", Name: "requests", At: at(10), Value: 1},
	}, noopCommit); err != nil {
		t.Fatal(err)
	}
	// A batch with both an invalid sample and an id conflict must report the
	// validation failure.
	_, err := s.Ingest([]Sample{
		{ID: "a", Service: "gw", Name: "requests", At: at(10), Value: 2},
		{ID: " ", Service: "gw", Name: "requests", At: at(11), Value: 1},
	}, noopCommit)
	if !IsValidationError(err) {
		t.Fatalf("validation must take precedence, got %v", err)
	}
}

func TestIngestConcurrentIdenticalBatchCommitsOnce(t *testing.T) {
	s := NewStore()
	var commits int
	var mu sync.Mutex
	commit := func(persisted []Sample) error {
		mu.Lock()
		commits++
		mu.Unlock()
		return nil
	}
	const n = 50
	var wg sync.WaitGroup
	var createdTotal, replayedTotal int
	var cmu sync.Mutex
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := s.Ingest([]Sample{
				{ID: "a", Service: "gw", Name: "requests", At: at(10), Value: 1},
			}, commit)
			if err != nil {
				t.Errorf("concurrent ingest: %v", err)
				return
			}
			cmu.Lock()
			createdTotal += res.Created
			replayedTotal += res.Replayed
			cmu.Unlock()
		}()
	}
	wg.Wait()
	if commits != 1 || createdTotal != 1 || replayedTotal != n-1 {
		t.Fatalf("commits=%d created=%d replayed=%d", commits, createdTotal, replayedTotal)
	}
}

func TestIngestConcurrentDistinctBatchesAtSamePoint(t *testing.T) {
	// Many batches each claim the same (series, time) under distinct ids:
	// exactly one must commit, every other must be a conflict, and nothing may
	// panic or partially apply.
	s := NewStore()
	const n = 50
	var wg sync.WaitGroup
	var successes, conflicts int
	var cmu sync.Mutex
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := s.Ingest([]Sample{
				{ID: "id-" + strconv.Itoa(i), Service: "gw", Name: "requests", At: at(10), Value: float64(i)},
			}, noopCommit)
			cmu.Lock()
			switch {
			case err == nil:
				successes++
			case IsConflictError(err):
				conflicts++
			default:
				t.Errorf("unexpected error: %v", err)
			}
			cmu.Unlock()
		}(i)
	}
	wg.Wait()
	if successes != 1 || conflicts != n-1 {
		t.Fatalf("successes=%d conflicts=%d", successes, conflicts)
	}
}

// parsePoint parses an RFC3339Nano timestamp, failing the test on error.
// Wide dates (before 1678 or after 2262) are legal inputs for the store.
func parsePoint(t *testing.T, literal string) time.Time {
	t.Helper()
	ts, err := time.Parse(time.RFC3339Nano, literal)
	if err != nil {
		t.Fatalf("parse %q: %v", literal, err)
	}
	return ts
}

// aggregateCount returns the total sample count Aggregate reports over the
// given window. It is the queryable evidence that a sample was stored.
func aggregateCount(t *testing.T, store *Store, service, name string, labels map[string]string, since, until time.Time) (int, int) {
	t.Helper()
	series, err := store.Aggregate(AggregateQuery{
		Service: service,
		Name:    name,
		Labels:  labels,
		Since:   since,
		Until:   until,
		Step:    until.Sub(since),
	})
	if err != nil {
		t.Fatalf("aggregate [%s,%s): %v", since.Format(time.RFC3339Nano), until.Format(time.RFC3339Nano), err)
	}
	if len(series) == 0 {
		return 0, 0
	}
	if len(series) != 1 {
		t.Fatalf("want one matching series, got %d: %+v", len(series), series)
	}
	total := 0
	for _, seg := range series[0].Segments {
		total += seg.Count
	}
	return total, len(series)
}

// TestIngestWideDatePointsDoNotAlias covers the regression behind the
// overflow-safe point key: the naive UnixNano values of 1600-01-01T00:00:00Z
// and 2184-07-20T23:34:33.709551616Z are identical because the first's
// negative seconds overflow, so a nanosecond-based key would reject the
// second as a clash of one series at one instant.
func TestIngestWideDatePointsDoNotAlias(t *testing.T) {
	oldPoint := parsePoint(t, "1600-01-01T00:00:00Z")
	futurePoint := parsePoint(t, "2184-07-20T23:34:33.709551616Z")
	// Guard the pair: if this ever stops aliasing under UnixNano, this test
	// no longer exercises the bug it was written for.
	if oldPoint.UnixNano() != futurePoint.UnixNano() {
		t.Fatalf("test fixtures lost the UnixNano alias: %d vs %d", oldPoint.UnixNano(), futurePoint.UnixNano())
	}

	// Both submission orders must succeed; the same series owns both points
	// under different ids and times.
	for _, reverse := range []bool{false, true} {
		t.Run("order", func(t *testing.T) {
			store := NewStore()
			batch := []Sample{
				{ID: "era-old", Service: "gw", Name: "requests", At: oldPoint, Value: 1},
				{ID: "era-new", Service: "gw", Name: "requests", At: futurePoint, Value: 2},
			}
			if reverse {
				batch[0], batch[1] = batch[1], batch[0]
			}
			res, err := store.Ingest(batch, noopCommit)
			if err != nil {
				t.Fatalf("distant legal dates must both ingest (reverse=%v): %v", reverse, err)
			}
			if res.Created != 2 || res.Replayed != 0 {
				t.Fatalf("created=%d replayed=%d (reverse=%v)", res.Created, res.Replayed, reverse)
			}

			oldSince := parsePoint(t, "1599-12-31T23:59:59Z")
			oldUntil := parsePoint(t, "1600-01-01T00:00:01Z")
			newSince := parsePoint(t, "2184-07-20T23:34:33Z")
			newUntil := parsePoint(t, "2184-07-20T23:34:34Z")
			if count, _ := aggregateCount(t, store, "gw", "requests", nil, oldSince, oldUntil); count != 1 {
				t.Fatalf("old-era window must contain its one point, got count %d (reverse=%v)", count, reverse)
			}
			if count, _ := aggregateCount(t, store, "gw", "requests", nil, newSince, newUntil); count != 1 {
				t.Fatalf("future-era window must contain its one point, got count %d (reverse=%v)", count, reverse)
			}
		})
	}
}

// TestIngestDateAfter2262Accepted proves there is no artificial upper bound:
// a legal post-2262 date is a distinct point and is queryable.
func TestIngestDateAfter2262Accepted(t *testing.T) {
	store := NewStore()
	far := parsePoint(t, "2300-01-02T03:04:05.678901234Z")
	res, err := store.Ingest([]Sample{
		{ID: "far", Service: "gw", Name: "requests", At: far, Value: 7},
	}, noopCommit)
	if err != nil {
		t.Fatalf("post-2262 date must ingest: %v", err)
	}
	if res.Created != 1 || res.Replayed != 0 {
		t.Fatalf("result: %+v", res)
	}
	since := parsePoint(t, "2300-01-02T03:04:05Z")
	until := parsePoint(t, "2300-01-02T03:04:06Z")
	if count, _ := aggregateCount(t, store, "gw", "requests", nil, since, until); count != 1 {
		t.Fatalf("post-2262 point must be queryable, got count %d", count)
	}
}

// TestIngestNanosecondApartPointsCoexist checks that points one nanosecond
// apart in one second are distinct samples of one series, in either order.
func TestIngestNanosecondApartPointsCoexist(t *testing.T) {
	first := parsePoint(t, "2026-10-01T10:00:00.123456789Z")
	second := first.Add(time.Nanosecond)
	if !first.Before(second) {
		t.Fatal("fixture times must be one nanosecond apart")
	}
	for _, reverse := range []bool{false, true} {
		t.Run("order", func(t *testing.T) {
			store := NewStore()
			batch := []Sample{
				{ID: "n0", Service: "gw", Name: "requests", At: first, Value: 1},
				{ID: "n1", Service: "gw", Name: "requests", At: second, Value: 2},
			}
			if reverse {
				batch[0], batch[1] = batch[1], batch[0]
			}
			res, err := store.Ingest(batch, noopCommit)
			if err != nil {
				t.Fatalf("nanosecond-apart points must coexist (reverse=%v): %v", reverse, err)
			}
			if res.Created != 2 || res.Replayed != 0 {
				t.Fatalf("result (reverse=%v): %+v", reverse, res)
			}
			since := parsePoint(t, "2026-10-01T10:00:00Z")
			until := parsePoint(t, "2026-10-01T10:00:01Z")
			if count, _ := aggregateCount(t, store, "gw", "requests", nil, since, until); count != 2 {
				t.Fatalf("one-second window must contain both points, got %d (reverse=%v)", count, reverse)
			}
		})
	}
}

// TestIngestWideDateSameInstantTimezoneConflicts proves the point identity
// is the absolute instant even at dates whose seconds overflow UnixNano:
// another timezone spelling is the same point and another id cannot claim it.
func TestIngestWideDateSameInstantTimezoneConflicts(t *testing.T) {
	store := NewStore()
	oldUTC := parsePoint(t, "1600-01-01T00:00:00Z")
	oldEast := parsePoint(t, "1600-01-01T08:00:00+08:00")
	futureUTC := parsePoint(t, "2184-07-20T23:34:33.709551616Z")
	futureWest := parsePoint(t, "2184-07-20T16:34:33.709551616-07:00")
	if !oldUTC.Equal(oldEast) || !futureUTC.Equal(futureWest) {
		t.Fatal("fixtures must describe equal absolute instants")
	}

	if _, err := store.Ingest([]Sample{
		{ID: "a-old", Service: "gw", Name: "requests", At: oldUTC, Value: 1},
		{ID: "a-new", Service: "gw", Name: "requests", At: futureUTC, Value: 2},
	}, noopCommit); err != nil {
		t.Fatalf("seed batch: %v", err)
	}

	// Clash with already-accepted data in another timezone spelling.
	for _, clash := range []Sample{
		{ID: "b-old", Service: "gw", Name: "requests", At: oldEast, Value: 3},
		{ID: "b-new", Service: "gw", Name: "requests", At: futureWest, Value: 4},
	} {
		if _, err := store.Ingest([]Sample{clash}, noopCommit); !IsConflictError(err) {
			t.Fatalf("timezone-equivalent clash for %s must conflict, got %v", clash.ID, err)
		}
	}

	// The same clash entirely within one batch must also be rejected.
	inBatch := []Sample{
		{ID: "c-old", Service: "gw", Name: "requests", At: oldUTC, Value: 5},
		{ID: "d-old", Service: "gw", Name: "requests", At: oldEast, Value: 6},
	}
	if _, err := store.Ingest(inBatch, noopCommit); !IsConflictError(err) {
		t.Fatalf("in-batch timezone clash must conflict, got %v", err)
	}
	inBatch = []Sample{
		{ID: "c-new", Service: "gw", Name: "requests", At: futureWest, Value: 5},
		{ID: "d-new", Service: "gw", Name: "requests", At: futureUTC, Value: 6},
	}
	if _, err := store.Ingest(inBatch, noopCommit); !IsConflictError(err) {
		t.Fatalf("in-batch future timezone clash must conflict, got %v", err)
	}

	// Rejected attempts left no trace: each era still has exactly one point.
	oldSince := parsePoint(t, "1599-12-31T23:59:59Z")
	oldUntil := parsePoint(t, "1600-01-01T00:00:01Z")
	if count, _ := aggregateCount(t, store, "gw", "requests", nil, oldSince, oldUntil); count != 1 {
		t.Fatalf("old era must still hold one point after rejections, got %d", count)
	}
	newSince := parsePoint(t, "2184-07-20T23:34:33Z")
	newUntil := parsePoint(t, "2184-07-20T23:34:34Z")
	if count, _ := aggregateCount(t, store, "gw", "requests", nil, newSince, newUntil); count != 1 {
		t.Fatalf("future era must still hold one point after rejections, got %d", count)
	}
}

// TestIngestWideDateMixedBatchRejectsAtomically covers a batch containing
// both a conflicting sample and a legal new sample at a wide date: the whole
// batch is refused, nothing becomes visible, and the legal sample ingests
// when submitted alone.
func TestIngestWideDateMixedBatchRejectsAtomically(t *testing.T) {
	store := NewStore()
	occupied := parsePoint(t, "1600-01-01T00:00:00Z")
	occupiedAltZone := parsePoint(t, "1600-01-01T08:00:00+08:00")
	legal := parsePoint(t, "2184-07-20T23:34:33.709551616Z")
	if !occupied.Equal(occupiedAltZone) {
		t.Fatal("fixture must describe the same absolute instant")
	}
	if _, err := store.Ingest([]Sample{
		{ID: "owner", Service: "gw", Name: "requests", At: occupied, Value: 1},
	}, noopCommit); err != nil {
		t.Fatal(err)
	}

	var committed int
	countCommit := func([]Sample) error { committed++; return nil }
	_, err := store.Ingest([]Sample{
		{ID: "intruder", Service: "gw", Name: "requests", At: occupiedAltZone, Value: 9}, // same instant, +08:00 spelling
		{ID: "fresh", Service: "gw", Name: "requests", At: legal, Value: 2},
	}, countCommit)
	if !IsConflictError(err) {
		t.Fatalf("mixed batch must conflict, got %v", err)
	}
	if committed != 0 {
		t.Fatalf("a rejected batch must never reach the commit hook, commits=%d", committed)
	}

	newSince := parsePoint(t, "2184-07-20T23:34:33Z")
	newUntil := parsePoint(t, "2184-07-20T23:34:34Z")
	if count, _ := aggregateCount(t, store, "gw", "requests", nil, newSince, newUntil); count != 0 {
		t.Fatalf("legal sample in a rejected batch must not be visible, got count %d", count)
	}

	// Submitted on its own, the formerly-rejected legal sample is new.
	res, err := store.Ingest([]Sample{
		{ID: "fresh", Service: "gw", Name: "requests", At: legal, Value: 2},
	}, noopCommit)
	if err != nil {
		t.Fatalf("legal sample alone must ingest: %v", err)
	}
	if res.Created != 1 || res.Replayed != 0 {
		t.Fatalf("standalone ingest: %+v", res)
	}
	if count, _ := aggregateCount(t, store, "gw", "requests", nil, newSince, newUntil); count != 1 {
		t.Fatalf("future window must now contain the point, got %d", count)
	}
}

// TestIngestWideDateReplayByTimezoneAndNanosecondMoveConflict checks the id
// rules at wide dates and nanosecond granularity: same id with only the
// timezone representation changed is a replay; same id with the instant
// moved by even one nanosecond is a conflict.
func TestIngestWideDateReplayByTimezoneAndNanosecondMoveConflict(t *testing.T) {
	store := NewStore()
	future := parsePoint(t, "2184-07-20T23:34:33.709551616Z")
	futureAltZone := parsePoint(t, "2184-07-21T04:34:33.709551616+05:00")
	if !future.Equal(futureAltZone) {
		t.Fatal("fixtures must describe the same absolute instant")
	}
	if _, err := store.Ingest([]Sample{
		{ID: "r", Service: "gw", Name: "requests", At: future, Value: 3},
	}, noopCommit); err != nil {
		t.Fatal(err)
	}

	// Same id, same content, only the timezone spelling differs: pure replay.
	res, err := store.Ingest([]Sample{
		{ID: "r", Service: "gw", Name: "requests", At: futureAltZone, Value: 3},
	}, noopCommit)
	if err != nil {
		t.Fatalf("timezone-only resubmission must replay: %v", err)
	}
	if res.Created != 0 || res.Replayed != 1 {
		t.Fatalf("replay result: %+v", res)
	}

	// Same id with the instant actually moved by one nanosecond: conflict.
	if _, err := store.Ingest([]Sample{
		{ID: "r", Service: "gw", Name: "requests", At: future.Add(time.Nanosecond), Value: 3},
	}, noopCommit); !IsConflictError(err) {
		t.Fatalf("one-nanosecond move under the same id must conflict, got %v", err)
	}
	if _, err := store.Ingest([]Sample{
		{ID: "r", Service: "gw", Name: "requests", At: future.Add(-time.Nanosecond), Value: 3},
	}, noopCommit); !IsConflictError(err) {
		t.Fatalf("one-nanosecond move backwards under the same id must conflict, got %v", err)
	}

	// The replay and failed moves added nothing.
	since := parsePoint(t, "2184-07-20T23:34:33Z")
	until := parsePoint(t, "2184-07-20T23:34:34Z")
	if count, _ := aggregateCount(t, store, "gw", "requests", nil, since, until); count != 1 {
		t.Fatalf("only the original point must be present, got %d", count)
	}
}

// TestIngestClashScopedToSameLabelSetAtWideDates proves point conflicts are
// per series: two series differing only in labels may both hold a sample at
// the same distant instant, while another id on either series still clashes.
func TestIngestClashScopedToSameLabelSetAtWideDates(t *testing.T) {
	store := NewStore()
	shared := parsePoint(t, "1600-01-01T00:00:00Z")
	prod := map[string]string{"env": "prod"}
	staging := map[string]string{"env": "staging"}
	batch := []Sample{
		{ID: "p1", Service: "gw", Name: "requests", At: shared, Value: 1, Labels: prod},
		{ID: "s1", Service: "gw", Name: "requests", At: shared, Value: 1, Labels: staging},
	}
	if res, err := store.Ingest(batch, noopCommit); err != nil {
		t.Fatalf("different label sets share an instant: %v", err)
	} else if res.Created != 2 {
		t.Fatalf("created=%d", res.Created)
	}

	// Another id on either label set at the same instant still clashes.
	if _, err := store.Ingest([]Sample{
		{ID: "p2", Service: "gw", Name: "requests", At: shared, Value: 2, Labels: prod},
	}, noopCommit); !IsConflictError(err) {
		t.Fatalf("same label set, same instant must clash, got %v", err)
	}
	if _, err := store.Ingest([]Sample{
		{ID: "s2", Service: "gw", Name: "requests", At: shared, Value: 2, Labels: staging},
	}, noopCommit); !IsConflictError(err) {
		t.Fatalf("same label set, same instant must clash, got %v", err)
	}

	// Each label series is independently queryable at the shared instant.
	since := parsePoint(t, "1599-12-31T23:59:59Z")
	until := parsePoint(t, "1600-01-01T00:00:01Z")
	if count, n := aggregateCount(t, store, "gw", "requests", prod, since, until); count != 1 || n != 1 {
		t.Fatalf("prod series count=%d series=%d", count, n)
	}
	if count, n := aggregateCount(t, store, "gw", "requests", staging, since, until); count != 1 || n != 1 {
		t.Fatalf("staging series count=%d series=%d", count, n)
	}
}

func TestIngestCommitFailureRejectsBatchAndRetries(t *testing.T) {
	s := NewStore()
	fail := errors.New("disk full")
	_, err := s.Ingest([]Sample{
		{ID: "a", Service: "gw", Name: "requests", At: at(10), Value: 1},
	}, func([]Sample) error {
		return fail
	})
	if !errors.Is(err, fail) {
		t.Fatalf("commit error must surface, got %v", err)
	}
	// Nothing became visible: re-ingesting the same id creates it.
	res, err := s.Ingest([]Sample{
		{ID: "a", Service: "gw", Name: "requests", At: at(10), Value: 1},
	}, noopCommit)
	if err != nil {
		t.Fatal(err)
	}
	if res.Created != 1 {
		t.Fatalf("failed batch must not be visible, got %+v", res)
	}
}
