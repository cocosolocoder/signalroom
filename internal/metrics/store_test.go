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
