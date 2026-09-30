package events

import (
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func mkEvent(id, service, severity, message string, at time.Time) Event {
	return Event{ID: id, Service: service, Severity: severity, Message: message, At: at}
}

func TestIngestCreatesAndReplays(t *testing.T) {
	tl := NewTimeline()
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	batch := []Event{
		mkEvent("evt-2", "checkout", " WARN ", " latency ", base.Add(time.Minute)),
		mkEvent("evt-1", "gateway", "CRITICAL", "spike", base),
	}
	var persisted []Event
	res, err := tl.Ingest(batch, func(created []Event) error {
		persisted = append(persisted, created...)
		return nil
	})
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if res.Created != 2 || res.Replayed != 0 {
		t.Fatalf("unexpected result: %+v", res)
	}
	if len(persisted) != 2 {
		t.Fatalf("commit hook got %d events", len(persisted))
	}
	if persisted[0].Severity != "warn" || persisted[0].Service != "checkout" {
		t.Fatalf("normalization not applied before commit: %+v", persisted[0])
	}

	// Identical retry, even with different formatting, is fully replayed.
	retry := []Event{
		mkEvent("  evt-1  ", "gateway", "critical", "spike", base),
		mkEvent("evt-2", " checkout ", "warn", "latency", base.Add(time.Minute)),
	}
	res, err = tl.Ingest(retry, func(created []Event) error {
		t.Fatalf("nothing should be persisted on replay, got %v", created)
		return nil
	})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if res.Created != 0 || res.Replayed != 2 {
		t.Fatalf("unexpected replay result: %+v", res)
	}

	all := tl.Query(Query{})
	if len(all) != 2 || all[0].ID != "evt-1" || all[1].ID != "evt-2" {
		t.Fatalf("timeline not ordered by time then id: %+v", all)
	}
}

func TestIngestAbsoluteTimeEquality(t *testing.T) {
	tl := NewTimeline()
	at := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	sameInstant := at.In(time.FixedZone("CST", 8*60*60))
	if !at.Equal(sameInstant) {
		t.Fatal("test setup: instants differ")
	}
	if _, err := tl.Ingest([]Event{mkEvent("e", "s", "info", "m", at)}, nil); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	res, err := tl.Ingest([]Event{mkEvent("e", "s", "info", "m", sameInstant)}, nil)
	if err != nil {
		t.Fatalf("same instant in another zone should replay: %v", err)
	}
	if res.Replayed != 1 {
		t.Fatalf("want replay, got %+v", res)
	}

	other := sameInstant.Add(time.Nanosecond)
	_, err = tl.Ingest([]Event{mkEvent("e", "s", "info", "m", other)}, nil)
	if !IsConflictError(err) {
		t.Fatalf("different instant should conflict, got %v", err)
	}
}

func TestIngestValidationPrecedesConflicts(t *testing.T) {
	tl := NewTimeline()
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	_, err := tl.Ingest([]Event{mkEvent("dup", "s", "info", "old", base)}, nil)
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	called := false
	commit := func(created []Event) error { called = true; return nil }

	// Conflict present, but an invalid event comes first: validation wins.
	batch := []Event{
		mkEvent("dup", "s", "info", "different", base),
		mkEvent("bad", "s", "info", "   ", base),
	}
	_, err = tl.Ingest(batch, commit)
	if !IsValidationError(err) {
		t.Fatalf("want validation error, got %v", err)
	}
	if called {
		t.Fatal("commit hook must not run on validation failure")
	}

	// Pure conflict once the batch is valid.
	_, err = tl.Ingest([]Event{mkEvent("dup", "s", "info", "different", base)}, commit)
	if !IsConflictError(err) {
		t.Fatalf("want conflict error, got %v", err)
	}
	if called {
		t.Fatal("commit hook must not run on conflict")
	}
	if got := tl.Query(Query{}); len(got) != 1 || got[0].Message != "old" {
		t.Fatalf("rejected batch changed data: %+v", got)
	}
}

func TestIngestRejectsDuplicateIDWithinBatch(t *testing.T) {
	tl := NewTimeline()
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	cases := [][]Event{
		{mkEvent("a", "s", "info", "one", base), mkEvent("a", "s", "info", "one", base)},
		{mkEvent(" a ", "s", "info", "one", base), mkEvent("a", "s2", "info", "two", base)},
	}
	for i, batch := range cases {
		if _, err := tl.Ingest(batch, nil); !IsConflictError(err) {
			t.Fatalf("case %d: want conflict, got %v", i, err)
		}
	}
	if got := tl.Query(Query{}); len(got) != 0 {
		t.Fatalf("rejected batches must not store anything: %+v", got)
	}
}

func TestIngestCommitFailureRejectsBatch(t *testing.T) {
	tl := NewTimeline()
	base := time.Now()
	want := errors.New("disk gone")
	_, err := tl.Ingest([]Event{mkEvent("e", "s", "info", "m", base)}, func(created []Event) error {
		return want
	})
	if !errors.Is(err, want) {
		t.Fatalf("want commit error propagated, got %v", err)
	}
	if got := tl.Query(Query{}); len(got) != 0 {
		t.Fatalf("failed commit must not be visible: %+v", got)
	}
}

func TestTimelineQueryFiltersAndOrder(t *testing.T) {
	tl := NewTimeline()
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	batch := []Event{
		mkEvent("c", "gateway", "info", "c", base.Add(2*time.Minute)),
		mkEvent("a", "gateway", "critical", "a", base),
		mkEvent("b", "api", "critical", "b", base),
		mkEvent("d", "api", "info", "d", base.Add(time.Minute)),
	}
	if _, err := tl.Ingest(batch, nil); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	got := tl.Query(Query{Service: "gateway"})
	if ids(got) != "a,c" {
		t.Fatalf("service filter: %s", ids(got))
	}
	got = tl.Query(Query{Severity: " CRITICAL "})
	if ids(got) != "a,b" {
		t.Fatalf("severity filter (same-time tie by id): %s", ids(got))
	}
	got = tl.Query(Query{Since: base.Add(30 * time.Second), Until: base.Add(90 * time.Second)})
	if ids(got) != "d" {
		t.Fatalf("inclusive range: %s", ids(got))
	}
}

func ids(events []Event) string {
	parts := make([]string, len(events))
	for i, e := range events {
		parts[i] = e.ID
	}
	return strings.Join(parts, ",")
}

func TestIngestConcurrentSameContent(t *testing.T) {
	tl := NewTimeline()
	at := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	var commits int64
	commit := func(created []Event) error {
		atomic.AddInt64(&commits, 1)
		return nil
	}

	const n = 64
	start := make(chan struct{})
	var wg sync.WaitGroup
	var created, replayed, failures int64
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			res, err := tl.Ingest([]Event{mkEvent("same", "svc", "info", "body", at)}, commit)
			if err != nil {
				atomic.AddInt64(&failures, 1)
				return
			}
			atomic.AddInt64(&created, int64(res.Created))
			atomic.AddInt64(&replayed, int64(res.Replayed))
		}()
	}
	close(start)
	wg.Wait()

	if failures != 0 {
		t.Fatalf("identical concurrent posts must all succeed, failures=%d", failures)
	}
	if created != 1 || replayed != n-1 || atomic.LoadInt64(&commits) != 1 {
		t.Fatalf("created=%d replayed=%d commits=%d", created, replayed, commits)
	}
	if got := tl.Query(Query{}); len(got) != 1 {
		t.Fatalf("exactly one event must be stored, got %d", len(got))
	}
}

func TestIngestConcurrentConflictingContent(t *testing.T) {
	tl := NewTimeline()
	at := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)

	const n = 64
	start := make(chan struct{})
	var wg sync.WaitGroup
	var wins, conflicts int64
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, err := tl.Ingest(
				[]Event{mkEvent("same", "svc", "info", "body-"+strconv.Itoa(i), at)},
				func(created []Event) error {
					atomic.AddInt64(&wins, 1)
					return nil
				},
			)
			if IsConflictError(err) {
				atomic.AddInt64(&conflicts, 1)
			} else if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		}(i)
	}
	close(start)
	wg.Wait()

	if wins != 1 || conflicts != n-1 {
		t.Fatalf("want exactly one winner, got wins=%d conflicts=%d", wins, conflicts)
	}
	got := tl.Query(Query{})
	if len(got) != 1 || !strings.HasPrefix(got[0].Message, "body-") {
		t.Fatalf("one version must be stored, got %+v", got)
	}
}

func TestIngestBatchAtomicVisibility(t *testing.T) {
	tl := NewTimeline()
	at := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	batch := []Event{
		mkEvent("a", "svc", "info", "one", at),
		mkEvent("b", "svc", "info", "two", at.Add(time.Second)),
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	commitDone := make(chan struct{})
	go func() {
		_, err := tl.Ingest(batch, func(created []Event) error {
			close(entered)
			<-release
			return nil
		})
		if err != nil {
			t.Errorf("ingest: %v", err)
		}
		close(commitDone)
	}()
	<-entered

	// Readers launched while the commit hook is blocked must not observe a
	// partial batch: they wait for the writer and then see both events.
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got := tl.Query(Query{})
			if len(got) != 0 && len(got) != 2 {
				t.Errorf("reader observed partial batch of %d", len(got))
			}
		}()
	}
	close(release)
	<-commitDone
	wg.Wait()

	if got := tl.Query(Query{}); len(got) != 2 {
		t.Fatalf("final timeline: %d events", len(got))
	}
}
