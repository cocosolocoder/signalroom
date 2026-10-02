package metrics

import (
	"errors"
	"fmt"
	"math"
	"testing"
	"time"
)

func sample(id, service, name string, at time.Time, value float64) Sample {
	return Sample{ID: id, Service: service, Name: name, At: at, Value: value}
}

func TestNormalizeTrimsAndValidates(t *testing.T) {
	at := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	got, err := Normalize(Sample{
		ID:      "  m1  ",
		Service: "  svc  ",
		Name:    "  cpu  ",
		At:      at,
		Value:   1.5,
		Labels:  map[string]string{" env ": " prod "},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "m1" || got.Service != "svc" || got.Name != "cpu" {
		t.Fatalf("trim failed: %+v", got)
	}
	if got.Labels["env"] != "prod" {
		t.Fatalf("labels not trimmed: %+v", got.Labels)
	}
}

func TestNormalizeRejectsInvalid(t *testing.T) {
	at := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	base := Sample{ID: "m1", Service: "svc", Name: "cpu", At: at, Value: 1}
	cases := map[string]Sample{
		"empty id":      func() Sample { s := base; s.ID = "   "; return s }(),
		"empty service": func() Sample { s := base; s.Service = "  "; return s }(),
		"empty name":    func() Sample { s := base; s.Name = ""; return s }(),
		"zero at":       func() Sample { s := base; s.At = time.Time{}; return s }(),
		"negative":      func() Sample { s := base; s.Value = -1; return s }(),
		"nan":           func() Sample { s := base; s.Value = math.NaN(); return s }(),
		"inf":           func() Sample { s := base; s.Value = math.Inf(1); return s }(),
	}
	for name, input := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := Normalize(input); err == nil {
				t.Fatal("expected validation error")
			} else if !IsValidationError(err) {
				t.Fatalf("not a validation error: %v", err)
			}
		})
	}
}

func TestIngestCreatedAndReplayed(t *testing.T) {
	tl := NewTimeline()
	at := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	batch := []Sample{
		sample("m1", "svc", "cpu", at, 1),
		sample("m2", "svc", "cpu", at.Add(time.Minute), 2),
	}
	res, err := tl.Ingest(batch, nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Created != 2 || res.Replayed != 0 {
		t.Fatalf("created=%d replayed=%d", res.Created, res.Replayed)
	}

	// Retry with 1.0 instead of 1: same content, replays.
	retry := []Sample{
		sample("m1", "svc", "cpu", at, 1.0),
		sample("m2", "svc", "cpu", at.Add(time.Minute), 2.0),
	}
	res, err = tl.Ingest(retry, nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Created != 0 || res.Replayed != 2 {
		t.Fatalf("retry created=%d replayed=%d", res.Created, res.Replayed)
	}
	if len(tl.samples) != 2 {
		t.Fatalf("replay must not add samples: %d", len(tl.samples))
	}
}

func TestIngestRejectsConflict(t *testing.T) {
	tl := NewTimeline()
	at := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	_, err := tl.Ingest([]Sample{sample("m1", "svc", "cpu", at, 1)}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Same id, different value: conflict.
	_, err = tl.Ingest([]Sample{sample("m1", "svc", "cpu", at, 2)}, nil)
	if err == nil || !IsConflictError(err) {
		t.Fatalf("expected conflict, got %v", err)
	}

	// Same id, different timestamp: conflict.
	_, err = tl.Ingest([]Sample{sample("m1", "svc", "cpu", at.Add(time.Second), 1)}, nil)
	if err == nil || !IsConflictError(err) {
		t.Fatalf("expected conflict, got %v", err)
	}
}

func TestIngestRejectsBatchDuplicateID(t *testing.T) {
	tl := NewTimeline()
	at := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	batch := []Sample{
		sample("m1", "svc", "cpu", at, 1),
		sample("m1", "svc", "cpu", at.Add(time.Minute), 2),
	}
	_, err := tl.Ingest(batch, nil)
	if err == nil || !IsConflictError(err) {
		t.Fatalf("expected conflict for duplicate id in batch, got %v", err)
	}
	if len(tl.samples) != 0 {
		t.Fatal("nothing should be stored on conflict")
	}
}

func TestIngestRejectsSeriesTimeConflict(t *testing.T) {
	tl := NewTimeline()
	at := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	_, err := tl.Ingest([]Sample{sample("m1", "svc", "cpu", at, 1)}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Same series, same instant, different id: conflict.
	_, err = tl.Ingest([]Sample{sample("m2", "svc", "cpu", at, 5)}, nil)
	if err == nil || !IsConflictError(err) {
		t.Fatalf("expected series-time conflict, got %v", err)
	}

	// Different series (different labels) at same instant: OK.
	other := sample("m3", "svc", "cpu", at, 5)
	other.Labels = map[string]string{"env": "prod"}
	_, err = tl.Ingest([]Sample{other}, nil)
	if err != nil {
		t.Fatalf("different labels should be a different series: %v", err)
	}
}

func TestIngestValidationBeforeConflict(t *testing.T) {
	tl := NewTimeline()
	at := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	_, err := tl.Ingest([]Sample{sample("m1", "svc", "cpu", at, 1)}, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Batch has both a validation error (negative value) and a conflict.
	// Validation must win.
	batch := []Sample{
		sample("m1", "svc", "cpu", at, 1), // would be a replay/conflict
		{ID: "m2", Service: "svc", Name: "cpu", At: at.Add(time.Minute), Value: -5},
	}
	_, err = tl.Ingest(batch, nil)
	if err == nil || !IsValidationError(err) {
		t.Fatalf("validation should precede conflict, got %v", err)
	}
}

func TestAggregateBasicIncrements(t *testing.T) {
	tl := NewTimeline()
	base := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	// Samples at 10:00 (10), 10:01 (15), 10:02 (12 — reset), 10:03 (20).
	batch := []Sample{
		sample("m1", "svc", "cpu", base, 10),
		sample("m2", "svc", "cpu", base.Add(time.Minute), 15),
		sample("m3", "svc", "cpu", base.Add(2*time.Minute), 12),
		sample("m4", "svc", "cpu", base.Add(3*time.Minute), 20),
	}
	if _, err := tl.Ingest(batch, nil); err != nil {
		t.Fatal(err)
	}

	query := AggregateQuery{
		Name:  "cpu",
		Since: base,
		Until: base.Add(4 * time.Minute),
		Step:  time.Minute,
	}
	res, err := tl.Aggregate(query)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Series) != 1 {
		t.Fatalf("expected 1 series, got %d", len(res.Series))
	}
	segs := res.Series[0].Segments
	if len(segs) != 4 {
		t.Fatalf("expected 4 segments, got %d", len(segs))
	}
	// seg0: sample 10:00 has no predecessor (it's at since), no increment.
	// seg1: 15-10=5. seg2: 12<15 reset → 12. seg3: 20-12=8.
	want := []float64{0, 5, 12, 8}
	for i, w := range want {
		if segs[i].Increment != w {
			t.Fatalf("seg%d increment=%v, want %v", i, segs[i].Increment, w)
		}
	}
	wantCounts := []int{1, 1, 1, 1}
	for i, w := range wantCounts {
		if segs[i].Count != w {
			t.Fatalf("seg%d count=%d, want %d", i, segs[i].Count, w)
		}
	}
}

func TestAggregatePredecessorBeforeSince(t *testing.T) {
	tl := NewTimeline()
	base := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	// Sample before since: 10:00 value 100. In-window: 10:02 value 130.
	batch := []Sample{
		sample("m1", "svc", "cpu", base, 100),
		sample("m2", "svc", "cpu", base.Add(2*time.Minute), 130),
	}
	if _, err := tl.Ingest(batch, nil); err != nil {
		t.Fatal(err)
	}
	query := AggregateQuery{
		Name:  "cpu",
		Since: base.Add(1 * time.Minute),
		Until: base.Add(3 * time.Minute),
		Step:  time.Minute,
	}
	res, err := tl.Aggregate(query)
	if err != nil {
		t.Fatal(err)
	}
	segs := res.Series[0].Segments
	// seg0 [10:01,10:02): empty. seg1 [10:02,10:03): 130-100=30.
	if segs[0].Increment != 0 || segs[0].Count != 0 {
		t.Fatalf("seg0 should be empty: %+v", segs[0])
	}
	if segs[1].Increment != 30 || segs[1].Count != 1 {
		t.Fatalf("seg1 should carry the predecessor delta: %+v", segs[1])
	}
}

func TestAggregateExcludesEndpointAndEmpty(t *testing.T) {
	tl := NewTimeline()
	base := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	batch := []Sample{
		sample("m1", "svc", "cpu", base, 10),
		sample("m2", "svc", "cpu", base.Add(2*time.Minute), 20),
	}
	if _, err := tl.Ingest(batch, nil); err != nil {
		t.Fatal(err)
	}
	query := AggregateQuery{
		Name:  "cpu",
		Since: base,
		Until: base.Add(2 * time.Minute), // sample at 10:02 excluded
		Step:  time.Minute,
	}
	res, err := tl.Aggregate(query)
	if err != nil {
		t.Fatal(err)
	}
	// Only the 10:00 sample is in range; it has no predecessor → 0 increment.
	if len(res.Series) != 1 {
		t.Fatalf("expected 1 series, got %d", len(res.Series))
	}
	segs := res.Series[0].Segments
	if len(segs) != 2 {
		t.Fatalf("expected 2 segments, got %d", len(segs))
	}
	if segs[0].Count != 1 || segs[0].Increment != 0 {
		t.Fatalf("seg0: %+v", segs[0])
	}
	if segs[1].Count != 0 || segs[1].Increment != 0 {
		t.Fatalf("seg1 should be empty: %+v", segs[1])
	}
}

func TestAggregateNoMatchReturnsEmpty(t *testing.T) {
	tl := NewTimeline()
	base := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	if _, err := tl.Ingest([]Sample{sample("m1", "svc", "cpu", base, 10)}, nil); err != nil {
		t.Fatal(err)
	}
	query := AggregateQuery{
		Name:  "memory",
		Since: base,
		Until: base.Add(time.Minute),
		Step:  time.Minute,
	}
	res, err := tl.Aggregate(query)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Series) != 0 {
		t.Fatalf("expected empty series, got %d", len(res.Series))
	}
}

func TestAggregateSegmentLimits(t *testing.T) {
	tl := NewTimeline()
	base := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	if _, err := tl.Ingest([]Sample{sample("m1", "svc", "cpu", base, 10)}, nil); err != nil {
		t.Fatal(err)
	}
	// 10001 segments: window of 10001 seconds with step 1.
	query := AggregateQuery{
		Name:  "cpu",
		Since: base,
		Until: base.Add(10001 * time.Second),
		Step:  time.Second,
	}
	_, err := tl.Aggregate(query)
	if err == nil || !errors.Is(err, ErrTooManySegments) {
		t.Fatalf("expected ErrTooManySegments, got %v", err)
	}
}

func TestAggregateNonFiniteSum(t *testing.T) {
	tl := NewTimeline()
	base := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	// Increments in the same segment must sum past MaxFloat64:
	// 0 -> MaxFloat64 (diff MaxFloat64), reset to 0, then back to
	// MaxFloat64 (diff MaxFloat64). Sum overflows to +Inf.
	batch := []Sample{
		sample("m1", "svc", "cpu", base, 0),
		sample("m2", "svc", "cpu", base.Add(time.Second), math.MaxFloat64),
		sample("m3", "svc", "cpu", base.Add(2*time.Second), 0),
		sample("m4", "svc", "cpu", base.Add(3*time.Second), math.MaxFloat64),
	}
	if _, err := tl.Ingest(batch, nil); err != nil {
		t.Fatal(err)
	}
	query := AggregateQuery{
		Name:  "cpu",
		Since: base,
		Until: base.Add(4 * time.Second),
		Step:  4 * time.Second,
	}
	_, err := tl.Aggregate(query)
	if err == nil || !errors.Is(err, ErrNonFinite) {
		t.Fatalf("expected ErrNonFinite, got %v", err)
	}
}

func TestAggregateLateDataRecomputed(t *testing.T) {
	tl := NewTimeline()
	base := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	// Ingest out of order: later sample first, then earlier.
	batch := []Sample{
		sample("m2", "svc", "cpu", base.Add(2*time.Minute), 20),
		sample("m1", "svc", "cpu", base, 10),
	}
	if _, err := tl.Ingest(batch, nil); err != nil {
		t.Fatal(err)
	}
	query := AggregateQuery{
		Name:  "cpu",
		Since: base,
		Until: base.Add(3 * time.Minute),
		Step:  time.Minute,
	}
	res, err := tl.Aggregate(query)
	if err != nil {
		t.Fatal(err)
	}
	segs := res.Series[0].Segments
	// Order-independent: seg0=0 (first, no pred), seg1=0 (empty), seg2=10.
	if segs[0].Increment != 0 {
		t.Fatalf("seg0: %+v", segs[0])
	}
	if segs[2].Increment != 10 {
		t.Fatalf("seg2 should be 20-10=10 regardless of ingestion order: %+v", segs[2])
	}
}

func TestAggregateServiceAndLabelFilters(t *testing.T) {
	tl := NewTimeline()
	base := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	s1 := sample("m1", "svc-a", "cpu", base, 10)
	s2 := sample("m2", "svc-b", "cpu", base, 20)
	s3 := sample("m3", "svc-a", "cpu", base, 30)
	s3.Labels = map[string]string{"env": "prod"}
	if _, err := tl.Ingest([]Sample{s1, s2, s3}, nil); err != nil {
		t.Fatal(err)
	}
	query := AggregateQuery{
		Name:    "cpu",
		Service: "svc-a",
		Labels:  map[string]string{"env": "prod"},
		Since:   base,
		Until:   base.Add(time.Minute),
		Step:    time.Minute,
	}
	res, err := tl.Aggregate(query)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Series) != 1 {
		t.Fatalf("expected 1 matching series, got %d", len(res.Series))
	}
	if res.Series[0].Service != "svc-a" {
		t.Fatalf("wrong service: %s", res.Series[0].Service)
	}
}

func TestAggregateSeriesSegmentLimit(t *testing.T) {
	tl := NewTimeline()
	base := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	// Create 100001 distinct series (different labels), each with one sample.
	batch := make([]Sample, 100001)
	for i := range batch {
		s := sample(fmt.Sprintf("m%d", i), "svc", "cpu", base, 1)
		s.Labels = map[string]string{"idx": fmt.Sprintf("%d", i)}
		batch[i] = s
	}
	if _, err := tl.Ingest(batch, nil); err != nil {
		t.Fatal(err)
	}
	query := AggregateQuery{
		Name:  "cpu",
		Since: base,
		Until: base.Add(time.Minute),
		Step:  time.Minute,
	}
	_, err := tl.Aggregate(query)
	if err == nil || !errors.Is(err, ErrTooManySeriesSegments) {
		t.Fatalf("expected ErrTooManySeriesSegments, got %v", err)
	}
}

func TestAggregateMultipleSeriesOrdering(t *testing.T) {
	tl := NewTimeline()
	base := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	// Ingest out of order: service b first, then a. Use different labels so
	// the two svc-a samples are distinct series.
	batch := []Sample{
		sample("m1", "svc-b", "cpu", base, 10),
		sample("m2", "svc-a", "cpu", base, 20),
		sample("m3", "svc-a", "cpu", base, 30),
	}
	batch[2].Labels = map[string]string{"env": "prod"}
	if _, err := tl.Ingest(batch, nil); err != nil {
		t.Fatal(err)
	}
	query := AggregateQuery{
		Name:  "cpu",
		Since: base,
		Until: base.Add(time.Minute),
		Step:  time.Minute,
	}
	res, err := tl.Aggregate(query)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Series) != 3 {
		t.Fatalf("expected 3 series, got %d", len(res.Series))
	}
	// Ordered by service: svc-a, svc-a, svc-b.
	if res.Series[0].Service != "svc-a" || res.Series[1].Service != "svc-a" || res.Series[2].Service != "svc-b" {
		t.Fatalf("series not ordered by service: %s, %s, %s",
			res.Series[0].Service, res.Series[1].Service, res.Series[2].Service)
	}
}

func TestConcurrentIngestAtomicity(t *testing.T) {
	tl := NewTimeline()
	base := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	// Two concurrent batches: one valid, one with a conflict.
	// The conflict batch must not partially commit.
	valid := []Sample{sample("m1", "svc", "cpu", base, 1)}
	conflict := []Sample{
		sample("m2", "svc", "cpu", base.Add(time.Second), 2),
		sample("m2", "svc", "cpu", base.Add(2*time.Second), 3), // duplicate id
	}

	errCh := make(chan error, 2)
	go func() {
		_, err := tl.Ingest(valid, nil)
		errCh <- err
	}()
	go func() {
		_, err := tl.Ingest(conflict, nil)
		errCh <- err
	}()

	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			// One should fail with conflict, the other should succeed.
			if !IsConflictError(err) {
				t.Fatalf("unexpected error: %v", err)
			}
		}
	}

	// The valid batch's sample must be present, the conflict batch's must not.
	query := AggregateQuery{
		Name:  "cpu",
		Since: base,
		Until: base.Add(3 * time.Second),
		Step:  time.Second,
	}
	res, err := tl.Aggregate(query)
	if err != nil {
		t.Fatal(err)
	}
	// Only m1 should be present (m2's batch failed entirely).
	totalSamples := 0
	for _, s := range res.Series {
		for _, seg := range s.Segments {
			totalSamples += seg.Count
		}
	}
	if totalSamples != 1 {
		t.Fatalf("expected only the valid batch's sample, got %d", totalSamples)
	}
}
