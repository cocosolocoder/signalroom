package events

import (
	"errors"
	"math"
	"testing"
	"time"
)

func TestValidateCompareWindowsSegments(t *testing.T) {
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	query := CompareQuery{
		BaselineSince: base,
		BaselineUntil: base.Add(10 * time.Minute),
		Since:         base.Add(time.Hour),
		Until:         base.Add(time.Hour + 10*time.Minute),
		Step:          4 * time.Minute,
	}
	segments, err := ValidateCompareWindows(query)
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	if len(segments) != 3 {
		t.Fatalf("ten minutes at four minutes must yield three segments, got %d", len(segments))
	}
	wantLengths := []time.Duration{4 * time.Minute, 4 * time.Minute, 2 * time.Minute}
	for i, want := range wantLengths {
		if got := segments[i].BaselineEnd.Sub(segments[i].BaselineStart); got != want {
			t.Fatalf("baseline segment %d length = %v, want %v", i, got, want)
		}
		if got := segments[i].ObservationEnd.Sub(segments[i].ObservationStart); got != want {
			t.Fatalf("observed segment %d length = %v, want %v", i, got, want)
		}
	}
	if !segments[0].BaselineStart.Equal(base) || !segments[2].BaselineEnd.Equal(base.Add(10*time.Minute)) {
		t.Fatalf("segments must cover the full window exactly: %+v", segments)
	}
	if !segments[0].ObservationStart.Equal(base.Add(time.Hour)) || !segments[2].ObservationEnd.Equal(base.Add(time.Hour+10*time.Minute)) {
		t.Fatalf("observed window boundary mismatch: %+v", segments)
	}
	// Adjacent segments meet without gaps or overlaps.
	for i := 1; i < len(segments); i++ {
		if !segments[i].BaselineStart.Equal(segments[i-1].BaselineEnd) {
			t.Fatalf("baseline segments not contiguous at %d", i)
		}
		if !segments[i].ObservationStart.Equal(segments[i-1].ObservationEnd) {
			t.Fatalf("observed segments not contiguous at %d", i)
		}
	}
}

func TestValidateCompareWindowsRejects(t *testing.T) {
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	tenMin := base.Add(10 * time.Minute)
	valid := CompareQuery{
		BaselineSince: base, BaselineUntil: tenMin,
		Since: base.Add(time.Hour), Until: base.Add(time.Hour + 10*time.Minute),
		Step: time.Minute,
	}
	cases := map[string]struct {
		mutate func(*CompareQuery)
		want   error
	}{
		"zero step":             {func(q *CompareQuery) { q.Step = 0 }, ErrCompareStep},
		"negative step":         {func(q *CompareQuery) { q.Step = -time.Second }, ErrCompareStep},
		"baseline inverted":     {func(q *CompareQuery) { q.BaselineSince, q.BaselineUntil = q.BaselineUntil, q.BaselineSince }, ErrBaselineWindow},
		"zero-length baseline":  {func(q *CompareQuery) { q.BaselineSince = q.BaselineUntil }, ErrBaselineWindow},
		"observation inverted":  {func(q *CompareQuery) { q.Since, q.Until = q.Until, q.Since }, ErrObservationWindow},
		"zero-length observed":  {func(q *CompareQuery) { q.Until = q.Since }, ErrObservationWindow},
		"unequal lengths":       {func(q *CompareQuery) { q.Until = q.Until.Add(time.Minute) }, ErrWindowLength},
		"unequal by nanosecond": {func(q *CompareQuery) { q.Until = q.Until.Add(time.Nanosecond) }, ErrWindowLength},
		"too many segments": {func(q *CompareQuery) {
			q.Step = time.Second
			q.BaselineUntil = base.Add(10001 * time.Second)
			q.Until = q.Since.Add(10001 * time.Second)
		}, ErrTooManySegments},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			query := valid
			tc.mutate(&query)
			_, err := ValidateCompareWindows(query)
			if !errors.Is(err, tc.want) {
				t.Fatalf("want %v, got %v", tc.want, err)
			}
		})
	}

	// Precedence: a broken baseline is reported even when the observation is
	// also broken or the lengths differ or the cap is exceeded.
	bothInverted := valid
	bothInverted.BaselineSince, bothInverted.BaselineUntil = bothInverted.BaselineUntil, bothInverted.BaselineSince
	bothInverted.Since, bothInverted.Until = bothInverted.Until, bothInverted.Since
	if _, err := ValidateCompareWindows(bothInverted); !errors.Is(err, ErrBaselineWindow) {
		t.Fatalf("baseline error must take precedence over observation error: %v", err)
	}
	lengthAndCap := valid
	lengthAndCap.BaselineUntil = base.Add(10001 * time.Second) // also unequal
	lengthAndCap.Step = time.Second
	if _, err := ValidateCompareWindows(lengthAndCap); !errors.Is(err, ErrWindowLength) {
		t.Fatalf("equal-length check must precede the segment cap: %v", err)
	}

	// Exactly the cap is accepted; one nanosecond more pushes to cap+1.
	ok := CompareQuery{
		BaselineSince: base, BaselineUntil: base.Add(maxCompareSegments * time.Second),
		Since: base.Add(2 * time.Hour), Until: base.Add(2*time.Hour + maxCompareSegments*time.Second),
		Step: time.Second,
	}
	segments, err := ValidateCompareWindows(ok)
	if err != nil || len(segments) != maxCompareSegments {
		t.Fatalf("exactly %d segments must be allowed, got %d err=%v", maxCompareSegments, len(segments), err)
	}
	over := ok
	over.BaselineUntil = base.Add(maxCompareSegments*time.Second + time.Nanosecond)
	over.Until = over.Since.Add(maxCompareSegments*time.Second + time.Nanosecond)
	if _, err := ValidateCompareWindows(over); !errors.Is(err, ErrTooManySegments) {
		t.Fatalf("one extra segment must be ErrTooManySegments, got %v", err)
	}
}

func TestCompareBucketsHalfOpenWithNanoseconds(t *testing.T) {
	tl := NewTimeline()
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	batch := []Event{
		mkEvent("at-start", "s", "info", "m", base),
		mkEvent("one-nano", "s", "info", "m", base.Add(time.Nanosecond)),
		mkEvent("just-before-boundary", "s", "info", "m", base.Add(time.Second-time.Nanosecond)),
		mkEvent("at-boundary", "s", "info", "m", base.Add(time.Second)),
		mkEvent("at-end", "s", "info", "m", base.Add(3*time.Second)),
		mkEvent("after-end", "s", "info", "m", base.Add(3*time.Second+time.Nanosecond)),
		mkEvent("before-start", "s", "info", "m", base.Add(-time.Nanosecond)),
	}
	if _, err := tl.Ingest(batch, nil); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	query := CompareQuery{
		BaselineSince: base, BaselineUntil: base.Add(3 * time.Second),
		Since: base.Add(time.Hour), Until: base.Add(time.Hour + 3*time.Second),
		Step: time.Second,
	}
	result, err := tl.Compare(query)
	if err != nil {
		t.Fatalf("compare: %v", err)
	}
	want := [][]int{
		{3, 0}, // at-start, one-nano, just-before-boundary
		{1, 0}, // at-boundary
		{0, 0}, // at-end is excluded: half-open
	}
	for i, row := range want {
		if result.Segments[i].BaselineCount != row[0] || result.Segments[i].ObservationCount != row[1] {
			t.Fatalf("segment %d counts = (%d,%d), want (%d,%d)",
				i, result.Segments[i].BaselineCount, result.Segments[i].ObservationCount, row[0], row[1])
		}
	}
	if result.BaselineTotal != 4 || result.ObservationTotal != 0 {
		t.Fatalf("totals = %d/%d, want 4/0", result.BaselineTotal, result.ObservationTotal)
	}
	if result.Difference != -4 || result.Ratio == nil || math.Abs(*result.Ratio+1) > 1e-12 {
		t.Fatalf("want difference -4 and ratio -1, got %d %v", result.Difference, result.Ratio)
	}
}

func TestCompareCountsAndRatio(t *testing.T) {
	tl := NewTimeline()
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	batch := []Event{
		mkEvent("b1", "s", "info", "m", base),
		mkEvent("b2", "s", "info", "m", base.Add(30*time.Second)),
		mkEvent("o1", "s", "info", "m", base.Add(time.Hour)),
		mkEvent("o2", "s", "info", "m", base.Add(time.Hour+30*time.Second)),
		mkEvent("o3", "s", "info", "m", base.Add(time.Hour+59*time.Second)),
	}
	if _, err := tl.Ingest(batch, nil); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	result, err := tl.Compare(CompareQuery{
		BaselineSince: base, BaselineUntil: base.Add(time.Minute),
		Since: base.Add(time.Hour), Until: base.Add(time.Hour + time.Minute),
		Step: time.Minute,
	})
	if err != nil {
		t.Fatalf("compare: %v", err)
	}
	if result.BaselineTotal != 2 || result.ObservationTotal != 3 || result.Difference != 1 {
		t.Fatalf("aggregates wrong: %+v", result)
	}
	if result.Ratio == nil || math.Abs(*result.Ratio-0.5) > 1e-12 {
		t.Fatalf("ratio must be 0.5, got %v", result.Ratio)
	}
	if got := result.Segments[0].ObservationCount - result.Segments[0].BaselineCount; got != 1 {
		t.Fatalf("segment difference = %d, want 1", got)
	}
}

func TestCompareOverlapCountsOnBothSidesOnceEach(t *testing.T) {
	tl := NewTimeline()
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	// Overlapping windows [00,04) and [02,06); events in [02,04) hit both.
	batch := []Event{
		mkEvent("only-baseline", "s", "info", "m", base.Add(time.Minute)),
		mkEvent("shared-1", "s", "info", "m", base.Add(2*time.Minute+time.Nanosecond)),
		mkEvent("shared-2", "s", "info", "m", base.Add(3*time.Minute)),
		mkEvent("only-observed", "s", "info", "m", base.Add(5*time.Minute)),
		mkEvent("at-observed-end", "s", "info", "m", base.Add(6*time.Minute)),
	}
	if _, err := tl.Ingest(batch, nil); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	result, err := tl.Compare(CompareQuery{
		BaselineSince: base, BaselineUntil: base.Add(4 * time.Minute),
		Since: base.Add(2 * time.Minute), Until: base.Add(6 * time.Minute),
		Step: 2 * time.Minute,
	})
	if err != nil {
		t.Fatalf("compare: %v", err)
	}
	// Baseline segments: [00,02) -> 1, [02,04) -> 2.
	// Observed segments: [02,04) -> 2, [04,06) -> 1.
	want := []struct{ b, o int }{
		{1, 2},
		{2, 1},
	}
	for i, w := range want {
		if result.Segments[i].BaselineCount != w.b || result.Segments[i].ObservationCount != w.o {
			t.Fatalf("segment %d = (b:%d o:%d), want (b:%d o:%d)",
				i, result.Segments[i].BaselineCount, result.Segments[i].ObservationCount, w.b, w.o)
		}
	}
	if result.BaselineTotal != 3 || result.ObservationTotal != 3 || result.Difference != 0 {
		t.Fatalf("overlap totals wrong: %+v", result)
	}
}

func TestCompareFilters(t *testing.T) {
	tl := NewTimeline()
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	batch := []Event{
		mkEvent("g1", "gateway", "critical", "m", base),
		mkEvent("g2", "gateway", "info", "m", base.Add(time.Second)),
		mkEvent("a1", "api", "critical", "m", base.Add(2*time.Second)),
	}
	if _, err := tl.Ingest(batch, nil); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	tmpl := CompareQuery{
		BaselineSince: base, BaselineUntil: base.Add(3 * time.Second),
		Since: base.Add(time.Hour), Until: base.Add(time.Hour + 3*time.Second),
		Step: 3 * time.Second,
	}

	byService := tmpl
	byService.Service = " gateway "
	result, err := tl.Compare(byService)
	if err != nil {
		t.Fatalf("compare: %v", err)
	}
	if result.BaselineTotal != 2 {
		t.Fatalf("service filter must keep both gateway events, got %d", result.BaselineTotal)
	}

	bySeverity := tmpl
	bySeverity.Severity = " CRITICAL "
	result, err = tl.Compare(bySeverity)
	if err != nil {
		t.Fatalf("compare: %v", err)
	}
	if result.BaselineTotal != 2 {
		t.Fatalf("severity filter must be case-insensitive, got %d", result.BaselineTotal)
	}

	both := tmpl
	both.Service = "gateway"
	both.Severity = "critical"
	result, err = tl.Compare(both)
	if err != nil {
		t.Fatalf("compare: %v", err)
	}
	if result.BaselineTotal != 1 {
		t.Fatalf("combined filters must keep one event, got %d", result.BaselineTotal)
	}
}

func TestCompareEmptyTimelineKeepsFullSegments(t *testing.T) {
	tl := NewTimeline()
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	result, err := tl.Compare(CompareQuery{
		BaselineSince: base, BaselineUntil: base.Add(10 * time.Minute),
		Since: base.Add(time.Hour), Until: base.Add(time.Hour + 10*time.Minute),
		Step: 4 * time.Minute,
	})
	if err != nil {
		t.Fatalf("compare: %v", err)
	}
	if result.BaselineTotal != 0 || result.ObservationTotal != 0 || result.Difference != 0 || result.Ratio != nil {
		t.Fatalf("empty aggregates wrong: %+v", result)
	}
	if len(result.Segments) != 3 {
		t.Fatalf("want three zeroed segments, got %d", len(result.Segments))
	}
	for i, segment := range result.Segments {
		if segment.BaselineCount != 0 || segment.ObservationCount != 0 {
			t.Fatalf("segment %d must be zeroed", i)
		}
	}
}

func TestCompareTimezoneIndependent(t *testing.T) {
	tl := NewTimeline()
	utcBase := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	cst := time.FixedZone("CST", 8*60*60)
	cstBase := time.Date(2026, 10, 1, 17, 0, 0, 0, cst) // 17:00+08 == 09:00 UTC
	if !cstBase.Equal(utcBase) {
		t.Fatal("test setup: instants differ")
	}
	if _, err := tl.Ingest([]Event{
		mkEvent("e1", "s", "info", "m", utcBase.Add(30*time.Second)),
		mkEvent("e2", "s", "info", "m", utcBase.Add(90*time.Second)),
	}, nil); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	build := func(start time.Time) CompareQuery {
		return CompareQuery{
			BaselineSince: start, BaselineUntil: start.Add(2 * time.Minute),
			Since: start.Add(24 * time.Hour), Until: start.Add(24*time.Hour + 2*time.Minute),
			Step: time.Minute,
		}
	}
	utcResult, err := tl.Compare(build(utcBase))
	if err != nil {
		t.Fatalf("utc compare: %v", err)
	}
	cstResult, err := tl.Compare(build(cstBase))
	if err != nil {
		t.Fatalf("cst compare: %v", err)
	}
	for i := range utcResult.Segments {
		u, c := utcResult.Segments[i], cstResult.Segments[i]
		if u.BaselineCount != c.BaselineCount || u.ObservationCount != c.ObservationCount {
			t.Fatalf("segment %d counts differ by timezone: %+v vs %+v", i, u, c)
		}
		if !u.BaselineStart.Equal(c.BaselineStart) || !u.BaselineEnd.Equal(c.BaselineEnd) {
			t.Fatalf("segment %d boundaries differ by timezone", i)
		}
	}
}

func TestCompareLateEventAndReplayDeterminism(t *testing.T) {
	tl := NewTimeline()
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	query := CompareQuery{
		BaselineSince: base, BaselineUntil: base.Add(time.Minute),
		Since: base.Add(time.Hour), Until: base.Add(time.Hour + time.Minute),
		Step: 30 * time.Second,
	}

	before, err := tl.Compare(query)
	if err != nil {
		t.Fatalf("compare: %v", err)
	}
	if before.BaselineTotal != 0 {
		t.Fatal("baseline must start empty")
	}

	// A late, historical event committed after the fact lands in segment 0.
	late := []Event{mkEvent("late", "s", "info", "m", base.Add(time.Nanosecond))}
	if _, err := tl.Ingest(late, nil); err != nil {
		t.Fatalf("ingest late: %v", err)
	}
	after, err := tl.Compare(query)
	if err != nil {
		t.Fatalf("compare after late ingest: %v", err)
	}
	if after.BaselineTotal != 1 || after.Segments[0].BaselineCount != 1 {
		t.Fatalf("late event must count into segment 0: %+v", after)
	}

	// Re-submitting identical data changes nothing.
	if res, err := tl.Ingest(late, nil); err != nil || res.Replayed != 1 || res.Created != 0 {
		t.Fatalf("replay: %+v %v", res, err)
	}
	replayed, err := tl.Compare(query)
	if err != nil {
		t.Fatalf("compare after replay: %v", err)
	}
	if replayed.BaselineTotal != after.BaselineTotal ||
		replayed.Segments[0].BaselineCount != after.Segments[0].BaselineCount {
		t.Fatal("identical replay must not alter comparison")
	}
}

func TestCompareSeesOneCommittedSnapshot(t *testing.T) {
	tl := NewTimeline()
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	query := CompareQuery{
		BaselineSince: base, BaselineUntil: base.Add(time.Minute),
		Since: base, Until: base.Add(time.Minute),
		Step: time.Minute,
	}
	batch := []Event{
		mkEvent("a", "s", "info", "one", base),
		mkEvent("b", "s", "info", "two", base.Add(30*time.Second)),
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

	// While the commit is in flight every compare must see either the empty
	// timeline or the complete two-event batch — never one event, and never a
	// response whose totals disagree with the sum of its segments.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 200; i++ {
			result, err := tl.Compare(query)
			if err != nil {
				t.Errorf("compare: %v", err)
				return
			}
			sumB, sumO := 0, 0
			for _, segment := range result.Segments {
				sumB += segment.BaselineCount
				sumO += segment.ObservationCount
			}
			if sumB != result.BaselineTotal || sumO != result.ObservationTotal {
				t.Errorf("totals disagree with segments: %+v", result)
				return
			}
			if result.BaselineTotal != 0 && result.BaselineTotal != 2 {
				t.Errorf("compare observed partial batch of %d", result.BaselineTotal)
				return
			}
		}
	}()
	close(release)
	<-commitDone
	<-done
}
