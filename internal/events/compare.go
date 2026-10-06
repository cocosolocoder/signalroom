package events

import (
	"errors"
	"time"
)

// maxCompareSegments caps how many segments a single comparison may split
// either window into. Requests needing more are rejected rather than
// truncated, so the response always covers the windows completely.
const maxCompareSegments = 10000

// CompareQuery selects two equal-length, half-open windows [Since, Until) and
// [BaselineSince, BaselineUntil) and divides them into intervals of Step,
// keeping only events that pass the same optional service, severity, and
// label filters Query uses.
type CompareQuery struct {
	Service       string
	Severity      string
	Labels        map[string]string
	BaselineSince time.Time
	BaselineUntil time.Time
	Since         time.Time
	Until         time.Time
	Step          time.Duration
}

// CompareSegment is one step-aligned slice of both windows, ordered from each
// window's start. Counts include segment start instants and exclude segment
// end instants; the final segment ends exactly at the window end even when
// that is short of a full step.
type CompareSegment struct {
	BaselineStart    time.Time
	BaselineEnd      time.Time
	ObservationStart time.Time
	ObservationEnd   time.Time
	BaselineCount    int
	ObservationCount int
}

// CompareResult holds the aggregate change between two windows plus every
// aligned segment. Ratio is the relative change (observation minus baseline
// over baseline); it is nil when the baseline total is zero.
type CompareResult struct {
	BaselineTotal    int
	ObservationTotal int
	Difference       int
	Ratio            *float64
	Segments         []CompareSegment
}

// Parameter problems a compare request can hit. The HTTP layer maps each to
// its own 400 message.
var (
	// ErrCompareStep reports a non-positive step; the HTTP layer bounds it
	// further to 1..86400 seconds.
	ErrCompareStep = errors.New("compare step must be positive")
	// ErrBaselineWindow reports a baseline whose start is not before its end.
	ErrBaselineWindow = errors.New("baseline window must start before it ends")
	// ErrObservationWindow reports an observation window whose start is not
	// before its end.
	ErrObservationWindow = errors.New("observation window must start before it ends")
	// ErrWindowLength reports two windows of different lengths.
	ErrWindowLength = errors.New("compare windows must have equal length")
	// ErrTooManySegments reports a request that needs more than
	// maxCompareSegments segments for either side.
	ErrTooManySegments = errors.New("compare windows require too many segments")
)

// Compare validates the windows and counts filtered events inside them using
// a single read of the timeline, so both sides, both totals, and every
// segment reflect one committed snapshot: a batch committed during the call
// is either fully present or fully absent.
func (t *Timeline) Compare(query CompareQuery) (CompareResult, error) {
	segments, err := ValidateCompareWindows(query)
	if err != nil {
		return CompareResult{}, err
	}

	filter := NewFilter(query.Service, query.Severity, query.Labels)

	baselineCounts := make([]int, len(segments))
	observationCounts := make([]int, len(segments))

	t.mu.RLock()
	for _, event := range t.events {
		if !filter.Matches(event) {
			continue
		}
		if i, ok := bucketIndex(event.At, query.BaselineSince, query.BaselineUntil, query.Step); ok {
			baselineCounts[i]++
		}
		if i, ok := bucketIndex(event.At, query.Since, query.Until, query.Step); ok {
			observationCounts[i]++
		}
	}
	t.mu.RUnlock()

	for i := range segments {
		segments[i].BaselineCount = baselineCounts[i]
		segments[i].ObservationCount = observationCounts[i]
	}

	result := CompareResult{Segments: segments}
	for i := range segments {
		result.BaselineTotal += baselineCounts[i]
		result.ObservationTotal += observationCounts[i]
	}
	result.Difference = result.ObservationTotal - result.BaselineTotal
	if result.BaselineTotal != 0 {
		ratio := float64(result.Difference) / float64(result.BaselineTotal)
		result.Ratio = &ratio
	}
	return result, nil
}

// ValidateCompareWindows enforces a positive step, strictly increasing
// windows (baseline first, then observation), equal lengths, and the per-side
// segment limit, then returns the aligned segment skeleton. Windows may
// overlap and are compared by absolute instant, so time zones are irrelevant.
func ValidateCompareWindows(query CompareQuery) ([]CompareSegment, error) {
	if query.Step <= 0 {
		return nil, ErrCompareStep
	}
	baselineLen := query.BaselineUntil.Sub(query.BaselineSince)
	observationLen := query.Until.Sub(query.Since)
	if baselineLen <= 0 {
		return nil, ErrBaselineWindow
	}
	if observationLen <= 0 {
		return nil, ErrObservationWindow
	}
	if baselineLen != observationLen {
		return nil, ErrWindowLength
	}

	// Ceiling division without an addition that could overflow for windows
	// close to the maximum representable duration.
	count := baselineLen / query.Step
	if baselineLen%query.Step != 0 {
		count++
	}
	if count > maxCompareSegments {
		return nil, ErrTooManySegments
	}

	out := make([]CompareSegment, count)
	for i := range out {
		baselineStart := query.BaselineSince.Add(query.Step * time.Duration(i))
		observationStart := query.Since.Add(query.Step * time.Duration(i))
		baselineEnd := baselineStart.Add(query.Step)
		if baselineEnd.After(query.BaselineUntil) {
			baselineEnd = query.BaselineUntil
		}
		observationEnd := observationStart.Add(query.Step)
		if observationEnd.After(query.Until) {
			observationEnd = query.Until
		}
		out[i] = CompareSegment{
			BaselineStart:    baselineStart,
			BaselineEnd:      baselineEnd,
			ObservationStart: observationStart,
			ObservationEnd:   observationEnd,
		}
	}
	return out, nil
}

// bucketIndex reports the zero-based segment, measured in whole steps from the
// window start, that contains at. The window is half-open: [start, end).
// A boundary aligned exactly with a segment start is counted in that segment;
// an instant equal to the window end is excluded.
func bucketIndex(at, start, end time.Time, step time.Duration) (int, bool) {
	if at.Before(start) || !at.Before(end) {
		return 0, false
	}
	offset := at.Sub(start)
	if offset < 0 {
		return 0, false
	}
	return int(offset / step), true
}
