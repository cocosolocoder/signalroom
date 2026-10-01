package events

import (
	"sort"
	"strings"
	"sync"
	"time"
)

// BeforeCommit persists a batch of brand-new events. It runs while the
// timeline lock is held and before the batch becomes visible to queries, so a
// successful return guarantees the batch is durable and committed atomically.
// Returning an error rejects the batch without touching the timeline.
type BeforeCommit func(persisted []Event) error

// Timeline is a concurrency-safe, de-duplicated event timeline that accepts
// whole batches and reports replays of already-stored events.
type Timeline struct {
	mu     sync.RWMutex
	byID   map[string]Event
	events []Event
}

// NewTimeline returns an empty timeline.
func NewTimeline() *Timeline {
	return &Timeline{byID: make(map[string]Event)}
}

// IngestResult counts how a batch was applied: Created events are new,
// Replayed events matched an existing event with the same id and content.
type IngestResult struct {
	Created  int
	Replayed int
}

// Ingest normalizes and validates every event first, then checks the whole
// batch against existing events and itself. Validation failures produce a
// ValidationError; duplicate ids with differing content, or a repeated id
// within the batch, produce a ConflictError. Nothing is stored when an error
// is returned.
//
// New events are passed to beforeCommit for durable storage; only after it
// succeeds does the batch become visible, so concurrent readers see either
// the complete batch or none of it.
func (t *Timeline) Ingest(batch []Event, beforeCommit BeforeCommit) (IngestResult, error) {
	normalized := make([]Event, len(batch))
	for i, event := range batch {
		norm, err := Normalize(event)
		if err != nil {
			return IngestResult{}, err
		}
		normalized[i] = norm
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	pending := make(map[string]struct{}, len(normalized))
	var created []Event
	var result IngestResult
	for _, event := range normalized {
		if _, repeated := pending[event.ID]; repeated {
			return IngestResult{}, &ConflictError{Reason: "event id repeats within batch"}
		}
		pending[event.ID] = struct{}{}
		if existing, ok := t.byID[event.ID]; ok {
			if !sameEvent(existing, event) {
				return IngestResult{}, &ConflictError{Reason: "event id already exists with different content"}
			}
			result.Replayed++
			continue
		}
		created = append(created, event)
		result.Created++
	}

	if len(created) > 0 {
		if beforeCommit != nil {
			if err := beforeCommit(created); err != nil {
				return IngestResult{}, err
			}
		}
		for _, event := range created {
			t.byID[event.ID] = event
			t.events = append(t.events, event)
		}
		sort.SliceStable(t.events, func(i, j int) bool {
			return byTimeThenID(t.events[i], t.events[j])
		})
	}
	return result, nil
}

// Load inserts events recovered from durable storage during startup. Every
// event must already be normalized; a duplicate id with differing content is
// reported as a conflict so the caller can fail rather than mutate data.
func (t *Timeline) Load(event Event) error {
	if _, err := Normalize(event); err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if existing, ok := t.byID[event.ID]; ok {
		if !sameEvent(existing, event) {
			return &ConflictError{Reason: "event id already exists with different content"}
		}
		return nil
	}
	t.byID[event.ID] = event
	t.events = append(t.events, event)
	return nil
}

// SortAll orders recovered events by time, then id. Call it once after Load.
func (t *Timeline) SortAll() {
	t.mu.Lock()
	defer t.mu.Unlock()
	sort.SliceStable(t.events, func(i, j int) bool {
		return byTimeThenID(t.events[i], t.events[j])
	})
}

// Query returns the events matching the optional filters, ordered by time and
// then by id. Time equality uses absolute instants.
func (t *Timeline) Query(query Query) []Event {
	service := strings.TrimSpace(query.Service)
	severity := strings.ToLower(strings.TrimSpace(query.Severity))

	t.mu.RLock()
	defer t.mu.RUnlock()
	result := make([]Event, 0, len(t.events))
	for _, event := range t.events {
		if service != "" && event.Service != service {
			continue
		}
		if severity != "" && event.Severity != severity {
			continue
		}
		if !query.Since.IsZero() && event.At.Before(query.Since) {
			continue
		}
		if !query.Until.IsZero() && event.At.After(query.Until) {
			continue
		}
		result = append(result, event)
	}
	return result
}

// sameEvent compares two normalized events on their absolute instant.
func sameEvent(a, b Event) bool {
	return a.ID == b.ID &&
		a.Service == b.Service &&
		a.Severity == b.Severity &&
		a.Message == b.Message &&
		a.At.Equal(b.At)
}

// CompareQuery describes two equal-length windows counted side by side. The
// baseline window is [BaselineSince, BaselineUntil) and the observation window
// is [Since, Until); Step is the segment width both windows are aligned to.
type CompareQuery struct {
	Service       string
	Severity      string
	BaselineSince time.Time
	BaselineUntil time.Time
	Since         time.Time
	Until         time.Time
	Step          time.Duration
}

// CompareSegment is one aligned interval of the two windows. Every interval
// contains its start and excludes its end; the final interval ends at the
// window end.
type CompareSegment struct {
	BaselineSince    time.Time
	BaselineUntil    time.Time
	Since            time.Time
	Until            time.Time
	BaselineCount    int
	ObservationCount int
	Difference       int
}

// CompareResult holds the per-side totals, the observation-minus-baseline
// difference, the ratio (nil when the baseline total is zero), and the
// aligned segments ordered from each window's start.
type CompareResult struct {
	BaselineTotal    int
	ObservationTotal int
	Difference       int
	Ratio            *float64
	Segments         []CompareSegment
}

// Compare counts the filtered events in the baseline and observation windows
// in one consistent snapshot, then aligns the counts by step-sized segments
// from each window's start. Both windows must be equal in length; segments
// contain their start and exclude their end, and the final segment ends at
// the window end. Overlapping windows count an event on both sides, but never
// twice within one side.
func (t *Timeline) Compare(query CompareQuery) CompareResult {
	service := strings.TrimSpace(query.Service)
	severity := strings.ToLower(strings.TrimSpace(query.Severity))

	stepNs := int64(query.Step)
	baselineCounts := make([]int, SegmentCount(query.BaselineSince, query.BaselineUntil, query.Step))
	observationCounts := make([]int, len(baselineCounts))

	// One lock-held pass keeps both sides, the totals, and every segment on
	// the same committed version of the timeline.
	t.mu.RLock()
	defer t.mu.RUnlock()
	var baselineTotal, observationTotal int
	for _, event := range t.events {
		if service != "" && event.Service != service {
			continue
		}
		if severity != "" && event.Severity != severity {
			continue
		}
		if index, ok := segmentIndex(query.BaselineSince, query.BaselineUntil, stepNs, event.At); ok {
			baselineCounts[index]++
			baselineTotal++
		}
		if index, ok := segmentIndex(query.Since, query.Until, stepNs, event.At); ok {
			observationCounts[index]++
			observationTotal++
		}
	}

	segments := make([]CompareSegment, len(baselineCounts))
	for i := range segments {
		segments[i] = CompareSegment{
			BaselineSince:    query.BaselineSince.Add(time.Duration(int64(i) * stepNs)),
			BaselineUntil:    segmentEnd(query.BaselineSince, query.BaselineUntil, stepNs, i),
			Since:            query.Since.Add(time.Duration(int64(i) * stepNs)),
			Until:            segmentEnd(query.Since, query.Until, stepNs, i),
			BaselineCount:    baselineCounts[i],
			ObservationCount: observationCounts[i],
			Difference:       observationCounts[i] - baselineCounts[i],
		}
	}

	difference := observationTotal - baselineTotal
	result := CompareResult{
		BaselineTotal:    baselineTotal,
		ObservationTotal: observationTotal,
		Difference:       difference,
		Segments:         segments,
	}
	if baselineTotal > 0 {
		ratio := float64(difference) / float64(baselineTotal)
		result.Ratio = &ratio
	}
	return result
}

// SegmentCount returns the number of step-sized segments in [start, end),
// rounding the final partial segment up to the window end.
func SegmentCount(start, end time.Time, step time.Duration) int {
	length := int64(end.Sub(start))
	segments := length / int64(step)
	if length%int64(step) != 0 {
		segments++
	}
	return int(segments)
}

// segmentIndex returns the segment containing at within [start, end); the
// instant at the window end or outside the window belongs to no segment.
func segmentIndex(start, end time.Time, stepNs int64, at time.Time) (int, bool) {
	if at.Before(start) || !at.Before(end) {
		return 0, false
	}
	return int(int64(at.Sub(start)) / stepNs), true
}

// segmentEnd returns the end of segment i, clipped to the window end.
func segmentEnd(start, end time.Time, stepNs int64, i int) time.Time {
	finish := start.Add(time.Duration(int64(i+1) * stepNs))
	if finish.After(end) {
		return end
	}
	return finish
}
