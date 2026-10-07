package metrics

import (
	"errors"
	"math"
	"slices"
	"sort"
	"strings"
	"time"
)

// Aggregation limits: at most this many segments cover one query window, and
// the response must not contain more than this many series-segment pairs.
// Requests beyond either bound are rejected rather than truncated.
const (
	MaxAggregateSegments       = 10000
	MaxAggregateSeriesSegments = 100000
)

// AggregateQuery selects samples by exact metric name, optional exact service,
// and the same label conditions event queries use, over the half-open window
// [Since, Until), split from Since into intervals of Step.
type AggregateQuery struct {
	Name    string
	Service string
	Since   time.Time
	Until   time.Time
	Step    time.Duration
	Labels  map[string]string
}

// AggregateSegment is one aligned slice of one series: the UTC boundaries,
// how many samples fall in it, and the cumulative-counter delta attributed to
// it. Segments without samples carry a zero count and delta.
type AggregateSegment struct {
	Start time.Time
	End   time.Time
	Count int
	Delta float64
}

// AggregateSeries is one matching series' complete segment set. Labels is nil
// when the series has none.
type AggregateSeries struct {
	Service  string
	Labels   map[string]string
	Segments []AggregateSegment
}

// Query problems aggregation can hit. The HTTP layer maps each to a status.
var (
	// ErrAggregateStep reports a non-positive step.
	ErrAggregateStep = errors.New("aggregate step must be positive")
	// ErrAggregateWindow reports a start not strictly before its end.
	ErrAggregateWindow = errors.New("aggregate window must start before it ends")
	// ErrTooManySegments reports a window needing more than
	// MaxAggregateSegments segments.
	ErrTooManySegments = errors.New("aggregate window requires too many segments")
	// ErrTooManySeriesSegments reports a response holding more than
	// MaxAggregateSeriesSegments series-segment pairs.
	ErrTooManySeriesSegments = errors.New("aggregate covers too many series segments")
	// ErrDeltaNotFinite reports increments whose sum overflows to a
	// non-finite value; the whole query fails rather than returning Inf/NaN.
	ErrDeltaNotFinite = errors.New("aggregate delta total is not a finite number")
)

// Aggregate computes per-series sample counts and cumulative deltas over the
// query window from one committed snapshot of the store. Selection runs under
// a single read lock: the matching series are picked and the samples the
// computation needs — the in-window points plus each series' nearest
// pre-window predecessor — are copied out. The segment math then runs after
// the lock is released, so a slow aggregation never blocks new batches from
// committing; those batches simply belong to the next query's snapshot. Late
// arrivals are bucketed by their own sample time, so an unchanged dataset
// always yields the same result regardless of ingestion order.
func (s *Store) Aggregate(query AggregateQuery) ([]AggregateSeries, error) {
	segments, err := ValidateAggregateQuery(query)
	if err != nil {
		return nil, err
	}

	selected, err := s.selectAggregateSeries(query, len(segments))
	if err != nil {
		return nil, err
	}
	return computeAggregate(selected, segments, query)
}

// computeAggregate fills one copy of the segment skeleton per selected series
// from the snapshots. It touches no store state and holds no lock, so it can
// run while new batches commit; those batches belong to later selections.
func computeAggregate(selected []seriesSnapshot, segments []AggregateSegment, query AggregateQuery) ([]AggregateSeries, error) {
	out := make([]AggregateSeries, 0, len(selected))
	var grandTotal float64
	for _, snap := range selected {
		seriesSegs := append([]AggregateSegment(nil), segments...)
		total, err := fillSeries(seriesSegs, snap, query)
		if err != nil {
			return nil, err
		}
		grandTotal += total
		if !isFinite(grandTotal) {
			return nil, ErrDeltaNotFinite
		}
		out = append(out, AggregateSeries{
			Service:  snap.key.service,
			Labels:   snap.labels,
			Segments: seriesSegs,
		})
	}
	return out, nil
}

// seriesSnapshot is one matching series' committed state, copied under the
// store lock so the segment computation can run after the lock is released.
// points holds the in-window points [Since, Until) in time order; prev is the
// nearest sample strictly before the window, which the first in-window sample
// needs as its predecessor reading.
type seriesSnapshot struct {
	key     seriesKey
	labels  map[string]string
	points  []point
	prev    float64
	hasPrev bool
}

// selectAggregateSeries picks the series matching the query that actually
// have a sample in the half-open window, in a stable order (service, then
// full label set), and copies the points the delta computation needs out of
// the store. It runs entirely under the read lock, so the returned snapshots
// all reflect the same committed dataset; samples committed afterwards are
// invisible to this selection and belong to later queries.
func (s *Store) selectAggregateSeries(query AggregateQuery, segmentCount int) ([]seriesSnapshot, error) {
	name := strings.TrimSpace(query.Name)
	service := strings.TrimSpace(query.Service)

	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := make([]seriesKey, 0, len(s.series))
	for key, state := range s.series {
		if key.name != name {
			continue
		}
		if service != "" && key.service != service {
			continue
		}
		if !labelsMatch(state.labels, query.Labels) {
			continue
		}
		if !state.hasPointBetween(query.Since, query.Until) {
			continue
		}
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].service != keys[j].service {
			return keys[i].service < keys[j].service
		}
		return keys[i].labels < keys[j].labels
	})

	if len(keys)*segmentCount > MaxAggregateSeriesSegments {
		return nil, ErrTooManySeriesSegments
	}

	out := make([]seriesSnapshot, 0, len(keys))
	for _, key := range keys {
		state := s.series[key]
		// First point at or after the window start; the point just before it
		// is the predecessor the first in-window sample computes against, so
		// it must be copied even though it lies outside the window.
		first := sort.Search(len(state.points), func(i int) bool {
			return !state.points[i].at.Before(query.Since)
		})
		// First point at or after the window end; it and anything later is
		// excluded, so the copy stops here.
		end := sort.Search(len(state.points), func(i int) bool {
			return !state.points[i].at.Before(query.Until)
		})
		snap := seriesSnapshot{
			key:    key,
			labels: state.labels,
			points: slices.Clone(state.points[first:end]),
		}
		if first > 0 {
			snap.prev = state.points[first-1].value
			snap.hasPrev = true
		}
		out = append(out, snap)
	}
	return out, nil
}

// labelsMatch reports whether a series' full label set satisfies every
// condition. A series missing a conditioned label never matches, and
// comparison is case-sensitive on names and values.
func labelsMatch(labels, conditions map[string]string) bool {
	for name, want := range conditions {
		got, ok := labels[name]
		if !ok || got != want {
			return false
		}
	}
	return true
}

// ValidateAggregateQuery enforces a positive step, a strictly increasing
// half-open window, and the segment bound, then returns the segment skeleton
// (UTC-relative boundaries are the caller's job at rendering time).
func ValidateAggregateQuery(query AggregateQuery) ([]AggregateSegment, error) {
	if query.Step <= 0 {
		return nil, ErrAggregateStep
	}
	length := query.Until.Sub(query.Since)
	if length <= 0 {
		return nil, ErrAggregateWindow
	}
	count := int64(length / query.Step)
	if length%query.Step != 0 {
		count++
	}
	if count > MaxAggregateSegments {
		return nil, ErrTooManySegments
	}

	out := make([]AggregateSegment, count)
	for i := range out {
		start := query.Since.Add(query.Step * time.Duration(i))
		end := start.Add(query.Step)
		if end.After(query.Until) {
			end = query.Until
		}
		out[i] = AggregateSegment{Start: start, End: end}
	}
	return out, nil
}

// hasPointBetween reports whether any point falls in [since, until).
func (st *seriesState) hasPointBetween(since, until time.Time) bool {
	idx := sort.Search(len(st.points), func(i int) bool {
		return !st.points[i].at.Before(since)
	})
	return idx < len(st.points) && st.points[idx].at.Before(until)
}

// fillSeries counts samples per segment and attributes cumulative deltas for
// one snapshotted series. The snapshot's predecessor — the nearest sample
// strictly before the window start — acts as the first in-window sample's
// predecessor reading; a sample with no predecessor anywhere contributes no
// delta. A value that does not move backwards contributes the difference; a
// backwards move is a counter reset and contributes the current value. Every
// delta lands in the segment containing the later sample; the snapshot holds
// only in-window points, so the sample at the window end never participates.
// It returns the series' total delta so the caller can fail the whole query
// when no finite grand total exists.
func fillSeries(segments []AggregateSegment, snap seriesSnapshot, query AggregateQuery) (float64, error) {
	step := query.Step

	prevValue, hasPrev := snap.prev, snap.hasPrev
	var total float64
	for _, p := range snap.points {
		idx := int(p.at.Sub(query.Since) / step)
		if idx >= len(segments) {
			idx = len(segments) - 1 // defensive; the clipped final segment keeps this in range
		}
		segments[idx].Count++

		var delta float64
		if hasPrev {
			if p.value >= prevValue {
				delta = p.value - prevValue
			} else {
				delta = p.value // counter reset
			}
			segments[idx].Delta += delta
			if !isFinite(segments[idx].Delta) {
				return 0, ErrDeltaNotFinite
			}
			total += delta
			if !isFinite(total) {
				return 0, ErrDeltaNotFinite
			}
		}
		prevValue = p.value
		hasPrev = true
	}
	return total, nil
}

func isFinite(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0)
}
