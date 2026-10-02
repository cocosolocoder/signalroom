package metrics

import (
	"errors"
	"math"
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
// query window using a single read of the store, so every series and segment
// reflects one committed snapshot. Late arrivals are bucketed by their own
// sample time, so an unchanged dataset always yields the same result
// regardless of ingestion order.
func (s *Store) Aggregate(query AggregateQuery) ([]AggregateSeries, error) {
	segments, err := ValidateAggregateQuery(query)
	if err != nil {
		return nil, err
	}

	name := strings.TrimSpace(query.Name)
	service := strings.TrimSpace(query.Service)

	s.mu.RLock()
	defer s.mu.RUnlock()

	// Select the matching series that actually have a sample in the
	// half-open window, in a stable order (service, then full label set).
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

	if len(keys)*len(segments) > MaxAggregateSeriesSegments {
		return nil, ErrTooManySeriesSegments
	}

	out := make([]AggregateSeries, 0, len(keys))
	var grandTotal float64
	for _, key := range keys {
		state := s.series[key]
		seriesSegs := append([]AggregateSegment(nil), segments...)
		total, err := fillSeries(seriesSegs, state.points, query)
		if err != nil {
			return nil, err
		}
		grandTotal += total
		if !isFinite(grandTotal) {
			return nil, ErrDeltaNotFinite
		}
		out = append(out, AggregateSeries{
			Service:  key.service,
			Labels:   state.labels,
			Segments: seriesSegs,
		})
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

// fillSeries counts samples per segment and attributes cumulative deltas.
// The nearest sample strictly before the window start acts as the first
// in-window sample's predecessor; a sample with no predecessor anywhere
// contributes no delta. A value that does not move backwards contributes the
// difference; a backwards move is a counter reset and contributes the current
// value. Every delta lands in the segment containing the later sample; the
// sample at the window end never participates. It returns the series' total
// delta so the caller can fail the whole query when no finite grand total
// exists.
func fillSeries(segments []AggregateSegment, points []point, query AggregateQuery) (float64, error) {
	step := query.Step

	// First point at or after the window start.
	first := sort.Search(len(points), func(i int) bool {
		return !points[i].at.Before(query.Since)
	})

	var prevValue, total float64
	hasPrev := false
	if first > 0 {
		prevValue = points[first-1].value
		hasPrev = true
	}

	for _, p := range points[first:] {
		if !p.at.Before(query.Until) {
			break // at == until and anything later is excluded
		}
		idx := int(p.at.Sub(query.Since) / step)
		if idx >= len(segments) {
			idx = len(segments) - 1 // defensive; excluded end keeps this in range
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
