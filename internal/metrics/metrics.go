// Package metrics holds the cumulative-counter register: samples are
// append-only data points identified by an id, grouped into series by
// service, metric name, and the full label set. Aggregation turns them into
// per-segment increments.
package metrics

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cocosolocoder/signalroom/internal/events"
)

// Sample is one normalized metric data point. Labels is nil when the sample
// carries none.
type Sample struct {
	ID      string
	Service string
	Name    string
	At      time.Time
	Value   float64
	Labels  map[string]string
}

// ValidationError reports that a sample failed normalization.
type ValidationError struct{ Reason string }

func (e *ValidationError) Error() string { return e.Reason }

// IsValidationError reports whether err is a sample validation failure.
func IsValidationError(err error) bool {
	var target *ValidationError
	return errors.As(err, &target)
}

// ConflictError reports that a sample id or a series-time point clashes.
type ConflictError struct{ Reason string }

func (e *ConflictError) Error() string { return e.Reason }

// IsConflictError reports whether err is an id-content or series-time
// conflict.
func IsConflictError(err error) bool {
	var target *ConflictError
	return errors.As(err, &target)
}

// Normalize applies the ingestion rules: trim surrounding whitespace from id,
// service, and name (case is kept), require each to be non-empty, require a
// non-zero instant, a non-negative finite value, and labels that pass the
// same trimming and limit rules as events.
func Normalize(sample Sample) (Sample, error) {
	sample.ID = strings.TrimSpace(sample.ID)
	sample.Service = strings.TrimSpace(sample.Service)
	sample.Name = strings.TrimSpace(sample.Name)
	if sample.ID == "" || sample.Service == "" || sample.Name == "" || sample.At.IsZero() {
		return Sample{}, &ValidationError{Reason: "metric id, service, name, and timestamp are required"}
	}
	if math.IsNaN(sample.Value) || math.IsInf(sample.Value, 0) || sample.Value < 0 {
		return Sample{}, &ValidationError{Reason: "metric value must be a non-negative finite number"}
	}
	labels, err := events.NormalizeLabels(sample.Labels)
	if err != nil {
		return Sample{}, err
	}
	sample.Labels = labels
	return sample, nil
}

// IngestResult counts how a batch was applied: Created samples are new,
// Replayed samples matched an existing sample with the same id and content.
type IngestResult struct {
	Created  int
	Replayed int
}

// BeforeCommit persists a batch of brand-new samples. It runs while the
// timeline lock is held and before the batch becomes visible, so a
// successful return guarantees the batch is durable and committed atomically.
// Returning an error rejects the batch without touching the timeline.
type BeforeCommit func(persisted []Sample) error

// Timeline is a concurrency-safe, de-duplicated sample timeline.
type Timeline struct {
	mu         sync.RWMutex
	byID       map[string]Sample
	bySeriesST map[seriesTimeKey]string // series-time -> id
	samples    []Sample
}

// NewTimeline returns an empty timeline.
func NewTimeline() *Timeline {
	return &Timeline{
		byID:       make(map[string]Sample),
		bySeriesST: make(map[seriesTimeKey]string),
	}
}

// seriesTimeKey identifies one point of a series: the canonical series plus
// the absolute instant. Two samples of the same series at the same instant
// may not coexist.
type seriesTimeKey struct {
	series string
	at     time.Time
}

// seriesKey builds the canonical identity of a series: service, metric name,
// and the sorted full label set.
func seriesKey(sample Sample) string {
	parts := []string{sample.Service, sample.Name}
	names := make([]string, 0, len(sample.Labels))
	for name := range sample.Labels {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		parts = append(parts, name+"="+sample.Labels[name])
	}
	return strings.Join(parts, "\x00")
}

// Ingest normalizes and validates every sample first, then checks the whole
// batch against existing samples and itself. Validation failures produce a
// ValidationError; duplicate ids with differing content, a repeated id within
// the batch, and a series-time point claimed by a different id produce a
// ConflictError. Nothing is stored when an error is returned.
//
// New samples are passed to beforeCommit for durable storage; only after it
// succeeds does the batch become visible, so concurrent readers see either
// the complete batch or none of it.
func (t *Timeline) Ingest(batch []Sample, beforeCommit BeforeCommit) (IngestResult, error) {
	normalized := make([]Sample, len(batch))
	for i, sample := range batch {
		norm, err := Normalize(sample)
		if err != nil {
			return IngestResult{}, err
		}
		normalized[i] = norm
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	pendingIDs := make(map[string]struct{}, len(normalized))
	pendingST := make(map[seriesTimeKey]string, len(normalized))
	var created []Sample
	var result IngestResult
	for _, sample := range normalized {
		if _, repeated := pendingIDs[sample.ID]; repeated {
			return IngestResult{}, &ConflictError{Reason: "metric id repeats within batch"}
		}
		pendingIDs[sample.ID] = struct{}{}

		if existing, ok := t.byID[sample.ID]; ok {
			if !sameSample(existing, sample) {
				return IngestResult{}, &ConflictError{Reason: "metric id already exists with different content"}
			}
			result.Replayed++
			continue
		}

		st := seriesTimeKey{series: seriesKey(sample), at: sample.At}
		if _, claimed := pendingST[st]; claimed {
			return IngestResult{}, &ConflictError{Reason: "same series and timestamp already exists with a different id"}
		}
		if _, claimed := t.bySeriesST[st]; claimed {
			return IngestResult{}, &ConflictError{Reason: "same series and timestamp already exists with a different id"}
		}
		pendingST[st] = sample.ID
		created = append(created, sample)
		result.Created++
	}

	if len(created) > 0 {
		if beforeCommit != nil {
			if err := beforeCommit(created); err != nil {
				return IngestResult{}, err
			}
		}
		for _, sample := range created {
			t.byID[sample.ID] = sample
			st := seriesTimeKey{series: seriesKey(sample), at: sample.At}
			t.bySeriesST[st] = sample.ID
			t.samples = append(t.samples, sample)
		}
		sort.SliceStable(t.samples, func(i, j int) bool {
			if t.samples[i].At.Equal(t.samples[j].At) {
				return t.samples[i].ID < t.samples[j].ID
			}
			return t.samples[i].At.Before(t.samples[j].At)
		})
	}
	return result, nil
}

// Load inserts samples recovered from durable storage during startup. Every
// sample must already be normalized; a duplicate id with differing content,
// or a series-time point claimed by a different id, is reported as a conflict
// so the caller can fail rather than mutate data.
func (t *Timeline) Load(sample Sample) error {
	if _, err := Normalize(sample); err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if existing, ok := t.byID[sample.ID]; ok {
		if !sameSample(existing, sample) {
			return &ConflictError{Reason: "metric id already exists with different content"}
		}
		return nil
	}
	st := seriesTimeKey{series: seriesKey(sample), at: sample.At}
	if _, claimed := t.bySeriesST[st]; claimed {
		return &ConflictError{Reason: "same series and timestamp already exists with a different id"}
	}
	t.byID[sample.ID] = sample
	t.bySeriesST[st] = sample.ID
	t.samples = append(t.samples, sample)
	return nil
}

// SortAll orders recovered samples by time, then id. Call it once after Load.
func (t *Timeline) SortAll() {
	t.mu.Lock()
	defer t.mu.Unlock()
	sort.SliceStable(t.samples, func(i, j int) bool {
		if t.samples[i].At.Equal(t.samples[j].At) {
			return t.samples[i].ID < t.samples[j].ID
		}
		return t.samples[i].At.Before(t.samples[j].At)
	})
}

// sameSample compares two normalized samples on their absolute instant and
// their value. 1 and 1.0 decode to the same float64, so numeric spelling is
// irrelevant; time zones are compared by absolute instant.
func sameSample(a, b Sample) bool {
	return a.ID == b.ID &&
		a.Service == b.Service &&
		a.Name == b.Name &&
		a.At.Equal(b.At) &&
		a.Value == b.Value &&
		mapsEqual(a.Labels, b.Labels)
}

// mapsEqual compares two label maps, treating nil and empty as equal.
func mapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for key, value := range a {
		if other, ok := b[key]; !ok || other != value {
			return false
		}
	}
	return true
}

// AggregateQuery selects the series and the half-open [Since, Until) window
// to aggregate, divided into intervals of Step.
type AggregateQuery struct {
	Name    string
	Service string
	Labels  map[string]string
	Since   time.Time
	Until   time.Time
	Step    time.Duration
}

// Segment is one step-aligned slice of a series window. Boundaries are UTC.
type Segment struct {
	Since     time.Time
	Until     time.Time
	Count     int
	Increment float64
}

// SeriesResult is one series with its service, full labels, and segments.
type SeriesResult struct {
	Service  string
	Labels   map[string]string
	Segments []Segment
}

// AggregateResult holds the series matching an aggregate query.
type AggregateResult struct {
	Series []SeriesResult
}

// Parameter problems an aggregate query can hit. The HTTP layer maps each to
// its own 400 message.
var (
	// ErrAggregateRange reports a window whose start is not before its end.
	ErrAggregateRange = errors.New("aggregate window must start before it ends")
	// ErrAggregateStep reports a non-positive step; the HTTP layer bounds it
	// further to 1..86400 seconds.
	ErrAggregateStep = errors.New("aggregate step must be positive")
	// ErrTooManySegments reports a window needing more than
	// maxAggregateSegments segments.
	ErrTooManySegments = errors.New("aggregate window requires too many segments")
	// ErrTooManySeriesSegments reports a result needing more than
	// maxSeriesSegments series-segments.
	ErrTooManySeriesSegments = errors.New("aggregate result requires too many series-segments")
	// ErrNonFinite reports an increment sum that overflowed to infinity.
	ErrNonFinite = errors.New("increment sum is not representable as a finite number")
)

// maxAggregateSegments caps how many segments a single aggregate window may
// be split into.
const maxAggregateSegments = 10000

// maxSeriesSegments caps the total series-segments an aggregate result may
// contain: one segment skeleton per returned series.
const maxSeriesSegments = 100000

// seriesGroup accumulates the samples of one series while aggregating.
type seriesGroup struct {
	service string
	labels  map[string]string
	samples []Sample
}

// Aggregate validates the window, groups the matching samples into series,
// and computes per-segment increments using a single read of the timeline, so
// the whole result reflects one committed snapshot: a batch committed during
// the call is either fully present or fully absent.
//
// For each series, adjacent samples in time order contribute their
// difference when the value did not decrease, or the current value when it
// did (a reset). The latest sample before the window start can serve as the
// predecessor, so the first in-window sample still contributes; its
// increment is attributed to the segment containing that later sample.
// Samples exactly at the window end are excluded.
func (t *Timeline) Aggregate(query AggregateQuery) (AggregateResult, error) {
	if query.Step <= 0 {
		return AggregateResult{}, ErrAggregateStep
	}
	if !query.Since.Before(query.Until) {
		return AggregateResult{}, ErrAggregateRange
	}
	windowLen := query.Until.Sub(query.Since)
	segCount := windowLen / query.Step
	if windowLen%query.Step != 0 {
		segCount++
	}
	if segCount > maxAggregateSegments {
		return AggregateResult{}, ErrTooManySegments
	}

	// Name is matched exactly (no trimming); service follows the event
	// query rules and is trimmed.
	name := query.Name
	service := strings.TrimSpace(query.Service)

	t.mu.RLock()
	defer t.mu.RUnlock()

	groups := make(map[string]*seriesGroup)
	var groupOrder []string
	for _, sample := range t.samples {
		if sample.Name != name {
			continue
		}
		if service != "" && sample.Service != service {
			continue
		}
		if !events.LabelsMatch(sample.Labels, query.Labels) {
			continue
		}
		key := seriesKey(sample)
		group, ok := groups[key]
		if !ok {
			group = &seriesGroup{service: sample.Service, labels: sample.Labels}
			groups[key] = group
			groupOrder = append(groupOrder, key)
		}
		group.samples = append(group.samples, sample)
	}

	var result AggregateResult
	var totalSeriesSegments int
	for _, key := range groupOrder {
		group := groups[key]
		// Samples are stored in time-then-id order, but sort explicitly so
		// the predecessor chain is well-defined regardless of ingestion order.
		sort.SliceStable(group.samples, func(i, j int) bool {
			if group.samples[i].At.Equal(group.samples[j].At) {
				return group.samples[i].ID < group.samples[j].ID
			}
			return group.samples[i].At.Before(group.samples[j].At)
		})

		// Only series with at least one sample in the half-open window are
		// returned.
		inWindow := false
		for _, sample := range group.samples {
			if !sample.At.Before(query.Since) && sample.At.Before(query.Until) {
				inWindow = true
				break
			}
		}
		if !inWindow {
			continue
		}

		segments := make([]Segment, segCount)
		for i := range segments {
			segStart := query.Since.Add(query.Step * time.Duration(i))
			segEnd := segStart.Add(query.Step)
			if segEnd.After(query.Until) {
				segEnd = query.Until
			}
			segments[i] = Segment{Since: segStart, Until: segEnd}
		}

		// The latest sample before the window start can be the predecessor of
		// the first in-window sample, so its increment is not lost.
		var predecessor *Sample
		for i := len(group.samples) - 1; i >= 0; i-- {
			if group.samples[i].At.Before(query.Since) {
				pred := group.samples[i]
				predecessor = &pred
				break
			}
		}

		for _, sample := range group.samples {
			if sample.At.Before(query.Since) || !sample.At.Before(query.Until) {
				continue
			}
			offset := sample.At.Sub(query.Since)
			idx := int(offset / query.Step)
			if idx < 0 {
				idx = 0
			}
			if idx >= len(segments) {
				idx = len(segments) - 1
			}
			segments[idx].Count++
			if predecessor != nil {
				if sample.Value >= predecessor.Value {
					segments[idx].Increment += sample.Value - predecessor.Value
				} else {
					// A decrease is a reset: the current value is the whole
					// increment.
					segments[idx].Increment += sample.Value
				}
			}
			pred := sample
			predecessor = &pred
		}

		for i := range segments {
			if math.IsInf(segments[i].Increment, 0) || math.IsNaN(segments[i].Increment) {
				return AggregateResult{}, ErrNonFinite
			}
		}

		totalSeriesSegments += int(segCount)
		if totalSeriesSegments > maxSeriesSegments {
			return AggregateResult{}, ErrTooManySeriesSegments
		}

		result.Series = append(result.Series, SeriesResult{
			Service:  group.service,
			Labels:   group.labels,
			Segments: segments,
		})
	}

	// Deterministic order: by service, then by canonical labels.
	sort.SliceStable(result.Series, func(i, j int) bool {
		if result.Series[i].Service != result.Series[j].Service {
			return result.Series[i].Service < result.Series[j].Service
		}
		return canonicalLabels(result.Series[i].Labels) < canonicalLabels(result.Series[j].Labels)
	})
	return result, nil
}

// canonicalLabels renders a label set as a sorted key=value string for
// ordering.
func canonicalLabels(labels map[string]string) string {
	names := make([]string, 0, len(labels))
	for name := range labels {
		names = append(names, name)
	}
	sort.Strings(names)
	parts := make([]string, len(names))
	for i, name := range names {
		parts[i] = fmt.Sprintf("%s=%s", name, labels[name])
	}
	return strings.Join(parts, "\x00")
}
