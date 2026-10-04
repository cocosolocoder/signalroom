// Package metrics holds cumulative metric samples: their normalized content,
// batch ingestion with de-duplication, and the delta aggregation over aligned
// segments. Sample ids are independent of event ids.
package metrics

import (
	"errors"
	"maps"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cocosolocoder/signalroom/internal/events"
)

// ValidationError reports that a sample failed normalization (or, during
// aggregation, that a query was invalid).
type ValidationError struct{ Reason string }

func (e *ValidationError) Error() string { return e.Reason }

// IsValidationError reports whether err is a metrics validation failure.
func IsValidationError(err error) bool {
	var target *ValidationError
	return errors.As(err, &target)
}

// ConflictError reports an id clash with different content, a repeated id in
// one batch, or a series/time clash between different ids.
type ConflictError struct{ Reason string }

func (e *ConflictError) Error() string { return e.Reason }

// IsConflictError reports whether err is a metrics conflict.
func IsConflictError(err error) bool {
	var target *ConflictError
	return errors.As(err, &target)
}

// Sample is one normalized cumulative counter reading. Labels carries the
// optional trimmed label set and is nil when the sample has none.
type Sample struct {
	ID      string
	Service string
	Name    string
	At      time.Time
	Value   float64
	Labels  map[string]string
}

// Normalize applies the ingestion rules: trim surrounding whitespace from id,
// service, and metric name (their case is kept) and require each non-empty
// afterwards; require a non-zero instant; require a non-negative, finite
// value; and trim/validate labels with the same rules events use.
func Normalize(s Sample) (Sample, error) {
	s.ID = strings.TrimSpace(s.ID)
	s.Service = strings.TrimSpace(s.Service)
	s.Name = strings.TrimSpace(s.Name)
	if s.ID == "" || s.Service == "" || s.Name == "" || s.At.IsZero() {
		return Sample{}, &ValidationError{Reason: "sample id, service, name, and timestamp are required"}
	}
	if !finiteNonNegative(s.Value) {
		return Sample{}, &ValidationError{Reason: "value must be a non-negative, finite JSON number"}
	}
	labels, err := events.NormalizeLabels(s.Labels)
	if err != nil {
		return Sample{}, &ValidationError{Reason: err.Error()}
	}
	s.Labels = labels
	return s, nil
}

// finiteNonNegative rejects NaN, infinities (which JSON cannot carry anyway),
// and negative readings.
func finiteNonNegative(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0) && v >= 0
}

// seriesKey identifies one time series: a metric name within a service plus
// the complete label set. Names are compared case-sensitively after trimming.
type seriesKey struct {
	service string
	name    string
	labels  string
}

// instantKey identifies one absolute instant without depending on
// time.Time's wall fields (which differ across timezone representations and
// therefore make time.Time unsafe as a map key) or UnixNano, whose int64
// nanoseconds overflow around the year 2262. Splitting the Unix epoch into
// seconds and a [0, 1e9) nanosecond offset keeps full nanosecond precision
// over the whole range time.Parse(RFC3339Nano, ...) accepts.
type instantKey struct {
	sec  int64
	nsec int32
}

// keyForInstant renders any instant, including ones outside the UnixNano
// int64 range, as an overflow-free, timezone-independent identity.
func keyForInstant(at time.Time) instantKey {
	return instantKey{sec: at.Unix(), nsec: int32(at.Nanosecond())}
}

// BeforeCommit persists a batch of brand-new samples. It runs while the store
// lock is held and before the batch becomes visible, so a successful return
// guarantees the batch is durable and committed atomically.
type BeforeCommit func(persisted []Sample) error

// Store is a concurrency-safe set of metric samples grouped into series. It
// accepts whole batches and reports retries of already-stored samples.
type Store struct {
	mu     sync.RWMutex
	byID   map[string]Sample
	series map[seriesKey]*seriesState
}

// NewStore returns an empty store.
func NewStore() *Store {
	return &Store{
		byID:   make(map[string]Sample),
		series: make(map[seriesKey]*seriesState),
	}
}

// IngestResult counts how a batch was applied: Created samples are new,
// Replayed samples matched an existing sample with the same id and content.
type IngestResult struct {
	Created  int
	Replayed int
}

// Ingest normalizes and validates every sample first, then checks the whole
// batch against stored samples and itself. Validation failures produce a
// ValidationError; duplicate ids with differing content, a repeated id within
// the batch, or a (series, time) clash between different ids produce a
// ConflictError. Nothing is stored when an error is returned.
//
// New samples are passed to beforeCommit for durable storage; only after it
// succeeds does the batch become visible, so concurrent readers see either the
// complete batch or none of it, and concurrent batches serialize so the whole
// batch either commits or fails.
func (s *Store) Ingest(batch []Sample, beforeCommit BeforeCommit) (IngestResult, error) {
	normalized := make([]Sample, len(batch))
	for i, sample := range batch {
		norm, err := Normalize(sample)
		if err != nil {
			return IngestResult{}, err
		}
		normalized[i] = norm
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	pendingID := make(map[string]struct{}, len(normalized))
	pendingPoint := make(map[seriesKey]map[instantKey]string, len(normalized))
	var created []Sample
	var result IngestResult
	for _, sample := range normalized {
		if _, repeated := pendingID[sample.ID]; repeated {
			return IngestResult{}, &ConflictError{Reason: "sample id repeats within batch"}
		}
		pendingID[sample.ID] = struct{}{}

		key := keyFor(sample)
		if existing, ok := s.byID[sample.ID]; ok {
			if !sameContent(existing, sample) {
				return IngestResult{}, &ConflictError{Reason: "sample id already exists with different content"}
			}
			// An id retry must never mask another id's point.
			if owner := s.series[key].pointOwner(sample.At); owner != "" && owner != sample.ID {
				return IngestResult{}, &ConflictError{
					Reason: "sample collides with a different sample of the same series at the same time"}
			}
			result.Replayed++
			continue
		}

		// A brand-new id must not claim a (series, time) point already stored
		// under another id.
		if owner := s.series[key].pointOwner(sample.At); owner != "" {
			return IngestResult{}, &ConflictError{
				Reason: "sample collides with a different sample of the same series at the same time"}
		}
		// Nor one introduced earlier in this same batch.
		if owners := pendingPoint[key]; owners != nil {
			if owner, dup := owners[keyForInstant(sample.At)]; dup && owner != sample.ID {
				return IngestResult{}, &ConflictError{
					Reason: "sample collides with a different sample of the same series at the same time"}
			}
		}
		created = append(created, sample)
		result.Created++
		if pendingPoint[key] == nil {
			pendingPoint[key] = make(map[instantKey]string)
		}
		pendingPoint[key][keyForInstant(sample.At)] = sample.ID
	}

	if len(created) > 0 {
		if beforeCommit != nil {
			if err := beforeCommit(created); err != nil {
				return IngestResult{}, err
			}
		}
		for _, sample := range created {
			s.byID[sample.ID] = sample
			key := keyFor(sample)
			state := s.series[key]
			if state == nil {
				state = &seriesState{labels: sample.Labels}
				s.series[key] = state
			}
			state.add(sample)
		}
	}
	return result, nil
}

// Load inserts samples recovered from durable storage during startup. Every
// sample must already be normalized; a contradictory record is reported as a
// conflict so the caller can fail rather than mutate data.
func (s *Store) Load(sample Sample) error {
	if _, err := Normalize(sample); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := keyFor(sample)
	if existing, ok := s.byID[sample.ID]; ok {
		if !sameContent(existing, sample) {
			return &ConflictError{Reason: "sample id already exists with different content"}
		}
		return nil
	}
	if owner := s.series[key].pointOwner(sample.At); owner != "" {
		return &ConflictError{Reason: "sample collides with a different sample of the same series at the same time"}
	}
	s.byID[sample.ID] = sample
	state := s.series[key]
	if state == nil {
		state = &seriesState{labels: sample.Labels}
		s.series[key] = state
	}
	state.add(sample)
	return nil
}

// keyFor builds the identity of a series from a normalized sample.
func keyFor(sample Sample) seriesKey {
	return seriesKey{service: sample.Service, name: sample.Name, labels: labelsKey(sample.Labels)}
}

// labelsKey renders a label set as a collision-free canonical string so two
// maps with the same complete set share a series regardless of iteration
// order. Name and value are each length-prefixed, which keeps values
// containing '=' or ';' from ever aliasing another set.
func labelsKey(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for name := range labels {
		keys = append(keys, name)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, name := range keys {
		value := labels[name]
		b.WriteString(strconv.Itoa(len(name)))
		b.WriteByte(':')
		b.WriteString(name)
		b.WriteByte(',')
		b.WriteString(strconv.Itoa(len(value)))
		b.WriteByte(':')
		b.WriteString(value)
		b.WriteByte(';')
	}
	return b.String()
}

// seriesState is one sorted run of (time, value, id) points sharing one
// label set.
type seriesState struct {
	labels map[string]string
	points []point
}

type point struct {
	at    time.Time
	value float64
	id    string
}

// add inserts one point keeping the run ordered by time. Points at the same
// instant for one series are rejected upstream.
func (st *seriesState) add(sample Sample) {
	p := point{at: sample.At, value: sample.Value, id: sample.ID}
	idx := sort.Search(len(st.points), func(i int) bool {
		return st.points[i].at.After(p.at)
	})
	st.points = append(st.points, point{})
	copy(st.points[idx+1:], st.points[idx:])
	st.points[idx] = p
}

// pointOwner returns the id of the sample at exactly at, or "" when the point
// is absent. A nil receiver reports no owner.
func (st *seriesState) pointOwner(at time.Time) string {
	if st == nil {
		return ""
	}
	idx := sort.Search(len(st.points), func(i int) bool {
		return !st.points[i].at.Before(at)
	})
	if idx < len(st.points) && st.points[idx].at.Equal(at) {
		return st.points[idx].id
	}
	return ""
}

// sameContent compares two normalized samples on their absolute instant,
// value, and label set. Numeric equality makes 1 and 1.0 identical; map
// comparison makes label key order irrelevant.
func sameContent(a, b Sample) bool {
	return a.ID == b.ID &&
		a.Service == b.Service &&
		a.Name == b.Name &&
		a.At.Equal(b.At) &&
		a.Value == b.Value &&
		maps.Equal(a.Labels, b.Labels)
}
