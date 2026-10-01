package events

import (
	"maps"
	"sort"
	"strings"
	"sync"
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
		if !labelsMatch(event.Labels, query.Labels) {
			continue
		}
		result = append(result, event)
	}
	return result
}

// SnapshotIDs returns the ids of events matching the optional filters, in
// time-then-id order. The set is taken under the timeline lock, so a batch
// committed concurrently is either fully present or fully absent; events
// added after the call never appear in the returned set, which makes it
// suitable for stable pagination across later writes.
func (t *Timeline) SnapshotIDs(query Query) []string {
	service := strings.TrimSpace(query.Service)
	severity := strings.ToLower(strings.TrimSpace(query.Severity))

	t.mu.RLock()
	defer t.mu.RUnlock()
	ids := make([]string, 0, len(t.events))
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
		if !labelsMatch(event.Labels, query.Labels) {
			continue
		}
		ids = append(ids, event.ID)
	}
	return ids
}

// EventByID returns the event with the given id and whether it exists. The
// lookup runs under the read lock, so it is safe to call while holding
// another lock (e.g. the incident store's lock).
func (t *Timeline) EventByID(id string) (Event, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	event, ok := t.byID[id]
	return event, ok
}

// EventsByID returns the events for ids in the requested order. Every id
// must refer to a stored event; events are never removed, so a set captured
// by SnapshotIDs stays resolvable. Missing ids are skipped only when the
// caller passes ids not produced by SnapshotIDs.
func (t *Timeline) EventsByID(ids []string) []Event {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]Event, 0, len(ids))
	for _, id := range ids {
		if event, ok := t.byID[id]; ok {
			out = append(out, event)
		}
	}
	return out
}

// sameEvent compares two normalized events on their absolute instant and
// their labels. Normalized label sets are nil when empty, so the three "no
// labels" spellings compare equal, and map comparison makes key order and
// original whitespace irrelevant.
func sameEvent(a, b Event) bool {
	return a.ID == b.ID &&
		a.Service == b.Service &&
		a.Severity == b.Severity &&
		a.Message == b.Message &&
		a.At.Equal(b.At) &&
		maps.Equal(a.Labels, b.Labels)
}
