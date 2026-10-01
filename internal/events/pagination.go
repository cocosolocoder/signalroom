package events

import (
	"errors"
	"strings"
	"time"
)

// Cursor problems a resumed page read can hit. Both indicate a cursor that
// did not originate from this timeline's data directory; the HTTP layer
// answers either one with 400.
var (
	// ErrCursorInvalid reports a malformed cursor position.
	ErrCursorInvalid = errors.New("invalid pagination cursor")
	// ErrCursorEventMissing reports a cursor that names an unknown event.
	// Ids are never deleted, so in practice this is a foreign cursor.
	ErrCursorEventMissing = errors.New("pagination cursor references an unknown event")
)

// MaxPageLimit bounds a single /events/page request.
const MaxPageLimit = 1000

// PreparedQuery is a Query whose service and severity have been trimmed and
// lowercased and whose label conditions have already been normalized by
// ParseLabelConditions. Time bounds are absolute instants, so two time zones
// naming the same moment produce equal prepared queries.
type PreparedQuery struct {
	Service  string
	Severity string
	Since    time.Time
	Until    time.Time
	Labels   map[string]string
}

// PrepareQuery normalizes the scalar filters of query the same way Query does
// internally. Labels are taken as already normalized; pass the result of
// ParseLabelConditions there.
func PrepareQuery(query Query) PreparedQuery {
	return PreparedQuery{
		Service:  strings.TrimSpace(query.Service),
		Severity: strings.ToLower(strings.TrimSpace(query.Severity)),
		Since:    query.Since,
		Until:    query.Until,
		Labels:   query.Labels,
	}
}

// EqualTo reports whether two prepared queries carry identical normalized
// filters: same service, severity, inclusive time bounds, and label set. Map
// comparison makes original label order and surrounding whitespace irrelevant.
func (q PreparedQuery) EqualTo(other PreparedQuery) bool {
	if q.Service != other.Service || q.Severity != other.Severity {
		return false
	}
	if !q.Since.Equal(other.Since) || !q.Until.Equal(other.Until) {
		return false
	}
	return labelsEqual(q.Labels, other.Labels)
}

// labelsEqual treats nil and an empty map as the same empty condition set.
func labelsEqual(a, b map[string]string) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	if len(a) != len(b) {
		return false
	}
	for name, value := range a {
		if b[name] != value {
			return false
		}
	}
	return true
}

// Snapshot is an immutable read view of the timeline as of one moment. Every
// page derived from the same snapshot reads the same committed event set:
// batches committed before the snapshot was taken are present in full, later
// batches are absent in full. Snapshots are safe for concurrent use and do
// not block ingestion.
type Snapshot struct {
	events []Event // backing array is never mutated after publication
	query  PreparedQuery
}

// SnapshotRead captures the events matching query in one atomic read of the
// timeline and returns them as an immutable Snapshot ordered by time then id,
// exactly like Query. A batch committed while the read runs is either wholly
// inside the snapshot or wholly outside it.
func (t *Timeline) SnapshotRead(query PreparedQuery) *Snapshot {
	t.mu.RLock()
	defer t.mu.RUnlock()
	matched := make([]Event, 0, len(t.events))
	for _, event := range t.events {
		if eventMatches(event, query) {
			matched = append(matched, event)
		}
	}
	return &Snapshot{events: matched, query: query}
}

// Count reports how many matching events the snapshot holds.
func (s *Snapshot) Count() int {
	return len(s.events)
}

// Query returns the normalized filter the snapshot was taken with.
func (s *Snapshot) Query() PreparedQuery {
	return s.query
}

// IDs returns the snapshot's event ids in snapshot order. The slice is a
// fresh copy and the complete identity of the frozen set; a durable cursor
// stores it so the same set can be rebuilt after a restart.
func (s *Snapshot) IDs() []string {
	ids := make([]string, len(s.events))
	for i, event := range s.events {
		ids[i] = event.ID
	}
	return ids
}

// Page returns up to limit events starting at offset in snapshot order,
// together with the offset immediately following the returned slice. It fails
// for a negative or out-of-range offset or a non-positive limit.
func (s *Snapshot) Page(offset, limit int) (events []Event, nextOffset int, ok bool) {
	if offset < 0 || offset > len(s.events) || limit <= 0 {
		return nil, 0, false
	}
	end := offset + limit
	if end > len(s.events) || end < offset {
		end = len(s.events)
	}
	return s.events[offset:end], end, true
}

// ResumePage rebuilds one page of a previously frozen result set without
// consulting the current timeline contents: only events named by ids, in
// exactly that order, can appear, so events ingested after the set was frozen
// never intrude even when their time, filter match, or id would place them in
// the middle. Every named id must still exist (ids are never deleted); a
// missing one means the cursor came from another data directory.
//
// offset is measured within ids rather than in the live timeline. A negative
// offset, an offset past the end, or a non-positive limit is ErrCursorInvalid;
// an unknown id is ErrCursorEventMissing.
func (t *Timeline) ResumePage(ids []string, offset, limit int) (events []Event, nextOffset int, err error) {
	if offset < 0 || offset > len(ids) || limit <= 0 {
		return nil, 0, ErrCursorInvalid
	}
	end := offset + limit
	if end > len(ids) || end < offset {
		end = len(ids)
	}

	t.mu.RLock()
	defer t.mu.RUnlock()
	page := make([]Event, 0, end-offset)
	for i := offset; i < end; i++ {
		event, ok := t.byID[ids[i]]
		if !ok {
			return nil, 0, ErrCursorEventMissing
		}
		page = append(page, event)
	}
	return page, end, nil
}

// eventMatches applies one normalized prepared query to one event.
func eventMatches(event Event, query PreparedQuery) bool {
	if query.Service != "" && event.Service != query.Service {
		return false
	}
	if query.Severity != "" && event.Severity != query.Severity {
		return false
	}
	if !query.Since.IsZero() && event.At.Before(query.Since) {
		return false
	}
	if !query.Until.IsZero() && event.At.After(query.Until) {
		return false
	}
	if !labelsMatch(event.Labels, query.Labels) {
		return false
	}
	return true
}
