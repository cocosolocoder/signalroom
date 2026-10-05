package events

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// ampEvent is a one-event batch candidate whose message is n ampersands. The
// save format (encoding/json) HTML-escapes '&' as the six-byte sequence
// '&', so a modest request body can save far larger than it arrived —
// the exact condition the capacity guard exists to classify.
func ampEvent(id string, amps int, at time.Time) Event {
	return Event{
		ID:       id,
		Service:  "svc",
		Severity: "info",
		Message:  strings.Repeat("&", amps),
		At:       at,
	}
}

// ampOverhead and ampWidth describe MarshalBatch's encoding of ampEvent:
// encoded length == overhead + width*amps. The width is measured rather than
// assumed so the test follows the existing save format exactly.
func ampEncoding(t *testing.T, at time.Time) (overhead, width int) {
	t.Helper()
	empty, err := MarshalBatch([]Event{ampEvent("probe", 0, at)})
	if err != nil {
		t.Fatalf("marshal probe: %v", err)
	}
	full, err := MarshalBatch([]Event{ampEvent("probe", 1000, at)})
	if err != nil {
		t.Fatalf("marshal wide probe: %v", err)
	}
	width = (len(full) - len(empty)) / 1000
	if width <= 1 {
		t.Fatalf("test setup: escaping must expand each '&', got width %d", width)
	}
	return len(empty), width
}

func overheadForID(t *testing.T, id string, at time.Time) int {
	t.Helper()
	empty, err := MarshalBatch([]Event{ampEvent(id, 0, at)})
	if err != nil {
		t.Fatalf("marshal probe: %v", err)
	}
	return len(empty)
}

// TestIngestCapacityExactLimitAccepted proves the boundary is inclusive: a
// batch encoding to exactly MaxBatchPayloadBytes is saved, while one extra
// escaped character makes it an OversizeBatchError.
func TestIngestCapacityExactLimitAccepted(t *testing.T) {
	tl := NewTimeline()
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	overhead, width := ampEncoding(t, base)

	// Pad the id (one encoded byte per character) until the fixed overhead
	// makes the limit reachable exactly with whole '&' runs.
	id := "exact-limit"
	for overhead%width != MaxBatchPayloadBytes%width {
		id += "x"
		overhead = overheadForID(t, id, base)
	}
	amps := (MaxBatchPayloadBytes - overhead) / width
	if amps <= 0 {
		t.Fatalf("test setup: overhead %d leaves no room", overhead)
	}

	atLimit := ampEvent(id, amps, base)
	encoded, err := MarshalBatch([]Event{atLimit})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if len(encoded) != MaxBatchPayloadBytes {
		t.Fatalf("setup: encoded %d bytes, want %d", len(encoded), MaxBatchPayloadBytes)
	}

	committed := false
	res, err := tl.Ingest([]Event{atLimit}, func(created []Event) error {
		committed = true
		return nil
	})
	if err != nil {
		t.Fatalf("batch at exactly the limit must be accepted: %v", err)
	}
	if !committed || res.Created != 1 {
		t.Fatalf("exact-limit batch not committed: %+v committed=%v", res, committed)
	}

	// One more '&' pushes the saved content past the limit.
	over := ampEvent("over-limit", amps+1, base)
	encoded, _ = MarshalBatch([]Event{over})
	if len(encoded) <= MaxBatchPayloadBytes {
		t.Fatalf("setup: over batch encodes to %d", len(encoded))
	}
	var oversized *OversizeBatchError
	_, err = tl.Ingest([]Event{over}, func(created []Event) error {
		t.Fatalf("oversize batch must not reach the commit hook")
		return nil
	})
	if !errors.As(err, &oversized) {
		t.Fatalf("want OversizeBatchError, got %v", err)
	}
	if oversized.Size != len(encoded) || oversized.Limit != MaxBatchPayloadBytes {
		t.Fatalf("error size=%d limit=%d, encoded=%d", oversized.Size, oversized.Limit, len(encoded))
	}
	if got := tl.Query(Query{}); len(got) != 1 {
		t.Fatalf("rejected event must not be visible, got %d", len(got))
	}

	// The rejected id stays free: the same id in a smaller later batch
	// succeeds with no conflict and no restart.
	small := ampEvent("over-limit", 4, base.Add(time.Second))
	res, err = tl.Ingest([]Event{small}, nil)
	if err != nil {
		t.Fatalf("rejected id must remain usable: %v", err)
	}
	if res.Created != 1 {
		t.Fatalf("later small batch: %+v", res)
	}
}

// TestIngestCapacityEscapingExpandsContent is the reported bug in miniature:
// a request whose raw content is modest saves far larger because its '&'
// characters are escaped in the save format. The rejection is a content
// error, not a storage failure.
func TestIngestCapacityEscapingExpandsContent(t *testing.T) {
	tl := NewTimeline()
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	overhead, width := ampEncoding(t, base)
	amps := (MaxBatchPayloadBytes-overhead)/width + 1
	big := ampEvent("big", amps, base)

	if len(big.Message) >= MaxBatchPayloadBytes {
		t.Fatalf("setup: raw message already %d bytes", len(big.Message))
	}
	encoded, err := MarshalBatch([]Event{big})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if len(encoded) <= MaxBatchPayloadBytes {
		t.Fatalf("setup: escaping did not expand past the limit: %d", len(encoded))
	}

	commitCalls := 0
	_, err = tl.Ingest([]Event{big}, func(created []Event) error {
		commitCalls++
		return errors.New("must not be called")
	})
	if !IsOversizeBatchError(err) {
		t.Fatalf("want oversize error, got %v", err)
	}
	if commitCalls != 0 {
		t.Fatalf("commit hook ran %d times", commitCalls)
	}
	if !strings.Contains(err.Error(), "save capacity") {
		t.Fatalf("error must explain the save capacity, got %q", err.Error())
	}
	if strings.Contains(err.Error(), "restart") {
		t.Fatalf("oversize error must not mention restart, got %q", err.Error())
	}
	if got := tl.Query(Query{}); len(got) != 0 {
		t.Fatalf("nothing must be visible after rejection")
	}

	// Storage stays usable without any restart.
	if _, err := tl.Ingest([]Event{mkEvent("ok", "s", "info", "fine", base)}, nil); err != nil {
		t.Fatalf("store must remain usable after oversize rejection: %v", err)
	}
}

// TestIngestCapacityOnlyNewEventsCount checks mixed replay/new batches: the
// save capacity measures only what would be newly written. A replay whose
// saved representation is huge does not count against the new events, while
// an oversized new event is still rejected even when accompanied by a small
// replay.
func TestIngestCapacityOnlyNewEventsCount(t *testing.T) {
	tl := NewTimeline()
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	overhead, width := ampEncoding(t, base)

	// Seed an event whose saved form is roughly 62 MiB.
	bigAmps := (62 << 20) / width
	bigSaved := overhead + width*bigAmps
	if bigSaved >= MaxBatchPayloadBytes {
		t.Fatalf("setup: seed payload %d must fit", bigSaved)
	}
	big := ampEvent("big-stored", bigAmps, base)
	if _, err := tl.Ingest([]Event{big}, nil); err != nil {
		t.Fatalf("seed big event: %v", err)
	}

	// A retry of the ~62 MiB event plus ~5 MiB of brand-new content encodes
	// past the limit together; only the new content is saved, so it commits.
	newEvent := ampEvent("new", (5<<20)/width, base.Add(time.Second))
	newOnly, _ := MarshalBatch([]Event{newEvent})
	if len(newOnly) >= MaxBatchPayloadBytes {
		t.Fatalf("setup: new event alone must fit, got %d", len(newOnly))
	}
	combined, _ := MarshalBatch([]Event{big, newEvent})
	if len(combined) <= MaxBatchPayloadBytes {
		t.Fatalf("setup: combined payload %d must exceed the limit", len(combined))
	}
	res, err := tl.Ingest([]Event{big, newEvent}, func(created []Event) error {
		if len(created) != 1 || created[0].ID != "new" {
			t.Fatalf("commit hook must see only the new event, got %+v", created)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("replay must not count toward capacity: %v", err)
	}
	if res.Created != 1 || res.Replayed != 1 {
		t.Fatalf("mixed batch counts: %+v", res)
	}

	// A small replay accompanying an oversized new event cannot save it.
	small := mkEvent("small", "s", "info", "m", base.Add(2*time.Second))
	if _, err := tl.Ingest([]Event{small}, nil); err != nil {
		t.Fatalf("seed small event: %v", err)
	}
	overAmps := (MaxBatchPayloadBytes-overhead)/width + 1
	overNew := ampEvent("over-new", overAmps, base.Add(3*time.Second))
	_, err = tl.Ingest([]Event{small, overNew}, func(created []Event) error {
		t.Fatal("oversize mixed batch must not commit")
		return nil
	})
	if !IsOversizeBatchError(err) {
		t.Fatalf("want oversize error for new content, got %v", err)
	}
	if got := tl.Query(Query{}); len(got) != 3 {
		t.Fatalf("rejected mixed batch must add nothing, got %d events", len(got))
	}

	// An all-replay batch, however large its content, never saves and always
	// reports the original created/replayed result.
	res, err = tl.Ingest([]Event{big}, func(created []Event) error {
		t.Fatalf("pure replay must not save, got %+v", created)
		return nil
	})
	if err != nil {
		t.Fatalf("pure replay: %v", err)
	}
	if res.Created != 0 || res.Replayed != 1 {
		t.Fatalf("pure replay counts: %+v", res)
	}
}

// TestIngestCapacityChecksAfterValidationAndConflicts preserves the existing
// error precedence: validation failures and id conflicts are both reported
// ahead of the capacity check.
func TestIngestCapacityChecksAfterValidationAndConflicts(t *testing.T) {
	tl := NewTimeline()
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	overhead, width := ampEncoding(t, base)
	overAmps := (MaxBatchPayloadBytes-overhead)/width + 1

	seed := mkEvent("dup", "s", "info", "old", base)
	if _, err := tl.Ingest([]Event{seed}, nil); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Invalid event ordered before the oversized one: validation wins.
	invalid := mkEvent("bad", "s", "info", "   ", base.Add(time.Second))
	_, err := tl.Ingest([]Event{invalid, ampEvent("huge", overAmps, base.Add(2*time.Second))}, nil)
	if !IsValidationError(err) {
		t.Fatalf("validation must precede capacity, got %v", err)
	}

	// Conflicting id ordered before the oversized event: conflict wins.
	conflict := mkEvent("dup", "s", "info", "different", base)
	_, err = tl.Ingest([]Event{conflict, ampEvent("huge2", overAmps, base.Add(2*time.Second))}, nil)
	if !IsConflictError(err) {
		t.Fatalf("conflict must precede capacity, got %v", err)
	}
	if got := tl.Query(Query{}); len(got) != 1 || got[0].Message != "old" {
		t.Fatalf("precedence rejections must not change data: %+v", got)
	}
}
