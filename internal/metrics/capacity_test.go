package metrics

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// ampSample is a one-sample batch candidate whose id ends in amps ampersands.
// The save format (encoding/json) HTML-escapes '&' as the six-byte sequence
// '&', so a modest request body can save far larger than it arrived —
// the exact condition the capacity guard exists to classify. The id (rather
// than a label value, which labels cap at 256 runes) carries the run so no
// other ingestion limit interferes.
func ampSample(prefix string, amps int, at time.Time) Sample {
	return Sample{
		ID:      prefix + strings.Repeat("&", amps),
		Service: "gw",
		Name:    "requests",
		At:      at,
		Value:   1,
	}
}

// ampEncoding describes MarshalBatch's encoding of ampSample:
// encoded length == overhead + width*amps. The width is measured rather than
// assumed so the test follows the existing save format exactly.
func ampEncoding(t *testing.T, prefix string, at time.Time) (overhead, width int) {
	t.Helper()
	empty, err := MarshalBatch([]Sample{ampSample(prefix, 0, at)})
	if err != nil {
		t.Fatalf("marshal probe: %v", err)
	}
	full, err := MarshalBatch([]Sample{ampSample(prefix, 1000, at)})
	if err != nil {
		t.Fatalf("marshal wide probe: %v", err)
	}
	width = (len(full) - len(empty)) / 1000
	if width <= 1 {
		t.Fatalf("test setup: escaping must expand each '&', got width %d", width)
	}
	return len(empty), width
}

func overheadForPrefix(t *testing.T, prefix string, at time.Time) int {
	t.Helper()
	empty, err := MarshalBatch([]Sample{ampSample(prefix, 0, at)})
	if err != nil {
		t.Fatalf("marshal probe: %v", err)
	}
	return len(empty)
}

// TestIngestCapacityExactLimitAccepted proves the boundary is inclusive: a
// batch encoding to exactly MaxBatchPayloadBytes is saved, while one extra
// escaped character makes it an OversizeBatchError.
func TestIngestCapacityExactLimitAccepted(t *testing.T) {
	s := NewStore()
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	prefix := "exact-limit"
	overhead, width := ampEncoding(t, prefix, base)

	// Pad the fixed id prefix (one encoded byte per character) until the
	// overhead makes the limit reachable exactly with whole '&' runs.
	for overhead%width != MaxBatchPayloadBytes%width {
		prefix += "x"
		overhead = overheadForPrefix(t, prefix, base)
	}
	amps := (MaxBatchPayloadBytes - overhead) / width
	if amps <= 0 {
		t.Fatalf("test setup: overhead %d leaves no room", overhead)
	}

	atLimit := ampSample(prefix, amps, base)
	encoded, err := MarshalBatch([]Sample{atLimit})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if len(encoded) != MaxBatchPayloadBytes {
		t.Fatalf("setup: encoded %d bytes, want %d", len(encoded), MaxBatchPayloadBytes)
	}

	committed := false
	res, err := s.Ingest([]Sample{atLimit}, func(created []Sample) error {
		committed = true
		return nil
	})
	if err != nil {
		t.Fatalf("batch at exactly the limit must be accepted: %v", err)
	}
	if !committed || res.Created != 1 {
		t.Fatalf("exact-limit batch not committed: %+v committed=%v", res, committed)
	}

	// One more '&' pushes the saved content past the limit. It must claim a
	// different instant than the at-limit sample, which already owns this
	// series at base.
	over := ampSample("over-limit", amps+1, base.Add(time.Second))
	encoded, _ = MarshalBatch([]Sample{over})
	if len(encoded) <= MaxBatchPayloadBytes {
		t.Fatalf("setup: over batch encodes to %d", len(encoded))
	}
	var oversized *OversizeBatchError
	_, err = s.Ingest([]Sample{over}, func(created []Sample) error {
		t.Fatalf("oversize batch must not reach the commit hook")
		return nil
	})
	if !errors.As(err, &oversized) {
		t.Fatalf("want OversizeBatchError, got %v", err)
	}
	if oversized.Size != len(encoded) || oversized.Limit != MaxBatchPayloadBytes {
		t.Fatalf("error size=%d limit=%d, encoded=%d", oversized.Size, oversized.Limit, len(encoded))
	}

	// The rejected id stays free: the same id in a smaller later batch
	// succeeds with no conflict and no restart.
	small := ampSample("over-limit", 4, base.Add(2*time.Second))
	res, err = s.Ingest([]Sample{small}, nil)
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
	s := NewStore()
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	overhead, width := ampEncoding(t, "big", base)
	amps := (MaxBatchPayloadBytes-overhead)/width + 1
	big := ampSample("big", amps, base)

	if len(big.ID) >= MaxBatchPayloadBytes {
		t.Fatalf("setup: raw id already %d bytes", len(big.ID))
	}
	encoded, err := MarshalBatch([]Sample{big})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if len(encoded) <= MaxBatchPayloadBytes {
		t.Fatalf("setup: escaping did not expand past the limit: %d", len(encoded))
	}

	commitCalls := 0
	_, err = s.Ingest([]Sample{big}, func(created []Sample) error {
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
	if strings.Contains(strings.ToLower(err.Error()), "restart") {
		t.Fatalf("oversize error must not mention restart, got %q", err.Error())
	}
	if strings.Contains(strings.ToLower(err.Error()), "unavailable") {
		t.Fatalf("oversize error must not claim storage is unavailable, got %q", err.Error())
	}

	// Storage stays usable without any restart, and the rejected id is free.
	if _, err := s.Ingest([]Sample{
		{ID: "ok", Service: "gw", Name: "requests", At: base.Add(time.Second), Value: 1},
	}, nil); err != nil {
		t.Fatalf("store must remain usable after oversize rejection: %v", err)
	}
	if _, err := s.Ingest([]Sample{ampSample("big", 4, base.Add(2*time.Second))}, nil); err != nil {
		t.Fatalf("rejected id must remain usable: %v", err)
	}
}

// TestIngestCapacityOnlyNewEventsCount checks mixed replay/new batches: the
// save capacity measures only what would be newly written. A replay whose
// saved representation is huge does not count against the new samples, while
// an oversized new sample is still rejected even alongside a small replay.
func TestIngestCapacityOnlyNewEventsCount(t *testing.T) {
	s := NewStore()
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	overhead, width := ampEncoding(t, "big", base)

	// Seed a sample whose saved form is roughly 62 MiB.
	bigAmps := (62 << 20) / width
	bigSaved := overhead + width*bigAmps
	if bigSaved >= MaxBatchPayloadBytes {
		t.Fatalf("setup: seed payload %d must fit", bigSaved)
	}
	big := ampSample("big", bigAmps, base)
	if _, err := s.Ingest([]Sample{big}, nil); err != nil {
		t.Fatalf("seed big sample: %v", err)
	}

	// A retry of the ~62 MiB sample plus ~5 MiB of brand-new content encodes
	// past the limit together; only the new content is saved, so it commits.
	fresh := ampSample("fresh", (5<<20)/width, base.Add(time.Second))
	newOnly, _ := MarshalBatch([]Sample{fresh})
	if len(newOnly) >= MaxBatchPayloadBytes {
		t.Fatalf("setup: new sample alone must fit, got %d", len(newOnly))
	}
	combined, _ := MarshalBatch([]Sample{big, fresh})
	if len(combined) <= MaxBatchPayloadBytes {
		t.Fatalf("setup: combined payload %d must exceed the limit", len(combined))
	}
	res, err := s.Ingest([]Sample{big, fresh}, func(created []Sample) error {
		if len(created) != 1 || created[0].ID != fresh.ID {
			t.Fatalf("commit hook must see only the new sample, got %+v", created)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("replay must not count toward capacity: %v", err)
	}
	if res.Created != 1 || res.Replayed != 1 {
		t.Fatalf("mixed batch counts: %+v", res)
	}

	// A small replay accompanying an oversized new sample cannot save it.
	overAmps := (MaxBatchPayloadBytes-overhead)/width + 1
	overNew := ampSample("over-new", overAmps, base.Add(2*time.Second))
	_, err = s.Ingest([]Sample{big, overNew}, func(created []Sample) error {
		t.Fatal("oversize mixed batch must not commit")
		return nil
	})
	if !IsOversizeBatchError(err) {
		t.Fatalf("want oversize error for new content, got %v", err)
	}
	// The rejected id remains free and the rejected point was not claimed.
	if _, err := s.Ingest([]Sample{ampSample("over-new", 4, base.Add(3*time.Second))}, nil); err != nil {
		t.Fatalf("rejected id must remain usable: %v", err)
	}

	// An all-replay batch, however large its content, never saves and always
	// reports created zero with the replay count.
	res, err = s.Ingest([]Sample{big}, func(created []Sample) error {
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
// error precedence: field/value validation and every flavor of conflict are
// reported ahead of the capacity check.
func TestIngestCapacityChecksAfterValidationAndConflicts(t *testing.T) {
	s := NewStore()
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	overhead, width := ampEncoding(t, "huge", base)
	overAmps := (MaxBatchPayloadBytes-overhead)/width + 1
	huge := func(id string, at time.Time) Sample { return ampSample(id, overAmps, at) }

	// One stored point that later batches can clash against.
	seed := Sample{ID: "dup", Service: "gw", Name: "requests", At: base, Value: 1}
	if _, err := s.Ingest([]Sample{seed}, nil); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Invalid sample ordered before the oversized one: validation wins.
	invalid := Sample{ID: "bad", Service: "  ", Name: "requests", At: base.Add(time.Second), Value: 1}
	if _, err := s.Ingest([]Sample{invalid, huge("huge", base.Add(2*time.Second))}, nil); !IsValidationError(err) {
		t.Fatalf("validation must precede capacity, got %v", err)
	}

	// Same id, different content: conflict wins.
	conflict := Sample{ID: "dup", Service: "gw", Name: "requests", At: base, Value: 99}
	if _, err := s.Ingest([]Sample{conflict, huge("huge2", base.Add(2*time.Second))}, nil); !IsConflictError(err) {
		t.Fatalf("id-content conflict must precede capacity, got %v", err)
	}

	// Different id occupying the stored sample's series/instant: conflict wins.
	intruder := Sample{ID: "intruder", Service: "gw", Name: "requests", At: base, Value: 2}
	if _, err := s.Ingest([]Sample{intruder, huge("huge3", base.Add(2*time.Second))}, nil); !IsConflictError(err) {
		t.Fatalf("series/time clash must precede capacity, got %v", err)
	}

	// A repeated id within the batch: conflict wins.
	dupInBatch := []Sample{
		{ID: "twice", Service: "gw", Name: "requests", At: base.Add(4 * time.Second), Value: 1},
		{ID: "twice", Service: "gw", Name: "requests", At: base.Add(5 * time.Second), Value: 2},
		huge("huge4", base.Add(6*time.Second)),
	}
	if _, err := s.Ingest(dupInBatch, nil); !IsConflictError(err) {
		t.Fatalf("in-batch duplicate id must precede capacity, got %v", err)
	}

	// The same large content on its own is finally classified as oversize,
	// proving the earlier batches really were large enough to trip it.
	if _, err := s.Ingest([]Sample{huge("solo", base.Add(7*time.Second))}, nil); !IsOversizeBatchError(err) {
		t.Fatalf("oversize batch must be classified as such alone, got %v", err)
	}
}
