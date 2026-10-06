package metrics

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// ltSample is a valid, normalized sample whose id carries n '<' characters
// after prefix. The save format (encoding/json) HTML-escapes '<' as the
// six-byte sequence '<', so a modest request-side value saves far
// larger than it arrived — the exact condition the capacity guard exists to
// classify. The run rides in the id (which normalization trims but does not
// length-cap), and every sample stays in the (gw, requests) label-less
// series so aggregate counts observe them uniformly.
func ltSample(prefix string, n int, at time.Time) Sample {
	return Sample{
		ID:      prefix + strings.Repeat("<", n),
		Service: "gw",
		Name:    "requests",
		At:      at,
		Value:   1,
	}
}

// sampleEncoding measures MarshalBatch's encoding of ltSample: encoded
// length == overhead + width*n. The width is measured rather than assumed so
// the test follows the existing save format exactly.
func sampleEncoding(t *testing.T, at time.Time) (overhead, width int) {
	t.Helper()
	empty, err := MarshalBatch([]Sample{ltSample("probe", 0, at)})
	if err != nil {
		t.Fatalf("marshal probe: %v", err)
	}
	full, err := MarshalBatch([]Sample{ltSample("probe", 1000, at)})
	if err != nil {
		t.Fatalf("marshal wide probe: %v", err)
	}
	width = (len(full) - len(empty)) / 1000
	if width <= 1 {
		t.Fatalf("test setup: escaping must expand each '<', got width %d", width)
	}
	return len(empty), width
}

// sampleOverhead reports the encoded length of ltSample(prefix, 0, at).
func sampleOverhead(t *testing.T, prefix string, at time.Time) int {
	t.Helper()
	empty, err := MarshalBatch([]Sample{ltSample(prefix, 0, at)})
	if err != nil {
		t.Fatalf("marshal probe: %v", err)
	}
	return len(empty)
}

// ltNameSample puts the expanding run in the metric name and keeps the id
// fixed, so a rejected id can later be resubmitted with genuinely small
// content (the oversized content and the identity live in different fields).
func ltNameSample(id string, n int, at time.Time) Sample {
	return Sample{ID: id, Service: "gw", Name: strings.Repeat("<", n), At: at, Value: 1}
}

// nameEncoding reports the fixed overhead and per-'<' width of MarshalBatch's
// encoding of the name-run shape ltNameSample(id, n, at).
func nameEncoding(t *testing.T, id string, at time.Time) (overhead, width int) {
	t.Helper()
	empty, err := MarshalBatch([]Sample{ltNameSample(id, 0, at)})
	if err != nil {
		t.Fatalf("marshal probe: %v", err)
	}
	full, err := MarshalBatch([]Sample{ltNameSample(id, 1000, at)})
	if err != nil {
		t.Fatalf("marshal wide probe: %v", err)
	}
	width = (len(full) - len(empty)) / 1000
	if width <= 1 {
		t.Fatalf("test setup: escaping must expand each '<', got width %d", width)
	}
	return len(empty), width
}

// windowCount is the total sample count Aggregate reports over the label-less
// gw/requests series between base and a few seconds later.
func windowCount(t *testing.T, store *Store, base time.Time, seconds int64) int {
	t.Helper()
	count, _ := aggregateCount(t, store, "gw", "requests", nil, base, base.Add(time.Duration(seconds)*time.Second))
	return count
}

// TestIngestCapacityExactLimitAccepted proves the boundary is inclusive: a
// batch encoding to exactly MaxBatchPayloadBytes is saved, while one extra
// escaped '<' makes it an OversizeBatchError. The measurement is byte length
// in the save format, never a rune or character count.
func TestIngestCapacityExactLimitAccepted(t *testing.T) {
	store := NewStore()
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	_, width := sampleEncoding(t, base)

	// Pad the id prefix (one encoded byte per 'x') until the fixed overhead
	// makes the limit reachable exactly with a whole number of escaped '<'
	// runs. The accepted, exactly-at-limit batch stays in the requests series.
	prefix := "exact-limit"
	overhead := sampleOverhead(t, prefix, base)
	for overhead%width != MaxBatchPayloadBytes%width {
		prefix += "x"
		overhead = sampleOverhead(t, prefix, base)
	}
	lt := (MaxBatchPayloadBytes - overhead) / width
	if lt <= 0 {
		t.Fatalf("test setup: overhead %d leaves no room", overhead)
	}

	atLimit := ltSample(prefix, lt, base)
	encoded, err := MarshalBatch([]Sample{atLimit})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if len(encoded) != MaxBatchPayloadBytes {
		t.Fatalf("setup: encoded %d bytes, want %d", len(encoded), MaxBatchPayloadBytes)
	}

	committed := false
	res, err := store.Ingest([]Sample{atLimit}, func(created []Sample) error {
		committed = true
		return nil
	})
	if err != nil {
		t.Fatalf("batch at exactly the limit must be accepted: %v", err)
	}
	if !committed || res.Created != 1 {
		t.Fatalf("exact-limit batch not committed: %+v committed=%v", res, committed)
	}

	// One more '<' pushes the saved content (not the request content) past
	// the limit. This one rides in the metric name with a fixed id, so its id
	// can later be reused with small content; it lands in its own big-name
	// series.
	nameOH, nameW := nameEncoding(t, "over-limit", base.Add(time.Second))
	overLt := (MaxBatchPayloadBytes-nameOH)/nameW + 1
	bigName := strings.Repeat("<", overLt)
	over := ltNameSample("over-limit", overLt, base.Add(time.Second))
	encoded, _ = MarshalBatch([]Sample{over})
	if len(encoded) <= MaxBatchPayloadBytes {
		t.Fatalf("setup: over batch encodes to %d", len(encoded))
	}
	var oversized *OversizeBatchError
	_, err = store.Ingest([]Sample{over}, func(created []Sample) error {
		t.Fatalf("oversize batch must not reach the commit hook")
		return nil
	})
	if !errors.As(err, &oversized) {
		t.Fatalf("want OversizeBatchError, got %v", err)
	}
	if oversized.Size != len(encoded) || oversized.Limit != MaxBatchPayloadBytes {
		t.Fatalf("error size=%d limit=%d, encoded=%d", oversized.Size, oversized.Limit, len(encoded))
	}
	if count, _ := aggregateCount(t, store, "gw", bigName, nil, base, base.Add(10*time.Second)); count != 0 {
		t.Fatalf("rejected sample must not be visible, got %d", count)
	}

	// The rejected id stays free: the same id in a smaller later batch on the
	// requests series succeeds with no conflict and no restart.
	small := Sample{ID: "over-limit", Service: "gw", Name: "requests", At: base.Add(2 * time.Second), Value: 2}
	res, err = store.Ingest([]Sample{small}, nil)
	if err != nil {
		t.Fatalf("rejected id must remain usable: %v", err)
	}
	if res.Created != 1 {
		t.Fatalf("later small batch: %+v", res)
	}
}

// TestIngestCapacityEscapingExpandsContent is the reported bug in miniature:
// request content that is modest in characters saves far larger because its
// '<' characters are JSON-escaped in the save format. The rejection is a
// content error, not a storage failure.
func TestIngestCapacityEscapingExpandsContent(t *testing.T) {
	store := NewStore()
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	overhead, width := sampleEncoding(t, base)
	lt := (MaxBatchPayloadBytes-overhead)/width + 1
	big := ltSample("big", lt, base)

	// The id's character/rune count is far below the byte limit; it is the
	// escaped byte length that crosses it.
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
	_, err = store.Ingest([]Sample{big}, func(created []Sample) error {
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
	lower := strings.ToLower(err.Error())
	if strings.Contains(lower, "restart") || strings.Contains(lower, "unavailable") {
		t.Fatalf("oversize error must not hint at storage failure, got %q", err.Error())
	}
	if got := windowCount(t, store, base, 10); got != 0 {
		t.Fatalf("nothing must be visible after rejection, got %d", got)
	}

	// Storage stays usable without any restart.
	next := Sample{ID: "ok", Service: "gw", Name: "requests", At: base.Add(2 * time.Second), Value: 1}
	if _, err := store.Ingest([]Sample{next}, noopCommit); err != nil {
		t.Fatalf("store must remain usable after oversize rejection: %v", err)
	}
}

// TestIngestCapacityOnlyNewSamplesCount checks mixed replay/new batches: the
// save capacity measures only what would be newly written. A replay whose
// saved representation is huge does not count against the new samples, while
// an oversized new sample is still rejected even when accompanied by a small
// replay.
func TestIngestCapacityOnlyNewSamplesCount(t *testing.T) {
	store := NewStore()
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	_, width := sampleEncoding(t, base)

	// Seed a sample whose saved form is roughly 62 MiB.
	bigLt := (62 << 20) / width
	big := ltSample("big-stored-", bigLt, base)
	bigSaved, err := MarshalBatch([]Sample{big})
	if err != nil {
		t.Fatalf("marshal seed: %v", err)
	}
	if len(bigSaved) >= MaxBatchPayloadBytes {
		t.Fatalf("setup: seed payload %d must fit", len(bigSaved))
	}
	if _, err := store.Ingest([]Sample{big}, nil); err != nil {
		t.Fatalf("seed big sample: %v", err)
	}

	// A retry of the ~62 MiB sample plus ~5 MiB of brand-new content encodes
	// past the limit together; only the new content is saved, so it commits.
	newSample := ltSample("new-", (5<<20)/width, base.Add(time.Second))
	newOnly, _ := MarshalBatch([]Sample{newSample})
	if len(newOnly) >= MaxBatchPayloadBytes {
		t.Fatalf("setup: new sample alone must fit, got %d", len(newOnly))
	}
	combined, _ := MarshalBatch([]Sample{big, newSample})
	if len(combined) <= MaxBatchPayloadBytes {
		t.Fatalf("setup: combined payload %d must exceed the limit", len(combined))
	}
	res, err := store.Ingest([]Sample{big, newSample}, func(created []Sample) error {
		if len(created) != 1 || created[0].ID != newSample.ID {
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
	small := Sample{ID: "small", Service: "gw", Name: "requests", At: base.Add(2 * time.Second), Value: 1}
	if _, err := store.Ingest([]Sample{small}, nil); err != nil {
		t.Fatalf("seed small sample: %v", err)
	}
	overLt := (MaxBatchPayloadBytes-sampleOverhead(t, "over-new-", base.Add(3*time.Second)))/width + 1
	overNew := ltSample("over-new-", overLt, base.Add(3*time.Second))
	_, err = store.Ingest([]Sample{small, overNew}, func(created []Sample) error {
		t.Fatal("oversize mixed batch must not commit")
		return nil
	})
	if !IsOversizeBatchError(err) {
		t.Fatalf("want oversize error for new content, got %v", err)
	}
	if got := windowCount(t, store, base, 10); got != 3 {
		t.Fatalf("rejected mixed batch must add nothing, got %d samples", got)
	}

	// An all-replay batch, however large its content, never saves and always
	// reports the original created/replayed result.
	res, err = store.Ingest([]Sample{big}, func(created []Sample) error {
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
// error precedence: validation failures and conflicts (id content clash,
// repeated id in the batch, and a series/time clash between ids) are all
// reported ahead of the capacity check.
func TestIngestCapacityChecksAfterValidationAndConflicts(t *testing.T) {
	store := NewStore()
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	overhead, width := sampleEncoding(t, base)
	overLt := (MaxBatchPayloadBytes-overhead)/width + 1

	seed := Sample{ID: "dup", Service: "gw", Name: "requests", At: base, Value: 1}
	if _, err := store.Ingest([]Sample{seed}, nil); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Invalid sample (negative value) plus an oversized new sample: the
	// validation failure wins.
	invalid := Sample{ID: "bad", Service: "gw", Name: "requests", At: base.Add(time.Second), Value: -1}
	_, err := store.Ingest([]Sample{invalid, ltSample("huge", overLt, base.Add(2*time.Second))}, nil)
	if !IsValidationError(err) {
		t.Fatalf("validation must precede capacity, got %v", err)
	}

	// Conflicting id content plus an oversized sample: the conflict wins.
	conflict := Sample{ID: "dup", Service: "gw", Name: "requests", At: base, Value: 9}
	_, err = store.Ingest([]Sample{conflict, ltSample("huge2", overLt, base.Add(3*time.Second))}, nil)
	if !IsConflictError(err) {
		t.Fatalf("id conflict must precede capacity, got %v", err)
	}

	// A series/time clash between different ids plus an oversized sample:
	// the conflict still wins.
	clash := Sample{ID: "intruder", Service: "gw", Name: "requests", At: base, Value: 2}
	_, err = store.Ingest([]Sample{clash, ltSample("huge3", overLt, base.Add(4*time.Second))}, nil)
	if !IsConflictError(err) {
		t.Fatalf("series/time clash must precede capacity, got %v", err)
	}

	// A repeated id within the batch plus an oversized sample: conflict wins.
	twice := ltSample("twice", overLt, base.Add(5*time.Second))
	again := Sample{ID: twice.ID, Service: "gw", Name: "requests", At: base.Add(6 * time.Second), Value: 3}
	_, err = store.Ingest([]Sample{twice, again}, nil)
	if !IsConflictError(err) {
		t.Fatalf("in-batch duplicate id must precede capacity, got %v", err)
	}

	if got := windowCount(t, store, base, 10); got != 1 {
		t.Fatalf("precedence rejections must not change data, got %d", got)
	}
}

// TestIngestCapacityMeasuresNormalizedContent confirms the measurement uses
// the normalized samples that would actually be saved: surrounding
// whitespace trimmed from the id does not count, while the '<' run that
// remains still pays its escaping cost.
func TestIngestCapacityMeasuresNormalizedContent(t *testing.T) {
	store := NewStore()
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	overhead, width := sampleEncoding(t, base)
	overLt := (MaxBatchPayloadBytes-overhead)/width + 1

	tooBig := ltSample("trimmed", overLt, base)
	if _, err := store.Ingest([]Sample{tooBig}, func([]Sample) error {
		t.Fatal("oversize normalized batch must not commit")
		return nil
	}); !IsOversizeBatchError(err) {
		t.Fatalf("escaped id content must exceed capacity, got %v", err)
	}

	// Surrounding whitespace is stripped by normalization, so padding it
	// around the same id neither inflates nor saves the batch.
	padded := tooBig
	padded.ID = "   " + padded.ID + "   "
	_, err := store.Ingest([]Sample{padded}, func(created []Sample) error {
		t.Fatal("trimmed oversized batch must not commit")
		return nil
	})
	if !IsOversizeBatchError(err) {
		t.Fatalf("whitespace padding must not change the capacity decision, got %v", err)
	}
}
