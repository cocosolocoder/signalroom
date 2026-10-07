package incidents

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// ampNoteRequest is an add_note candidate whose content is n ampersands. The
// save format (encoding/json) HTML-escapes '&' as the six-byte sequence
// '&', so the saved record is far larger than the request text — the
// exact condition the per-record capacity guard classifies.
func ampNoteRequest(actionID string, amps int) Request {
	return Request{
		IncidentID:      "INC-1",
		ActionID:        actionID,
		Operator:        "alice",
		ExpectedVersion: 1,
		Type:            ActionNote,
		Content:         strings.Repeat("&", amps),
	}
}

// encodedNote measures the exact saved JSON of ampNoteRequest at a fixed
// timestamp, matching the bytes Apply measures before persisting.
func encodedNote(t *testing.T, at time.Time, actionID string, amps int) []byte {
	t.Helper()
	payload, err := MarshalRecord(NewActionRecord(ampNoteRequest(actionID, amps), at))
	if err != nil {
		t.Fatalf("marshal record: %v", err)
	}
	return payload
}

// ampEncoding reports the fixed overhead and the per-'&' width of an action
// record's saved form. The width is measured rather than assumed so the test
// follows the existing save format exactly.
func ampEncoding(t *testing.T, at time.Time) (overhead, width int) {
	t.Helper()
	empty := encodedNote(t, at, "probe", 0)
	full := encodedNote(t, at, "probe", 1000)
	width = (len(full) - len(empty)) / 1000
	if width <= 1 {
		t.Fatalf("test setup: escaping must expand each '&', got width %d", width)
	}
	return len(empty), width
}

// fixedClockRegistry builds a registry whose clock stamps a fixed RFC3339Nano
// instant, so saved-record lengths are stable across a test.
func fixedClockRegistry(t *testing.T, at time.Time) (*Registry, *fakeCommit) {
	t.Helper()
	commit := &fakeCommit{}
	reg := NewRegistry(&fakeEvents{}, commit.commit, WithClock(func() time.Time { return at }))
	if _, err := reg.Create(validCreation()); err != nil {
		t.Fatalf("create: %v", err)
	}
	return reg, commit
}

// TestApplyCapacityExactLimitAccepted proves the boundary is inclusive: an
// action record encoding to exactly MaxRecordPayloadBytes is saved, while one
// extra escaped character makes it an OversizeRecordError.
func TestApplyCapacityExactLimitAccepted(t *testing.T) {
	at := time.Date(2026, 10, 1, 12, 0, 0, 1, time.UTC)
	reg, commit := fixedClockRegistry(t, at)
	overhead, width := ampEncoding(t, at)

	// Pad the action id (one encoded byte per character) until the fixed
	// overhead makes the limit reachable exactly with whole '&' runs.
	actionID := "exact-limit"
	for overhead%width != MaxRecordPayloadBytes%width {
		actionID += "x"
		overhead = len(encodedNote(t, at, actionID, 0))
	}
	amps := (MaxRecordPayloadBytes - overhead) / width
	if amps <= 0 {
		t.Fatalf("test setup: overhead %d leaves no room", overhead)
	}

	atLimit := ampNoteRequest(actionID, amps)
	if encoded := encodedNote(t, at, actionID, amps); len(encoded) != MaxRecordPayloadBytes {
		t.Fatalf("setup: encoded %d bytes, want %d", len(encoded), MaxRecordPayloadBytes)
	}

	res, err := reg.Apply(atLimit)
	if err != nil {
		t.Fatalf("record at exactly the limit must be accepted: %v", err)
	}
	if res != (ActionResult{IncidentID: "INC-1", ActionID: actionID, Version: 2}) {
		t.Fatalf("unexpected result %+v", res)
	}
	if commit.count() != 2 { // create + note
		t.Fatalf("accepted note must persist, count=%d", commit.count())
	}
	got, _ := reg.Get("INC-1")
	if got.Version != 2 || len(got.History) != 2 || got.History[1].Content != atLimit.Content {
		t.Fatalf("accepted note must be saved verbatim: %+v", got)
	}

	// One more '&' pushes the saved record past the limit. The rejected
	// action id stays free. It must have the same encoded overhead (same id
	// length, plain characters), so amps+1 lands one escape sequence over.
	overID := strings.Repeat("v", len(actionID))
	over := ampNoteRequest(overID, amps+1)
	over.ExpectedVersion = 2
	overEncoded := encodedNote(t, at, overID, amps+1)
	if len(overEncoded) <= MaxRecordPayloadBytes {
		t.Fatalf("setup: over record encodes to %d", len(overEncoded))
	}
	_, err = reg.Apply(over)
	var oversized *OversizeRecordError
	if !errors.As(err, &oversized) {
		t.Fatalf("want OversizeRecordError, got %v", err)
	}
	if oversized.Size != len(overEncoded) || oversized.Limit != MaxRecordPayloadBytes {
		t.Fatalf("error size=%d limit=%d", oversized.Size, oversized.Limit)
	}
	if got, _ := reg.Get("INC-1"); got.Version != 2 || len(got.History) != 2 {
		t.Fatalf("rejected note must not be visible: %+v", got)
	}
	if commit.count() != 2 {
		t.Fatalf("rejected note must not persist, count=%d", commit.count())
	}

	// The rejected action id accepts a shorter follow-up at the unchanged
	// version with no restart.
	small := ampNoteRequest(overID, 4)
	small.ExpectedVersion = 2
	if res, err := reg.Apply(small); err != nil {
		t.Fatalf("rejected action id must remain usable: %v", err)
	} else if res.Version != 3 {
		t.Fatalf("shortened retry result %+v", res)
	}
}

// TestApplyOversizeNoteIsContentErrorNotStorageFailure is the reported bug in
// miniature: a note whose raw text is modest saves far larger because its
// '<' characters are escaped in the save format. The rejection is a content
// error, not a storage failure: nothing is written, the incident is untouched,
// and the store keeps working.
func TestApplyOversizeNoteIsContentErrorNotStorageFailure(t *testing.T) {
	at := time.Date(2026, 10, 1, 12, 0, 0, 1, time.UTC)
	reg, commit := fixedClockRegistry(t, at)

	// 11.2 million '<' characters: ~11.2 MiB of note text (well under the
	// 16 MiB request cap) but ~67.2 million saved bytes once '<' escapes to
	// '<'.
	const ltCount = 11_200_000
	big := Request{
		IncidentID:      "INC-1",
		ActionID:        "big",
		Operator:        "alice",
		ExpectedVersion: 1,
		Type:            ActionNote,
		Content:         strings.Repeat("<", ltCount),
	}
	if len(big.Content) >= MaxRecordPayloadBytes {
		t.Fatalf("setup: raw note already %d bytes", len(big.Content))
	}
	record := NewActionRecord(big, at)
	encoded, err := MarshalRecord(record)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if len(encoded) <= MaxRecordPayloadBytes {
		t.Fatalf("setup: escaping did not expand past the limit: %d", len(encoded))
	}

	_, err = reg.Apply(big)
	if !IsOversizeRecordError(err) {
		t.Fatalf("want oversize record error, got %v", err)
	}
	if !strings.Contains(err.Error(), "save capacity") {
		t.Fatalf("error must explain the save capacity, got %q", err.Error())
	}
	if !strings.Contains(err.Error(), "64 MiB") {
		t.Fatalf("error must name the 64 MiB capacity, got %q", err.Error())
	}
	if !strings.Contains(strings.ToLower(err.Error()), "reduce") {
		t.Fatalf("error must ask for less content, got %q", err.Error())
	}
	if strings.Contains(strings.ToLower(err.Error()), "restart") {
		t.Fatalf("oversize error must not mention restart, got %q", err.Error())
	}
	if commit.count() != 1 { // creation only
		t.Fatalf("oversize note must not reach storage, count=%d", commit.count())
	}
	got, _ := reg.Get("INC-1")
	if got.Version != 1 || len(got.History) != 1 || got.Status != StatusOpen {
		t.Fatalf("rejected note must leave the incident at creation: %+v", got)
	}

	// Storage keeps working with no restart: a normal note commits.
	normal := ampNoteRequest("ok", 4)
	if _, err := reg.Apply(normal); err != nil {
		t.Fatalf("registry must stay usable after an oversize rejection: %v", err)
	}
	if commit.count() != 2 { // creation + the normal note; the rejection wrote nothing
		t.Fatalf("normal note must persist after rejection, count=%d", commit.count())
	}
}

// TestApplyOversizeRejectionLeavesActionIDFree covers the retry contract:
// after an oversize rejection at version 1, the same action id succeeds with
// the original expected_version once the incident has not moved, adding
// exactly one history entry.
func TestApplyOversizeRejectionLeavesActionIDFree(t *testing.T) {
	reg, commit, _ := newTestRegistry(&fakeEvents{})
	if _, err := reg.Create(validCreation()); err != nil {
		t.Fatal(err)
	}

	overhead, width := ampEncoding(t, time.Date(2026, 10, 1, 12, 0, 0, 9, time.UTC))
	amps := (MaxRecordPayloadBytes-overhead)/width + 1
	big := ampNoteRequest("retry-me", amps)

	if _, err := reg.Apply(big); !IsOversizeRecordError(err) {
		t.Fatalf("want oversize, got %v", err)
	}
	got, _ := reg.Get("INC-1")
	if got.Version != 1 || len(got.History) != 1 {
		t.Fatalf("rejected request must add nothing: %+v", got)
	}

	// The original expected_version (1) is still current, so the identical
	// action id with shortened content commits normally and adds one entry.
	small := ampNoteRequest("retry-me", 3)
	res, err := reg.Apply(small)
	if err != nil {
		t.Fatalf("shortened retry with the original expected version: %v", err)
	}
	if res != (ActionResult{IncidentID: "INC-1", ActionID: "retry-me", Version: 2}) {
		t.Fatalf("unexpected retry result %+v", res)
	}
	got, _ = reg.Get("INC-1")
	if got.Version != 2 || len(got.History) != 2 {
		t.Fatalf("shortened retry must add exactly one history entry: %+v", got)
	}
	if got.History[1].Content != "&&&" {
		t.Fatalf("shortened note must be saved verbatim, got %q", got.History[1].Content)
	}
	if commit.count() != 2 {
		t.Fatalf("exactly one action record may persist, count=%d", commit.count())
	}
}

// TestApplyCapacityCheckPrecedence preserves the existing error order:
// validation, unknown incident, stale version, resolved state, and
// replayed-but-changed content are all reported ahead of the capacity check,
// even when the submitted note would save past 64 MiB.
func TestApplyCapacityCheckPrecedence(t *testing.T) {
	at := time.Date(2026, 10, 1, 12, 0, 0, 1, time.UTC)
	overhead, width := ampEncoding(t, at)
	amps := (MaxRecordPayloadBytes-overhead)/width + 1
	big := func(actionID string, expected int) Request {
		req := ampNoteRequest(actionID, amps)
		req.ExpectedVersion = expected
		return req
	}

	t.Run("validation wins", func(t *testing.T) {
		reg, commit := fixedClockRegistry(t, at)
		bad := big("v", 0) // expected_version must be positive
		if _, err := reg.Apply(bad); !IsValidationError(err) {
			t.Fatalf("want validation error, got %v", err)
		}
		if commit.count() != 1 {
			t.Fatalf("rejected request must not persist, count=%d", commit.count())
		}
	})

	t.Run("unknown incident wins", func(t *testing.T) {
		reg, commit := fixedClockRegistry(t, at)
		bad := big("n", 1)
		bad.IncidentID = "ghost"
		if _, err := reg.Apply(bad); !IsNotFoundError(err) {
			t.Fatalf("want not-found error, got %v", err)
		}
		if commit.count() != 1 {
			t.Fatalf("rejected request must not persist, count=%d", commit.count())
		}
	})

	t.Run("stale version wins", func(t *testing.T) {
		reg, commit := fixedClockRegistry(t, at)
		if _, err := reg.Apply(ampNoteRequest("seed", 2)); err != nil {
			t.Fatal(err)
		}
		if _, err := reg.Apply(big("stale", 1)); !IsConflictError(err) || CurrentVersionOf(err) != 2 {
			t.Fatalf("want stale-version conflict, got %v", err)
		}
		if got, _ := reg.Get("INC-1"); len(got.History) != 2 {
			t.Fatalf("rejected note must add nothing: %+v", got)
		}
		if commit.count() != 2 {
			t.Fatalf("count=%d", commit.count())
		}
	})

	t.Run("resolved state wins", func(t *testing.T) {
		reg, commit := fixedClockRegistry(t, at)
		resolve := Request{IncidentID: "INC-1", ActionID: "r1", Operator: "alice",
			ExpectedVersion: 1, Type: ActionResolve, Content: "done"}
		if _, err := reg.Apply(resolve); err != nil {
			t.Fatal(err)
		}
		if _, err := reg.Apply(big("late", 2)); !IsConflictError(err) {
			t.Fatalf("want resolved-state conflict, got %v", err)
		}
		if got, _ := reg.Get("INC-1"); got.Status != StatusResolved || len(got.History) != 2 {
			t.Fatalf("rejected note must add nothing: %+v", got)
		}
		if commit.count() != 2 {
			t.Fatalf("count=%d", commit.count())
		}
	})

	t.Run("replayed changed content wins", func(t *testing.T) {
		reg, commit := fixedClockRegistry(t, at)
		if _, err := reg.Apply(ampNoteRequest("dup", 2)); err != nil {
			t.Fatal(err)
		}
		if _, err := reg.Apply(big("dup", 2)); !IsConflictError(err) {
			t.Fatalf("same action id with different content must conflict, got %v", err)
		}
		if got, _ := reg.Get("INC-1"); got.Version != 2 || len(got.History) != 2 {
			t.Fatalf("changed replay must add nothing: %+v", got)
		}
		if commit.count() != 2 {
			t.Fatalf("count=%d", commit.count())
		}
	})
}

// TestApplySuccessfulReplayNotMeasured shows that only a brand-new saved note
// counts toward the capacity: an action already committed (here recovered
// through the startup Load path, which performs no capacity check) replays
// its first result even when its content would exceed the save capacity, and
// even after the incident has since been resolved.
func TestApplySuccessfulReplayNotMeasured(t *testing.T) {
	at := time.Date(2026, 10, 1, 12, 0, 0, 1, time.UTC)
	overhead, width := ampEncoding(t, at)
	amps := (MaxRecordPayloadBytes-overhead)/width + 1
	big := ampNoteRequest("big-old", amps)

	reg, commit, _ := newTestRegistry(&fakeEvents{})
	if _, err := reg.Create(validCreation()); err != nil {
		t.Fatal(err)
	}
	// Seed the oversized note through recovery, as if an older build had
	// written it; Load never re-measures.
	if err := reg.Load(NewActionRecord(big, at)); err != nil {
		t.Fatalf("seed big note: %v", err)
	}
	resolve := Request{IncidentID: "INC-1", ActionID: "r1", Operator: "alice",
		ExpectedVersion: 2, Type: ActionResolve, Content: "done"}
	if _, err := reg.Apply(resolve); err != nil {
		t.Fatalf("resolve: %v", err)
	}

	// An identical retry returns the first result, writes nothing, and is
	// neither capacity-checked nor rejected for the resolved state.
	res, err := reg.Apply(big)
	if err != nil {
		t.Fatalf("identical replay must succeed regardless of size or state: %v", err)
	}
	if res != (ActionResult{IncidentID: "INC-1", ActionID: "big-old", Version: 2}) {
		t.Fatalf("replay result %+v", res)
	}
	got, _ := reg.Get("INC-1")
	if got.Version != 3 || len(got.History) != 3 {
		t.Fatalf("replay must add no history: %+v", got)
	}
	if commit.count() != 2 { // creation and resolve persisted live; Load and replay did not
		t.Fatalf("replay must not persist, count=%d", commit.count())
	}
}

// TestApplyRealCommitFailureStillReportedAfterOversize keeps the two failure
// modes distinct: a genuine commit failure still surfaces after an oversize
// rejection; once the hook recovers the next valid note commits.
func TestApplyRealCommitFailureStillReportedAfterOversize(t *testing.T) {
	at := time.Date(2026, 10, 1, 12, 0, 0, 1, time.UTC)
	reg, commit := fixedClockRegistry(t, at)
	overhead, width := ampEncoding(t, at)
	amps := (MaxRecordPayloadBytes-overhead)/width + 1

	if _, err := reg.Apply(ampNoteRequest("probe", amps)); !IsOversizeRecordError(err) {
		t.Fatalf("oversize note must be a content error, got %v", err)
	}

	commit.failOnce = true
	_, err := reg.Apply(ampNoteRequest("small", 2))
	if err == nil || IsOversizeRecordError(err) {
		t.Fatalf("real commit failure must pass through, got %v", err)
	}
	if got, _ := reg.Get("INC-1"); got.Version != 1 || len(got.History) != 1 {
		t.Fatalf("failed write must leave the incident untouched: %+v", got)
	}

	commit.failOnce = false
	if _, err := reg.Apply(ampNoteRequest("small", 2)); err != nil {
		t.Fatalf("retry after a recovered write failure must succeed: %v", err)
	}
	if got, _ := reg.Get("INC-1"); got.Version != 2 || len(got.History) != 2 {
		t.Fatalf("retry must commit exactly one note: %+v", got)
	}
}
