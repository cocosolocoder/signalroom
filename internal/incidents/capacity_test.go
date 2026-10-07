package incidents

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// capacityAt serializes without a fractional second, keeping the saved
// record length exactly predictable.
var capacityAt = time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)

// newCapacityRegistry is a registry whose clock always reports at, so the
// server timestamp embedded in a measured record is the same length on every
// call.
func newCapacityRegistry(at time.Time) (*Registry, *fakeCommit) {
	commit := &fakeCommit{}
	reg := NewRegistry(&fakeEvents{}, commit.commit, WithClock(func() time.Time { return at }))
	return reg, commit
}

func createFor(t *testing.T, reg *Registry, id string) {
	t.Helper()
	if _, err := reg.Create(Creation{ID: id, Title: "Outage", Service: "gateway", Operator: "alice"}); err != nil {
		t.Fatalf("create: %v", err)
	}
}

// noteReq is one add_note request carrying amps ampersands.
func noteReq(incidentID, actionID string, amps, expected int) Request {
	return Request{
		IncidentID:      incidentID,
		ActionID:        actionID,
		Operator:        "bob",
		ExpectedVersion: expected,
		Type:            ActionNote,
		Content:         strings.Repeat("&", amps),
	}
}

// withSize clones probe's identity fields and replaces only the note size
// and expected version, so the measured record keeps probe's exact overhead.
func withSize(probe Request, amps, expected int) Request {
	out := probe
	out.ExpectedVersion = expected
	out.Content = strings.Repeat("&", amps)
	return out
}

func mustMarshalRecord(t *testing.T, req Request, at time.Time) []byte {
	t.Helper()
	payload, err := MarshalRecord(NewActionRecord(req, at))
	if err != nil {
		t.Fatalf("marshal record: %v", err)
	}
	return payload
}

// oversizeAmps returns an amp count that pushes a note shaped like probe just
// past the save capacity, and reports the measured record length.
func oversizeAmps(t *testing.T, probe Request, at time.Time) (amps int, encodedLen int) {
	t.Helper()
	empty := probe
	empty.Content = ""
	full := probe
	full.Content = strings.Repeat("&", 1000)
	width := (len(mustMarshalRecord(t, full, at)) - len(mustMarshalRecord(t, empty, at))) / 1000
	if width <= 1 {
		t.Fatalf("test setup: escaping must expand each '&', got width %d", width)
	}
	overhead := len(mustMarshalRecord(t, empty, at))
	amps = (MaxActionPayloadBytes-overhead)/width + 1
	encodedLen = len(mustMarshalRecord(t, withSize(probe, amps, probe.ExpectedVersion), at))
	if encodedLen <= MaxActionPayloadBytes {
		t.Fatalf("test setup: encoded %d does not exceed %d", encodedLen, MaxActionPayloadBytes)
	}
	return amps, encodedLen
}

// TestApplyNoteCapacityExactLimitAccepted proves the boundary is inclusive:
// a note whose normalized record encodes to exactly MaxActionPayloadBytes is
// saved, while one more escaped '&' makes it an OversizeActionError.
func TestApplyNoteCapacityExactLimitAccepted(t *testing.T) {
	probe := noteReq("INC-1", "exact", 0, 1)
	empty := probe
	empty.Content = ""
	full := probe
	full.Content = strings.Repeat("&", 1000)
	width := (len(mustMarshalRecord(t, full, capacityAt)) - len(mustMarshalRecord(t, empty, capacityAt))) / 1000

	// Pad the incident id (one saved byte per character) until the fixed
	// overhead makes the limit reachable exactly with whole '&' runs.
	for {
		overhead := len(mustMarshalRecord(t, probe, capacityAt))
		if (MaxActionPayloadBytes-overhead)%width == 0 {
			break
		}
		probe.IncidentID += "x"
	}
	overhead := len(mustMarshalRecord(t, probe, capacityAt))
	amps := (MaxActionPayloadBytes - overhead) / width
	atLimit := withSize(probe, amps, 1)
	if encoded := mustMarshalRecord(t, atLimit, capacityAt); len(encoded) != MaxActionPayloadBytes {
		t.Fatalf("setup: encoded %d bytes, want %d", len(encoded), MaxActionPayloadBytes)
	}

	reg, commit := newCapacityRegistry(capacityAt)
	createFor(t, reg, probe.IncidentID)
	res, err := reg.Apply(atLimit)
	if err != nil {
		t.Fatalf("note at exactly the limit must be accepted: %v", err)
	}
	if res.Version != 2 || commit.count() != 2 {
		t.Fatalf("exact-limit note not committed: %+v count=%d", res, commit.count())
	}

	// One more '&' pushes the saved record past the limit.
	over := withSize(probe, amps+1, 1)
	over.ActionID = "over"
	encoded := mustMarshalRecord(t, over, capacityAt)
	if len(encoded) <= MaxActionPayloadBytes {
		t.Fatalf("setup: over record encodes to %d", len(encoded))
	}
	reg2, commit2 := newCapacityRegistry(capacityAt)
	createFor(t, reg2, probe.IncidentID)
	_, err = reg2.Apply(over)
	var oversized *OversizeActionError
	if !errors.As(err, &oversized) {
		t.Fatalf("want OversizeActionError, got %v", err)
	}
	if oversized.Size != len(encoded) || oversized.Limit != MaxActionPayloadBytes {
		t.Fatalf("error size=%d limit=%d encoded=%d", oversized.Size, oversized.Limit, len(encoded))
	}
	if commit2.count() != 1 {
		t.Fatalf("oversize note must not reach storage, count=%d", commit2.count())
	}

	// The rejected action id is still usable, with the same expected version,
	// for a shortened note: ordinary success, exactly one new history entry.
	short := over
	short.Content = "shortened note"
	res, err = reg2.Apply(short)
	if err != nil {
		t.Fatalf("shortened retry must succeed: %v", err)
	}
	if res.Version != 2 {
		t.Fatalf("shortened retry version=%d, want 2", res.Version)
	}
	got, _ := reg2.Get(probe.IncidentID)
	if got.Version != 2 || len(got.History) != 2 {
		t.Fatalf("retry must add exactly one history entry: version=%d history=%d", got.Version, len(got.History))
	}
	if got.History[1].Content != "shortened note" {
		t.Fatalf("saved content %q", got.History[1].Content)
	}
}

// TestApplyNoteCapacityEscapingExpandsContent is the reported bug in
// miniature: a legal note whose raw text is modest (well under both the
// request body limit and 64 MiB) saves far larger because the save format
// JSON-escapes '&', '<', and '>'. Such a note is a content error, not a
// storage failure.
func TestApplyNoteCapacityEscapingExpandsContent(t *testing.T) {
	probe := noteReq("INC-1", "big", 0, 1)
	amps, _ := oversizeAmps(t, probe, capacityAt)
	big := withSize(probe, amps, 1)

	if len(big.Content) >= MaxActionPayloadBytes {
		t.Fatalf("setup: raw note already %d bytes", len(big.Content))
	}

	reg, commit := newCapacityRegistry(capacityAt)
	createFor(t, reg, "INC-1")
	before := commit.count()
	_, err := reg.Apply(big)
	if !IsOversizeActionError(err) {
		t.Fatalf("want oversize error, got %v", err)
	}
	if !strings.Contains(err.Error(), "64 MiB") || !strings.Contains(err.Error(), "save capacity") {
		t.Fatalf("error must name the 64 MiB save capacity, got %q", err.Error())
	}
	if !strings.Contains(strings.ToLower(err.Error()), "reduce") {
		t.Fatalf("error must ask for less content, got %q", err.Error())
	}
	if strings.Contains(strings.ToLower(err.Error()), "restart") {
		t.Fatalf("oversize error must not demand a restart, got %q", err.Error())
	}
	if commit.count() != before {
		t.Fatalf("oversize note must not persist, count before=%d after=%d", before, commit.count())
	}

	// Nothing moves: version, history, people, owner, and links are exactly
	// the creation state.
	got, _ := reg.Get("INC-1")
	if got.Version != 1 || len(got.History) != 1 {
		t.Fatalf("rejected note must not advance version or history: %+v", got)
	}
	if len(got.Participants) != 1 || got.Participants[0] != "alice" || got.Owner != "alice" {
		t.Fatalf("rejected note must not change people: participants=%v owner=%q", got.Participants, got.Owner)
	}
	if len(got.Links) != 0 || got.Status != StatusOpen {
		t.Fatalf("rejected note must not change links/status: links=%v status=%s", got.Links, got.Status)
	}

	// Incident storage stays usable without a restart, including more notes.
	if _, err := reg.Apply(Request{
		IncidentID: "INC-1", ActionID: "ok", Operator: "bob", ExpectedVersion: 1,
		Type: ActionNote, Content: "all good now",
	}); err != nil {
		t.Fatalf("storage must remain usable after oversize rejection: %v", err)
	}
}

// TestApplyNoteCapacityChecksAfterExistingRules preserves error precedence:
// unknown incidents, field errors, stale versions, and the resolved state
// rule are all reported ahead of the capacity check, and a changed replay is
// a conflict even when the new content would be oversize.
func TestApplyNoteCapacityChecksAfterExistingRules(t *testing.T) {
	probe := noteReq("INC-1", "big", 0, 1)
	amps, _ := oversizeAmps(t, probe, capacityAt)

	reg, _ := newCapacityRegistry(capacityAt)

	// Field error: expected_version must be positive before anything else.
	if _, err := reg.Apply(withSize(probe, amps, 0)); !IsValidationError(err) {
		t.Fatalf("validation must precede capacity, got %v", err)
	}

	// Unknown incident.
	unknown := withSize(probe, amps, 1)
	unknown.IncidentID = "ghost"
	if _, err := reg.Apply(unknown); !IsNotFoundError(err) {
		t.Fatalf("unknown incident must precede capacity, got %v", err)
	}

	createFor(t, reg, "INC-1")

	// Stale version.
	_, err := reg.Apply(withSize(probe, amps, 9))
	if !IsConflictError(err) || CurrentVersionOf(err) != 1 {
		t.Fatalf("stale version must precede capacity, got %v", err)
	}

	// A successful small note first, then the incident is resolved.
	if _, err := reg.Apply(Request{
		IncidentID: "INC-1", ActionID: "n1", Operator: "bob", ExpectedVersion: 1,
		Type: ActionNote, Content: "tiny",
	}); err != nil {
		t.Fatalf("seed note: %v", err)
	}
	if _, err := reg.Apply(Request{
		IncidentID: "INC-1", ActionID: "r1", Operator: "bob", ExpectedVersion: 2,
		Type: ActionResolve, Content: "fixed",
	}); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	// Resolved state rule wins over capacity.
	if _, err := reg.Apply(withSize(probe, amps, 3)); !IsConflictError(err) {
		t.Fatalf("resolved state must precede capacity, got %v", err)
	}

	// Same action id with different content is a replay conflict, checked
	// before capacity — here even using the committed id with a short body.
	changed := Request{
		IncidentID: "INC-1", ActionID: "n1", Operator: "bob", ExpectedVersion: 3,
		Type: ActionNote, Content: "changed",
	}
	if _, err := reg.Apply(changed); !IsConflictError(err) {
		t.Fatalf("changed replay must conflict regardless of size, got %v", err)
	}

	got, _ := reg.Get("INC-1")
	if got.Version != 3 || len(got.History) != 3 {
		t.Fatalf("precedence rejections must not change data: version=%d history=%d", got.Version, len(got.History))
	}
}

// TestApplyNoteCapacitySkipsReplays shows an identical retry of a note that
// was committed successfully (here seeded through the recovery path) is not
// measured again: it returns the first result even though its saved form
// exceeds the capacity, and keeps doing so after the incident is resolved.
func TestApplyNoteCapacitySkipsReplays(t *testing.T) {
	probe := noteReq("INC-1", "big", 0, 1)
	amps, _ := oversizeAmps(t, probe, capacityAt)
	big := withSize(probe, amps, 1)

	reg, commit := newCapacityRegistry(capacityAt)
	createFor(t, reg, "INC-1")
	// Seed the oversized note the way startup recovery would: Load performs
	// no capacity check, and no beforeCommit hook runs.
	if err := reg.Load(NewActionRecord(big, capacityAt)); err != nil {
		t.Fatalf("seed big note through recovery: %v", err)
	}

	res, err := reg.Apply(big)
	if err != nil {
		t.Fatalf("identical retry must replay instead of failing capacity: %v", err)
	}
	if res.Version != 2 {
		t.Fatalf("replay must return the first version, got %d", res.Version)
	}
	if commit.count() != 1 {
		t.Fatalf("replay must not persist again, count=%d", commit.count())
	}

	// Advance the incident to resolved; the big-note retry still returns its
	// first result and adds no history.
	if _, err := reg.Apply(Request{
		IncidentID: "INC-1", ActionID: "r1", Operator: "bob", ExpectedVersion: 2,
		Type: ActionResolve, Content: "fixed",
	}); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	res, err = reg.Apply(big)
	if err != nil {
		t.Fatalf("retry after resolve must replay, got %v", err)
	}
	if res.Version != 2 {
		t.Fatalf("retry after resolve must return version 2, got %d", res.Version)
	}
	got, _ := reg.Get("INC-1")
	if got.Version != 3 || got.Status != StatusResolved || len(got.History) != 3 {
		t.Fatalf("retry must not add history: version=%d status=%s history=%d", got.Version, got.Status, len(got.History))
	}
}

// TestApplyNoteCapacityOnlyNotes documents the scope: other action types
// keep their previous behavior and are not subject to the note pre-check.
func TestApplyNoteCapacityOnlyNotes(t *testing.T) {
	probe := Request{
		IncidentID: "INC-1", ActionID: "r-big", Operator: "bob", ExpectedVersion: 1,
		Type: ActionResolve, Content: "",
	}
	empty := probe
	emptyPayload := mustMarshalRecord(t, empty, capacityAt)
	full := probe
	full.Content = strings.Repeat("&", 1000)
	fullPayload := mustMarshalRecord(t, full, capacityAt)
	width := (len(fullPayload) - len(emptyPayload)) / 1000
	hugeReason := strings.Repeat("&", (MaxActionPayloadBytes-len(emptyPayload))/width+1)

	reg, commit := newCapacityRegistry(capacityAt)
	createFor(t, reg, "INC-1")
	// An oversized resolve reason is not an OversizeActionError: only notes
	// carry the new content check.
	probe.Content = hugeReason
	res, err := reg.Apply(probe)
	if err != nil {
		t.Fatalf("non-note actions are outside the note capacity check: %v", err)
	}
	if res.Version != 2 || commit.count() != 2 {
		t.Fatalf("resolve must commit: %+v count=%d", res, commit.count())
	}
}

// TestApplyNoteCapacityRejectionDoesNotMaskRealFailure keeps the two failure
// modes distinct: an oversize rejection never reaches the durable write path,
// and a genuine write failure afterwards still surfaces as a storage error.
func TestApplyNoteCapacityRejectionDoesNotMaskRealFailure(t *testing.T) {
	probe := noteReq("INC-1", "big", 0, 1)
	amps, _ := oversizeAmps(t, probe, capacityAt)

	reg, commit := newCapacityRegistry(capacityAt)
	createFor(t, reg, "INC-1")

	if _, err := reg.Apply(withSize(probe, amps, 1)); !IsOversizeActionError(err) {
		t.Fatalf("setup: want oversize error, got %v", err)
	}
	// The rejection did not invoke the commit hook, so the armed failure is
	// still waiting for a real write.
	commit.failOnce = true
	_, err := reg.Apply(Request{
		IncidentID: "INC-1", ActionID: "note", Operator: "bob", ExpectedVersion: 1,
		Type: ActionNote, Content: "ordinary",
	})
	if err == nil || IsOversizeActionError(err) || IsValidationError(err) ||
		IsConflictError(err) || IsNotFoundError(err) {
		t.Fatalf("genuine write failure must pass through as a storage error, got %v", err)
	}
	got, _ := reg.Get("INC-1")
	if got.Version != 1 || len(got.History) != 1 {
		t.Fatalf("failed write must leave the incident untouched: version=%d history=%d", got.Version, len(got.History))
	}
}
