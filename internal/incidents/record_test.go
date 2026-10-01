package incidents

import (
	"testing"
	"time"
)

func TestRecordRoundTrip(t *testing.T) {
	at := time.Date(2026, 10, 1, 12, 0, 0, 42, time.FixedZone("CST", 8*3600))
	records := []Record{
		NewCreationRecord(Creation{ID: "I1", Title: "T", Service: "svc", Operator: "alice"}, at),
		NewActionRecord(Request{IncidentID: "I1", ActionID: "a1", Operator: "bob",
			ExpectedVersion: 1, Type: ActionResolve, Content: "because"}, at.Add(time.Nanosecond)),
	}
	for _, record := range records {
		data, err := MarshalRecord(record)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		back, err := DecodeRecord(data)
		if err != nil {
			t.Fatalf("decode %s: %v", data, err)
		}
		if back.Kind != record.Kind || !back.At.Equal(record.At) {
			t.Fatalf("kind/time mismatch: %+v vs %+v", back, record)
		}
		if back.At.Location() != time.UTC {
			t.Fatalf("replayed timestamp must be UTC, got %v", back.At.Location())
		}
		switch record.Kind {
		case RecordCreation:
			if *back.Creation != *record.Creation {
				t.Fatalf("creation mismatch %+v", *back.Creation)
			}
		case RecordAction:
			if back.Action.Request != record.Action.Request {
				t.Fatalf("action mismatch %+v", back.Action.Request)
			}
		}
	}
}

func TestDecodeRecordRejectsGarbage(t *testing.T) {
	cases := map[string]string{
		"unknown kind":     `{"kind":"boom","at":"2026-10-01T12:00:00Z"}`,
		"missing time":     `{"kind":"create","creation":{"id":"i","title":"t","service":"s","operator":"o"}}`,
		"blank id":         `{"kind":"create","at":"2026-10-01T12:00:00Z","creation":{"id":" ","title":"t","service":"s","operator":"o"}}`,
		"unknown action":   `{"kind":"action","at":"2026-10-01T12:00:00Z","action":{"incident_id":"i","action_id":"a","operator":"o","expected_version":1,"type":"boom","content":"c"}}`,
		"zero exp version": `{"kind":"action","at":"2026-10-01T12:00:00Z","action":{"incident_id":"i","action_id":"a","operator":"o","expected_version":0,"type":"add_note","content":"c"}}`,
		"unknown field":    `{"kind":"create","at":"2026-10-01T12:00:00Z","creation":{"id":"i","title":"t","service":"s","operator":"o"},"extra":1}`,
		"trailing data":    `{"kind":"create","at":"2026-10-01T12:00:00Z","creation":{"id":"i","title":"t","service":"s","operator":"o"}}junk`,
	}
	for name, body := range cases {
		if _, err := DecodeRecord([]byte(body)); err == nil {
			t.Fatalf("%s: expected error", name)
		}
	}
}

func TestReplayRebuildsState(t *testing.T) {
	lookup := &fakeEvents{services: map[string]string{"e1": "gateway"}}
	times := []time.Time{
		time.Date(2026, 10, 1, 12, 0, 0, 1, time.UTC),
		time.Date(2026, 10, 1, 12, 0, 0, 2, time.UTC),
		time.Date(2026, 10, 1, 12, 0, 0, 3, time.UTC),
		time.Date(2026, 10, 1, 12, 0, 0, 4, time.UTC),
		time.Date(2026, 10, 1, 12, 0, 0, 5, time.UTC),
	}
	records := []Record{
		NewCreationRecord(Creation{ID: "I1", Title: "T", Service: "gateway", Operator: "alice"}, times[0]),
		NewActionRecord(Request{IncidentID: "I1", ActionID: "n1", Operator: "bob", ExpectedVersion: 1, Type: ActionNote, Content: "c"}, times[1]),
		NewActionRecord(Request{IncidentID: "I1", ActionID: "l1", Operator: "bob", ExpectedVersion: 2, Type: ActionLinkEvent, Content: "e1"}, times[2]),
		NewActionRecord(Request{IncidentID: "I1", ActionID: "r1", Operator: "bob", ExpectedVersion: 3, Type: ActionResolve, Content: "fixed"}, times[3]),
		NewActionRecord(Request{IncidentID: "I1", ActionID: "o1", Operator: "bob", ExpectedVersion: 4, Type: ActionReopen, Content: "again"}, times[4]),
	}

	reg := NewRegistry(lookup, nil)
	for _, record := range records {
		if err := reg.Load(record); err != nil {
			t.Fatalf("load: %v", err)
		}
	}
	inc, err := reg.Get("I1")
	if err != nil {
		t.Fatal(err)
	}
	if inc.Status != StatusOpen || inc.Version != 5 {
		t.Fatalf("rebuilt state %+v", inc)
	}
	if len(inc.Links) != 1 || inc.Links[0] != "e1" {
		t.Fatalf("rebuilt links %+v", inc.Links)
	}
	if len(inc.History) != 5 {
		t.Fatalf("rebuilt history len %d", len(inc.History))
	}
	for i, entry := range inc.History {
		if !entry.At.Equal(times[i]) {
			t.Fatalf("history[%d] time %s want %s", i, entry.At, times[i])
		}
	}

	// After replay, an idempotent retry of a loaded action returns its first
	// result and changes nothing.
	replay := Request{IncidentID: "I1", ActionID: "r1", Operator: "bob", ExpectedVersion: 3, Type: ActionResolve, Content: "fixed"}
	res, err := reg.Apply(replay)
	if err != nil {
		t.Fatalf("retry after replay: %v", err)
	}
	if res.Version != 4 {
		t.Fatalf("retry result version %d want 4", res.Version)
	}
	inc, _ = reg.Get("I1")
	if inc.Version != 5 || len(inc.History) != 5 {
		t.Fatalf("retry must not change state: %+v", inc)
	}
}

func TestReplayRejectsInconsistentLog(t *testing.T) {
	lookup := &fakeEvents{services: map[string]string{"e1": "gateway", "other": "checkout"}}
	base := func() *Registry { return NewRegistry(lookup, nil) }

	load := func(t *testing.T, reg *Registry, records ...Record) error {
		t.Helper()
		for _, record := range records {
			if err := reg.Load(record); err != nil {
				return err
			}
		}
		return nil
	}
	creation := func(at time.Time) Record {
		return NewCreationRecord(Creation{ID: "I1", Title: "T", Service: "gateway", Operator: "alice"}, at)
	}
	t0 := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)

	t.Run("duplicate creation", func(t *testing.T) {
		reg := base()
		if err := load(t, reg, creation(t0), creation(t0.Add(time.Second))); err == nil {
			t.Fatal("duplicate creation must fail")
		}
	})
	t.Run("action for unknown incident", func(t *testing.T) {
		reg := base()
		act := NewActionRecord(Request{IncidentID: "ghost", ActionID: "a", Operator: "o", ExpectedVersion: 1, Type: ActionNote, Content: "c"}, t0)
		if err := reg.Load(act); err == nil {
			t.Fatal("action for unknown incident must fail")
		}
	})
	t.Run("non sequential version", func(t *testing.T) {
		reg := base()
		_ = load(t, reg, creation(t0))
		act := NewActionRecord(Request{IncidentID: "I1", ActionID: "a", Operator: "o", ExpectedVersion: 7, Type: ActionNote, Content: "c"}, t0.Add(time.Second))
		if err := reg.Load(act); err == nil {
			t.Fatal("gap in versions must fail")
		}
	})
	t.Run("duplicate action id", func(t *testing.T) {
		reg := base()
		_ = load(t, reg, creation(t0))
		act := func(ev int) Record {
			return NewActionRecord(Request{IncidentID: "I1", ActionID: "dup", Operator: "o", ExpectedVersion: ev, Type: ActionNote, Content: "c"}, t0.Add(time.Duration(ev)*time.Second))
		}
		if err := reg.Load(act(1)); err != nil {
			t.Fatal(err)
		}
		if err := reg.Load(act(2)); err == nil {
			t.Fatal("reused action id must fail")
		}
	})
	t.Run("linked event wrong service", func(t *testing.T) {
		reg := base()
		_ = load(t, reg, creation(t0))
		act := NewActionRecord(Request{IncidentID: "I1", ActionID: "l", Operator: "o", ExpectedVersion: 1, Type: ActionLinkEvent, Content: "other"}, t0.Add(time.Second))
		if err := reg.Load(act); err == nil {
			t.Fatal("link to another service must fail recovery")
		}
	})
	t.Run("linked event missing", func(t *testing.T) {
		reg := base()
		_ = load(t, reg, creation(t0))
		act := NewActionRecord(Request{IncidentID: "I1", ActionID: "l", Operator: "o", ExpectedVersion: 1, Type: ActionLinkEvent, Content: "gone"}, t0.Add(time.Second))
		if err := reg.Load(act); err == nil {
			t.Fatal("link to a vanished event must fail recovery")
		}
	})
}
