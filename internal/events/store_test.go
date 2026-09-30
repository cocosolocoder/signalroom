package events

import (
	"testing"
	"time"
)

func TestStoreOrdersAndFiltersEvents(t *testing.T) {
	store := NewStore()
	base := time.Date(2026, time.October, 1, 9, 0, 0, 0, time.UTC)
	input := []Event{
		{ID: "evt-2", Service: "checkout", Severity: "WARN", Message: "latency elevated", At: base.Add(time.Minute)},
		{ID: "evt-1", Service: "gateway", Severity: "critical", Message: "error rate spike", At: base},
	}
	for _, event := range input {
		if err := store.Add(event); err != nil {
			t.Fatalf("add event: %v", err)
		}
	}

	got := store.Query(Query{Severity: "critical"})
	if len(got) != 1 || got[0].ID != "evt-1" {
		t.Fatalf("unexpected filtered events: %#v", got)
	}
	all := store.Query(Query{})
	if len(all) != 2 || all[0].ID != "evt-1" || all[1].ID != "evt-2" {
		t.Fatalf("events are not deterministic: %#v", all)
	}
}

func TestStoreRejectsDuplicateID(t *testing.T) {
	store := NewStore()
	event := Event{ID: "same", Service: "api", Severity: "info", Message: "started", At: time.Now()}
	if err := store.Add(event); err != nil {
		t.Fatalf("first add: %v", err)
	}
	if err := store.Add(event); err == nil {
		t.Fatal("expected duplicate id error")
	}
}
