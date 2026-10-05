package logfile

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cocosolocoder/signalroom/internal/events"
	"github.com/cocosolocoder/signalroom/internal/metrics"
)

// TestPoisonStatesIndependent verifies the central reliability rule: a write
// failure in one log kind poisons only that kind. The other two must keep
// accepting and durably persisting records in the same process, and a later
// success must never clear a poisoned state.
func TestPoisonStatesIndependent(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)

	eventLog, mLog, batches := openMetricsForTest(t, dir)
	if len(batches) != 0 {
		t.Fatalf("fresh metrics log: %d batches", len(batches))
	}
	incLog, records, err := OpenIncidentLog(dir)
	if err != nil {
		t.Fatalf("open incident log: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("fresh incident log: %d records", len(records))
	}

	// Break the metrics log only.
	if err := mLog.file.Close(); err != nil {
		t.Fatalf("break metrics fd: %v", err)
	}
	if err := mLog.Append([]metrics.Sample{metricSample("mx", 10, 1)}); err == nil {
		t.Fatal("broken metrics write must fail")
	}
	if !mLog.Poisoned() {
		t.Fatal("metrics log must be poisoned")
	}
	// A later attempt must keep failing with ErrPoisoned and must not recover.
	if err := mLog.Append([]metrics.Sample{metricSample("my", 11, 1)}); !errors.Is(err, ErrPoisoned) {
		t.Fatalf("poisoned metrics must return ErrPoisoned, got %v", err)
	}

	// Events and incidents stay fully available despite the metric failure.
	if err := eventLog.Append([]events.Event{testEvent("e1", base)}); err != nil {
		t.Fatalf("event append after metric failure: %v", err)
	}
	if eventLog.Poisoned() {
		t.Fatal("event log must not be poisoned by a metric failure")
	}
	if err := incLog.AppendRecord(creationRecord("I1", base)); err != nil {
		t.Fatalf("incident append after metric failure: %v", err)
	}
	if incLog.Poisoned() {
		t.Fatal("incident log must not be poisoned by a metric failure")
	}

	// Break the incident log as well; events must still be unaffected.
	if err := incLog.file.Close(); err != nil {
		t.Fatalf("break incident fd: %v", err)
	}
	if err := incLog.AppendRecord(creationRecord("I2", base.Add(time.Second))); err == nil {
		t.Fatal("broken incident write must fail")
	}
	if !incLog.Poisoned() {
		t.Fatal("incident log must be poisoned")
	}
	if err := eventLog.Append([]events.Event{testEvent("e2", base.Add(2*time.Second))}); err != nil {
		t.Fatalf("event append after incident failure: %v", err)
	}
	if eventLog.Poisoned() {
		t.Fatal("an incident failure must not poison the event log")
	}

	// Everything the healthy log(s) acknowledged is on disk and recovers.
	if err := eventLog.Close(); err != nil {
		t.Fatalf("close event log: %v", err)
	}

	reopenedEvents, recoveredEvents, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopenedEvents.Close()
	if len(recoveredEvents) != 2 || recoveredEvents[0][0].ID != "e1" || recoveredEvents[1][0].ID != "e2" {
		t.Fatalf("only acknowledged events must recover: %+v", recoveredEvents)
	}

	reopenedIncidents, recoveredIncidents, err := OpenIncidentLog(dir)
	if err != nil {
		t.Fatalf("reopen incident log: %v", err)
	}
	defer reopenedIncidents.Close()
	if len(recoveredIncidents) != 1 || recoveredIncidents[0].Creation.ID != "I1" {
		t.Fatalf("only the acknowledged incident must recover: %+v", recoveredIncidents)
	}
}

// TestConcurrentAppendsKeepFramesWhole hammers one log from many goroutines.
// Each frame must land as one contiguous unit: after recovery every frame
// decodes as exactly the batch that was submitted, with no interleaving.
func TestConcurrentAppendsKeepFramesWhole(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)

	log, _, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	const writers = 16
	const perWriter = 25
	var wg sync.WaitGroup
	errCh := make(chan error, writers)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				id := fmt.Sprintf("w%db%d", w, i)
				batch := []events.Event{
					testEvent(id+"-a", base.Add(time.Duration(w*perWriter+i)*time.Second)),
					testEvent(id+"-b", base.Add(time.Duration(w*perWriter+i)*time.Second+time.Nanosecond)),
				}
				if err := log.Append(batch); err != nil {
					errCh <- err
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent append: %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	_, recovered, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen after concurrent appends: %v", err)
	}
	if len(recovered) != writers*perWriter {
		t.Fatalf("recovered %d batches, want %d", len(recovered), writers*perWriter)
	}
	seen := make(map[string]bool, writers*perWriter*2)
	for _, batch := range recovered {
		if len(batch) != 2 || batch[0].ID[len(batch[0].ID)-2:] != "-a" ||
			batch[1].ID[len(batch[1].ID)-2:] != "-b" ||
			batch[0].ID[:len(batch[0].ID)-2] != batch[1].ID[:len(batch[1].ID)-2] {
			t.Fatalf("batch boundary broken by interleaving: %+v", batch)
		}
		for _, event := range batch {
			if seen[event.ID] {
				t.Fatalf("event %q recovered twice", event.ID)
			}
			seen[event.ID] = true
		}
	}
	if len(seen) != writers*perWriter*2 {
		t.Fatalf("recovered %d events, want %d", len(seen), writers*perWriter*2)
	}
}
