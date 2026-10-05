package logfile

import (
	"errors"
	"testing"
	"time"

	"github.com/cocosolocoder/signalroom/internal/events"
)

// TestAppendOversizePayloadRejectedWithoutPoisoning checks the safety
// distinction: an oversize frame is a property of the content, not a storage
// fault. It must come back as ErrPayloadTooLarge, leave the log unpoisoned,
// and allow later writes to keep working without a restart.
func TestAppendOversizePayloadRejectedWithoutPoisoning(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)

	log, _, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer log.Close()

	// A payload of exactly the limit is accepted (boundary is inclusive).
	atLimit := make([]byte, MaxPayloadBytes)
	for i := range atLimit {
		atLimit[i] = 'x'
	}
	if err := log.appendFrame(atLimit); err != nil {
		t.Fatalf("payload at exactly the limit must append: %v", err)
	}

	// One byte over is rejected as ErrPayloadTooLarge, not as a write fault.
	over := make([]byte, MaxPayloadBytes+1)
	err = log.appendFrame(over)
	if !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("want ErrPayloadTooLarge, got %v", err)
	}
	if log.Poisoned() {
		t.Fatal("oversize payload must not poison the log")
	}

	// Subsequent normal writes still succeed in the same process.
	if err := log.Append([]events.Event{testEvent("after", base)}); err != nil {
		t.Fatalf("log must stay usable after an oversize rejection: %v", err)
	}
}
