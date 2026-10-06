package logfile

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cocosolocoder/signalroom/internal/events"
)

// rawFrame wraps an arbitrary JSON payload the same way Log.Append frames
// encoded batches, letting tests hand the recovery code deliberately
// unnormalized spellings and duplicate ids.
func rawFrame(t *testing.T, payload string) []byte {
	t.Helper()
	return frameFor([]byte(payload))
}

func writeLog(t *testing.T, dir string, frames ...[]byte) []byte {
	t.Helper()
	var data []byte
	for _, frame := range frames {
		data = append(data, frame...)
	}
	if err := os.WriteFile(filepath.Join(dir, logName), data, 0o644); err != nil {
		t.Fatal(err)
	}
	return data
}

func tornTail() []byte {
	payload := []byte(`[{"id":"torn","service":"svc","severity":"info","message":"m","at":"2026-10-01T09:05:00Z"}]`)
	tail := make([]byte, headerSize+4)
	binary.BigEndian.PutUint32(tail[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(tail[4:8], crc32.ChecksumIEEE(payload))
	copy(tail[8:], payload[:4])
	return tail
}

func eventFrame(t *testing.T, event events.Event) []byte {
	t.Helper()
	payload, err := events.MarshalBatch([]events.Event{event})
	if err != nil {
		t.Fatal(err)
	}
	return frameFor(payload)
}

// Two complete frames that share an id but carry different messages abort
// recovery; an incomplete trailing write after them must survive untouched.
func TestRecoveryRejectsConflictingCompleteBatchesWithoutMutation(t *testing.T) {
	dir := t.TempDir()
	original := writeLog(t, dir,
		rawFrame(t, `[{"id":"dup","service":"svc","severity":"info","message":"alpha","at":"2026-10-01T09:00:00Z"}]`),
		rawFrame(t, `[{"id":"other","service":"svc","severity":"info","message":"m","at":"2026-10-01T09:01:00Z"},
		              {"id":"dup","service":"svc","severity":"info","message":"beta","at":"2026-10-01T09:02:00Z"}]`),
		tornTail(),
	)

	log, batches, err := Open(dir)
	if err == nil {
		log.Close()
		t.Fatal("conflicting complete batches must fail recovery")
	}
	if batches != nil {
		t.Fatal("failed recovery must not return batches")
	}
	if !isCorruption(err) {
		t.Fatalf("want corruption error, got %v", err)
	}
	if !strings.Contains(err.Error(), "dup") {
		t.Fatalf("error must name the conflicting event id: %v", err)
	}
	if !strings.Contains(err.Error(), "alpha") || !strings.Contains(err.Error(), "beta") {
		t.Fatalf("error should describe the content conflict: %v", err)
	}

	after, readErr := os.ReadFile(filepath.Join(dir, logName))
	if readErr != nil {
		t.Fatal(readErr)
	}
	if !bytes.Equal(after, original) {
		t.Fatal("failed startup must leave events.log byte-for-byte identical, torn tail included")
	}
}

// A conflict inside a single complete batch is rejected just like one across
// batches, without trimming the torn tail behind it.
func TestRecoveryRejectsConflictWithinSingleBatch(t *testing.T) {
	dir := t.TempDir()
	original := writeLog(t, dir,
		rawFrame(t, `[{"id":"dup","service":"svc","severity":"info","message":"one","at":"2026-10-01T09:00:00Z"},
		              {"id":"dup","service":"svc","severity":"info","message":"two","at":"2026-10-01T09:00:00Z"}]`),
		tornTail(),
	)

	if _, _, err := Open(dir); !isCorruption(err) {
		t.Fatalf("within-batch conflict must fail recovery, got %v", err)
	}
	if after, _ := os.ReadFile(filepath.Join(dir, logName)); !bytes.Equal(after, original) {
		t.Fatal("failed startup must not trim the tail or otherwise modify the log")
	}
}

// A changed label value is a genuine conflict, even though both records are
// individually well-formed and pass normalization.
func TestRecoveryRejectsConflictingLabels(t *testing.T) {
	dir := t.TempDir()
	original := writeLog(t, dir,
		rawFrame(t, `[{"id":"e1","service":"svc","severity":"info","message":"m","at":"2026-10-01T09:00:00Z","labels":{"env":"prod"}}]`),
		rawFrame(t, `[{"id":"e1","service":"svc","severity":"info","message":"m","at":"2026-10-01T09:00:00Z","labels":{"env":"stage"}}]`),
		tornTail(),
	)

	if _, _, err := Open(dir); !isCorruption(err) {
		t.Fatalf("different label value must conflict, got %v", err)
	}
	if after, _ := os.ReadFile(filepath.Join(dir, logName)); !bytes.Equal(after, original) {
		t.Fatal("failed startup must leave the log untouched")
	}
}

// Identical content recovered through every spelling the comparison rules
// normalize away loads successfully and de-duplicates to one event; the torn
// tail behind those frames is still trimmed.
func TestRecoveryAcceptsNormalizedDuplicatesAndTrimsTail(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	first := eventFrame(t, events.Event{
		ID: "e1", Service: "svc", Severity: "info", Message: "hello", At: base,
		Labels: map[string]string{"env": "prod", "version": "v2"},
	})
	// Same event with surrounding whitespace, different severity case, a
	// time-zone spelling of the same instant, reordered and padded labels,
	// plus the null/"{}"/absent spellings compared against each other on
	// label-less events.
	equivalent := rawFrame(t, `[
		{"id":" e1 ","service":" svc ","severity":" INFO ","message":" hello ","at":"2026-10-01T17:00:00+08:00","labels":{"version":" v2 ","env":" prod "}},
		{"id":"plain","service":"svc","severity":"info","message":"m","at":"2026-10-01T10:00:00Z","labels":null},
		{"id":"plain","service":"svc","severity":"info","message":"m","at":"2026-10-01T10:00:00Z","labels":{}},
		{"id":"plain","service":"svc","severity":"info","message":"m","at":"2026-10-01T10:00:00Z"}
	]`)
	original := writeLog(t, dir, first, equivalent, tornTail())

	log, batches, err := Open(dir)
	if err != nil {
		t.Fatalf("normalization-equivalent duplicates must recover: %v", err)
	}
	defer log.Close()
	if len(batches) != 2 {
		t.Fatalf("want 2 recovered batches, got %d", len(batches))
	}
	if err := log.Append([]events.Event{testEvent("after", base.Add(time.Hour))}); err != nil {
		t.Fatalf("append after recovery: %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}

	// The torn tail is gone and the new frame lands immediately after the
	// two complete frames; all duplicate content collapses on load.
	log2, recovered, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer log2.Close()
	if len(recovered) != 3 || recovered[2][0].ID != "after" {
		t.Fatalf("trimmed tail must not pollute later frames: %+v", recovered)
	}
	seen := map[string]int{}
	for _, batch := range recovered {
		for _, event := range batch {
			seen[event.ID]++
		}
	}
	if seen["e1"] != 2 || seen["plain"] != 3 || seen["after"] != 1 {
		t.Fatalf("recovered frames keep their records: %v", seen)
	}
	if onDisk, _ := os.ReadFile(filepath.Join(dir, logName)); bytes.Equal(onDisk, original) {
		t.Fatal("successful recovery should have trimmed the torn tail")
	}
}

// A conflict must be detected even when the first sign of trouble is only
// visible in a frame after the torn tail boundary logic has run: validation
// always precedes truncation.
func TestRecoveryConflictErrorDoesNotWrapRecoverySentinel(t *testing.T) {
	dir := t.TempDir()
	writeLog(t, dir,
		rawFrame(t, `[{"id":"x","service":"svc","severity":"info","message":"a","at":"2026-10-01T09:00:00Z"}]`),
		rawFrame(t, `[{"id":"x","service":"svc","severity":"info","message":"b","at":"2026-10-01T09:00:00Z"}]`),
	)
	_, _, err := Open(dir)
	var corrupt *CorruptionError
	if !errors.As(err, &corrupt) {
		t.Fatalf("want *CorruptionError, got %T: %v", err, err)
	}
}
