package logfile

import (
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

var errFaultInjectionUnsupported = errors.New("deterministic fault injection unsupported on this platform")

func testEvent(id string, at time.Time) events.Event {
	return events.Event{ID: id, Service: "svc", Severity: "info", Message: "msg-" + id, At: at}
}

func TestAppendAndReopen(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)

	log, batches, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := log.Append([]events.Event{testEvent("a", base), testEvent("b", base.Add(time.Second))}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := log.Append([]events.Event{testEvent("c", base.Add(2*time.Second))}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	log2, batches, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer log2.Close()
	if len(batches) != 2 || len(batches[0]) != 2 || len(batches[1]) != 1 {
		t.Fatalf("unexpected recovered batches: %+v", batches)
	}
	flat := []string{batches[0][0].ID, batches[0][1].ID, batches[1][0].ID}
	if strings.Join(flat, ",") != "a,b,c" {
		t.Fatalf("recovery order: %v", flat)
	}
	if !batches[0][0].At.Equal(base) {
		t.Fatalf("timestamp not preserved absolutely: %v", batches[0][0].At)
	}

	// A new batch after recovery appends cleanly and reads back as frame 3.
	if err := log2.Append([]events.Event{testEvent("d", base.Add(3*time.Second))}); err != nil {
		t.Fatalf("append after recovery: %v", err)
	}
	if err := log2.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	_, batches, err = Open(dir)
	if err != nil {
		t.Fatalf("reopen 2: %v", err)
	}
	if len(batches) != 3 || batches[2][0].ID != "d" {
		t.Fatalf("post-tail-write corrupted recovery: %+v", batches)
	}
}

func TestReplayIgnoresTornTailOnly(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	path := filepath.Join(dir, logName)

	log, _, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := log.Append([]events.Event{testEvent("a", base)}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := log.Append([]events.Event{testEvent("b", base.Add(time.Second))}); err != nil {
		t.Fatalf("append: %v", err)
	}
	log.Close()

	good, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	// Torn payload: full header promising more bytes than exist.
	payload, _ := events.MarshalBatch([]events.Event{testEvent("torn", base.Add(2*time.Second))})
	partialHeader := make([]byte, 5) // also exercises a short partial header
	binary.BigEndian.PutUint32(partialHeader[0:4], uint32(len(payload)))

	cases := map[string][]byte{
		"partial header":  partialHeader,
		"partial payload": frameFor(payload)[:headerSize+3],
		"full header only": func() []byte {
			b := make([]byte, headerSize)
			binary.BigEndian.PutUint32(b[0:4], uint32(len(payload)))
			binary.BigEndian.PutUint32(b[4:8], crc32.ChecksumIEEE(payload))
			return b
		}(),
	}
	for name, tail := range cases {
		t.Run(name, func(t *testing.T) {
			if err := os.WriteFile(path, append(append([]byte(nil), good...), tail...), 0o644); err != nil {
				t.Fatalf("seed torn file: %v", err)
			}
			log2, batches, err := Open(dir)
			if err != nil {
				t.Fatalf("torn tail must be tolerated: %v", err)
			}
			if len(batches) != 2 {
				t.Fatalf("want 2 intact batches, got %d", len(batches))
			}
			// Ignored tail must not pollute the next write.
			if err := log2.Append([]events.Event{testEvent("after", base.Add(3*time.Second))}); err != nil {
				t.Fatalf("append over torn tail: %v", err)
			}
			log2.Close()

			log3, recovered, err := Open(dir)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			if len(recovered) != 3 || recovered[2][0].ID != "after" {
				log3.Close()
				t.Fatalf("tail leaked into later frames: %+v", recovered)
			}
			log3.Close()
		})
	}
}

// frameFor wraps a payload the same way Log.Append does.
func frameFor(payload []byte) []byte {
	frame := make([]byte, headerSize+len(payload))
	binary.BigEndian.PutUint32(frame[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(payload))
	copy(frame[headerSize:], payload)
	return frame
}

func TestReplayFailsOnMidCorruption(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	path := filepath.Join(dir, logName)

	log, _, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := log.Append([]events.Event{testEvent("a", base)}); err != nil {
		t.Fatalf("append: %v", err)
	}
	log.Close()

	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	firstFrameLen := headerSize + int(binary.BigEndian.Uint32(original[0:4]))

	// A complete-looking second frame with a bad checksum.
	payload, _ := events.MarshalBatch([]events.Event{testEvent("x", base.Add(time.Second))})
	bad := frameFor(payload)
	bad[headerSize] ^= 0xFF
	corrupt := append(append([]byte(nil), original[:firstFrameLen]...), bad...)
	if err := os.WriteFile(path, corrupt, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	if _, _, err := Open(dir); !isCorruption(err) {
		t.Fatalf("mid corruption must fail startup, got %v", err)
	}
	if after, _ := os.ReadFile(path); string(after) != string(corrupt) {
		t.Fatal("failed startup must not modify the data file")
	}
}

func isCorruption(err error) bool {
	var target *CorruptionError
	return errors.As(err, &target)
}

func TestReplayFailsOnCompleteIllegalFrame(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	path := filepath.Join(dir, logName)

	log, _, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := log.Append([]events.Event{testEvent("a", base)}); err != nil {
		t.Fatalf("append: %v", err)
	}
	log.Close()

	original, _ := os.ReadFile(path)
	firstFrameLen := headerSize + int(binary.BigEndian.Uint32(original[0:4]))

	// Complete frame with valid checksum carrying an unknown field.
	illegal := []byte(`[{"id":"z","service":"s","severity":"info","message":"m","at":"2026-10-01T09:00:01Z","bogus":true}]`)
	withIllegal := append(append([]byte(nil), original[:firstFrameLen]...), frameFor(illegal)...)
	if err := os.WriteFile(path, withIllegal, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	// A complete frame containing an empty batch is also illegal.
	withEmpty := append(append([]byte(nil), original[:firstFrameLen]...), frameFor([]byte("[]"))...)
	if err := os.WriteFile(path, withEmpty, 0o644); err != nil {
		t.Fatalf("write empty frame: %v", err)
	}
	if _, _, err := Open(dir); !isCorruption(err) {
		t.Fatalf("empty frame must fail startup, got %v", err)
	}
}

func TestAppendFailurePoisonsLog(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)

	log, _, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := log.Append([]events.Event{testEvent("a", base)}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := breakWriter(log); err != nil {
		if errors.Is(err, errFaultInjectionUnsupported) {
			t.Skip("deterministic write fault injection requires /dev/full")
		}
		t.Fatalf("inject fault: %v", err)
	}

	err = log.Append([]events.Event{testEvent("b", base.Add(time.Second))})
	if err == nil {
		t.Fatal("write failure must return an error")
	}
	if !log.Poisoned() {
		t.Fatalf("log must be poisoned after write error")
	}
	for range 3 {
		if err := log.Append([]events.Event{testEvent("c", base.Add(2*time.Second))}); !errors.Is(err, ErrPoisoned) {
			t.Fatalf("later writes must return ErrPoisoned, got %v", err)
		}
	}
}

func breakWriter(log *Log) error {
	full, err := os.OpenFile("/dev/full", os.O_WRONLY, 0)
	if err != nil {
		if os.IsNotExist(err) {
			return errFaultInjectionUnsupported
		}
		return err
	}
	log.mu.Lock()
	old := log.file
	log.file = full
	log.mu.Unlock()
	return old.Close()
}

func TestDirectoryLockExclusiveAndReusable(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)

	first, _, err := Open(dir)
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	if err := first.Append([]events.Event{testEvent("a", base)}); err != nil {
		t.Fatalf("append: %v", err)
	}

	second, batches, err := Open(dir)
	if err == nil {
		second.Close()
		t.Fatal("second instance must fail while the first holds the directory")
	}
	if batches != nil {
		t.Fatal("failed second open must not return recovered data")
	}
	if !strings.Contains(err.Error(), "in use") {
		t.Fatalf("error should explain the directory is in use, got: %v", err)
	}

	// Data written before the failed takeover must be intact.
	if onDisk, _ := os.ReadFile(filepath.Join(dir, logName)); !strings.Contains(string(onDisk), `"a"`) {
		t.Fatal("rejected second instance must not change data")
	}

	if err := first.Close(); err != nil {
		t.Fatalf("close first: %v", err)
	}

	third, batches, err := Open(dir)
	if err != nil {
		t.Fatalf("restart after clean exit must succeed: %v", err)
	}
	defer third.Close()
	if len(batches) != 1 || batches[0][0].ID != "a" {
		t.Fatalf("restart must recover prior data: %+v", batches)
	}
}
