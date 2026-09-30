package events

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func openTestStore(t *testing.T) (*PersistentStore, string) {
	t.Helper()
	dir := t.TempDir()
	ps, err := OpenStore(dir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { ps.Close() })
	return ps, dir
}

func sampleEvent(id string, at time.Time) Event {
	return Event{ID: id, Service: "gateway", Severity: "critical", Message: "error rate spike", At: at}
}

func TestPersistentStoreAppendsAndReopens(t *testing.T) {
	ps, dir := openTestStore(t)
	at := time.Date(2026, time.October, 1, 10, 0, 0, 123456789, time.UTC)
	ev := sampleEvent("evt-1", at)

	created, replayed, err := ps.AppendBatch([]Event{ev})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if created != 1 || replayed != 0 {
		t.Fatalf("got created=%d replayed=%d, want 1/0", created, replayed)
	}
	ps.Close()

	ps2, err := OpenStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer ps2.Close()

	got := ps2.Query(Query{})
	if len(got) != 1 {
		t.Fatalf("got %d events, want 1", len(got))
	}
	if got[0].ID != "evt-1" || !got[0].At.Equal(at) {
		t.Fatalf("unexpected event: %#v", got[0])
	}

	// Replaying the same event is idempotent.
	created, replayed, err = ps2.AppendBatch([]Event{ev})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if created != 0 || replayed != 1 {
		t.Fatalf("got created=%d replayed=%d, want 0/1", created, replayed)
	}
	if len(ps2.Query(Query{})) != 1 {
		t.Fatalf("replay changed event count")
	}

	// A different event with the same id conflicts.
	conflict := sampleEvent("evt-1", at)
	conflict.Message = "different message"
	created, replayed, err = ps2.AppendBatch([]Event{conflict})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("got err=%v, want ErrConflict", err)
	}
	if created != 0 || replayed != 0 {
		t.Fatalf("conflict wrote data: created=%d replayed=%d", created, replayed)
	}
	if len(ps2.Query(Query{})) != 1 {
		t.Fatalf("conflict changed event count")
	}
}

func TestPersistentStoreNormalizesAndDedupesBatch(t *testing.T) {
	ps, _ := openTestStore(t)
	at := time.Date(2026, time.October, 1, 10, 0, 0, 0, time.UTC)

	// Whitespace and case are normalized before storage.
	ev := Event{ID: "  evt-1 ", Service: "  gateway ", Severity: " CRITICAL ", Message: "  spike ", At: at}
	created, _, err := ps.AppendBatch([]Event{ev})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if created != 1 {
		t.Fatalf("created=%d, want 1", created)
	}
	got := ps.Query(Query{})[0]
	if got.ID != "evt-1" || got.Service != "gateway" || got.Severity != "critical" || got.Message != "spike" {
		t.Fatalf("not normalized: %#v", got)
	}

	// Batch-internal duplicates (after normalization) conflict and write nothing.
	dup := []Event{
		{ID: "evt-2", Service: "api", Severity: "info", Message: "a", At: at},
		{ID: " evt-2", Service: "api", Severity: "info", Message: "a", At: at},
	}
	created, replayed, err := ps.AppendBatch(dup)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("got err=%v, want ErrConflict", err)
	}
	if created != 0 || replayed != 0 {
		t.Fatalf("dup wrote data: created=%d replayed=%d", created, replayed)
	}
	if len(ps.Query(Query{})) != 1 {
		t.Fatalf("dup changed event count")
	}

	// Invalid events fail validation before any conflict check.
	invalid := []Event{
		{ID: "evt-3", Service: "api", Severity: "info", Message: "ok", At: at},
		{ID: "evt-3", Service: "api", Severity: "info", Message: "ok", At: at},
	}
	invalid[1].ID = ""
	_, _, err = ps.AppendBatch(invalid)
	if !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("got err=%v, want ErrInvalidEvent", err)
	}
	if len(ps.Query(Query{})) != 1 {
		t.Fatalf("invalid batch changed event count")
	}
}

func TestOpenStoreIgnoresIncompleteTail(t *testing.T) {
	ps, dir := openTestStore(t)
	at := time.Date(2026, time.October, 1, 10, 0, 0, 0, time.UTC)
	ev1 := sampleEvent("evt-1", at)
	ev2 := sampleEvent("evt-2", at.Add(time.Minute))
	if _, _, err := ps.AppendBatch([]Event{ev1, ev2}); err != nil {
		t.Fatalf("append: %v", err)
	}
	ps.Close()

	// Append a partial frame (simulating a crashed write).
	frame, err := encodeFrame([]Event{sampleEvent("evt-3", at.Add(2*time.Minute))})
	if err != nil {
		t.Fatalf("encode frame: %v", err)
	}
	logPath := filepath.Join(dir, logFileName)
	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("open log: %v", err)
	}
	if _, err := f.Write(frame[:len(frame)/2]); err != nil {
		t.Fatalf("write partial frame: %v", err)
	}
	f.Close()

	before, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read log: %v", err)
	}

	ps2, err := OpenStore(dir)
	if err != nil {
		t.Fatalf("open with incomplete tail: %v", err)
	}
	defer ps2.Close()

	got := ps2.Query(Query{})
	if len(got) != 2 {
		t.Fatalf("got %d events, want 2", len(got))
	}

	// The tail was truncated and did not pollute subsequent writes.
	after, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read log after open: %v", err)
	}
	if len(after) != len(before)-len(frame)/2 {
		t.Fatalf("tail not truncated: before=%d after=%d", len(before), len(after))
	}

	ev3 := sampleEvent("evt-3", at.Add(2*time.Minute))
	if _, _, err := ps2.AppendBatch([]Event{ev3}); err != nil {
		t.Fatalf("append after recovery: %v", err)
	}
	ps2.Close()

	ps3, err := OpenStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer ps3.Close()
	if len(ps3.Query(Query{})) != 3 {
		t.Fatalf("got %d events, want 3", len(ps3.Query(Query{})))
	}
}

func TestOpenStoreFailsOnCorruption(t *testing.T) {
	ps, dir := openTestStore(t)
	at := time.Date(2026, time.October, 1, 10, 0, 0, 0, time.UTC)
	if _, _, err := ps.AppendBatch([]Event{sampleEvent("evt-1", at)}); err != nil {
		t.Fatalf("append: %v", err)
	}
	ps.Close()

	logPath := filepath.Join(dir, logFileName)
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	// Flip a byte in the middle of the payload.
	corrupt := make([]byte, len(data))
	copy(corrupt, data)
	corrupt[len(corrupt)/2] ^= 0xff
	if err := os.WriteFile(logPath, corrupt, 0o644); err != nil {
		t.Fatalf("write corrupt log: %v", err)
	}

	if _, err := OpenStore(dir); err == nil {
		t.Fatal("expected OpenStore to fail on corrupt log")
	}

	// The on-disk data was not modified by the failed open.
	after, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read log after failed open: %v", err)
	}
	if !bytes.Equal(after, corrupt) {
		t.Fatal("failed open modified the data")
	}
}

func TestOpenStoreFailsOnIllegalData(t *testing.T) {
	at := time.Date(2026, time.October, 1, 10, 0, 0, 0, time.UTC)

	for _, tc := range []struct {
		name    string
		payload []byte
	}{
		{"invalid json", []byte(`{not json}`)},
		{"duplicate ids", mustEncodeEvents(t, []Event{
			{ID: "evt-1", Service: "api", Severity: "info", Message: "a", At: at},
			{ID: " evt-1", Service: "api", Severity: "info", Message: "a", At: at},
		})},
		{"invalid event", mustEncodeEvents(t, []Event{
			{ID: "", Service: "api", Severity: "info", Message: "a", At: at},
		})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.MkdirAll(dir, 0o755); err != nil {
				t.Fatal(err)
			}
			logPath := filepath.Join(dir, logFileName)
			frame := mustEncodeFrameRaw(t, tc.payload)
			if err := os.WriteFile(logPath, frame, 0o644); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(logPath)
			if err != nil {
				t.Fatal(err)
			}

			if _, err := OpenStore(dir); err == nil {
				t.Fatal("expected OpenStore to fail on illegal data")
			}

			after, err := os.ReadFile(logPath)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(after, before) {
				t.Fatal("failed open modified the data")
			}
		})
	}
}

func TestStoreLock(t *testing.T) {
	dir := t.TempDir()
	ps1, err := OpenStore(dir)
	if err != nil {
		t.Fatalf("first open: %v", err)
	}

	if _, err := OpenStore(dir); err == nil {
		t.Fatal("expected second open to fail while locked")
	}

	ps1.Close()

	ps2, err := OpenStore(dir)
	if err != nil {
		t.Fatalf("open after close: %v", err)
	}
	ps2.Close()
}

func TestStoreBrokenFlag(t *testing.T) {
	ps, _ := openTestStore(t)
	at := time.Date(2026, time.October, 1, 10, 0, 0, 0, time.UTC)

	ps.broken = true
	_, _, err := ps.AppendBatch([]Event{sampleEvent("evt-1", at)})
	if !errors.Is(err, ErrStoreBroken) {
		t.Fatalf("got err=%v, want ErrStoreBroken", err)
	}
}

// TestStoreWriteFailureMarksBroken runs in a subprocess with RLIMIT_FSIZE
// set to 0 so every file write fails. The store must mark itself broken and
// stay broken across subsequent calls.
func TestStoreWriteFailureMarksBroken(t *testing.T) {
	if os.Getenv("SIGNALROOM_TEST_RLIMIT") == "1" {
		if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &syscall.Rlimit{Cur: 0, Max: 0}); err != nil {
			t.Fatalf("setrlimit: %v", err)
		}
		signal.Ignore(syscall.SIGXFSZ)

		dir := t.TempDir()
		ps, err := OpenStore(dir)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		at := time.Date(2026, time.October, 1, 10, 0, 0, 0, time.UTC)
		_, _, err = ps.AppendBatch([]Event{sampleEvent("evt-1", at)})
		if !errors.Is(err, ErrStoreBroken) {
			t.Fatalf("first append err=%v, want ErrStoreBroken", err)
		}
		if !ps.Broken() {
			t.Fatal("store not marked broken")
		}
		_, _, err = ps.AppendBatch([]Event{sampleEvent("evt-2", at)})
		if !errors.Is(err, ErrStoreBroken) {
			t.Fatalf("second append err=%v, want ErrStoreBroken", err)
		}
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestStoreWriteFailureMarksBroken")
	cmd.Env = append(os.Environ(), "SIGNALROOM_TEST_RLIMIT=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("subprocess failed: %v\n%s", err, out)
	}
}

func mustEncodeEvents(t *testing.T, events []Event) []byte {
	t.Helper()
	payload, err := encodeEvents(events)
	if err != nil {
		t.Fatalf("encode events: %v", err)
	}
	return payload
}

func mustEncodeFrameRaw(t *testing.T, payload []byte) []byte {
	t.Helper()
	frame, err := encodeFrameRaw(payload)
	if err != nil {
		t.Fatalf("encode frame: %v", err)
	}
	return frame
}

func encodeFrameRaw(payload []byte) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteString(frameMagic)
	buf.WriteByte(frameVersion)
	if err := binary.Write(&buf, binary.BigEndian, uint64(len(payload))); err != nil {
		return nil, err
	}
	header := buf.Bytes()
	if err := binary.Write(&buf, binary.BigEndian, crc32.ChecksumIEEE(header)); err != nil {
		return nil, err
	}
	buf.Write(payload)
	if err := binary.Write(&buf, binary.BigEndian, crc32.ChecksumIEEE(payload)); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
