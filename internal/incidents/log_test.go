package incidents

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLogAppendAndReopen(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)

	log, records, err := OpenLog(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("fresh log should have no records: %d", len(records))
	}
	if err := log.Append(record{Kind: kindCreate, ID: "INC-1", Title: "Title", Service: "svc", Operator: "alice", At: base}); err != nil {
		t.Fatalf("append create: %v", err)
	}
	if err := log.Append(record{Kind: kindAction, ID: "INC-1", ActionID: "ACT-1", Action: ActionAddNote, Operator: "bob", ExpectedVersion: 1, Content: "note", At: base.Add(time.Second)}); err != nil {
		t.Fatalf("append action: %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	log2, records, err := OpenLog(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer log2.Close()
	if len(records) != 2 {
		t.Fatalf("recovered records: %d", len(records))
	}
	if records[0].Kind != kindCreate || records[0].ID != "INC-1" || records[0].Title != "Title" {
		t.Fatalf("create record: %+v", records[0])
	}
	if records[1].Kind != kindAction || records[1].ActionID != "ACT-1" || records[1].Content != "note" {
		t.Fatalf("action record: %+v", records[1])
	}
}

func TestLogReplayIntoStore(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)

	log, _, err := OpenLog(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	_ = log.Append(record{Kind: kindCreate, ID: "INC-1", Title: "Title", Service: "svc", Operator: "alice", At: base})
	_ = log.Append(record{Kind: kindAction, ID: "INC-1", ActionID: "ACT-1", Action: ActionResolve, Operator: "bob", ExpectedVersion: 1, Content: "fixed", At: base.Add(time.Second)})
	log.Close()

	log2, records, err := OpenLog(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer log2.Close()

	s := NewStore(log2, nil)
	if err := s.Load(records); err != nil {
		t.Fatalf("load: %v", err)
	}
	inc, err := s.Get("INC-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if inc.Status != StatusResolved || inc.Version != 2 || len(inc.History) != 2 {
		t.Fatalf("replayed incident: %+v", inc)
	}
	if inc.History[1].Content != "fixed" || inc.History[1].ExpectedVersion != 1 {
		t.Fatalf("replayed entry: %+v", inc.History[1])
	}
}

func TestLogTornTailDiscarded(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)

	log, _, err := OpenLog(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	_ = log.Append(record{Kind: kindCreate, ID: "INC-1", Title: "Title", Service: "svc", Operator: "alice", At: base})
	log.Close()

	// Append a torn partial frame (full header, truncated payload).
	payload := []byte(`{"kind":"action","id":"INC-1","action_id":"ACT-1"}`)
	torn := make([]byte, headerSize+4)
	binary.BigEndian.PutUint32(torn[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(torn[4:8], crc32.ChecksumIEEE(payload))
	copy(torn[headerSize:], payload[:4])
	path := filepath.Join(dir, logName)
	existing, _ := os.ReadFile(path)
	if err := os.WriteFile(path, append(existing, torn...), 0o644); err != nil {
		t.Fatalf("write torn: %v", err)
	}

	log2, records, err := OpenLog(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer log2.Close()
	if len(records) != 1 || records[0].ID != "INC-1" {
		t.Fatalf("torn tail should be discarded, got %+v", records)
	}
}

func TestLogCorruptionFatal(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)

	log, _, err := OpenLog(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	_ = log.Append(record{Kind: kindCreate, ID: "INC-1", Title: "Title", Service: "svc", Operator: "alice", At: base})
	log.Close()

	// Corrupt the checksum of the complete frame.
	path := filepath.Join(dir, logName)
	data, _ := os.ReadFile(path)
	data[headerSize] ^= 0xFF
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write corrupt: %v", err)
	}

	if _, _, err := OpenLog(dir); err == nil {
		t.Fatal("corrupt log must be fatal")
	}
}

func TestLogMissingDirCreated(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "data")
	log, records, err := OpenLog(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer log.Close()
	if len(records) != 0 {
		t.Fatalf("records: %d", len(records))
	}
}
