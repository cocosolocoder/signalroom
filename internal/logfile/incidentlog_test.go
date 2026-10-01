package logfile

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cocosolocoder/signalroom/internal/incidents"
)

// putFrameHeader writes the length/CRC prefix for payload into the first
// headerSize bytes of frame.
func putFrameHeader(frame, payload []byte) {
	binary.BigEndian.PutUint32(frame[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(payload))
}

// framePrefixLen reads the payload length out of a frame's length prefix.
func framePrefixLen(prefix []byte) int64 {
	return int64(binary.BigEndian.Uint32(prefix))
}

// openIncidentForTest acquires the directory lock via Open and then opens
// the incident log, mirroring serve.go ordering.
func openIncidentForTest(t *testing.T, dir string) (*Log, *IncidentLog, []incidents.Record) {
	t.Helper()
	eventLog, _, err := Open(dir)
	if err != nil {
		t.Fatalf("open dir: %v", err)
	}
	incLog, records, err := OpenIncidentLog(dir)
	if err != nil {
		t.Fatalf("open incident log: %v", err)
	}
	return eventLog, incLog, records
}

func creationRecord(id string, at time.Time) incidents.Record {
	return incidents.NewCreationRecord(
		incidents.Creation{ID: id, Title: "t", Service: "svc", Operator: "alice"}, at)
}

func TestIncidentLogAppendAndRecover(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)

	eventLog, incLog, records := openIncidentForTest(t, dir)
	if len(records) != 0 {
		t.Fatalf("fresh log should have no records: %d", len(records))
	}
	if err := incLog.AppendRecord(creationRecord("I1", base)); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := incLog.AppendRecord(incidents.NewActionRecord(incidents.Request{
		IncidentID: "I1", ActionID: "a1", Operator: "bob", ExpectedVersion: 1,
		Type: incidents.ActionResolve, Content: "fixed",
	}, base.Add(time.Second))); err != nil {
		t.Fatalf("append action: %v", err)
	}
	if err := incLog.Close(); err != nil {
		t.Fatalf("close incident: %v", err)
	}
	if err := eventLog.Close(); err != nil {
		t.Fatalf("close event log: %v", err)
	}

	eventLog2, incLog2, recovered := openIncidentForTest(t, dir)
	defer incLog2.Close()
	defer eventLog2.Close()
	if len(recovered) != 2 {
		t.Fatalf("recovered %d records", len(recovered))
	}
	if recovered[0].Kind != incidents.RecordCreation || recovered[0].Creation.ID != "I1" {
		t.Fatalf("record 0 %+v", recovered[0])
	}
	if recovered[1].Kind != incidents.RecordAction || recovered[1].Action.Request.ActionID != "a1" {
		t.Fatalf("record 1 %+v", recovered[1])
	}
	if !recovered[0].At.Equal(base) {
		t.Fatalf("timestamp not preserved: %v", recovered[0].At)
	}
}

func TestIncidentLogTornTailTrimmed(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)

	eventLog, incLog, _ := openIncidentForTest(t, dir)
	if err := incLog.AppendRecord(creationRecord("I1", base)); err != nil {
		t.Fatal(err)
	}
	if err := incLog.Close(); err != nil {
		t.Fatal(err)
	}
	if err := eventLog.Close(); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(dir, incidentLogName)
	existing, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := incidents.MarshalRecord(creationRecord("I2", base.Add(time.Second)))
	if err != nil {
		t.Fatal(err)
	}
	torn := make([]byte, headerSize+4)
	putFrameHeader(torn, payload)
	if err := os.WriteFile(path, append(existing, torn...), 0o644); err != nil {
		t.Fatal(err)
	}

	eventLog2, incLog2, recovered := openIncidentForTest(t, dir)
	if len(recovered) != 1 || recovered[0].Creation.ID != "I1" {
		t.Fatalf("torn tail must be discarded, got %+v", recovered)
	}

	// A write after trimming lands cleanly.
	if err := incLog2.AppendRecord(creationRecord("I3", base.Add(2*time.Second))); err != nil {
		t.Fatalf("append after trim: %v", err)
	}
	if err := incLog2.Close(); err != nil {
		t.Fatal(err)
	}
	if err := eventLog2.Close(); err != nil {
		t.Fatal(err)
	}
	eventLog3, incLog3, recovered3 := openIncidentForTest(t, dir)
	defer incLog3.Close()
	defer eventLog3.Close()
	if len(recovered3) != 2 || recovered3[1].Creation.ID != "I3" {
		t.Fatalf("post-trim recovery %+v", recovered3)
	}
}

func TestIncidentLogMidCorruptionFails(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)

	eventLog, incLog, _ := openIncidentForTest(t, dir)
	if err := incLog.AppendRecord(creationRecord("I1", base)); err != nil {
		t.Fatal(err)
	}
	if err := incLog.Close(); err != nil {
		t.Fatal(err)
	}
	if err := eventLog.Close(); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(dir, incidentLogName)
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	firstLen := int64(headerSize) + framePrefixLen(original[0:4])

	payload, _ := incidents.MarshalRecord(creationRecord("I2", base.Add(time.Second)))
	bad := make([]byte, headerSize+len(payload))
	putFrameHeader(bad, payload)
	bad[headerSize] ^= 0xFF // complete frame, bad checksum
	corrupt := append(append([]byte{}, original[:firstLen]...), bad...)
	if err := os.WriteFile(path, corrupt, 0o644); err != nil {
		t.Fatal(err)
	}

	eventLog2, _, err := Open(dir)
	if err != nil {
		t.Fatalf("event log should still open: %v", err)
	}
	defer eventLog2.Close()
	if _, _, err := OpenIncidentLog(dir); err == nil {
		t.Fatal("corrupt incident log must fail startup")
	}

	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(corrupt) {
		t.Fatal("failed startup must not truncate or modify the corrupt log")
	}
}

func TestIncidentLogPoisonedAfterWriteFailure(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	eventLog, incLog, _ := openIncidentForTest(t, dir)
	defer eventLog.Close()

	if err := incLog.AppendRecord(creationRecord("I1", base)); err != nil {
		t.Fatal(err)
	}
	if err := incLog.file.Close(); err != nil { // force later writes to fail
		t.Fatal(err)
	}
	err := incLog.AppendRecord(creationRecord("I2", base.Add(time.Second)))
	if err == nil {
		t.Fatal("writing to a closed file descriptor must fail")
	}
	if !incLog.Poisoned() {
		t.Fatal("a write failure must poison the log")
	}
	if err := incLog.AppendRecord(creationRecord("I3", base.Add(2*time.Second))); err == nil {
		t.Fatal("poisoned log must keep failing")
	}
}
