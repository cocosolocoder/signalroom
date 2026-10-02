package logfile

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cocosolocoder/signalroom/internal/metrics"
)

func openMetricsForTest(t *testing.T, dir string) (*Log, *MetricsLog, [][]metrics.Sample) {
	t.Helper()
	eventLog, _, err := Open(dir)
	if err != nil {
		t.Fatalf("open dir: %v", err)
	}
	mLog, batches, err := OpenMetricsLog(dir)
	if err != nil {
		t.Fatalf("open metrics log: %v", err)
	}
	return eventLog, mLog, batches
}

func metricSample(id string, sec int64, value float64) metrics.Sample {
	return metrics.Sample{
		ID: id, Service: "gw", Name: "requests", At: time.Unix(sec, 0).UTC(), Value: value,
	}
}

func TestMetricsLogAppendAndRecover(t *testing.T) {
	dir := t.TempDir()
	eventLog, mLog, batches := openMetricsForTest(t, dir)
	defer eventLog.Close()
	defer mLog.Close()
	if len(batches) != 0 {
		t.Fatalf("fresh log should have no batches: %d", len(batches))
	}

	if err := mLog.Append([]metrics.Sample{
		metricSample("m1", 10, 1), metricSample("m2", 11, 2),
	}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := mLog.Append([]metrics.Sample{metricSample("m3", 12, 3)}); err != nil {
		t.Fatalf("append: %v", err)
	}

	if err := mLog.Close(); err != nil {
		t.Fatal(err)
	}
	if err := eventLog.Close(); err != nil {
		t.Fatal(err)
	}

	eventLog, mLog, batches = openMetricsForTest(t, dir)
	defer eventLog.Close()
	defer mLog.Close()
	if len(batches) != 2 || len(batches[0]) != 2 || len(batches[1]) != 1 {
		t.Fatalf("recovered shape: %+v", batches)
	}
	if batches[0][0].ID != "m1" || batches[1][0].Value != 3 {
		t.Fatalf("recovered content: %+v", batches)
	}
}

func TestMetricsLogTrimsTornTail(t *testing.T) {
	dir := t.TempDir()
	eventLog, mLog, _ := openMetricsForTest(t, dir)
	if err := mLog.Append([]metrics.Sample{metricSample("good", 10, 1)}); err != nil {
		t.Fatal(err)
	}
	mLog.Close()
	eventLog.Close()

	path := filepath.Join(dir, metricsLogName)
	existing, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte(`[{"id":"torn","service":"gw","name":"requests","at":"2026-10-01T00:00:11Z","value":2}]`)
	torn := make([]byte, headerSize+4)
	putFrameHeader(torn, payload)
	copy(torn[headerSize:], payload[:4])
	if err := os.WriteFile(path, append(append([]byte(nil), existing...), torn...), 0o644); err != nil {
		t.Fatal(err)
	}

	eventLog2, mLog2, batches := openMetricsForTest(t, dir)
	defer eventLog2.Close()
	defer mLog2.Close()
	if len(batches) != 1 || batches[0][0].ID != "good" {
		t.Fatalf("torn tail must be trimmed, recovered: %+v", batches)
	}
}

func TestMetricsLogFailsOnMidCorruption(t *testing.T) {
	dir := t.TempDir()
	eventLog, mLog, _ := openMetricsForTest(t, dir)
	if err := mLog.Append([]metrics.Sample{metricSample("good", 10, 1)}); err != nil {
		t.Fatal(err)
	}
	mLog.Close()
	eventLog.Close()

	path := filepath.Join(dir, metricsLogName)
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	firstLen := headerSize + int(framePrefixLen(original[0:4]))
	payload := []byte(`[{"id":"bad","service":"gw","name":"requests","at":"2026-10-01T00:00:11Z","value":2}]`)
	bad := make([]byte, headerSize+len(payload))
	putFrameHeader(bad, payload)
	copy(bad[headerSize:], payload)
	bad[headerSize] ^= 0xFF // checksum mismatch in a complete middle frame
	corrupt := append(original[:firstLen], bad...)
	if err := os.WriteFile(path, corrupt, 0o644); err != nil {
		t.Fatal(err)
	}

	eventLog2, _, openErr := Open(dir)
	if openErr != nil {
		t.Fatalf("event log must still open: %v", openErr)
	}
	defer eventLog2.Close()
	if _, _, err := OpenMetricsLog(dir); err == nil {
		t.Fatal("corrupt metrics log must prevent startup")
	}

	// The corrupt bytes must be left untouched.
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(corrupt) {
		t.Fatal("failed startup must not modify the corrupt data")
	}
}

func TestMetricsLogFailurePoisons(t *testing.T) {
	dir := t.TempDir()
	eventLog, mLog, _ := openMetricsForTest(t, dir)
	defer eventLog.Close()
	defer mLog.Close()
	if err := mLog.Append([]metrics.Sample{metricSample("m", 10, 1)}); err != nil {
		t.Fatal(err)
	}
	if err := mLog.file.Close(); err != nil {
		t.Fatal(err)
	}
	// The next write must fail and poison every subsequent call.
	if err := mLog.Append([]metrics.Sample{metricSample("x", 11, 1)}); err == nil {
		t.Fatal("write to a closed file must fail")
	}
	if !mLog.Poisoned() {
		t.Fatal("log must be poisoned after a write failure")
	}
	if err := mLog.Append([]metrics.Sample{metricSample("y", 12, 1)}); err == nil {
		t.Fatal("poisoned log must keep failing")
	}
}
