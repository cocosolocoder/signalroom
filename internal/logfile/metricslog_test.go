package logfile

import (
	"os"
	"path/filepath"
	"strings"
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

// metricFrame builds one complete on-disk frame around a raw JSON payload.
func metricFrame(t *testing.T, payload string) []byte {
	t.Helper()
	raw := []byte(payload)
	frame := make([]byte, headerSize+len(raw))
	putFrameHeader(frame, raw)
	copy(frame[headerSize:], raw)
	return frame
}

// tornMetricFrame builds the frame header plus only the first payload bytes,
// simulating a write that never fully landed.
func tornMetricFrame(t *testing.T, payload string, keep int) []byte {
	t.Helper()
	raw := []byte(payload)
	if keep > len(raw) {
		keep = len(raw)
	}
	frame := make([]byte, headerSize+keep)
	putFrameHeader(frame, raw)
	copy(frame[headerSize:], raw[:keep])
	return frame
}

// writeMetricsLog seeds the metrics log directly, bypassing the writer.
func writeMetricsLog(t *testing.T, dir string, content []byte) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, metricsLogName), content, 0o644); err != nil {
		t.Fatal(err)
	}
}

// assertConflictOpen opens the metrics log against a seeded file, expects a
// conflict failure that names the sample clash, and proves the file is
// byte-identical afterwards (torn tail included).
func assertConflictOpen(t *testing.T, dir string, original []byte) {
	t.Helper()
	eventLog, _, err := Open(dir)
	if err != nil {
		t.Fatalf("event log must still open: %v", err)
	}
	defer eventLog.Close()

	_, _, openErr := OpenMetricsLog(dir)
	if openErr == nil {
		t.Fatal("conflicting metrics history must prevent startup")
	}
	if !metrics.IsConflictError(openErr) {
		t.Fatalf("failure must be a sample conflict, got: %v", openErr)
	}
	if !containsAll(openErr.Error(), "sample conflict") {
		t.Fatalf("error must state the metrics history has a sample conflict: %v", openErr)
	}

	after, err := os.ReadFile(filepath.Join(dir, metricsLogName))
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(original) {
		t.Fatalf("failed startup must leave every byte in place:\nwant %x\n got %x", original, after)
	}
}

func containsAll(haystack string, needles ...string) bool {
	for _, needle := range needles {
		if !strings.Contains(haystack, needle) {
			return false
		}
	}
	return true
}

// TestMetricsLogIDConflictAcrossBatchesKeepsTornTail is the core regression:
// two complete, individually readable batches carry the same id with
// different content, and a torn frame trails them. Startup must fail before
// the tail is trimmed, leaving the full original file for the operator.
func TestMetricsLogIDConflictAcrossBatchesKeepsTornTail(t *testing.T) {
	dir := t.TempDir()
	first := metricFrame(t, `[{"id":"m1","service":"gw","name":"requests","at":"2026-10-01T10:00:00Z","value":1}]`)
	second := metricFrame(t, `[{"id":"m1","service":"gw","name":"requests","at":"2026-10-01T10:00:00Z","value":2}]`)
	torn := tornMetricFrame(t, `[{"id":"torn","service":"gw","name":"requests","at":"2026-10-01T10:02:00Z","value":9}]`, 4)
	content := append(append(append([]byte(nil), first...), second...), torn...)
	writeMetricsLog(t, dir, content)

	assertConflictOpen(t, dir, content)
}

// TestMetricsLogIDConflictWithoutTornTail fails identically when the file
// ends on a complete frame: no tail exists, yet startup must still refuse.
func TestMetricsLogIDConflictWithoutTornTail(t *testing.T) {
	dir := t.TempDir()
	first := metricFrame(t, `[{"id":"m1","service":"gw","name":"requests","at":"2026-10-01T10:00:00Z","value":1}]`)
	second := metricFrame(t, `[{"id":"m1","service":"gw","name":"requests","at":"2026-10-01T10:00:00Z","value":2}]`)
	content := append(append([]byte(nil), first...), second...)
	writeMetricsLog(t, dir, content)

	assertConflictOpen(t, dir, content)
}

// TestMetricsLogSeriesTimeConflictAcrossBatches rejects two different ids on
// the same series at the same instant even when the readings are equal,
// whether or not a torn tail trails them.
func TestMetricsLogSeriesTimeConflictAcrossBatches(t *testing.T) {
	for _, withTorn := range []bool{false, true} {
		dir := t.TempDir()
		first := metricFrame(t, `[{"id":"m1","service":"gw","name":"requests","at":"2026-10-01T10:00:00Z","value":7}]`)
		second := metricFrame(t, `[{"id":"m2","service":"gw","name":"requests","at":"2026-10-01T10:00:00Z","value":7}]`)
		content := append(append([]byte(nil), first...), second...)
		if withTorn {
			content = append(content, tornMetricFrame(t,
				`[{"id":"torn","service":"gw","name":"requests","at":"2026-10-01T10:02:00Z","value":9}]`, 4)...)
		}
		writeMetricsLog(t, dir, content)

		assertConflictOpen(t, dir, content)
	}
}

// TestMetricsLogConflictWithinOneBatch rejects a clash inside a single
// complete batch: per-batch readability does not make the history legal.
func TestMetricsLogConflictWithinOneBatch(t *testing.T) {
	dir := t.TempDir()
	content := metricFrame(t, `[
		{"id":"m1","service":"gw","name":"requests","at":"2026-10-01T10:00:00Z","value":7},
		{"id":"m2","service":"gw","name":"requests","at":"2026-10-01T10:00:00Z","value":7}
	]`)
	writeMetricsLog(t, dir, content)

	assertConflictOpen(t, dir, content)
}

// TestMetricsLogToleratesNormalizedDuplicates keeps the normal recovery
// rules: same-id/same-content repeats across batches collapse to one sample,
// timezone spelling, label order, trimmable whitespace, and 1 vs 1.0 do not
// clash, and a one-nanosecond move or differing label set lands on a
// different position/series. With no conflict, the trailing torn frame is
// the only thing discarded and the confirmed samples recover.
func TestMetricsLogToleratesNormalizedDuplicates(t *testing.T) {
	dir := t.TempDir()
	frames := []byte(nil)
	// Same id and normalized content, rewritten with a timezone offset,
	// reordered/padded labels, and value 1 vs 1.0: a legal replay.
	frames = append(frames, metricFrame(t, `[
		{"id":" m1 ","service":" gw ","name":" requests ","at":"2026-10-01T10:00:00Z","value":1,"labels":{"env":"prod","zone":"a"}}
	]`)...)
	frames = append(frames, metricFrame(t, `[
		{"id":"m1","service":"gw","name":"requests","at":"2026-10-01T10:00:00+00:00","value":1.0,"labels":{" zone ":" a "," env ":" prod "}}
	]`)...)
	// One nanosecond later is a distinct point even for the same series.
	frames = append(frames, metricFrame(t, `[{"id":"m2","service":"gw","name":"requests","at":"2026-10-01T10:00:00.000000001Z","value":7}]`)...)
	// A different complete label set is a different series, so the same
	// instant under a new id is allowed.
	frames = append(frames, metricFrame(t, `[{"id":"m3","service":"gw","name":"requests","at":"2026-10-01T10:00:00Z","value":8,"labels":{"env":"canary"}}]`)...)
	// Torn tail: the only bytes recovery may drop.
	frames = append(frames, tornMetricFrame(t,
		`[{"id":"torn","service":"gw","name":"requests","at":"2026-10-01T10:02:00Z","value":9}]`, 6)...)
	original := append([]byte(nil), frames...)
	writeMetricsLog(t, dir, frames)

	eventLog, _, err := Open(dir)
	if err != nil {
		t.Fatalf("open dir: %v", err)
	}
	defer eventLog.Close()
	mLog, batches, err := OpenMetricsLog(dir)
	if err != nil {
		t.Fatalf("normalized duplicates must recover, got: %v", err)
	}
	defer mLog.Close()

	if len(batches) != 4 {
		t.Fatalf("four complete batches recover, torn tail dropped: %d", len(batches))
	}
	// Loading into a store must collapse the identical retry to one sample,
	// proving recovery applies the same de-duplication as serving.
	store := metrics.NewStore()
	ids := map[string]struct{}{}
	for _, batch := range batches {
		for _, sample := range batch {
			if err := store.Load(sample); err != nil {
				t.Fatalf("recovered sample must load: %v", err)
			}
			ids[sample.ID] = struct{}{}
		}
	}
	if len(ids) != 3 {
		t.Fatalf("m1 replay counts once alongside m2 and m3: %v", ids)
	}

	// Exactly the torn tail was removed; all confirmed frames remain.
	after, err := os.ReadFile(filepath.Join(dir, metricsLogName))
	if err != nil {
		t.Fatal(err)
	}
	if len(after) >= len(original) {
		t.Fatalf("only the torn tail may be trimmed: before=%d after=%d", len(original), len(after))
	}
	if string(original[:len(after)]) != string(after) {
		t.Fatal("truncation must remove only the trailing torn bytes")
	}
}
