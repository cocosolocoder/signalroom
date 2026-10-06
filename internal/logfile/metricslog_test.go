package logfile

import (
	"bytes"
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

// writeRawMetricsLog seeds the directory with a metrics.log made of the given
// prebuilt frames and returns every byte written.
func writeRawMetricsLog(t *testing.T, dir string, frames ...[]byte) []byte {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	var content []byte
	for _, frame := range frames {
		content = append(content, frame...)
	}
	if err := os.WriteFile(filepath.Join(dir, metricsLogName), content, 0o644); err != nil {
		t.Fatal(err)
	}
	return content
}

// openDirForMetrics acquires the directory lock the way startup does, so the
// metrics open attempt sees the same state as serving.
func openDirForMetrics(t *testing.T, dir string) *Log {
	t.Helper()
	eventLog, _, err := Open(dir)
	if err != nil {
		t.Fatalf("open dir: %v", err)
	}
	return eventLog
}

func TestMetricsLogFailsOnIDConflictAcrossBatchesWithoutMutation(t *testing.T) {
	dir := t.TempDir()
	// Two complete, individually readable batches disagree on id "dup"; a
	// torn frame trails them. The clash must be rejected before the tail is
	// trimmed, leaving every byte in place.
	first := frameFor([]byte(`[{"id":"dup","service":"gw","name":"requests","at":"2026-10-01T00:00:10Z","value":1}]`))
	second := frameFor([]byte(`[{"id":"dup","service":"gw","name":"requests","at":"2026-10-01T00:00:10Z","value":2}]`))
	tornPayload := []byte(`[{"id":"torn","service":"gw","name":"requests","at":"2026-10-01T00:00:11Z","value":3}]`)
	torn := frameFor(tornPayload)[:headerSize+4]
	content := writeRawMetricsLog(t, dir, first, second, torn)

	eventLog := openDirForMetrics(t, dir)
	defer eventLog.Close()
	if _, _, err := OpenMetricsLog(dir); err == nil {
		t.Fatal("conflicting metric history must fail startup")
	} else {
		if !metrics.IsConflictError(err) {
			t.Fatalf("want a conflict error, got %v", err)
		}
		if !strings.Contains(err.Error(), "conflicting samples") {
			t.Fatalf("error must state metric history has sample conflicts: %v", err)
		}
	}
	if after, err := os.ReadFile(filepath.Join(dir, metricsLogName)); err != nil || !bytes.Equal(after, content) {
		t.Fatalf("failed startup must not modify the log, torn tail included: %v", err)
	}
}

func TestMetricsLogFailsOnConflictWithoutTornTail(t *testing.T) {
	dir := t.TempDir()
	// No unfinished suffix at all: the rejection must be identical.
	first := frameFor([]byte(`[{"id":"dup","service":"gw","name":"requests","at":"2026-10-01T00:00:10Z","value":1}]`))
	second := frameFor([]byte(`[{"id":"dup","service":"gw","name":"requests","at":"2026-10-01T00:00:10Z","value":2}]`))
	content := writeRawMetricsLog(t, dir, first, second)

	eventLog := openDirForMetrics(t, dir)
	defer eventLog.Close()
	_, _, err := OpenMetricsLog(dir)
	if err == nil {
		t.Fatal("conflicting complete records must fail startup even without a torn tail")
	}
	if !metrics.IsConflictError(err) {
		t.Fatalf("want a conflict error, got %v", err)
	}
	if after, err := os.ReadFile(filepath.Join(dir, metricsLogName)); err != nil || !bytes.Equal(after, content) {
		t.Fatalf("failed startup must not modify the log: %v", err)
	}
}

func TestMetricsLogFailsOnSeriesTimeConflictWithinOneBatch(t *testing.T) {
	dir := t.TempDir()
	// Two different ids claim the exact same (service, name, labels, time)
	// point inside one complete batch, even though the readings are equal.
	clash := frameFor([]byte(`[` +
		`{"id":"a","service":"gw","name":"requests","at":"2026-10-01T00:00:10Z","value":5},` +
		`{"id":"b","service":"gw","name":"requests","at":"2026-10-01T00:00:10Z","value":5}` +
		`]`))
	tornPayload := []byte(`[{"id":"torn","service":"gw","name":"requests","at":"2026-10-01T00:00:11Z","value":3}]`)
	torn := frameFor(tornPayload)[:headerSize+2]
	content := writeRawMetricsLog(t, dir, clash, torn)

	eventLog := openDirForMetrics(t, dir)
	defer eventLog.Close()
	_, _, err := OpenMetricsLog(dir)
	if err == nil {
		t.Fatal("a series/time clash inside one batch must fail startup")
	}
	if !metrics.IsConflictError(err) {
		t.Fatalf("want a conflict error, got %v", err)
	}
	if after, err := os.ReadFile(filepath.Join(dir, metricsLogName)); err != nil || !bytes.Equal(after, content) {
		t.Fatalf("failed startup must not modify the log, torn tail included: %v", err)
	}
}

func TestMetricsLogFailsOnSeriesTimeConflictAcrossBatches(t *testing.T) {
	dir := t.TempDir()
	// Each batch reads on its own; only the cross-batch view exposes that
	// two different ids occupy one sampling position of one series.
	first := frameFor([]byte(`[{"id":"a","service":"gw","name":"requests","at":"2026-10-01T00:00:10Z","value":5}]`))
	second := frameFor([]byte(`[{"id":"b","service":"gw","name":"requests","at":"2026-10-01T00:00:10Z","value":9}]`))
	content := writeRawMetricsLog(t, dir, first, second)

	eventLog := openDirForMetrics(t, dir)
	defer eventLog.Close()
	_, _, err := OpenMetricsLog(dir)
	if err == nil {
		t.Fatal("a series/time clash across batches must fail startup")
	}
	if !metrics.IsConflictError(err) {
		t.Fatalf("want a conflict error, got %v", err)
	}
	if after, err := os.ReadFile(filepath.Join(dir, metricsLogName)); err != nil || !bytes.Equal(after, content) {
		t.Fatalf("failed startup must not modify the log: %v", err)
	}
}

func TestMetricsLogToleratesEquivalentDuplicates(t *testing.T) {
	dir := t.TempDir()
	// The same sample spelled differently across complete batches: timezone
	// offset, label order, surrounding whitespace, and 1 vs 1.0 are not
	// conflicts. Identical repeats collapse to one sample. A torn tail still
	// gets trimmed.
	first := frameFor([]byte(`[` +
		`{"id":"m1","service":" gw ","name":"requests","at":"2026-10-01T02:00:00+02:00","value":1,"labels":{"env":"prod","version":"v2"}},` +
		`{"id":"m2","service":"gw","name":"requests","at":"2026-10-01T00:00:11Z","value":2}` +
		`]`))
	second := frameFor([]byte(`[` +
		`{"id":"m1","service":"gw","name":" requests ","at":"2026-10-01T00:00:00Z","value":1.0,"labels":{"version":"v2","env":"prod"}},` +
		`{"id":"m2","service":"gw","name":"requests","at":"2026-10-01T00:00:11Z","value":2}` +
		`]`))
	tornPayload := []byte(`[{"id":"torn","service":"gw","name":"requests","at":"2026-10-01T00:00:12Z","value":3}]`)
	torn := frameFor(tornPayload)[:headerSize+4]
	content := writeRawMetricsLog(t, dir, first, second, torn)

	eventLog := openDirForMetrics(t, dir)
	defer eventLog.Close()
	mLog, batches, err := OpenMetricsLog(dir)
	if err != nil {
		t.Fatalf("equivalent duplicates must recover: %v", err)
	}
	defer mLog.Close()
	if len(batches) != 2 {
		t.Fatalf("both complete batches must be returned, got %d", len(batches))
	}
	recovered := metrics.NewStore()
	for _, batch := range batches {
		for _, sample := range batch {
			if err := recovered.Load(sample); err != nil {
				t.Fatalf("equivalent repeats must collapse: %v", err)
			}
		}
	}
	if info, err := os.Stat(filepath.Join(dir, metricsLogName)); err != nil || info.Size() != int64(len(content)-len(torn)) {
		t.Fatalf("torn tail must be trimmed after a consistent recovery, got %v", err)
	}
}

func TestMetricsLogDistinguishesNearbyPointsAndLabelSets(t *testing.T) {
	dir := t.TempDir()
	// One nanosecond apart is a different position; a different complete
	// label set is a different series even at the same instant. Neither is a
	// conflict.
	samePoint := frameFor([]byte(`[{"id":"m1","service":"gw","name":"requests","at":"2026-10-01T00:00:10Z","value":1}]`))
	oneNanoLater := frameFor([]byte(`[{"id":"m2","service":"gw","name":"requests","at":"2026-10-01T00:00:10.000000001Z","value":1}]`))
	otherSeries := frameFor([]byte(`[{"id":"m3","service":"gw","name":"requests","at":"2026-10-01T00:00:10Z","value":1,"labels":{"env":"prod"}}]`))
	writeRawMetricsLog(t, dir, samePoint, oneNanoLater, otherSeries)

	eventLog := openDirForMetrics(t, dir)
	defer eventLog.Close()
	mLog, batches, err := OpenMetricsLog(dir)
	if err != nil {
		t.Fatalf("distinct positions/series must recover: %v", err)
	}
	defer mLog.Close()
	if len(batches) != 3 {
		t.Fatalf("all three batches must recover, got %d", len(batches))
	}
}
