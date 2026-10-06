package logfile

import (
	"errors"
	"fmt"

	"github.com/cocosolocoder/signalroom/internal/metrics"
)

// metricsLogName is the append-only batch log for metric samples. It sits
// next to events.log in the locked data directory and uses the same
// length-prefixed, CRC-checked frame layout, so the durability and recovery
// rules are identical: fsync before acknowledging, trim only a torn tail,
// and refuse to start on any other corruption.
const metricsLogName = "metrics.log"

// MetricsLog is the concurrency-safe durable writer for metric batches. It is
// independent from both the event and incident logs: a metric write failure
// poisons only metric handling, leaving events and incidents available.
type MetricsLog struct {
	*frameLog
}

// OpenMetricsLog opens (creating if absent) the metrics log inside an
// already-locked data directory, replays its batches, and truncates at most
// one incomplete trailing frame. A corrupt frame in the middle, or a complete
// frame containing illegal data, is fatal and leaves the file untouched.
// Call it only after Open has acquired the directory lock.
func OpenMetricsLog(dir string) (*MetricsLog, [][]metrics.Sample, error) {
	if dir == "" {
		return nil, nil, fmt.Errorf("data directory is required")
	}
	base, batches, err := openFrameLog(dir, metricsLogName, "metrics", "metric batch", decodeMetricBatch, nil)
	if err != nil {
		return nil, nil, err
	}
	return &MetricsLog{frameLog: base}, batches, nil
}

// decodeMetricBatch decodes one frame payload into a validated, normalized
// metric batch. An empty batch or an invalid sample is rejected as
// corruption.
func decodeMetricBatch(payload []byte) ([]metrics.Sample, error) {
	batch, err := metrics.DecodeBatch(payload)
	if err != nil {
		return nil, err
	}
	if len(batch) == 0 {
		return nil, errors.New("empty metric batch frame")
	}
	for i := range batch {
		normalized, err := metrics.Normalize(batch[i])
		if err != nil {
			return nil, err
		}
		batch[i] = normalized
	}
	return batch, nil
}

// Append durably writes one batch: the frame is written and fsynced before the
// call returns. Any write or sync failure poisons the log so that this call
// and every later one fail until restart.
func (l *MetricsLog) Append(batch []metrics.Sample) error {
	payload, err := metrics.MarshalBatch(batch)
	if err != nil {
		return fmt.Errorf("encode metric batch: %w", err)
	}
	return l.appendFrame(payload)
}
