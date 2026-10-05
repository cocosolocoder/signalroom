package logfile

import (
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
	dir string
}

// OpenMetricsLog opens (creating if absent) the metrics log inside an
// already-locked data directory, replays its batches, and truncates at most
// one incomplete trailing frame. A corrupt frame in the middle, or a complete
// frame containing illegal data, is fatal and leaves the file untouched.
// Call it only after Open has acquired the directory lock.
func OpenMetricsLog(dir string) (*MetricsLog, [][]metrics.Sample, error) {
	var batches [][]metrics.Sample
	decode := func(payload []byte, offset int64) error {
		batch, err := metrics.DecodeBatch(payload)
		if err != nil {
			return corruption("illegal metric frame payload at offset %d: %v", offset, err)
		}
		if len(batch) == 0 {
			return corruption("empty metric batch frame at offset %d", offset)
		}
		for i := range batch {
			normalized, err := metrics.Normalize(batch[i])
			if err != nil {
				return corruption("invalid metric sample in frame at offset %d: %v", offset, err)
			}
			batch[i] = normalized
		}
		batches = append(batches, batch)
		return nil
	}

	frame, trimmed, err := openFrameFile(dir, metricsLogName, metricsFrameSpec, decode)
	if err != nil {
		return nil, nil, err
	}
	if trimmed {
		if err := syncDir(dir); err != nil {
			frame.Close()
			return nil, nil, fmt.Errorf("sync data directory: %w", err)
		}
	}
	return &MetricsLog{frameLog: frame, dir: dir}, batches, nil
}

// Append durably writes one batch: the frame is written and fsynced before the
// call returns. Any write or sync failure poisons the log so that this call
// and every later one fail until restart. Encoding failures happen before the
// shared append path, so they do not poison the log.
func (l *MetricsLog) Append(batch []metrics.Sample) error {
	payload, err := metrics.MarshalBatch(batch)
	if err != nil {
		return fmt.Errorf("encode metric batch: %w", err)
	}
	if len(payload) > maxPayload {
		return fmt.Errorf("metric batch exceeds %d byte frame limit", maxPayload)
	}
	return l.appendFrame(metricsFrameSpec, payload)
}
