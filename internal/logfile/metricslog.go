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
// one incomplete trailing frame. A corrupt frame in the middle, a complete
// frame containing illegal data, or two complete records that clash — the
// same sample id with different normalized content, or two different ids on
// one series at one sampling instant — is fatal and leaves the file
// untouched, any torn tail included. Call it only after Open has acquired the
// directory lock.
func OpenMetricsLog(dir string) (*MetricsLog, [][]metrics.Sample, error) {
	if dir == "" {
		return nil, nil, fmt.Errorf("data directory is required")
	}
	base, batches, err := openFrameLog(dir, metricsLogName, "metrics", "metric batch", decodeMetricBatch, checkMetricConflicts)
	if err != nil {
		return nil, nil, err
	}
	return &MetricsLog{frameLog: base}, batches, nil
}

// checkMetricConflicts rejects a recovered log whose complete, valid records
// contradict one another: one sample id carrying different normalized
// content, or two different ids occupying the same series (service, metric
// name, full label set) at the same sampling instant. It replays every
// complete batch into a fresh store, reusing the store's own recovery rules,
// so the comparison matches what serving would apply: identical retries
// collapse to one sample, timezone/label-order/trim spelling and 1 vs 1.0 do
// not clash, while a one-nanosecond move or a differing label set do. The
// clash may sit inside one batch or span several; that each batch reads on
// its own does not make the history legal. It runs before any torn-tail
// truncation, so a conflicting log keeps every byte it had before startup.
func checkMetricConflicts(batches [][]metrics.Sample) error {
	recovered := metrics.NewStore()
	for _, batch := range batches {
		for _, sample := range batch {
			if err := recovered.Load(sample); err != nil {
				return fmt.Errorf("metrics history contains a sample conflict: %w", err)
			}
		}
	}
	return nil
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
