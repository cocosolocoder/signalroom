package logfile

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"

	"github.com/cocosolocoder/signalroom/internal/metrics"
)

// metricLogName is the append-only batch log for metric samples. It sits next
// to events.log and incidents.log in the locked data directory and uses the
// same length-prefixed, CRC-checked frame layout, so the durability and
// recovery rules are identical: fsync before acknowledging, trim only a torn
// tail, and refuse to start on any other corruption.
const metricLogName = "metrics.log"

// MetricLog is the concurrency-safe durable writer for metric batches. It is
// independent from the event and incident logs: a metric write failure
// poisons only metric handling, exactly as an event write failure poisons
// only event ingestion.
type MetricLog struct {
	mu       sync.Mutex
	file     *os.File
	dir      string
	poisoned bool
}

// OpenMetricLog opens (creating if absent) the metric log inside an
// already-locked data directory, replays its batches, and truncates at most
// one incomplete trailing frame. A corrupt frame in the middle, or a complete
// frame containing illegal data, is fatal and leaves the file untouched. Call
// it only after Open has acquired the directory lock.
func OpenMetricLog(dir string) (*MetricLog, []metrics.Sample, error) {
	if dir == "" {
		return nil, nil, fmt.Errorf("data directory is required")
	}
	path := filepath.Join(dir, metricLogName)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, nil, fmt.Errorf("open metric log: %w", err)
	}

	batches, validLen, err := replayMetrics(file)
	if err != nil {
		file.Close()
		return nil, nil, err
	}
	if info, statErr := file.Stat(); statErr == nil && validLen < info.Size() {
		// Drop exactly one incomplete trailing frame; the only mutation
		// recovery performs, fsynced before serving.
		if err := file.Truncate(validLen); err != nil {
			file.Close()
			return nil, nil, fmt.Errorf("truncate incomplete trailing metric frame: %w", err)
		}
		if err := file.Sync(); err != nil {
			file.Close()
			return nil, nil, fmt.Errorf("sync metric log: %w", err)
		}
		if err := syncDir(dir); err != nil {
			file.Close()
			return nil, nil, fmt.Errorf("sync data directory: %w", err)
		}
	}

	return &MetricLog{file: file, dir: dir}, batches, nil
}

// replayMetrics scans the log and returns the valid samples, the byte offset
// at which the log is known good, and an error for any corruption before
// that offset.
func replayMetrics(file *os.File) ([]metrics.Sample, int64, error) {
	data, err := os.ReadFile(file.Name())
	if err != nil {
		return nil, 0, fmt.Errorf("read metric log: %w", err)
	}

	var samples []metrics.Sample
	pos := int64(0)
	for pos < int64(len(data)) {
		remaining := int64(len(data)) - pos
		if remaining < headerSize {
			break // torn partial header
		}
		payloadLen := int64(binary.BigEndian.Uint32(data[pos : pos+4]))
		if payloadLen <= 0 || payloadLen > maxPayload {
			return nil, 0, corruption("corrupt metric frame header at offset %d", pos)
		}
		frameEnd := pos + headerSize + payloadLen
		if frameEnd > int64(len(data)) {
			break // header landed but its payload did not: torn tail
		}
		payload := data[pos+headerSize : frameEnd]
		wantCRC := binary.BigEndian.Uint32(data[pos+4 : pos+headerSize])
		if crc32.ChecksumIEEE(payload) != wantCRC {
			return nil, 0, corruption("metric checksum mismatch at offset %d", pos)
		}
		batch, err := metrics.DecodeBatch(payload)
		if err != nil {
			return nil, 0, corruption("illegal metric frame payload at offset %d: %v", pos, err)
		}
		if len(batch) == 0 {
			return nil, 0, corruption("empty metric batch frame at offset %d", pos)
		}
		for i := range batch {
			normalized, err := metrics.Normalize(batch[i])
			if err != nil {
				return nil, 0, corruption("invalid metric in frame at offset %d: %v", pos, err)
			}
			batch[i] = normalized
		}
		samples = append(samples, batch...)
		pos = frameEnd
	}
	return samples, pos, nil
}

// AppendBatch durably writes one batch: the frame is written and fsynced
// before the call returns. Any write or sync failure poisons the log so that
// this call and every later one fail until restart.
func (l *MetricLog) AppendBatch(batch []metrics.Sample) (err error) {
	payload, err := metrics.MarshalBatch(batch)
	if err != nil {
		return fmt.Errorf("encode metric batch: %w", err)
	}
	if len(payload) > maxPayload {
		return fmt.Errorf("metric batch exceeds %d byte frame limit", maxPayload)
	}

	frame := make([]byte, headerSize+len(payload))
	binary.BigEndian.PutUint32(frame[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(payload))
	copy(frame[headerSize:], payload)

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.poisoned {
		return ErrPoisoned
	}
	defer func() {
		if err != nil {
			l.poisoned = true
		}
	}()

	if _, err = l.file.Write(frame); err != nil {
		return fmt.Errorf("write metric log: %w", err)
	}
	if err = l.file.Sync(); err != nil {
		return fmt.Errorf("sync metric log: %w", err)
	}
	return nil
}

// Poisoned reports whether the metric log has suffered a write failure.
func (l *MetricLog) Poisoned() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.poisoned
}

// Close releases the metric log file. The shared directory lock is owned by
// the event log's Log and released there.
func (l *MetricLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.file.Close()
}
