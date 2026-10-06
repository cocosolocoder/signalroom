// Package logfile persists event batches as an append-only sequence of
// length-prefixed, CRC-checked frames.
//
// Frame layout (all integers big-endian):
//
//	[4 bytes payload length][4 bytes CRC32-IEEE of payload][payload bytes]
//
// The payload is a JSON array encoded with events.MarshalBatch.
package logfile

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/cocosolocoder/signalroom/internal/events"
)

const (
	logName  = "events.log"
	lockName = "lock"
)

// Log is an append-only, concurrency-safe frame writer holding an exclusive
// directory lock for its lifetime.
type Log struct {
	*frameLog
	lockFile *os.File
	key      []byte
}

// Open acquires an exclusive lock on the data directory, creates it if
// needed, replays the log, and truncates at most one incomplete trailing
// frame (a torn final write). A corrupt frame in the middle, a complete
// frame containing illegal data, or two complete records carrying the same
// event id with different normalized content, is fatal: Open returns an
// error without modifying the file, torn tail included.
//
// Recovered batches are returned in write order, ready to load into a
// timeline.
func Open(dir string) (*Log, [][]events.Event, error) {
	if dir == "" {
		return nil, nil, errors.New("data directory is required")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, nil, fmt.Errorf("create data directory: %w", err)
	}

	lockPath := filepath.Join(dir, lockName)
	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, nil, fmt.Errorf("open directory lock: %w", err)
	}
	if err := tryLock(lockFile); err != nil {
		lockFile.Close()
		if errors.Is(err, errLocked) {
			return nil, nil, fmt.Errorf("data directory %q is in use by another signalroom instance", dir)
		}
		return nil, nil, fmt.Errorf("acquire directory lock: %w", err)
	}

	base, batches, err := openFrameLog(dir, logName, "event", "batch", decodeEventBatch, checkEventConflicts)
	if err != nil {
		unlock(lockFile)
		lockFile.Close()
		return nil, nil, err
	}

	key, err := loadOrCreateKey(dir)
	if err != nil {
		base.Close()
		unlock(lockFile)
		lockFile.Close()
		return nil, nil, err
	}

	return &Log{frameLog: base, lockFile: lockFile, key: key}, batches, nil
}

// decodeEventBatch decodes one frame payload into a validated, normalized
// event batch. An empty batch or an invalid event is rejected as corruption.
func decodeEventBatch(payload []byte) ([]events.Event, error) {
	batch, err := events.DecodeBatch(payload)
	if err != nil {
		return nil, err
	}
	if len(batch) == 0 {
		return nil, errors.New("empty batch frame")
	}
	for i := range batch {
		normalized, err := events.Normalize(batch[i])
		if err != nil {
			return nil, err
		}
		batch[i] = normalized
	}
	return batch, nil
}

// checkEventConflicts rejects a recovered log in which the same event id
// carries different normalized content, whether the clash sits inside one
// batch or across batches. It reuses the timeline's own recovery rules, so
// the comparison (trimmed strings, case-folded severity, absolute instants,
// order-independent labels) matches what serving would apply, and identical
// repeats collapse to one copy. It runs before any torn-tail truncation, so
// a conflicting log keeps every byte it had before startup.
func checkEventConflicts(batches [][]events.Event) error {
	recovered := events.NewTimeline()
	for _, batch := range batches {
		for _, event := range batch {
			if err := recovered.Load(event); err != nil {
				return err
			}
		}
	}
	return nil
}

// Append durably writes one batch. The bytes hit the disk (write followed by
// fsync) before Append returns. Any write or sync failure poisons the log:
// this call and every later one return ErrPoisoned until restart.
func (l *Log) Append(batch []events.Event) error {
	payload, err := events.MarshalBatch(batch)
	if err != nil {
		return fmt.Errorf("encode batch: %w", err)
	}
	return l.appendFrame(payload)
}

// Close releases the log and the exclusive directory lock.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	var closeErr error
	if err := l.file.Close(); err != nil {
		closeErr = err
	}
	if err := unlock(l.lockFile); err != nil && closeErr == nil {
		closeErr = err
	}
	if err := l.lockFile.Close(); err != nil && closeErr == nil {
		closeErr = err
	}
	return closeErr
}
