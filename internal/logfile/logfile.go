// Package logfile persists event batches as an append-only sequence of
// length-prefixed, CRC-checked frames.
//
// The frame layout and the shared append/poison/recovery machinery live in
// frame.go; this file owns the event-specific payload encoding
// (events.MarshalBatch), the data-directory lock, and the cursor signing key.
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
	dir      string
	key      []byte
}

// Open acquires an exclusive lock on the data directory, creates it if
// needed, replays the log, and truncates at most one incomplete trailing
// frame (a torn final write). A corrupt frame in the middle, or a complete
// frame containing illegal data, is fatal: Open returns an error without
// modifying the file.
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

	var batches [][]events.Event
	decode := func(payload []byte, offset int64) error {
		batch, err := events.DecodeBatch(payload)
		if err != nil {
			return corruption("illegal frame payload at offset %d: %v", offset, err)
		}
		if len(batch) == 0 {
			return corruption("empty batch frame at offset %d", offset)
		}
		for i := range batch {
			normalized, err := events.Normalize(batch[i])
			if err != nil {
				return corruption("invalid event in frame at offset %d: %v", offset, err)
			}
			batch[i] = normalized
		}
		batches = append(batches, batch)
		return nil
	}

	frame, _, err := openFrameFile(dir, logName, eventFrameSpec, decode)
	if err != nil {
		unlock(lockFile)
		lockFile.Close()
		return nil, nil, err
	}
	// The event log owns the directory and syncs it on every startup, even
	// without a trim, so the freshly created file entry is durable.
	if err := syncDir(dir); err != nil {
		frame.Close()
		unlock(lockFile)
		lockFile.Close()
		return nil, nil, fmt.Errorf("sync data directory: %w", err)
	}

	key, err := loadOrCreateKey(dir)
	if err != nil {
		frame.Close()
		unlock(lockFile)
		lockFile.Close()
		return nil, nil, err
	}

	return &Log{frameLog: frame, lockFile: lockFile, dir: dir, key: key}, batches, nil
}

// Append durably writes one batch. The bytes hit the disk (write followed by
// fsync) before Append returns. Any write or sync failure poisons the log:
// this call and every later one return ErrPoisoned until restart. Encoding
// failures happen before the shared append path, so they do not poison the
// log.
func (l *Log) Append(batch []events.Event) error {
	payload, err := events.MarshalBatch(batch)
	if err != nil {
		return fmt.Errorf("encode batch: %w", err)
	}
	if len(payload) > maxPayload {
		return fmt.Errorf("batch exceeds %d byte frame limit", maxPayload)
	}
	return l.appendFrame(eventFrameSpec, payload)
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
