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
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"

	"github.com/cocosolocoder/signalroom/internal/events"
)

const (
	logName  = "events.log"
	lockName = "lock"

	headerSize = 8
	// maxPayload guards against absurd allocations when a corrupt length
	// prefix happens to point inside the file.
	maxPayload = 64 << 20
)

// ErrPoisoned is returned after a write or sync failure. The log refuses all
// further writes until the process is restarted and recovery succeeds.
var ErrPoisoned = errors.New("event log is unavailable after a storage failure")

// Log is an append-only, concurrency-safe frame writer holding an exclusive
// directory lock for its lifetime.
type Log struct {
	mu       sync.Mutex
	file     *os.File
	lockFile *os.File
	dir      string
	key      []byte
	poisoned bool
}

// CorruptionError marks a complete frame that fails its checksum or contains
// illegal data, as opposed to an incomplete trailing frame.
type CorruptionError struct{ Reason string }

func (e *CorruptionError) Error() string { return e.Reason }

func corruption(format string, args ...any) error {
	return &CorruptionError{Reason: fmt.Sprintf(format, args...)}
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

	path := filepath.Join(dir, logName)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		unlock(lockFile)
		lockFile.Close()
		return nil, nil, fmt.Errorf("open event log: %w", err)
	}

	batches, validLen, err := replay(file)
	if err != nil {
		file.Close()
		unlock(lockFile)
		lockFile.Close()
		return nil, nil, err
	}
	if info, statErr := file.Stat(); statErr == nil && validLen < info.Size() {
		// Drop exactly one incomplete trailing frame. This is the only
		// mutation recovery performs, so it is fsynced before serving.
		if err := file.Truncate(validLen); err != nil {
			file.Close()
			unlock(lockFile)
			lockFile.Close()
			return nil, nil, fmt.Errorf("truncate incomplete trailing frame: %w", err)
		}
		if err := file.Sync(); err != nil {
			file.Close()
			unlock(lockFile)
			lockFile.Close()
			return nil, nil, fmt.Errorf("sync event log: %w", err)
		}
	}
	if err := syncDir(dir); err != nil {
		file.Close()
		unlock(lockFile)
		lockFile.Close()
		return nil, nil, fmt.Errorf("sync data directory: %w", err)
	}

	key, err := loadOrCreateKey(dir)
	if err != nil {
		file.Close()
		unlock(lockFile)
		lockFile.Close()
		return nil, nil, err
	}

	return &Log{file: file, lockFile: lockFile, dir: dir, key: key}, batches, nil
}

// replay scans the log and returns the valid batches, the byte offset at
// which the log is known good, and an error for any corruption before that
// offset.
func replay(file *os.File) ([][]events.Event, int64, error) {
	data, err := os.ReadFile(file.Name())
	if err != nil {
		return nil, 0, fmt.Errorf("read event log: %w", err)
	}

	var batches [][]events.Event
	pos := int64(0)
	for pos < int64(len(data)) {
		remaining := int64(len(data)) - pos
		if remaining < headerSize {
			// Torn partial header: ignore everything from here on.
			break
		}
		payloadLen := int64(binary.BigEndian.Uint32(data[pos : pos+4]))
		if payloadLen <= 0 || payloadLen > maxPayload {
			return nil, 0, corruption("corrupt frame header at offset %d", pos)
		}
		frameEnd := pos + headerSize + payloadLen
		if frameEnd > int64(len(data)) {
			// Header landed but its payload did not: torn trailing frame.
			break
		}
		payload := data[pos+headerSize : frameEnd]
		wantCRC := binary.BigEndian.Uint32(data[pos+4 : pos+headerSize])
		if crc32.ChecksumIEEE(payload) != wantCRC {
			return nil, 0, corruption("checksum mismatch at offset %d", pos)
		}
		batch, err := events.DecodeBatch(payload)
		if err != nil {
			return nil, 0, corruption("illegal frame payload at offset %d: %v", pos, err)
		}
		if len(batch) == 0 {
			return nil, 0, corruption("empty batch frame at offset %d", pos)
		}
		for i := range batch {
			normalized, err := events.Normalize(batch[i])
			if err != nil {
				return nil, 0, corruption("invalid event in frame at offset %d: %v", pos, err)
			}
			batch[i] = normalized
		}
		batches = append(batches, batch)
		pos = frameEnd
	}
	return batches, pos, nil
}

// Append durably writes one batch. The bytes hit the disk (write followed by
// fsync) before Append returns. Any write or sync failure poisons the log:
// this call and every later one return ErrPoisoned until restart.
func (l *Log) Append(batch []events.Event) (err error) {
	payload, err := events.MarshalBatch(batch)
	if err != nil {
		return fmt.Errorf("encode batch: %w", err)
	}
	if len(payload) > maxPayload {
		return fmt.Errorf("batch exceeds %d byte frame limit", maxPayload)
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
		return fmt.Errorf("write event log: %w", err)
	}
	if err = l.file.Sync(); err != nil {
		return fmt.Errorf("sync event log: %w", err)
	}
	return nil
}

// Poisoned reports whether the log has suffered an unrecoverable write error.
func (l *Log) Poisoned() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.poisoned
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
