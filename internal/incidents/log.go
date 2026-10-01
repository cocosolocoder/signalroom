package incidents

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Incident log frame layout (all integers big-endian), matching the event
// log's framing so the two share recovery semantics:
//
//	[4 bytes payload length][4 bytes CRC32-IEEE of payload][payload bytes]
//
// The payload is a JSON-encoded record.
const (
	logName    = "incidents.log"
	headerSize = 8
	// maxPayload guards against absurd allocations when a corrupt length
	// prefix happens to point inside the file.
	maxPayload = 64 << 20
)

// Record kinds.
const (
	kindCreate = "create"
	kindAction = "action"
)

// ErrPoisoned is returned after a write or sync failure. The log refuses all
// further writes until the process is restarted and recovery succeeds.
var ErrPoisoned = errors.New("incident log is unavailable after a storage failure")

// record is the durable representation of one incident create or action.
// Content holds the note text, linked event id, or resolve/reopen reason,
// depending on the action.
type record struct {
	Kind            string    `json:"kind"`
	ID              string    `json:"id"`
	Title           string    `json:"title,omitempty"`
	Service         string    `json:"service,omitempty"`
	Operator        string    `json:"operator"`
	ActionID        string    `json:"action_id,omitempty"`
	Action          string    `json:"action,omitempty"`
	ExpectedVersion int       `json:"expected_version,omitempty"`
	Content         string    `json:"content,omitempty"`
	At              time.Time `json:"at"`
}

// Log is an append-only, concurrency-safe frame writer for incident records.
// It does not acquire the directory lock itself; the events log holds the
// exclusive flock for the data directory, and serve opens this log only
// after acquiring it.
type Log struct {
	mu       sync.Mutex
	file     *os.File
	dir      string
	poisoned bool
}

// CorruptionError marks a complete frame that fails its checksum or contains
// illegal data, as opposed to an incomplete trailing frame.
type CorruptionError struct{ Reason string }

func (e *CorruptionError) Error() string { return e.Reason }

func corruption(format string, args ...any) error {
	return &CorruptionError{Reason: fmt.Sprintf(format, args...)}
}

// OpenLog opens the incident log in dir, creating it if missing, and replays
// its records. At most one incomplete trailing frame (a torn final write) is
// truncated; any other corruption is fatal and leaves the file untouched.
// A missing log is not an error: the directory may predate incidents.
func OpenLog(dir string) (*Log, []record, error) {
	if dir == "" {
		return nil, nil, errors.New("data directory is required")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, nil, fmt.Errorf("create data directory: %w", err)
	}
	path := filepath.Join(dir, logName)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, nil, fmt.Errorf("open incident log: %w", err)
	}

	records, validLen, err := replayLog(file)
	if err != nil {
		file.Close()
		return nil, nil, err
	}
	if info, statErr := file.Stat(); statErr == nil && validLen < info.Size() {
		// Drop exactly one incomplete trailing frame. This is the only
		// mutation recovery performs, so it is fsynced before serving.
		if err := file.Truncate(validLen); err != nil {
			file.Close()
			return nil, nil, fmt.Errorf("truncate incomplete trailing frame: %w", err)
		}
		if err := file.Sync(); err != nil {
			file.Close()
			return nil, nil, fmt.Errorf("sync incident log: %w", err)
		}
	}

	return &Log{file: file, dir: dir}, records, nil
}

// replayLog scans the log and returns the valid records, the byte offset at
// which the log is known good, and an error for any corruption before that
// offset.
func replayLog(file *os.File) ([]record, int64, error) {
	data, err := os.ReadFile(file.Name())
	if err != nil {
		return nil, 0, fmt.Errorf("read incident log: %w", err)
	}

	var records []record
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
		var rec record
		if err := json.Unmarshal(payload, &rec); err != nil {
			return nil, 0, corruption("illegal frame payload at offset %d: %v", pos, err)
		}
		records = append(records, rec)
		pos = frameEnd
	}
	return records, pos, nil
}

// Append durably writes one record. The bytes hit the disk (write followed by
// fsync) before Append returns. Any write or sync failure poisons the log:
// this call and every later one return ErrPoisoned until restart.
func (l *Log) Append(rec record) (err error) {
	payload, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("encode record: %w", err)
	}
	if len(payload) > maxPayload {
		return fmt.Errorf("record exceeds %d byte frame limit", maxPayload)
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
		return fmt.Errorf("write incident log: %w", err)
	}
	if err = l.file.Sync(); err != nil {
		return fmt.Errorf("sync incident log: %w", err)
	}
	return nil
}

// Poisoned reports whether the log has suffered an unrecoverable write error.
func (l *Log) Poisoned() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.poisoned
}

// Close releases the log file.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.file.Close()
}
