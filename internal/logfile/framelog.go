package logfile

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
)

const (
	headerSize = 8
	// maxPayload guards against absurd allocations when a corrupt length
	// prefix happens to point inside the file.
	maxPayload = 64 << 20
)

// ErrPoisoned is returned after a write or sync failure. The log refuses all
// further writes until the process is restarted and recovery succeeds.
var ErrPoisoned = errors.New("log is unavailable after a storage failure")

// OversizeError reports a payload that exceeds the frame payload limit. It
// marks a content-size problem in the submitted batch, not a storage
// failure: the log is left untouched and stays usable for later writes
// within the limit.
type OversizeError struct {
	Unit  string // what one frame holds, e.g. "batch"
	Size  int    // encoded payload bytes
	Limit int    // maximum payload bytes per frame
}

func (e *OversizeError) Error() string {
	return fmt.Sprintf("%s of %d bytes exceeds the %d byte storage capacity", e.Unit, e.Size, e.Limit)
}

// IsOversize reports whether err is an OversizeError.
func IsOversize(err error) bool {
	var target *OversizeError
	return errors.As(err, &target)
}

// CorruptionError marks a complete frame that fails its checksum or contains
// illegal data, as opposed to an incomplete trailing frame.
type CorruptionError struct{ Reason string }

func (e *CorruptionError) Error() string { return e.Reason }

func corruption(format string, args ...any) error {
	return &CorruptionError{Reason: fmt.Sprintf(format, args...)}
}

// frameLog is the shared append-only frame writer behind the event, incident,
// and metric logs. It owns the frame layout, torn-tail recovery, and the
// poison-on-failure write path; each concrete log supplies only its own
// payload encoding and content validation, so the durability rule lives in
// exactly one place.
type frameLog struct {
	mu       sync.Mutex
	file     *os.File
	dir      string
	noun     string // the log's name in error messages: "event", "incident", "metrics"
	unit     string // what one frame holds: "batch", "incident record", "metric batch"
	poisoned bool
}

// openFrameLog opens (creating if absent) the named log inside an
// already-locked data directory, replays its frames with decode, and
// truncates at most one incomplete trailing frame. A corrupt frame in the
// middle, or a complete frame whose payload decode rejects, is fatal and
// leaves the file untouched.
func openFrameLog[T any](dir, name, noun, unit string, decode func(payload []byte) (T, error)) (*frameLog, []T, error) {
	path := filepath.Join(dir, name)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, nil, fmt.Errorf("open %s log: %w", noun, err)
	}

	items, validLen, err := replayFrames(file, noun, decode)
	if err != nil {
		file.Close()
		return nil, nil, err
	}
	if info, statErr := file.Stat(); statErr == nil && validLen < info.Size() {
		// Drop exactly one incomplete trailing frame; the only mutation
		// recovery performs, fsynced before serving.
		if err := file.Truncate(validLen); err != nil {
			file.Close()
			return nil, nil, fmt.Errorf("truncate incomplete trailing %s frame: %w", noun, err)
		}
		if err := file.Sync(); err != nil {
			file.Close()
			return nil, nil, fmt.Errorf("sync %s log: %w", noun, err)
		}
	}
	// Flush the directory so the log file's own entry is durable, whether it
	// was just created or just truncated.
	if err := syncDir(dir); err != nil {
		file.Close()
		return nil, nil, fmt.Errorf("sync data directory: %w", err)
	}

	return &frameLog{file: file, dir: dir, noun: noun, unit: unit}, items, nil
}

// replayFrames scans the log and returns the decoded frames and the byte
// offset at which the log is known good. A torn trailing frame — a partial
// header, or a header whose payload never fully landed — ends the scan
// without error; any corruption before that offset is fatal.
func replayFrames[T any](file *os.File, noun string, decode func(payload []byte) (T, error)) ([]T, int64, error) {
	data, err := os.ReadFile(file.Name())
	if err != nil {
		return nil, 0, fmt.Errorf("read %s log: %w", noun, err)
	}

	var items []T
	pos := int64(0)
	for pos < int64(len(data)) {
		remaining := int64(len(data)) - pos
		if remaining < headerSize {
			break // torn partial header
		}
		payloadLen := int64(binary.BigEndian.Uint32(data[pos : pos+4]))
		if payloadLen <= 0 || payloadLen > maxPayload {
			return nil, 0, corruption("corrupt %s frame header at offset %d", noun, pos)
		}
		frameEnd := pos + headerSize + payloadLen
		if frameEnd > int64(len(data)) {
			break // header landed but its payload did not: torn tail
		}
		payload := data[pos+headerSize : frameEnd]
		wantCRC := binary.BigEndian.Uint32(data[pos+4 : pos+headerSize])
		if crc32.ChecksumIEEE(payload) != wantCRC {
			return nil, 0, corruption("%s checksum mismatch at offset %d", noun, pos)
		}
		item, err := decode(payload)
		if err != nil {
			return nil, 0, corruption("illegal %s frame payload at offset %d: %v", noun, pos, err)
		}
		items = append(items, item)
		pos = frameEnd
	}
	return items, pos, nil
}

// appendFrame durably writes one already-encoded payload: the frame is
// written and fsynced before the call returns. Any write or sync failure
// poisons the log, so this call and every later one fail until restart. A
// payload that fails the size check is rejected with an OversizeError before
// the log is touched and does not poison it.
func (l *frameLog) appendFrame(payload []byte) (err error) {
	if len(payload) > maxPayload {
		return &OversizeError{Unit: l.unit, Size: len(payload), Limit: maxPayload}
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
		return fmt.Errorf("write %s log: %w", l.noun, err)
	}
	if err = l.file.Sync(); err != nil {
		return fmt.Errorf("sync %s log: %w", l.noun, err)
	}
	return nil
}

// Poisoned reports whether the log has suffered an unrecoverable write error.
func (l *frameLog) Poisoned() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.poisoned
}

// Close releases the log file.
func (l *frameLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.file.Close()
}
