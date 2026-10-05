// Frame layout (all integers big-endian):
//
//	[4 bytes payload length][4 bytes CRC32-IEEE of payload][payload bytes]
//
// The payload encoding differs per log kind (events.MarshalBatch,
// metrics.MarshalBatch, or incidents.MarshalRecord), but the frame handling,
// durability, poisoning, and recovery rules are shared in this file.

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

// ErrPoisoned is returned after a write or sync failure. A log refuses all
// further writes until the process is restarted and recovery succeeds. Each
// log kind carries its own poisoned state, so a failure in one never disables
// the others.
var ErrPoisoned = errors.New("event log is unavailable after a storage failure")

// CorruptionError marks a complete frame that fails its checksum or contains
// illegal data, as opposed to an incomplete trailing frame.
type CorruptionError struct{ Reason string }

func (e *CorruptionError) Error() string { return e.Reason }

func corruption(format string, args ...any) error {
	return &CorruptionError{Reason: fmt.Sprintf(format, args...)}
}

// frameSpec carries the wording one log kind uses in storage errors and
// corruption reports. Noun names the file ("event log", "metrics log",
// "incident log"); Word prefixes frame-level messages and is empty for the
// event log, which owns the unqualified wording ("metric ", "incident ").
type frameSpec struct {
	noun string
	word string
}

var (
	eventFrameSpec    = frameSpec{noun: "event log"}
	metricsFrameSpec  = frameSpec{noun: "metrics log", word: "metric "}
	incidentFrameSpec = frameSpec{noun: "incident log", word: "incident "}
)

// frameLog is the shared append-only, concurrency-safe frame writer. Each log
// kind owns one, so write failures poison that kind independently.
type frameLog struct {
	mu       sync.Mutex
	file     *os.File
	poisoned bool
}

// encodeFrame wraps a payload with its length and CRC prefix.
func encodeFrame(payload []byte) []byte {
	frame := make([]byte, headerSize+len(payload))
	binary.BigEndian.PutUint32(frame[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(payload))
	copy(frame[headerSize:], payload)
	return frame
}

// appendFrame durably writes one pre-encoded payload: the frame is written and
// fsynced before the call returns. Any write or sync failure poisons the log
// so that this call and every later one fail until restart. The caller is
// responsible for payload encoding and the frame-size limit, which happen
// before the lock is taken and must not poison the log.
func (l *frameLog) appendFrame(spec frameSpec, payload []byte) (err error) {
	frame := encodeFrame(payload)

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
		return fmt.Errorf("write %s: %w", spec.noun, err)
	}
	if err = l.file.Sync(); err != nil {
		return fmt.Errorf("sync %s: %w", spec.noun, err)
	}
	return nil
}

// Poisoned reports whether the log has suffered an unrecoverable write error.
func (l *frameLog) Poisoned() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.poisoned
}

// Close releases the log file. Log kinds that also own the directory lock
// (the event log) override Close to release it as well.
func (l *frameLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.file.Close()
}

// frameDecoder validates one complete, checksum-verified frame payload found
// at offset and folds it into the caller's recovery collection. Returning an
// error aborts startup as corruption.
type frameDecoder func(payload []byte, offset int64) error

// scanFrames walks the in-memory contents of a log and hands each complete,
// checksum-valid payload to decode, in file order. A torn tail — a partial
// header or a header whose payload did not fully land — ends the scan, and
// the returned offset marks where the known-good log ends. Any corruption
// before that offset is fatal.
func scanFrames(data []byte, spec frameSpec, decode frameDecoder) (int64, error) {
	pos := int64(0)
	for pos < int64(len(data)) {
		if remaining := int64(len(data)) - pos; remaining < headerSize {
			break // torn partial header
		}
		payloadLen := int64(binary.BigEndian.Uint32(data[pos : pos+4]))
		if payloadLen <= 0 || payloadLen > maxPayload {
			return 0, corruption("corrupt %sframe header at offset %d", spec.word, pos)
		}
		frameEnd := pos + headerSize + payloadLen
		if frameEnd > int64(len(data)) {
			break // header landed but its payload did not: torn tail
		}
		payload := data[pos+headerSize : frameEnd]
		wantCRC := binary.BigEndian.Uint32(data[pos+4 : pos+headerSize])
		if crc32.ChecksumIEEE(payload) != wantCRC {
			return 0, corruption("%schecksum mismatch at offset %d", spec.word, pos)
		}
		if err := decode(payload, pos); err != nil {
			return 0, err
		}
		pos = frameEnd
	}
	return pos, nil
}

// openFrameFile opens (creating if absent) one log file inside an
// already-locked data directory, replays its frames through decode, and
// truncates at most one incomplete trailing frame. A corrupt frame in the
// middle, or a complete frame the decoder rejects, is fatal and leaves the
// file untouched. When a torn tail is trimmed the file is fsynced and
// trimmed is reported true; the data-directory fsync is left to the caller,
// because the event log always syncs the directory while the other logs sync
// it only after a trim. On success the returned frameLog is open for
// appends; on failure the file is already closed.
func openFrameFile(dir, name string, spec frameSpec, decode frameDecoder) (_ *frameLog, trimmed bool, _ error) {
	if dir == "" {
		return nil, false, fmt.Errorf("data directory is required")
	}
	path := filepath.Join(dir, name)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, false, fmt.Errorf("open %s: %w", spec.noun, err)
	}
	ok := false
	defer func() {
		if !ok {
			file.Close()
		}
	}()

	data, err := os.ReadFile(file.Name())
	if err != nil {
		return nil, false, fmt.Errorf("read %s: %w", spec.noun, err)
	}
	validLen, err := scanFrames(data, spec, decode)
	if err != nil {
		return nil, false, err
	}
	if info, statErr := file.Stat(); statErr == nil && validLen < info.Size() {
		// Drop exactly one incomplete trailing frame. This is the only
		// mutation recovery performs, so it is fsynced before serving; the
		// caller syncs the directory entry.
		if err := file.Truncate(validLen); err != nil {
			return nil, false, fmt.Errorf("truncate incomplete trailing %sframe: %w", spec.word, err)
		}
		if err := file.Sync(); err != nil {
			return nil, false, fmt.Errorf("sync %s: %w", spec.noun, err)
		}
		trimmed = true
	}

	ok = true
	return &frameLog{file: file}, trimmed, nil
}
