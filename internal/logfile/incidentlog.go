package logfile

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"

	"github.com/cocosolocoder/signalroom/internal/incidents"
)

// incidentLogName is the append-only record log for incidents. It sits next
// to events.log in the locked data directory and uses the same
// length-prefixed, CRC-checked frame layout, so the durability and recovery
// rules are identical: fsync before acknowledging, trim only a torn tail,
// and refuse to start on any other corruption.
const incidentLogName = "incidents.log"

// IncidentLog is the concurrency-safe durable writer for incident records.
// It is independent from the event log: an incident write failure poisons
// only incident handling, exactly as an event write failure poisons only
// event ingestion.
type IncidentLog struct {
	mu       sync.Mutex
	file     *os.File
	dir      string
	poisoned bool
}

// OpenIncidentLog opens (creating if absent) the incident log inside an
// already-locked data directory, replays its records, and truncates at most
// one incomplete trailing frame. A corrupt frame in the middle, or a
// complete frame containing illegal data, is fatal and leaves the file
// untouched. Call it only after Open has acquired the directory lock.
func OpenIncidentLog(dir string) (*IncidentLog, []incidents.Record, error) {
	if dir == "" {
		return nil, nil, fmt.Errorf("data directory is required")
	}
	path := filepath.Join(dir, incidentLogName)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, nil, fmt.Errorf("open incident log: %w", err)
	}

	records, validLen, err := replayIncidents(file)
	if err != nil {
		file.Close()
		return nil, nil, err
	}
	if info, statErr := file.Stat(); statErr == nil && validLen < info.Size() {
		// Drop exactly one incomplete trailing frame; the only mutation
		// recovery performs, fsynced before serving.
		if err := file.Truncate(validLen); err != nil {
			file.Close()
			return nil, nil, fmt.Errorf("truncate incomplete trailing incident frame: %w", err)
		}
		if err := file.Sync(); err != nil {
			file.Close()
			return nil, nil, fmt.Errorf("sync incident log: %w", err)
		}
		if err := syncDir(dir); err != nil {
			file.Close()
			return nil, nil, fmt.Errorf("sync data directory: %w", err)
		}
	}

	return &IncidentLog{file: file, dir: dir}, records, nil
}

// replayIncidents scans the log and returns the valid records, the byte
// offset at which the log is known good, and an error for any corruption
// before that offset.
func replayIncidents(file *os.File) ([]incidents.Record, int64, error) {
	data, err := os.ReadFile(file.Name())
	if err != nil {
		return nil, 0, fmt.Errorf("read incident log: %w", err)
	}

	var records []incidents.Record
	pos := int64(0)
	for pos < int64(len(data)) {
		remaining := int64(len(data)) - pos
		if remaining < headerSize {
			break // torn partial header
		}
		payloadLen := int64(binary.BigEndian.Uint32(data[pos : pos+4]))
		if payloadLen <= 0 || payloadLen > maxPayload {
			return nil, 0, corruption("corrupt incident frame header at offset %d", pos)
		}
		frameEnd := pos + headerSize + payloadLen
		if frameEnd > int64(len(data)) {
			break // header landed but its payload did not: torn tail
		}
		payload := data[pos+headerSize : frameEnd]
		wantCRC := binary.BigEndian.Uint32(data[pos+4 : pos+headerSize])
		if crc32.ChecksumIEEE(payload) != wantCRC {
			return nil, 0, corruption("incident checksum mismatch at offset %d", pos)
		}
		record, err := incidents.DecodeRecord(payload)
		if err != nil {
			return nil, 0, corruption("illegal incident frame payload at offset %d: %v", pos, err)
		}
		records = append(records, record)
		pos = frameEnd
	}
	return records, pos, nil
}

// AppendRecord durably writes one record: the frame is written and fsynced
// before the call returns. Any write or sync failure poisons the log so that
// this call and every later one fail until restart.
func (l *IncidentLog) AppendRecord(record incidents.Record) (err error) {
	payload, err := incidents.MarshalRecord(record)
	if err != nil {
		return fmt.Errorf("encode incident record: %w", err)
	}
	if len(payload) > maxPayload {
		return fmt.Errorf("incident record exceeds %d byte frame limit", maxPayload)
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

// Poisoned reports whether the incident log has suffered a write failure.
func (l *IncidentLog) Poisoned() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.poisoned
}

// Close releases the incident log file. The shared directory lock is owned by
// the event log's Log and released there.
func (l *IncidentLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.file.Close()
}
