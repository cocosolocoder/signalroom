package events

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
	"time"
)

// On-disk layout of the event log:
//
//	+--------+---------+----------+-----------+---------+-----------+
//	| magic  | version | length   | headerCRC | payload | payloadCRC|
//	| 4 bytes| 1 byte  | 8 bytes  | 4 bytes   | length  | 4 bytes   |
//	+--------+---------+----------+-----------+---------+-----------+
//
// magic is frameMagic. version is frameVersion. length is the payload size in
// bytes. headerCRC covers magic, version, and length. payloadCRC covers the
// payload. A frame whose declared size runs past EOF is an incomplete tail
// left by a crashed write and may be ignored; any other checksum mismatch or
// illegal payload is fatal.

const (
	frameMagic   = "SIGB"
	frameVersion = byte(1)
	headerSize   = 4 + 1 + 8 + 4 // magic + version + length + headerCRC
	trailerSize  = 4             // payloadCRC
	maxFrameSize = 1 << 30       // 1 GiB, sanity bound against forged lengths

	logFileName  = "events.log"
	lockFileName = "lock"
)

// ErrStoreBroken is returned after a write or sync failure. The store stays
// unavailable until the process is restarted.
var ErrStoreBroken = errors.New("event store is broken: write or sync failed")

// ErrConflict is returned when an event id conflicts with existing data.
var ErrConflict = errors.New("event id conflicts with an existing event")

// WireEvent is the JSON representation of an event on the wire and in the
// log. At is an RFC3339Nano timestamp string.
type WireEvent struct {
	ID       string `json:"id"`
	Service  string `json:"service"`
	Severity string `json:"severity"`
	Message  string `json:"message"`
	At       string `json:"at"`
}

// ToEvent parses the RFC3339Nano timestamp and returns an Event.
func (w WireEvent) ToEvent() (Event, error) {
	at, err := time.Parse(time.RFC3339Nano, w.At)
	if err != nil {
		return Event{}, fmt.Errorf("invalid timestamp %q: %w", w.At, err)
	}
	return Event{ID: w.ID, Service: w.Service, Severity: w.Severity, Message: w.Message, At: at}, nil
}

// EventToWire formats an event with an RFC3339Nano timestamp.
func EventToWire(ev Event) WireEvent {
	return WireEvent{
		ID:       ev.ID,
		Service:  ev.Service,
		Severity: ev.Severity,
		Message:  ev.Message,
		At:       ev.At.Format(time.RFC3339Nano),
	}
}

// PersistentStore is a concurrency-safe event store backed by an append-only
// framed log in a data directory.
type PersistentStore struct {
	mu       sync.Mutex
	mem      *Store
	file     *os.File
	path     string
	lockFile *os.File
	broken   bool
}

// OpenStore opens (or creates) the event log in dir, acquires the exclusive
// instance lock, and recovers all complete frames. An incomplete tail at EOF
// is ignored and truncated; any corruption or illegal data is fatal and
// leaves the on-disk files untouched.
func OpenStore(dir string) (*PersistentStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create data directory: %w", err)
	}

	lockPath := filepath.Join(dir, lockFileName)
	lf, err := os.OpenFile(lockPath, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open lock file: %w", err)
	}
	if err := syscall.Flock(int(lf.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lf.Close()
		return nil, fmt.Errorf("data directory %q is already in use by another signalroom instance", dir)
	}

	logPath := filepath.Join(dir, logFileName)
	f, err := os.OpenFile(logPath, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		lf.Close()
		return nil, fmt.Errorf("open event log: %w", err)
	}

	p := &PersistentStore{mem: NewStore(), file: f, path: logPath, lockFile: lf}
	if err := p.recover(); err != nil {
		f.Close()
		lf.Close()
		return nil, err
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		lf.Close()
		return nil, fmt.Errorf("seek event log: %w", err)
	}
	return p, nil
}

// Close releases the file and instance lock.
func (p *PersistentStore) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	var firstErr error
	if p.file != nil {
		if err := p.file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		p.file = nil
	}
	if p.lockFile != nil {
		if err := p.lockFile.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		p.lockFile = nil
	}
	return firstErr
}

// Broken reports whether the store has failed a write or sync.
func (p *PersistentStore) Broken() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.broken
}

// Query returns events matching the filter.
func (p *PersistentStore) Query(q Query) []Event {
	return p.mem.Query(q)
}

// AppendBatch validates and atomically accepts a batch of events.
//
// Input validation (normalization, required fields, timestamp parsing) runs
// before any conflict check. The whole batch is accepted or rejected: new
// events are appended to the log and fsynced before a success is reported;
// events identical to already-stored events are counted as replayed and are
// not written again. Conflicting ids return ErrConflict without writing.
func (p *PersistentStore) AppendBatch(events []Event) (created, replayed int, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.broken {
		return 0, 0, ErrStoreBroken
	}

	normalized := make([]Event, len(events))
	for i, ev := range events {
		ev, err = NormalizeEvent(ev)
		if err != nil {
			return 0, 0, err
		}
		normalized[i] = ev
	}

	seen := make(map[string]Event, len(normalized))
	for _, ev := range normalized {
		if _, dup := seen[ev.ID]; dup {
			return 0, 0, ErrConflict
		}
		seen[ev.ID] = ev
	}

	newOnes := make([]Event, 0, len(normalized))
	for _, ev := range normalized {
		existing, exists := p.mem.byID[ev.ID]
		if exists {
			if !EventsEqual(existing, ev) {
				return 0, 0, ErrConflict
			}
			replayed++
			continue
		}
		newOnes = append(newOnes, ev)
	}
	if len(newOnes) == 0 {
		return 0, replayed, nil
	}

	frame, err := encodeFrame(newOnes)
	if err != nil {
		return 0, 0, err
	}
	if n, err := p.file.Write(frame); err != nil || n != len(frame) {
		p.broken = true
		return 0, 0, ErrStoreBroken
	}
	if err := p.file.Sync(); err != nil {
		p.broken = true
		return 0, 0, ErrStoreBroken
	}

	for _, ev := range newOnes {
		p.mem.byID[ev.ID] = ev
		p.mem.events = append(p.mem.events, ev)
	}
	sort.SliceStable(p.mem.events, func(i, j int) bool {
		return less(p.mem.events[i], p.mem.events[j])
	})
	return len(newOnes), replayed, nil
}

// recover reads every complete frame and applies it to the in-memory store.
// An incomplete tail at EOF is truncated. Corruption or illegal data returns
// an error without modifying the file.
func (p *PersistentStore) recover() error {
	data, err := io.ReadAll(p.file)
	if err != nil {
		return fmt.Errorf("read event log: %w", err)
	}

	off := 0
	for off < len(data) {
		frame, next, err := decodeFrame(data, off)
		if err != nil {
			return err
		}
		if frame == nil {
			// Incomplete tail: ignore and truncate.
			break
		}
		if err := p.applyFrame(frame); err != nil {
			return err
		}
		off = next
	}

	if off < len(data) {
		if err := p.file.Truncate(int64(off)); err != nil {
			return fmt.Errorf("truncate incomplete tail: %w", err)
		}
	}

	sort.SliceStable(p.mem.events, func(i, j int) bool {
		return less(p.mem.events[i], p.mem.events[j])
	})
	return nil
}

// applyFrame validates and applies a single decoded frame.
func (p *PersistentStore) applyFrame(events []Event) error {
	seen := make(map[string]Event, len(events))
	for _, ev := range events {
		ev, err := NormalizeEvent(ev)
		if err != nil {
			return fmt.Errorf("illegal event log: invalid event: %w", err)
		}
		if _, dup := seen[ev.ID]; dup {
			return fmt.Errorf("illegal event log: duplicate event id %q", ev.ID)
		}
		seen[ev.ID] = ev

		if existing, exists := p.mem.byID[ev.ID]; exists {
			if !EventsEqual(existing, ev) {
				return fmt.Errorf("illegal event log: conflicting event id %q", ev.ID)
			}
			continue
		}
		p.mem.byID[ev.ID] = ev
		p.mem.events = append(p.mem.events, ev)
	}
	return nil
}

// decodeFrame parses the frame at off in data. It returns the decoded events
// and the offset of the next frame. A nil frame with no error means the
// remaining bytes are an incomplete tail.
func decodeFrame(data []byte, off int) (events []Event, next int, err error) {
	if len(data)-off < headerSize {
		return nil, off, nil
	}
	if string(data[off:off+4]) != frameMagic {
		return nil, 0, fmt.Errorf("corrupt event log: bad frame magic at offset %d", off)
	}
	length := binary.BigEndian.Uint64(data[off+5 : off+13])
	headerCRC := binary.BigEndian.Uint32(data[off+13 : off+17])
	if crc32.ChecksumIEEE(data[off:off+13]) != headerCRC {
		return nil, 0, fmt.Errorf("corrupt event log: header checksum mismatch at offset %d", off)
	}
	if length > maxFrameSize {
		return nil, 0, fmt.Errorf("corrupt event log: frame too large at offset %d", off)
	}
	frameEnd := off + headerSize + int(length) + trailerSize
	if frameEnd > len(data) {
		return nil, off, nil
	}
	payload := data[off+headerSize : off+headerSize+int(length)]
	payloadCRC := binary.BigEndian.Uint32(data[off+headerSize+int(length) : frameEnd])
	if crc32.ChecksumIEEE(payload) != payloadCRC {
		return nil, 0, fmt.Errorf("corrupt event log: payload checksum mismatch at offset %d", off)
	}
	events, err = decodeEvents(payload)
	if err != nil {
		return nil, 0, fmt.Errorf("illegal event log: invalid JSON at offset %d: %w", off, err)
	}
	return events, frameEnd, nil
}

// encodeFrame builds a framed payload for the given events.
func encodeFrame(events []Event) ([]byte, error) {
	payload, err := encodeEvents(events)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	buf.WriteString(frameMagic)
	buf.WriteByte(frameVersion)
	if err := binary.Write(&buf, binary.BigEndian, uint64(len(payload))); err != nil {
		return nil, err
	}
	header := buf.Bytes()
	if err := binary.Write(&buf, binary.BigEndian, crc32.ChecksumIEEE(header)); err != nil {
		return nil, err
	}
	buf.Write(payload)
	if err := binary.Write(&buf, binary.BigEndian, crc32.ChecksumIEEE(payload)); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// encodeEvents serializes events as a JSON array of WireEvents.
func encodeEvents(events []Event) ([]byte, error) {
	out := make([]WireEvent, len(events))
	for i, ev := range events {
		out[i] = EventToWire(ev)
	}
	return json.Marshal(out)
}

// decodeEvents parses a JSON array of WireEvents strictly (unknown fields are
// rejected) and converts them to Events.
func decodeEvents(data []byte) ([]Event, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var in []WireEvent
	if err := dec.Decode(&in); err != nil {
		return nil, err
	}
	if dec.More() {
		return nil, errors.New("unexpected trailing data")
	}
	out := make([]Event, len(in))
	for i, je := range in {
		ev, err := je.ToEvent()
		if err != nil {
			return nil, err
		}
		out[i] = ev
	}
	return out, nil
}
