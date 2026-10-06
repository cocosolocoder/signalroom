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
	"sort"
	"strconv"
	"time"

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
// frame containing illegal data, or complete frames that contradict each
// other (the same event id with different normalized content, inside one
// batch or across batches) is fatal: Open returns an error without
// modifying the file, so even the incomplete trailing bytes are kept for
// inspection.
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

	base, batches, err := openFrameLog(dir, logName, "event", "batch", decodeEventBatch, validateEventBatches)
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

// validateEventBatches checks the complete replay of complete frames before
// the torn tail is trimmed: the same event id may repeat only when every
// copy carries the same normalized content (a retried or replayed batch).
// Two complete records that share an id but differ in content contradict
// each other; startup must fail without choosing, skipping, or trimming
// anything. The comparison is exactly events.SameEvent, so whitespace
// trimming, severity case, absolute-instant timestamps, label order, and the
// three "no labels" spellings never turn identical events into a conflict.
func validateEventBatches(batches [][]events.Event) error {
	byID := make(map[string]events.Event)
	for _, batch := range batches {
		for _, event := range batch {
			if existing, ok := byID[event.ID]; ok {
				if !events.SameEvent(existing, event) {
					return corruption("event id %q appears with conflicting content in complete batches: %s",
						event.ID, eventConflictDetail(existing, event))
				}
				continue
			}
			byID[event.ID] = event
		}
	}
	return nil
}

// eventConflictDetail names the first field that distinguishes two
// normalized events sharing an id. The events are already normalized, so
// whitespace, severity case, and time-zone spelling never show up here; a
// reported difference is a genuine message, label, or instant conflict.
// Values are quoted but bounded in length so pathological content cannot
// blow up the startup error itself.
func eventConflictDetail(a, b events.Event) string {
	switch {
	case a.Service != b.Service:
		return fmt.Sprintf("service %s vs %s", quoteBounded(a.Service), quoteBounded(b.Service))
	case a.Severity != b.Severity:
		return fmt.Sprintf("severity %s vs %s", quoteBounded(a.Severity), quoteBounded(b.Severity))
	case a.Message != b.Message:
		return fmt.Sprintf("message %s vs %s", quoteBounded(a.Message), quoteBounded(b.Message))
	case !a.At.Equal(b.At):
		return fmt.Sprintf("time %s vs %s", a.At.UTC().Format(time.RFC3339Nano), b.At.UTC().Format(time.RFC3339Nano))
	default:
		return "labels " + labelConflictDetail(a.Labels, b.Labels)
	}
}

// quoteBounded quotes s for an error message, truncating over-long values so
// the diagnostic stays readable regardless of event content size.
func quoteBounded(s string) string {
	const maxRunes = 200
	runes := []rune(s)
	if len(runes) <= maxRunes {
		return strconv.Quote(s)
	}
	return strconv.Quote(string(runes[:maxRunes])) + "..."
}

// labelConflictDetail reports the first label name, in sorted order, whose
// presence or value differs between the two normalized label sets.
func labelConflictDetail(a, b map[string]string) string {
	names := make(map[string]struct{}, len(a)+len(b))
	for name := range a {
		names[name] = struct{}{}
	}
	for name := range b {
		names[name] = struct{}{}
	}
	sorted := make([]string, 0, len(names))
	for name := range names {
		sorted = append(sorted, name)
	}
	sort.Strings(sorted)
	for _, name := range sorted {
		av, ain := a[name]
		bv, bin := b[name]
		switch {
		case ain && !bin:
			return fmt.Sprintf("%s present vs absent", quoteBounded(name))
		case !ain && bin:
			return fmt.Sprintf("%s absent vs present", quoteBounded(name))
		case av != bv:
			return fmt.Sprintf("%s is %s vs %s", quoteBounded(name), quoteBounded(av), quoteBounded(bv))
		}
	}
	return "differ"
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
