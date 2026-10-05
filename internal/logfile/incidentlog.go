package logfile

import (
	"fmt"

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
	*frameLog
	dir string
}

// OpenIncidentLog opens (creating if absent) the incident log inside an
// already-locked data directory, replays its records, and truncates at most
// one incomplete trailing frame. A corrupt frame in the middle, or a
// complete frame containing illegal data, is fatal and leaves the file
// untouched. Call it only after Open has acquired the directory lock.
func OpenIncidentLog(dir string) (*IncidentLog, []incidents.Record, error) {
	var records []incidents.Record
	decode := func(payload []byte, offset int64) error {
		record, err := incidents.DecodeRecord(payload)
		if err != nil {
			return corruption("illegal incident frame payload at offset %d: %v", offset, err)
		}
		records = append(records, record)
		return nil
	}

	frame, trimmed, err := openFrameFile(dir, incidentLogName, incidentFrameSpec, decode)
	if err != nil {
		return nil, nil, err
	}
	if trimmed {
		if err := syncDir(dir); err != nil {
			frame.Close()
			return nil, nil, fmt.Errorf("sync data directory: %w", err)
		}
	}
	return &IncidentLog{frameLog: frame, dir: dir}, records, nil
}

// AppendRecord durably writes one record: the frame is written and fsynced
// before the call returns. Any write or sync failure poisons the log so that
// this call and every later one fail until restart. Encoding failures happen
// before the shared append path, so they do not poison the log.
func (l *IncidentLog) AppendRecord(record incidents.Record) error {
	payload, err := incidents.MarshalRecord(record)
	if err != nil {
		return fmt.Errorf("encode incident record: %w", err)
	}
	if len(payload) > maxPayload {
		return fmt.Errorf("incident record exceeds %d byte frame limit", maxPayload)
	}
	return l.appendFrame(incidentFrameSpec, payload)
}

// Poisoned and Close are provided by the embedded frameLog. The shared
// directory lock is owned by the event log's Log and released there.
