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
}

// OpenIncidentLog opens (creating if absent) the incident log inside an
// already-locked data directory, replays its records, and truncates at most
// one incomplete trailing frame. A corrupt frame in the middle, or a
// complete frame containing illegal data, is fatal and leaves the file
// untouched. Call it only after Open has acquired the directory lock.
func OpenIncidentLog(dir string) (*IncidentLog, []incidents.Record, error) {
	return OpenIncidentLogChecked(dir, nil)
}

// OpenIncidentLogChecked is OpenIncidentLog with an additional validation of
// the recovered records. The check runs after replay and before any torn
// tail is truncated, so a rejection — for example a complete record whose
// handover breaks the personnel rules — is fatal and leaves the file
// byte-for-byte intact, incomplete trailing frame included. Only a log that
// passes the check may have its one incomplete trailing frame dropped.
func OpenIncidentLogChecked(dir string, check func(records []incidents.Record) error) (*IncidentLog, []incidents.Record, error) {
	if dir == "" {
		return nil, nil, fmt.Errorf("data directory is required")
	}
	base, records, err := openFrameLog(dir, incidentLogName, "incident", "incident record", incidents.DecodeRecord, check)
	if err != nil {
		return nil, nil, err
	}
	return &IncidentLog{frameLog: base}, records, nil
}

// AppendRecord durably writes one record: the frame is written and fsynced
// before the call returns. Any write or sync failure poisons the log so that
// this call and every later one fail until restart.
func (l *IncidentLog) AppendRecord(record incidents.Record) error {
	payload, err := incidents.MarshalRecord(record)
	if err != nil {
		return fmt.Errorf("encode incident record: %w", err)
	}
	return l.appendFrame(payload)
}
