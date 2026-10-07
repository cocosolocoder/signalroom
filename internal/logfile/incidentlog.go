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
// one incomplete trailing frame. Every complete frame is replayed into a
// fresh registry before any truncation: a complete record that contradicts
// the history before it — a corrupt frame in the middle, illegal payload
// data, or, for example, an owner handover to somebody who was not a
// participant at that point — is fatal and leaves the file byte-for-byte
// untouched, torn tail included. The check deliberately runs before the
// trailing partial frame is dropped, so a history already doomed by an
// illegal handover cannot lose its original bytes, and its error reports
// the handover rather than the incomplete tail. events resolves linked
// event ids during that replay. Call it only after Open has acquired the
// directory lock.
func OpenIncidentLog(dir string, events incidents.EventLookup) (*IncidentLog, []incidents.Record, error) {
	if dir == "" {
		return nil, nil, fmt.Errorf("data directory is required")
	}
	base, records, err := openFrameLog(dir, incidentLogName, "incident", "incident record",
		incidents.DecodeRecord, checkIncidentHistory(events))
	if err != nil {
		return nil, nil, err
	}
	return &IncidentLog{frameLog: base}, records, nil
}

// checkIncidentHistory validates the complete recovered records against one
// another by replaying them, in log order, into a fresh registry. The
// registry runs the same state-machine, link, and personnel rules as the
// live request path, so a target's eligibility is judged solely on the
// history preceding that record: a later same-named join cannot legitimize
// an earlier handover to a stranger, and handing the incident to its current
// owner stays illegal. openFrameLog runs this before trimming an incomplete
// trailing frame, which is why a rejection here preserves every byte of the
// original log for inspection.
func checkIncidentHistory(eventLookup incidents.EventLookup) func([]incidents.Record) error {
	return func(records []incidents.Record) error {
		recovered := incidents.NewRegistry(eventLookup, nil)
		for _, record := range records {
			if err := recovered.Load(record); err != nil {
				return err
			}
		}
		return nil
	}
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
