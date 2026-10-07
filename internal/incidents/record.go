package incidents

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"
)

// Record kinds persisted in the durable incident log, one record per
// successfully committed creation or action.
const (
	RecordCreation = "create"
	RecordAction   = "action"
)

// MaxRecordPayloadBytes bounds what one newly saved incident record may hold
// in the on-disk save format (MarshalRecord): 64 MiB in 1024*1024-byte units,
// matching the durable frame capacity. It measures the complete encoded JSON
// of the normalized record that would actually be written, including the
// record's own fields and the server timestamp, but not the surrounding
// 8-byte storage frame header. A record encoding to exactly this size is
// still accepted; only a larger encoding is rejected.
const MaxRecordPayloadBytes = 64 << 20

// OversizeRecordError reports that a brand-new add_note record encodes to
// more than MaxRecordPayloadBytes in the save format. It is a property of
// the note content, not of the storage: nothing is written, the incident log
// stays usable without a restart, the incident is left exactly as it was,
// and the rejected action id remains free for a shorter follow-up request.
// Replays of already-stored actions are never measured.
type OversizeRecordError struct {
	Size  int
	Limit int
}

func (e *OversizeRecordError) Error() string {
	return fmt.Sprintf("incident record saves to %d bytes, exceeding the 64 MiB (%d bytes) save capacity of a single incident record; reduce the content of this submission", e.Size, e.Limit)
}

// IsOversizeRecordError reports whether err is a new record that exceeds the
// save capacity, as opposed to a validation, conflict, not-found, or storage
// failure.
func IsOversizeRecordError(err error) bool {
	var target *OversizeRecordError
	return errors.As(err, &target)
}

// StoredAction is one committed action with its server timestamp.
type StoredAction struct {
	Request Request
	At      time.Time
}

// Record is the unit of durable storage and of replay. Exactly one of
// Creation or Action is set, matching Kind.
type Record struct {
	Kind     string
	Creation *Creation
	Action   *StoredAction
	At       time.Time
}

// NewCreationRecord assembles the durable record for a creation.
func NewCreationRecord(creation Creation, at time.Time) Record {
	c := creation
	return Record{Kind: RecordCreation, Creation: &c, At: at}
}

// NewActionRecord assembles the durable record for an action.
func NewActionRecord(req Request, at time.Time) Record {
	a := StoredAction{Request: req, At: at}
	return Record{Kind: RecordAction, Action: &a, At: at}
}

// recordDTO is the JSON frame payload for one record. Timestamps use
// RFC3339Nano with a UTC location.
type recordDTO struct {
	Kind     string       `json:"kind"`
	At       time.Time    `json:"at"`
	Creation *creationDTO `json:"creation,omitempty"`
	Action   *actionDTO   `json:"action,omitempty"`
}

type creationDTO struct {
	ID       string `json:"id"`
	Title    string `json:"title"`
	Service  string `json:"service"`
	Operator string `json:"operator"`
}

type actionDTO struct {
	IncidentID      string `json:"incident_id"`
	ActionID        string `json:"action_id"`
	Operator        string `json:"operator"`
	ExpectedVersion int    `json:"expected_version"`
	Type            string `json:"type"`
	Content         string `json:"content"`
}

// MarshalRecord encodes one record as a strict JSON object.
func MarshalRecord(record Record) ([]byte, error) {
	dto := recordDTO{Kind: record.Kind, At: record.At.UTC()}
	switch record.Kind {
	case RecordCreation:
		if record.Creation == nil {
			return nil, fmt.Errorf("creation record is missing its content")
		}
		dto.Creation = &creationDTO{
			ID:       record.Creation.ID,
			Title:    record.Creation.Title,
			Service:  record.Creation.Service,
			Operator: record.Creation.Operator,
		}
	case RecordAction:
		if record.Action == nil {
			return nil, fmt.Errorf("action record is missing its content")
		}
		dto.Action = &actionDTO{
			IncidentID:      record.Action.Request.IncidentID,
			ActionID:        record.Action.Request.ActionID,
			Operator:        record.Action.Request.Operator,
			ExpectedVersion: record.Action.Request.ExpectedVersion,
			Type:            record.Action.Request.Type,
			Content:         record.Action.Request.Content,
		}
	default:
		return nil, fmt.Errorf("unknown record kind %q", record.Kind)
	}
	return json.Marshal(dto)
}

// DecodeRecord strictly decodes one record frame and re-validates it, so a
// frame containing illegal data is rejected as corruption at startup.
func DecodeRecord(data []byte) (Record, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var dto recordDTO
	if err := dec.Decode(&dto); err != nil {
		return Record{}, fmt.Errorf("decode incident record: %w", err)
	}
	var extra json.RawMessage
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return Record{}, fmt.Errorf("decode incident record: unexpected trailing data")
		}
		return Record{}, fmt.Errorf("decode incident record: %w", err)
	}
	if dto.At.IsZero() {
		return Record{}, fmt.Errorf("incident record is missing its timestamp")
	}

	record := Record{Kind: dto.Kind, At: dto.At.UTC()}
	switch dto.Kind {
	case RecordCreation:
		if dto.Creation == nil || dto.Action != nil {
			return Record{}, fmt.Errorf("creation record has the wrong shape")
		}
		creation := Creation{
			ID:       dto.Creation.ID,
			Title:    dto.Creation.Title,
			Service:  dto.Creation.Service,
			Operator: dto.Creation.Operator,
		}
		if _, err := NormalizeCreation(creation); err != nil {
			return Record{}, err
		}
		record.Creation = &creation
	case RecordAction:
		if dto.Action == nil || dto.Creation != nil {
			return Record{}, fmt.Errorf("action record has the wrong shape")
		}
		req := Request{
			IncidentID:      dto.Action.IncidentID,
			ActionID:        dto.Action.ActionID,
			Operator:        dto.Action.Operator,
			ExpectedVersion: dto.Action.ExpectedVersion,
			Type:            dto.Action.Type,
			Content:         dto.Action.Content,
		}
		if _, err := NormalizeRequest(req); err != nil {
			return Record{}, err
		}
		record.Action = &StoredAction{Request: req, At: dto.At.UTC()}
	default:
		return Record{}, fmt.Errorf("unknown record kind %q", dto.Kind)
	}
	return record, nil
}
