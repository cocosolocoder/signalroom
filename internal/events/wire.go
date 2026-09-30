package events

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

// eventDTO is the wire and on-disk representation of an event. Timestamps use
// RFC3339Nano, which time.Time produces and accepts natively.
type eventDTO struct {
	ID       string    `json:"id"`
	Service  string    `json:"service"`
	Severity string    `json:"severity"`
	Message  string    `json:"message"`
	At       time.Time `json:"at"`
}

// MarshalBatch encodes normalized events as a JSON array with sorted keys.
func MarshalBatch(batch []Event) ([]byte, error) {
	dtos := make([]eventDTO, len(batch))
	for i, event := range batch {
		dtos[i] = eventDTO{
			ID:       event.ID,
			Service:  event.Service,
			Severity: event.Severity,
			Message:  event.Message,
			At:       event.At,
		}
	}
	return json.Marshal(dtos)
}

// DecodeBatch strictly decodes a JSON array of events. Unknown fields, syntax
// errors, malformed timestamps, and trailing data are rejected; callers remain
// responsible for emptiness and field validation.
func DecodeBatch(data []byte) ([]Event, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var dtos []eventDTO
	if err := dec.Decode(&dtos); err != nil {
		return nil, fmt.Errorf("decode events array: %w", err)
	}
	var extra json.RawMessage
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("decode events array: unexpected trailing data")
		}
		return nil, fmt.Errorf("decode events array: %w", err)
	}
	batch := make([]Event, len(dtos))
	for i, dto := range dtos {
		batch[i] = Event{
			ID:       dto.ID,
			Service:  dto.Service,
			Severity: dto.Severity,
			Message:  dto.Message,
			At:       dto.At,
		}
	}
	return batch, nil
}
