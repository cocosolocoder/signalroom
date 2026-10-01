package events

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

// eventDTO is the wire and on-disk representation of an event. Timestamps use
// RFC3339Nano, which time.Time produces and accepts natively. Labels is
// omitted entirely when an event has none, so frames written before labels
// existed decode unchanged.
type eventDTO struct {
	ID       string            `json:"id"`
	Service  string            `json:"service"`
	Severity string            `json:"severity"`
	Message  string            `json:"message"`
	At       time.Time         `json:"at"`
	Labels   map[string]string `json:"labels,omitempty"`
}

// labelsObject decodes the raw labels member strictly: it must be null or an
// object whose values are all strings, and duplicate names are rejected
// instead of silently overwriting each other the way a plain map decode
// would. Name/value content rules are enforced later by NormalizeLabels.
func decodeLabels(raw json.RawMessage) (map[string]string, error) {
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return nil, nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	open, err := dec.Token()
	if err != nil {
		return nil, fmt.Errorf("decode labels: %w", err)
	}
	if delim, ok := open.(json.Delim); !ok || delim != '{' {
		return nil, fmt.Errorf("decode labels: must be an object of string values")
	}
	labels := make(map[string]string)
	for dec.More() {
		keyToken, err := dec.Token()
		if err != nil {
			return nil, fmt.Errorf("decode labels: %w", err)
		}
		key, ok := keyToken.(string)
		if !ok {
			return nil, fmt.Errorf("decode labels: names must be strings")
		}
		if _, dup := labels[key]; dup {
			return nil, fmt.Errorf("decode labels: duplicate label name %q", key)
		}
		var value *string
		if err := dec.Decode(&value); err != nil {
			return nil, fmt.Errorf("decode labels: value for %q must be a string", key)
		}
		if value == nil {
			return nil, fmt.Errorf("decode labels: value for %q must be a string", key)
		}
		labels[key] = *value
	}
	if _, err := dec.Token(); err != nil { // closing '}'
		return nil, fmt.Errorf("decode labels: %w", err)
	}
	if len(labels) == 0 {
		return nil, nil
	}
	return labels, nil
}

// rawEventDTO mirrors eventDTO but keeps labels unexamined so DecodeBatch can
// apply the strict duplicate-key and string-value checks.
type rawEventDTO struct {
	ID       string          `json:"id"`
	Service  string          `json:"service"`
	Severity string          `json:"severity"`
	Message  string          `json:"message"`
	At       time.Time       `json:"at"`
	Labels   json.RawMessage `json:"labels"`
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
			Labels:   event.Labels,
		}
	}
	return json.Marshal(dtos)
}

// DecodeBatch strictly decodes a JSON array of events. Unknown fields, syntax
// errors, malformed timestamps, malformed labels, and trailing data are
// rejected; callers remain responsible for emptiness and field validation.
func DecodeBatch(data []byte) ([]Event, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var dtos []rawEventDTO
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
		labels, err := decodeLabels(dto.Labels)
		if err != nil {
			return nil, err
		}
		batch[i] = Event{
			ID:       dto.ID,
			Service:  dto.Service,
			Severity: dto.Severity,
			Message:  dto.Message,
			At:       dto.At,
			Labels:   labels,
		}
	}
	return batch, nil
}
