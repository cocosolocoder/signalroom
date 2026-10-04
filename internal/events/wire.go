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

// allowedEventFields is the exact set of members one event object may carry.
var allowedEventFields = map[string]struct{}{
	"id":       {},
	"service":  {},
	"severity": {},
	"message":  {},
	"at":       {},
	"labels":   {},
}

// rawEventDTO is the product of strict token-stream decoding: it holds the
// scalar members while labels waits unexamined for the strict labels decoder.
type rawEventDTO struct {
	id       string
	service  string
	severity string
	message  string
	at       time.Time
	labels   map[string]string
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

// DecodeBatch strictly decodes a JSON array of event objects. Unknown fields,
// duplicate JSON members (at the event level or inside labels), syntax
// errors, malformed timestamps, non-string scalars, malformed labels, and
// trailing data are all rejected; callers remain responsible for the
// empty-array check and content validation. Duplicate names are judged after
// JSON unescaping, so a literal name and a \uXXXX spelling of it still
// collide.
func DecodeBatch(data []byte) ([]Event, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	open, err := dec.Token()
	if err != nil {
		return nil, fmt.Errorf("decode events array: %w", err)
	}
	if delim, ok := open.(json.Delim); !ok || delim != '[' {
		return nil, fmt.Errorf("decode events array: must be an array")
	}

	var batch []Event
	for dec.More() {
		raw, err := decodeEvent(dec)
		if err != nil {
			return nil, err
		}
		batch = append(batch, Event{
			ID:       raw.id,
			Service:  raw.service,
			Severity: raw.severity,
			Message:  raw.message,
			At:       raw.at,
			Labels:   raw.labels,
		})
	}
	if _, err := dec.Token(); err != nil { // closing ']'
		return nil, fmt.Errorf("decode events array: %w", err)
	}
	var extra json.RawMessage
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("decode events array: unexpected trailing data")
		}
		return nil, fmt.Errorf("decode events array: %w", err)
	}
	return batch, nil
}

// decodeEvent reads exactly one event object from the token stream, rejecting
// unknown and duplicate members instead of letting a struct decode silently
// keep the last value. json.Decoder hands back already-unescaped object keys,
// so literal and \uXXXX spellings of the same name are treated as the same
// member.
func decodeEvent(dec *json.Decoder) (rawEventDTO, error) {
	open, err := dec.Token()
	if err != nil {
		return rawEventDTO{}, fmt.Errorf("decode event: %w", err)
	}
	if delim, ok := open.(json.Delim); !ok || delim != '{' {
		return rawEventDTO{}, fmt.Errorf("decode event: each event must be an object")
	}

	var raw rawEventDTO
	var labelsRaw json.RawMessage
	seen := make(map[string]struct{})
	for dec.More() {
		keyToken, err := dec.Token()
		if err != nil {
			return rawEventDTO{}, fmt.Errorf("decode event: %w", err)
		}
		key, ok := keyToken.(string)
		if !ok {
			return rawEventDTO{}, fmt.Errorf("decode event: field names must be strings")
		}
		if _, known := allowedEventFields[key]; !known {
			return rawEventDTO{}, fmt.Errorf("decode event: unknown field %q", key)
		}
		if _, dup := seen[key]; dup {
			return rawEventDTO{}, fmt.Errorf("decode event: duplicate field %q", key)
		}
		seen[key] = struct{}{}

		switch key {
		case "id", "service", "severity", "message":
			var value string
			if err := dec.Decode(&value); err != nil {
				return rawEventDTO{}, fmt.Errorf("decode event: %s must be a string", key)
			}
			switch key {
			case "id":
				raw.id = value
			case "service":
				raw.service = value
			case "severity":
				raw.severity = value
			case "message":
				raw.message = value
			}
		case "at":
			var at time.Time
			if err := dec.Decode(&at); err != nil {
				return rawEventDTO{}, fmt.Errorf("decode event: at must be an RFC3339Nano timestamp")
			}
			raw.at = at
		case "labels":
			if err := dec.Decode(&labelsRaw); err != nil {
				return rawEventDTO{}, fmt.Errorf("decode event: labels must be an object")
			}
		}
	}
	if _, err := dec.Token(); err != nil { // closing '}'
		return rawEventDTO{}, fmt.Errorf("decode event: %w", err)
	}

	labels, err := DecodeLabelsObject(labelsRaw)
	if err != nil {
		return rawEventDTO{}, err
	}
	raw.labels = labels
	return raw, nil
}

// decodeLabels strictly decodes a JSON labels member; DecodeLabelsObject is
// the exported form used by request bodies that carry the same shape, such as
// the label filters in an alert preview request.
func decodeLabels(raw json.RawMessage) (map[string]string, error) {
	return DecodeLabelsObject(raw)
}

// DecodeLabelsObject strictly decodes a JSON labels object: null, absent, or
// an object whose values are all strings, and duplicate names are rejected
// instead of silently overwriting each other the way a plain map decode
// would. Name/value content rules are enforced later by NormalizeLabels or
// filter normalization.
func DecodeLabelsObject(raw json.RawMessage) (map[string]string, error) {
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
