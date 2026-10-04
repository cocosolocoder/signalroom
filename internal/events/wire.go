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

// allowedEventFields is the exact set of members each event object may
// carry; anything else is an unknown field.
var allowedEventFields = map[string]struct{}{
	"id":       {},
	"service":  {},
	"severity": {},
	"message":  {},
	"at":       {},
	"labels":   {},
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

// DecodeBatch strictly decodes a JSON array of events. Unknown fields,
// syntax errors, malformed timestamps, malformed labels, trailing data, and
// duplicate member names within the outer array's event objects are
// rejected; callers remain responsible for emptiness and field validation.
// Names are compared after JSON decoding, so a literal name and its \uXXXX
// escape of the same text count as a repeat.
func DecodeBatch(data []byte) ([]Event, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	open, err := dec.Token()
	if err != nil {
		return nil, fmt.Errorf("decode events array: %w", err)
	}
	if delim, ok := open.(json.Delim); !ok || delim != '[' {
		return nil, fmt.Errorf("decode events array: must be an array of event objects")
	}

	var batch []Event
	for dec.More() {
		event, err := decodeEventObject(dec)
		if err != nil {
			return nil, err
		}
		batch = append(batch, event)
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

// decodeEventObject reads one event object from dec, rejecting unknown
// members and repeated member names (after JSON unescaping). Values retain
// the same typed-decoding behavior as a struct decode: strings, RFC3339
// timestamps, and the strict labels shape.
func decodeEventObject(dec *json.Decoder) (Event, error) {
	var event Event
	open, err := dec.Token()
	if err != nil {
		return Event{}, fmt.Errorf("decode events array: %w", err)
	}
	if delim, ok := open.(json.Delim); !ok || delim != '{' {
		return Event{}, fmt.Errorf("decode events array: each entry must be an object")
	}

	seen := make(map[string]struct{})
	for dec.More() {
		keyToken, err := dec.Token()
		if err != nil {
			return Event{}, fmt.Errorf("decode events array: %w", err)
		}
		key, ok := keyToken.(string)
		if !ok {
			return Event{}, fmt.Errorf("decode events array: event field names must be strings")
		}
		if _, known := allowedEventFields[key]; !known {
			return Event{}, fmt.Errorf("decode events array: unknown field %q", key)
		}
		if _, dup := seen[key]; dup {
			return Event{}, fmt.Errorf("decode events array: duplicate field %q", key)
		}
		seen[key] = struct{}{}

		switch key {
		case "id":
			if err := dec.Decode(&event.ID); err != nil {
				return Event{}, fmt.Errorf("decode events array: id has the wrong type")
			}
		case "service":
			if err := dec.Decode(&event.Service); err != nil {
				return Event{}, fmt.Errorf("decode events array: service has the wrong type")
			}
		case "severity":
			if err := dec.Decode(&event.Severity); err != nil {
				return Event{}, fmt.Errorf("decode events array: severity has the wrong type")
			}
		case "message":
			if err := dec.Decode(&event.Message); err != nil {
				return Event{}, fmt.Errorf("decode events array: message has the wrong type")
			}
		case "at":
			if err := dec.Decode(&event.At); err != nil {
				return Event{}, fmt.Errorf("decode events array: at must be an RFC3339Nano timestamp")
			}
		case "labels":
			var raw json.RawMessage
			if err := dec.Decode(&raw); err != nil {
				return Event{}, fmt.Errorf("decode events array: %w", err)
			}
			labels, lerr := decodeLabels(raw)
			if lerr != nil {
				return Event{}, lerr
			}
			event.Labels = labels
		}
	}

	if _, err := dec.Token(); err != nil { // closing '}'
		return Event{}, fmt.Errorf("decode events array: %w", err)
	}
	return event, nil
}
