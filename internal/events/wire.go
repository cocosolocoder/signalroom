package events

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/cocosolocoder/signalroom/internal/strictjson"
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
	var batch []Event
	err := strictjson.EachArrayElement(data, "decode events array", func(dec *json.Decoder) error {
		event, err := decodeEventObject(dec)
		if err != nil {
			return err
		}
		batch = append(batch, event)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return batch, nil
}

// decodeEventObject reads one event object from dec; the shared object walk
// rejects unknown members and repeated member names (after JSON unescaping).
// Values retain the same typed-decoding behavior as a struct decode: strings,
// RFC3339 timestamps, and the strict labels shape.
func decodeEventObject(dec *json.Decoder) (Event, error) {
	var event Event
	_, err := strictjson.ObjectMembers(dec, "decode events array", allowedEventFields, func(key string) error {
		switch key {
		case "id":
			if err := dec.Decode(&event.ID); err != nil {
				return fmt.Errorf("decode events array: id has the wrong type")
			}
		case "service":
			if err := dec.Decode(&event.Service); err != nil {
				return fmt.Errorf("decode events array: service has the wrong type")
			}
		case "severity":
			if err := dec.Decode(&event.Severity); err != nil {
				return fmt.Errorf("decode events array: severity has the wrong type")
			}
		case "message":
			if err := dec.Decode(&event.Message); err != nil {
				return fmt.Errorf("decode events array: message has the wrong type")
			}
		case "at":
			if err := dec.Decode(&event.At); err != nil {
				return fmt.Errorf("decode events array: at must be an RFC3339Nano timestamp")
			}
		case "labels":
			var raw json.RawMessage
			if err := dec.Decode(&raw); err != nil {
				return fmt.Errorf("decode events array: %w", err)
			}
			labels, lerr := decodeLabels(raw)
			if lerr != nil {
				return lerr
			}
			event.Labels = labels
		}
		return nil
	})
	if err != nil {
		return Event{}, err
	}
	return event, nil
}
