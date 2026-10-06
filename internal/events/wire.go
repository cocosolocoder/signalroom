package events

import (
	"bytes"
	"encoding/json"
	"errors"
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
	const what = "decode labels"
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return nil, nil
	}
	fields, err := strictjson.DecodeObject(raw, what, nil)
	if err != nil {
		return nil, labelsStructureError(err)
	}
	labels := make(map[string]string, len(fields))
	for key, rawValue := range fields {
		if bytes.Equal(rawValue, []byte("null")) {
			return nil, fmt.Errorf("%s: value for %q must be a string", what, key)
		}
		var value string
		if err := json.Unmarshal(rawValue, &value); err != nil {
			return nil, fmt.Errorf("%s: value for %q must be a string", what, key)
		}
		labels[key] = value
	}
	if len(labels) == 0 {
		return nil, nil
	}
	return labels, nil
}

// labelsStructureError restores the labels wording on top of the shared
// structural rejection; the other kinds already render identically.
func labelsStructureError(err error) error {
	var se *strictjson.Error
	if !errors.As(err, &se) {
		return err
	}
	switch se.Kind {
	case strictjson.KindNotObject:
		return errors.New("decode labels: must be an object of string values")
	case strictjson.KindNameNotString:
		return errors.New("decode labels: names must be strings")
	case strictjson.KindDuplicateField:
		return fmt.Errorf("decode labels: duplicate label name %q", se.Field)
	default:
		return err
	}
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
	batch, err := strictjson.DecodeArray(data, "decode events array", decodeEventObject)
	if err != nil {
		return nil, eventsStructureError(err)
	}
	return batch, nil
}

// eventsStructureError restores the event-batch wording for the shapes the
// shared scanner describes generically; syntax, unknown-field, duplicate,
// and trailing-data messages already match.
func eventsStructureError(err error) error {
	var se *strictjson.Error
	if !errors.As(err, &se) {
		return err
	}
	switch se.Kind {
	case strictjson.KindNotArray:
		return errors.New("decode events array: must be an array of event objects")
	case strictjson.KindNotObject:
		return errors.New("decode events array: each entry must be an object")
	case strictjson.KindNameNotString:
		return errors.New("decode events array: event field names must be strings")
	default:
		return err
	}
}

// decodeEventObject reads one event object from dec, rejecting unknown
// members and repeated member names (after JSON unescaping). Values retain
// the same typed-decoding behavior as a struct decode: strings, RFC3339
// timestamps, and the strict labels shape.
func decodeEventObject(dec *json.Decoder) (Event, error) {
	const what = "decode events array"
	var event Event
	err := strictjson.EachObjectField(dec, what, allowedEventFields, func(key string, raw json.RawMessage) error {
		switch key {
		case "id":
			if err := json.Unmarshal(raw, &event.ID); err != nil {
				return fmt.Errorf("%s: id has the wrong type", what)
			}
		case "service":
			if err := json.Unmarshal(raw, &event.Service); err != nil {
				return fmt.Errorf("%s: service has the wrong type", what)
			}
		case "severity":
			if err := json.Unmarshal(raw, &event.Severity); err != nil {
				return fmt.Errorf("%s: severity has the wrong type", what)
			}
		case "message":
			if err := json.Unmarshal(raw, &event.Message); err != nil {
				return fmt.Errorf("%s: message has the wrong type", what)
			}
		case "at":
			if err := json.Unmarshal(raw, &event.At); err != nil {
				return fmt.Errorf("%s: at must be an RFC3339Nano timestamp", what)
			}
		case "labels":
			labels, lerr := decodeLabels(raw)
			if lerr != nil {
				return lerr
			}
			event.Labels = labels
		}
		return nil
	})
	if err != nil {
		return Event{}, eventsStructureError(err)
	}
	return event, nil
}
