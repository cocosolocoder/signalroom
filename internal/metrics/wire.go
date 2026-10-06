package metrics

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/cocosolocoder/signalroom/internal/events"
	"github.com/cocosolocoder/signalroom/internal/strictjson"
)

// sampleDTO is the on-disk representation of a normalized sample. Timestamps
// use RFC3339Nano; Labels is omitted entirely when a sample has none.
type sampleDTO struct {
	ID      string            `json:"id"`
	Service string            `json:"service"`
	Name    string            `json:"name"`
	At      time.Time         `json:"at"`
	Value   float64           `json:"value"`
	Labels  map[string]string `json:"labels,omitempty"`
}

// allowedSampleFields is the exact set of members one sample may carry.
var allowedSampleFields = map[string]struct{}{
	"id":      {},
	"service": {},
	"name":    {},
	"at":      {},
	"value":   {},
	"labels":  {},
}

// rawSample keeps the labels member unexamined so the same strict,
// duplicate-rejecting object decoder events use can validate it.
type rawSample struct {
	id      string
	service string
	name    string
	at      time.Time
	value   float64
	labels  map[string]string
}

// MarshalBatch encodes normalized samples as a JSON array.
func MarshalBatch(batch []Sample) ([]byte, error) {
	dtos := make([]sampleDTO, len(batch))
	for i, sample := range batch {
		dtos[i] = sampleDTO{
			ID:      sample.ID,
			Service: sample.Service,
			Name:    sample.Name,
			At:      sample.At,
			Value:   sample.Value,
			Labels:  sample.Labels,
		}
	}
	return json.Marshal(dtos)
}

// DecodeBatch strictly decodes a JSON array of samples. Unknown fields,
// duplicate JSON members (at the sample level or inside labels), syntax
// errors, malformed timestamps, non-numeric or non-finite values, malformed
// labels, and trailing data are all rejected; callers remain responsible for
// the empty-array check and content validation.
func DecodeBatch(data []byte) ([]Sample, error) {
	var batch []Sample
	err := strictjson.EachArrayElement(data, "decode samples array", func(dec *json.Decoder) error {
		raw, err := decodeSample(dec)
		if err != nil {
			return err
		}
		batch = append(batch, Sample{
			ID:      raw.id,
			Service: raw.service,
			Name:    raw.name,
			At:      raw.at,
			Value:   raw.value,
			Labels:  raw.labels,
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return batch, nil
}

// decodeSample reads exactly one sample object; the shared object walk
// rejects unknown and duplicate members rather than letting a struct decode
// silently keep the last value.
func decodeSample(dec *json.Decoder) (rawSample, error) {
	var raw rawSample
	var labelsRaw json.RawMessage
	seen, err := strictjson.ObjectMembers(dec, "decode sample", allowedSampleFields, func(key string) error {
		switch key {
		case "id", "service", "name":
			var value string
			if err := dec.Decode(&value); err != nil {
				return fmt.Errorf("decode sample: %s must be a string", key)
			}
			switch key {
			case "id":
				raw.id = value
			case "service":
				raw.service = value
			case "name":
				raw.name = value
			}
		case "at":
			var at time.Time
			if err := dec.Decode(&at); err != nil {
				return fmt.Errorf("decode sample: at must be an RFC3339Nano timestamp")
			}
			raw.at = at
		case "value":
			// Decode the raw token first: json.Number has an underlying string
			// type, so a quoted "1" would otherwise decode into it. Require
			// the token to literally be a JSON number before parsing.
			var token json.RawMessage
			if err := dec.Decode(&token); err != nil {
				return fmt.Errorf("decode sample: value must be a JSON number")
			}
			number := bytes.TrimSpace(token)
			if !isJSONNumber(number) {
				return fmt.Errorf("decode sample: value must be a non-negative, finite JSON number")
			}
			value, err := json.Number(number).Float64()
			if err != nil || !finiteNonNegative(value) {
				return fmt.Errorf("decode sample: value must be a non-negative, finite JSON number")
			}
			raw.value = value
		case "labels":
			if err := dec.Decode(&labelsRaw); err != nil {
				return fmt.Errorf("decode sample: labels must be an object")
			}
		}
		return nil
	})
	if err != nil {
		return rawSample{}, err
	}

	labels, err := events.DecodeLabelsObject(labelsRaw)
	if err != nil {
		return rawSample{}, err
	}
	// value has no useful zero value, so an absent member is an error rather
	// than a silent 0; id/service/name/at are caught by later normalization.
	if _, ok := seen["value"]; !ok {
		return rawSample{}, fmt.Errorf("decode sample: value is required")
	}
	raw.labels = labels
	return raw, nil
}

// isJSONNumber validates the strict JSON number grammar, so a quoted string,
// NaN, or Infinity never reach the float parser.
func isJSONNumber(b []byte) bool {
	i := 0
	if i < len(b) && b[i] == '-' {
		i++
	}
	if i >= len(b) {
		return false
	}
	if b[i] == '0' {
		i++
	} else if b[i] >= '1' && b[i] <= '9' {
		i++
		for i < len(b) && b[i] >= '0' && b[i] <= '9' {
			i++
		}
	} else {
		return false
	}
	if i < len(b) && b[i] == '.' {
		i++
		if i >= len(b) || b[i] < '0' || b[i] > '9' {
			return false
		}
		for i < len(b) && b[i] >= '0' && b[i] <= '9' {
			i++
		}
	}
	if i < len(b) && (b[i] == 'e' || b[i] == 'E') {
		i++
		if i < len(b) && (b[i] == '+' || b[i] == '-') {
			i++
		}
		if i >= len(b) || b[i] < '0' || b[i] > '9' {
			return false
		}
		for i < len(b) && b[i] >= '0' && b[i] <= '9' {
			i++
		}
	}
	return i == len(b)
}
