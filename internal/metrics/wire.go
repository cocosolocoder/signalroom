package metrics

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/cocosolocoder/signalroom/internal/events"
)

// sampleDTO is the wire and on-disk representation of a metric sample.
// Timestamps use RFC3339Nano, which time.Time produces and accepts natively.
// Labels is omitted entirely when a sample has none.
type sampleDTO struct {
	ID      string            `json:"id"`
	Service string            `json:"service"`
	Name    string            `json:"name"`
	At      time.Time         `json:"at"`
	Value   float64           `json:"value"`
	Labels  map[string]string `json:"labels,omitempty"`
}

// rawSampleDTO mirrors sampleDTO but keeps labels and value unexamined so
// DecodeBatch can apply the strict duplicate-key, string-value, and
// finite-number checks.
type rawSampleDTO struct {
	ID      string          `json:"id"`
	Service string          `json:"service"`
	Name    string          `json:"name"`
	At      time.Time       `json:"at"`
	Value   json.RawMessage `json:"value"`
	Labels  json.RawMessage `json:"labels"`
}

// MarshalBatch encodes normalized samples as a JSON array with sorted keys.
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
// syntax errors, malformed timestamps, malformed labels, non-numeric or
// non-finite values, and trailing data are rejected; callers remain
// responsible for emptiness and field validation.
func DecodeBatch(data []byte) ([]Sample, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var dtos []rawSampleDTO
	if err := dec.Decode(&dtos); err != nil {
		return nil, fmt.Errorf("decode metrics array: %w", err)
	}
	var extra json.RawMessage
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("decode metrics array: unexpected trailing data")
		}
		return nil, fmt.Errorf("decode metrics array: %w", err)
	}
	batch := make([]Sample, len(dtos))
	for i, dto := range dtos {
		labels, err := events.DecodeLabelsObject(dto.Labels)
		if err != nil {
			return nil, err
		}
		value, err := parseMetricValue(dto.Value)
		if err != nil {
			return nil, err
		}
		batch[i] = Sample{
			ID:      dto.ID,
			Service: dto.Service,
			Name:    dto.Name,
			At:      dto.At,
			Value:   value,
			Labels:  labels,
		}
	}
	return batch, nil
}

// parseMetricValue decodes a JSON number token into a float64. Strings,
// booleans, null, objects, and arrays are rejected; only numeric literals are
// accepted.
func parseMetricValue(raw json.RawMessage) (float64, error) {
	if len(raw) == 0 {
		return 0, fmt.Errorf("value is required and must be a JSON number")
	}
	switch raw[0] {
	case '"', 't', 'f', 'n', '{', '[':
		return 0, fmt.Errorf("value must be a JSON number")
	}
	value, err := strconv.ParseFloat(string(raw), 64)
	if err != nil {
		return 0, fmt.Errorf("value must be a JSON number: %w", err)
	}
	return value, nil
}
