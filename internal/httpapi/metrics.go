package httpapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/cocosolocoder/signalroom/internal/events"
	"github.com/cocosolocoder/signalroom/internal/metrics"
)

// metricEnvelope is the POST /metrics body.
type metricEnvelope struct {
	Samples json.RawMessage `json:"samples"`
}

// metricsRoot dispatches POST /metrics.
func (h *Handler) metricsRoot(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		h.postMetrics(w, r)
	default:
		w.Header().Set("Allow", "POST")
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (h *Handler) postMetrics(w http.ResponseWriter, r *http.Request) {
	if h.metricLogs == nil || h.metricLogs.Poisoned() {
		writeError(w, http.StatusServiceUnavailable, "metric storage is unavailable; restart required")
		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	// Strict body decode: unknown top-level fields are rejected.
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	var envelope metricEnvelope
	if err := dec.Decode(&envelope); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	var extra json.RawMessage
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	// Duplicate JSON keys at every level are rejected.
	if err := rejectDuplicateKeys(body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}

	if len(bytes.TrimSpace(envelope.Samples)) == 0 {
		writeError(w, http.StatusBadRequest, "samples must be a non-empty array")
		return
	}

	batch, err := metrics.DecodeBatch(envelope.Samples)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid samples array")
		return
	}
	if len(batch) == 0 {
		writeError(w, http.StatusBadRequest, "samples must not be empty")
		return
	}

	result, err := h.metrics.Ingest(batch, h.metricLogs.AppendBatch)
	if err != nil {
		switch {
		case metrics.IsValidationError(err):
			writeError(w, http.StatusBadRequest, err.Error())
		case metrics.IsConflictError(err):
			writeError(w, http.StatusConflict, err.Error())
		default:
			writeError(w, http.StatusServiceUnavailable, "metric storage is unavailable; restart required")
		}
		return
	}

	writeJSON(w, http.StatusOK, map[string]int{
		"created":  result.Created,
		"replayed": result.Replayed,
	})
}

// rejectDuplicateKeys walks a JSON document and reports the first object key
// that appears more than once, at any nesting level.
func rejectDuplicateKeys(raw json.RawMessage) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	return walkDuplicateKeys(dec)
}

func walkDuplicateKeys(dec *json.Decoder) error {
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	switch t := tok.(type) {
	case json.Delim:
		switch t {
		case '{':
			seen := make(map[string]struct{})
			for dec.More() {
				keyTok, err := dec.Token()
				if err != nil {
					return err
				}
				key, ok := keyTok.(string)
				if !ok {
					return errors.New("object key must be a string")
				}
				if _, dup := seen[key]; dup {
					return errors.New("duplicate JSON key " + strconv.Quote(key))
				}
				seen[key] = struct{}{}
				if err := walkDuplicateKeys(dec); err != nil {
					return err
				}
			}
			if _, err := dec.Token(); err != nil { // closing '}'
				return err
			}
		case '[':
			for dec.More() {
				if err := walkDuplicateKeys(dec); err != nil {
					return err
				}
			}
			if _, err := dec.Token(); err != nil { // closing ']'
				return err
			}
		default:
			return errors.New("unexpected JSON delimiter " + strconv.Quote(string(t)))
		}
	default:
		// Scalar token: consumed, nothing to check.
	}
	return nil
}

// aggregateMetrics dispatches GET /metrics/aggregate.
func (h *Handler) aggregateMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if h.metricLogs == nil || h.metricLogs.Poisoned() {
		writeError(w, http.StatusServiceUnavailable, "metric storage is unavailable; restart required")
		return
	}

	values := r.URL.Query()

	name, ok := singleValue(values, "name")
	if !ok || name == "" {
		writeError(w, http.StatusBadRequest, "name is required and must be a single value")
		return
	}
	since, ok := parseMetricTimestamp(w, values, "since")
	if !ok {
		return
	}
	until, ok := parseMetricTimestamp(w, values, "until")
	if !ok {
		return
	}
	if !since.Before(until) {
		writeError(w, http.StatusBadRequest, "since must be before until")
		return
	}

	rawStep, ok := singleValue(values, "step")
	if !ok || rawStep == "" {
		writeError(w, http.StatusBadRequest, "step is required and must be a single integer number of seconds")
		return
	}
	if !isDecimal(rawStep) {
		writeError(w, http.StatusBadRequest, "step must be an integer from 1 to 86400 seconds")
		return
	}
	stepSeconds, err := strconv.ParseInt(rawStep, 10, 64)
	if err != nil || stepSeconds < minStepSeconds || stepSeconds > maxStepSeconds {
		writeError(w, http.StatusBadRequest, "step must be an integer from 1 to 86400 seconds")
		return
	}

	// service is optional but must not be repeated (only label is repeatable).
	if vs, present := values["service"]; present && len(vs) != 1 {
		writeError(w, http.StatusBadRequest, "service must be a single value")
		return
	}
	service := values.Get("service")
	labels, lerr := events.ParseLabelConditions(values["label"])
	if lerr != nil {
		writeError(w, http.StatusBadRequest, lerr.Error())
		return
	}

	query := metrics.AggregateQuery{
		Name:    name,
		Service: service,
		Since:   since,
		Until:   until,
		Step:    time.Duration(stepSeconds) * time.Second,
		Labels:  labels,
	}
	result, aerr := h.metrics.Aggregate(query)
	if aerr != nil {
		switch {
		case errors.Is(aerr, metrics.ErrTooManySegments):
			writeError(w, http.StatusBadRequest, "range must not require more than 10000 segments")
		case errors.Is(aerr, metrics.ErrTooManySeriesSegments):
			writeError(w, http.StatusBadRequest, "result must not contain more than 100000 series-segments")
		case errors.Is(aerr, metrics.ErrNonFinite):
			writeError(w, http.StatusUnprocessableEntity, "increment sum is not representable as a finite number")
		default:
			writeError(w, http.StatusBadRequest, "invalid aggregate query")
		}
		return
	}

	dtos := make([]seriesDTO, len(result.Series))
	for i, series := range result.Series {
		segments := make([]segmentDTO, len(series.Segments))
		for j, seg := range series.Segments {
			segments[j] = segmentDTO{
				Since:     utcRFC3339Nano(seg.Since),
				Until:     utcRFC3339Nano(seg.Until),
				Count:     seg.Count,
				Increment: seg.Increment,
			}
		}
		dtos[i] = seriesDTO{
			Service:  series.Service,
			Labels:   series.Labels,
			Segments: segments,
		}
	}
	writeJSON(w, http.StatusOK, dtos)
}

// parseMetricTimestamp reads a required, single RFC3339Nano parameter.
func parseMetricTimestamp(w http.ResponseWriter, values map[string][]string, name string) (time.Time, bool) {
	raw, ok := singleValue(values, name)
	if !ok || raw == "" {
		writeError(w, http.StatusBadRequest, name+" is required and must be a single RFC3339Nano timestamp")
		return time.Time{}, false
	}
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		writeError(w, http.StatusBadRequest, name+" must be an RFC3339Nano timestamp")
		return time.Time{}, false
	}
	return parsed, true
}

type seriesDTO struct {
	Service  string            `json:"service"`
	Labels   map[string]string `json:"labels,omitempty"`
	Segments []segmentDTO      `json:"segments"`
}

type segmentDTO struct {
	Since     string  `json:"since"`
	Until     string  `json:"until"`
	Count     int     `json:"count"`
	Increment float64 `json:"increment"`
}
