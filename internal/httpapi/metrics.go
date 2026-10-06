package httpapi

import (
	"bytes"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/cocosolocoder/signalroom/internal/events"
	"github.com/cocosolocoder/signalroom/internal/metrics"
)

// MetricStorage durably persists a metric batch before it becomes visible and
// reports whether metric storage has failed.
type MetricStorage interface {
	Append(batch []metrics.Sample) error
	Poisoned() bool
}

// allowedMetricsFields is the exact set of members a POST /metrics body may
// carry.
var allowedMetricsFields = map[string]struct{}{
	"samples": {},
}

type ingestMetricsResponse struct {
	Created  int `json:"created"`
	Replayed int `json:"replayed"`
}

type aggregateSeriesDTO struct {
	Service  string                `json:"service"`
	Labels   map[string]string     `json:"labels,omitempty"`
	Segments []aggregateSegmentDTO `json:"segments"`
}

type aggregateSegmentDTO struct {
	Since string  `json:"since"`
	Until string  `json:"until"`
	Count int     `json:"count"`
	Delta float64 `json:"delta"`
}

func (h *Handler) metricsRoot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	h.postMetrics(w, r)
}

func (h *Handler) metricsAggregate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	h.getMetricsAggregate(w, r)
}

func (h *Handler) metricsUnavailable(w http.ResponseWriter) bool {
	if h.metricLogs == nil || h.metricLogs.Poisoned() {
		writeError(w, http.StatusServiceUnavailable, "metric storage is unavailable; restart required")
		return true
	}
	return false
}

func (h *Handler) postMetrics(w http.ResponseWriter, r *http.Request) {
	if h.metricsUnavailable(w) {
		return
	}

	body, err := readJSONObjectBody(w, r, allowedMetricsFields)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	rawSamples, present := body["samples"]
	trimmed := bytes.TrimSpace(rawSamples)
	if !present || bytes.Equal(trimmed, []byte("null")) || bytes.Equal(trimmed, []byte("[]")) {
		writeError(w, http.StatusBadRequest, "samples must be a non-empty array")
		return
	}

	batch, err := metrics.DecodeBatch(rawSamples)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid samples array")
		return
	}
	if len(batch) == 0 {
		writeError(w, http.StatusBadRequest, "samples must not be empty")
		return
	}

	result, err := h.metrics.Ingest(batch, h.metricLogs.Append)
	if err != nil {
		switch {
		case metrics.IsValidationError(err):
			writeError(w, http.StatusBadRequest, err.Error())
		case metrics.IsOversizeBatchError(err):
			writeError(w, http.StatusBadRequest, err.Error())
		case metrics.IsConflictError(err):
			writeError(w, http.StatusConflict, err.Error())
		default:
			writeError(w, http.StatusServiceUnavailable, "metric storage is unavailable; restart required")
		}
		return
	}

	writeJSON(w, http.StatusOK, ingestMetricsResponse{
		Created: result.Created, Replayed: result.Replayed,
	})
}

func (h *Handler) getMetricsAggregate(w http.ResponseWriter, r *http.Request) {
	if h.metricsUnavailable(w) {
		return
	}

	values := r.URL.Query()

	name, ok := singleValue(values, "name")
	if !ok || strings.TrimSpace(name) == "" {
		writeError(w, http.StatusBadRequest, "name is required and must be a single non-empty value")
		return
	}

	parseTime := func(param string) (time.Time, bool) {
		raw, ok := singleValue(values, param)
		if !ok || raw == "" {
			writeError(w, http.StatusBadRequest, param+" is required and must be a single RFC3339Nano timestamp")
			return time.Time{}, false
		}
		parsed, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			writeError(w, http.StatusBadRequest, param+" must be an RFC3339Nano timestamp")
			return time.Time{}, false
		}
		return parsed, true
	}
	since, ok := parseTime("since")
	if !ok {
		return
	}
	until, ok := parseTime("until")
	if !ok {
		return
	}
	if !since.Before(until) {
		writeError(w, http.StatusBadRequest, "since must be strictly before until")
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

	// service is optional but, like the other parameters, may not repeat.
	var service string
	if rawServices, present := values["service"]; present {
		if len(rawServices) != 1 {
			writeError(w, http.StatusBadRequest, "service must be a single value")
			return
		}
		service = rawServices[0]
	}

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
	found, aerr := h.metrics.Aggregate(query)
	if aerr != nil {
		switch {
		case errors.Is(aerr, metrics.ErrTooManySegments):
			writeError(w, http.StatusBadRequest, "window must not require more than 10000 segments")
		case errors.Is(aerr, metrics.ErrTooManySeriesSegments):
			writeError(w, http.StatusBadRequest, "aggregate must not contain more than 100000 series segments")
		case errors.Is(aerr, metrics.ErrDeltaNotFinite):
			writeError(w, http.StatusUnprocessableEntity, "aggregate delta total is not a finite number")
		default:
			writeError(w, http.StatusBadRequest, "invalid aggregate query")
		}
		return
	}

	writeAggregate(w, found)
}

// writeAggregate renders one entry per matching series; a result with no
// matching series is an empty JSON array.
func writeAggregate(w http.ResponseWriter, found []metrics.AggregateSeries) {
	dtos := make([]aggregateSeriesDTO, 0, len(found))
	for _, series := range found {
		segments := make([]aggregateSegmentDTO, len(series.Segments))
		for i, segment := range series.Segments {
			segments[i] = aggregateSegmentDTO{
				Since: utcRFC3339Nano(segment.Start),
				Until: utcRFC3339Nano(segment.End),
				Count: segment.Count,
				Delta: segment.Delta,
			}
		}
		dtos = append(dtos, aggregateSeriesDTO{
			Service:  series.Service,
			Labels:   series.Labels,
			Segments: segments,
		})
	}
	writeJSON(w, http.StatusOK, dtos)
}
