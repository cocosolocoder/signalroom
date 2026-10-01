// Package httpapi exposes the event timeline over HTTP.
package httpapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/cocosolocoder/signalroom/internal/events"
)

// maxBodyBytes bounds a single POST /events request.
const maxBodyBytes = 16 << 20

// maxCompareSegments bounds how many step-sized segments either window may
// require.
const maxCompareSegments = 10000

// Storage durably persists a batch before it becomes visible.
type Storage interface {
	Append(batch []events.Event) error
	Poisoned() bool
}

type envelope struct {
	Events json.RawMessage `json:"events"`
}

// Handler wires the timeline and its durable log to HTTP routes.
type Handler struct {
	timeline *events.Timeline
	log      Storage
	mux      *http.ServeMux
}

// NewHandler builds the HTTP handler for a timeline backed by log.
func NewHandler(timeline *events.Timeline, log Storage) *Handler {
	h := &Handler{timeline: timeline, log: log, mux: http.NewServeMux()}
	h.mux.HandleFunc("/events", h.events)
	h.mux.HandleFunc("/events/compare", h.compareEvents)
	h.mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusNotFound, "not found")
	})
	return h
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

func (h *Handler) unavailable() bool {
	return h.log.Poisoned()
}

func (h *Handler) events(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		h.postEvents(w, r)
	case http.MethodGet:
		h.getEvents(w, r)
	default:
		w.Header().Set("Allow", "GET, POST")
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (h *Handler) postEvents(w http.ResponseWriter, r *http.Request) {
	if h.unavailable() {
		writeError(w, http.StatusServiceUnavailable, "event storage is unavailable; restart required")
		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	var input envelope
	if err := dec.Decode(&input); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	var extra json.RawMessage
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if len(bytes.TrimSpace(input.Events)) == 0 {
		writeError(w, http.StatusBadRequest, "events must be a non-empty array")
		return
	}

	batch, err := events.DecodeBatch(input.Events)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid events array")
		return
	}
	if len(batch) == 0 {
		writeError(w, http.StatusBadRequest, "events must not be empty")
		return
	}

	result, err := h.timeline.Ingest(batch, h.log.Append)
	if err != nil {
		switch {
		case events.IsValidationError(err):
			writeError(w, http.StatusBadRequest, err.Error())
		case events.IsConflictError(err):
			writeError(w, http.StatusConflict, err.Error())
		default:
			writeError(w, http.StatusServiceUnavailable, "event storage is unavailable; restart required")
		}
		return
	}

	writeJSON(w, http.StatusOK, map[string]int{
		"created":  result.Created,
		"replayed": result.Replayed,
	})
}

func (h *Handler) getEvents(w http.ResponseWriter, r *http.Request) {
	if h.unavailable() {
		writeError(w, http.StatusServiceUnavailable, "event storage is unavailable; restart required")
		return
	}

	values := r.URL.Query()
	query := events.Query{
		Service:  values.Get("service"),
		Severity: values.Get("severity"),
	}
	if raw := values.Get("since"); raw != "" {
		parsed, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			writeError(w, http.StatusBadRequest, "since must be an RFC3339Nano timestamp")
			return
		}
		query.Since = parsed
	}
	if raw := values.Get("until"); raw != "" {
		parsed, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			writeError(w, http.StatusBadRequest, "until must be an RFC3339Nano timestamp")
			return
		}
		query.Until = parsed
	}
	if !query.Since.IsZero() && !query.Until.IsZero() && query.Since.After(query.Until) {
		writeError(w, http.StatusBadRequest, "since must not be after until")
		return
	}

	found := h.timeline.Query(query)
	writeEvents(w, found)
}

func (h *Handler) compareEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if h.unavailable() {
		writeError(w, http.StatusServiceUnavailable, "event storage is unavailable; restart required")
		return
	}

	values := r.URL.Query()
	baselineSince, err := requiredTime(values, "baseline_since")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	baselineUntil, err := requiredTime(values, "baseline_until")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	since, err := requiredTime(values, "since")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	until, err := requiredTime(values, "until")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	step, err := requiredStep(values)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if !baselineSince.Before(baselineUntil) {
		writeError(w, http.StatusBadRequest, "baseline_since must be before baseline_until")
		return
	}
	if !since.Before(until) {
		writeError(w, http.StatusBadRequest, "since must be before until")
		return
	}
	if baselineUntil.Sub(baselineSince) != until.Sub(since) {
		writeError(w, http.StatusBadRequest, "baseline and observation windows must have equal length")
		return
	}
	if events.SegmentCount(baselineSince, baselineUntil, step) > maxCompareSegments {
		writeError(w, http.StatusBadRequest, "windows must not require more than 10000 segments")
		return
	}

	result := h.timeline.Compare(events.CompareQuery{
		Service:       values.Get("service"),
		Severity:      values.Get("severity"),
		BaselineSince: baselineSince,
		BaselineUntil: baselineUntil,
		Since:         since,
		Until:         until,
		Step:          step,
	})
	writeJSON(w, http.StatusOK, compareResponseFromResult(result))
}

// requiredTime parses a query parameter that must be present exactly once as
// an RFC3339Nano timestamp.
func requiredTime(values url.Values, key string) (time.Time, error) {
	raw, ok := values[key]
	if !ok || len(raw) != 1 || raw[0] == "" {
		return time.Time{}, fmt.Errorf("%s is required and must be a single RFC3339Nano timestamp", key)
	}
	parsed, err := time.Parse(time.RFC3339Nano, raw[0])
	if err != nil {
		return time.Time{}, fmt.Errorf("%s must be an RFC3339Nano timestamp", key)
	}
	return parsed, nil
}

// requiredStep parses step as a single decimal integer of seconds in
// [1, 86400].
func requiredStep(values url.Values) (time.Duration, error) {
	raw, ok := values["step"]
	if !ok || len(raw) != 1 || raw[0] == "" {
		return 0, errors.New("step is required and must be a single integer number of seconds")
	}
	seconds, err := strconv.ParseInt(raw[0], 10, 64)
	if err != nil || seconds < 1 || seconds > 86400 {
		return 0, errors.New("step must be an integer from 1 to 86400 seconds")
	}
	return time.Duration(seconds) * time.Second, nil
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	_ = enc.Encode(payload)
}

func writeEvents(w http.ResponseWriter, found []events.Event) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if len(found) == 0 {
		_, _ = w.Write([]byte("[]\n"))
		return
	}
	if err := json.NewEncoder(w).Encode(eventsToDTOs(found)); err != nil {
		return
	}
}

type eventDTO struct {
	ID       string    `json:"id"`
	Service  string    `json:"service"`
	Severity string    `json:"severity"`
	Message  string    `json:"message"`
	At       time.Time `json:"at"`
}

func eventsToDTOs(found []events.Event) []eventDTO {
	dtos := make([]eventDTO, len(found))
	for i, event := range found {
		dtos[i] = eventDTO{
			ID:       event.ID,
			Service:  event.Service,
			Severity: event.Severity,
			Message:  event.Message,
			At:       event.At,
		}
	}
	return dtos
}

type compareResponse struct {
	BaselineTotal    int              `json:"baseline_total"`
	ObservationTotal int              `json:"observation_total"`
	Difference       int              `json:"difference"`
	Ratio            *float64         `json:"ratio"`
	Segments         []compareSegment `json:"segments"`
}

type compareSegment struct {
	BaselineSince    time.Time `json:"baseline_since"`
	BaselineUntil    time.Time `json:"baseline_until"`
	Since            time.Time `json:"since"`
	Until            time.Time `json:"until"`
	BaselineCount    int       `json:"baseline_count"`
	ObservationCount int       `json:"observation_count"`
	Difference       int       `json:"difference"`
}

func compareResponseFromResult(result events.CompareResult) compareResponse {
	segments := make([]compareSegment, len(result.Segments))
	for i, segment := range result.Segments {
		segments[i] = compareSegment{
			BaselineSince:    segment.BaselineSince.UTC(),
			BaselineUntil:    segment.BaselineUntil.UTC(),
			Since:            segment.Since.UTC(),
			Until:            segment.Until.UTC(),
			BaselineCount:    segment.BaselineCount,
			ObservationCount: segment.ObservationCount,
			Difference:       segment.Difference,
		}
	}
	return compareResponse{
		BaselineTotal:    result.BaselineTotal,
		ObservationTotal: result.ObservationTotal,
		Difference:       result.Difference,
		Ratio:            result.Ratio,
		Segments:         segments,
	}
}
