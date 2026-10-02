// Package httpapi exposes the event timeline over HTTP.
package httpapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/cocosolocoder/signalroom/internal/events"
	"github.com/cocosolocoder/signalroom/internal/incidents"
	"github.com/cocosolocoder/signalroom/internal/metrics"
)

// maxBodyBytes bounds a single POST request body.
const maxBodyBytes = 16 << 20

// Storage durably persists a batch before it becomes visible.
type Storage interface {
	Append(batch []events.Event) error
	Poisoned() bool
}

// MetricsStorage durably persists a metric batch before it becomes visible.
type MetricsStorage interface {
	AppendBatch(batch []metrics.Sample) error
	Poisoned() bool
}

type envelope struct {
	Events json.RawMessage `json:"events"`
}

// Handler wires the timeline, its durable log, and the cursor store to HTTP
// routes.
type Handler struct {
	timeline *events.Timeline
	log      Storage
	cursors  CursorStore

	incidents    *incidents.Registry
	incidentLogs IncidentStorage

	metrics    *metrics.Timeline
	metricLogs MetricsStorage

	mux *http.ServeMux
}

// Option customizes a Handler at construction.
type Option func(*Handler)

// WithIncidents enables the incident endpoints, backed by reg and its
// durable store.
func WithIncidents(reg *incidents.Registry, store IncidentStorage) Option {
	return func(h *Handler) {
		h.incidents = reg
		h.incidentLogs = store
	}
}

// WithMetrics enables the metric endpoints, backed by the metric timeline and
// its durable store.
func WithMetrics(timeline *metrics.Timeline, store MetricsStorage) Option {
	return func(h *Handler) {
		h.metrics = timeline
		h.metricLogs = store
	}
}

// NewHandler builds the HTTP handler for a timeline backed by log. cursors
// supplies the per-directory signing key and snapshot storage for paging.
func NewHandler(timeline *events.Timeline, log Storage, cursors CursorStore, opts ...Option) *Handler {
	h := &Handler{timeline: timeline, log: log, cursors: cursors, mux: http.NewServeMux()}
	for _, opt := range opts {
		opt(h)
	}
	h.mux.HandleFunc("/events", h.events)
	h.mux.HandleFunc("/alerts/preview", h.previewAlerts)
	h.mux.HandleFunc("/events/compare", h.compareEvents)
	h.mux.HandleFunc("/events/page", h.pageEvents)
	if h.metrics != nil {
		h.mux.HandleFunc("/metrics", h.metricsRoot)
		h.mux.HandleFunc("/metrics/aggregate", h.aggregateMetrics)
	}
	if h.incidents != nil {
		h.mux.HandleFunc("/incidents", h.incidentsRoot)
		h.mux.HandleFunc("/incidents/{id}", h.incidentItem)
		h.mux.HandleFunc("/incidents/{id}/actions", h.incidentActions)
	}
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
	labels, err := events.ParseLabelConditions(values["label"])
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	query := events.Query{
		Service:  values.Get("service"),
		Severity: values.Get("severity"),
		Labels:   labels,
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
	ID       string            `json:"id"`
	Service  string            `json:"service"`
	Severity string            `json:"severity"`
	Message  string            `json:"message"`
	At       time.Time         `json:"at"`
	Labels   map[string]string `json:"labels,omitempty"`
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
			Labels:   event.Labels,
		}
	}
	return dtos
}
