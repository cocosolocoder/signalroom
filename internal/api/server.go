// Package api implements the HTTP ingestion API for the event store.
package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/cocosolocoder/signalroom/internal/events"
)

// Server exposes the event store over HTTP.
type Server struct {
	store *events.PersistentStore
}

// NewServer wraps store.
func NewServer(store *events.PersistentStore) *Server {
	return &Server{store: store}
}

// Handler returns the HTTP handler for the API.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/events", s.handleEvents)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusNotFound, "not found")
	})
	return mux
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	if s.store.Broken() {
		writeError(w, http.StatusServiceUnavailable, "event store is unavailable")
		return
	}
	switch r.Method {
	case http.MethodPost:
		s.handlePost(w, r)
	case http.MethodGet:
		s.handleGet(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// postEventsRequest is the POST /events body: a JSON object containing only
// an events array.
type postEventsRequest struct {
	Events []events.WireEvent `json:"events"`
}

func (s *Server) handlePost(w http.ResponseWriter, r *http.Request) {
	var req postEventsRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}
	if req.Events == nil {
		writeError(w, http.StatusBadRequest, "events field is required")
		return
	}
	if len(req.Events) == 0 {
		writeError(w, http.StatusBadRequest, "events array must not be empty")
		return
	}
	if dec.More() {
		writeError(w, http.StatusBadRequest, "request body must contain a single JSON object")
		return
	}

	evs := make([]events.Event, len(req.Events))
	for i, we := range req.Events {
		ev, err := we.ToEvent()
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid event: "+err.Error())
			return
		}
		evs[i] = ev
	}

	created, replayed, err := s.store.AppendBatch(evs)
	if err != nil {
		switch {
		case errors.Is(err, events.ErrConflict):
			writeError(w, http.StatusConflict, err.Error())
		case errors.Is(err, events.ErrStoreBroken):
			writeError(w, http.StatusServiceUnavailable, "event store is unavailable")
		case errors.Is(err, events.ErrInvalidEvent):
			writeError(w, http.StatusBadRequest, err.Error())
		default:
			writeError(w, http.StatusInternalServerError, "internal error")
		}
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"created": created, "replayed": replayed})
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	q := events.Query{
		Service:  r.URL.Query().Get("service"),
		Severity: r.URL.Query().Get("severity"),
	}
	if v := r.URL.Query().Get("since"); v != "" {
		t, err := time.Parse(time.RFC3339Nano, v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid since timestamp: "+err.Error())
			return
		}
		q.Since = t
	}
	if v := r.URL.Query().Get("until"); v != "" {
		t, err := time.Parse(time.RFC3339Nano, v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid until timestamp: "+err.Error())
			return
		}
		q.Until = t
	}
	if !q.Since.IsZero() && !q.Until.IsZero() && q.Since.After(q.Until) {
		writeError(w, http.StatusBadRequest, "since must not be after until")
		return
	}

	result := s.store.Query(q)
	out := make([]events.WireEvent, 0, len(result))
	for _, ev := range result {
		out = append(out, events.EventToWire(ev))
	}
	writeJSON(w, http.StatusOK, out)
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
