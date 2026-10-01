package httpapi

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"

	"github.com/cocosolocoder/signalroom/internal/incidents"
)

// allowedIncidentCreateFields is the exact set of members POST /incidents
// accepts.
var allowedIncidentCreateFields = map[string]struct{}{
	"id":       {},
	"title":    {},
	"service":  {},
	"operator": {},
}

// allowedIncidentActionFields is the exact set of members POST
// /incidents/{id}/actions accepts.
var allowedIncidentActionFields = map[string]struct{}{
	"action":           {},
	"action_id":        {},
	"operator":         {},
	"expected_version": {},
	"content":          {},
	"reason":           {},
	"event_id":         {},
}

type createIncidentRequest struct {
	ID       string `json:"id"`
	Title    string `json:"title"`
	Service  string `json:"service"`
	Operator string `json:"operator"`
}

type incidentActionRequest struct {
	Action          string `json:"action"`
	ActionID        string `json:"action_id"`
	Operator        string `json:"operator"`
	ExpectedVersion int64  `json:"expected_version"`
	Content         string `json:"content"`
	Reason          string `json:"reason"`
	EventID         string `json:"event_id"`
}

type incidentResponse struct {
	ID      string             `json:"id"`
	Title   string             `json:"title"`
	Service string             `json:"service"`
	Status  string             `json:"status"`
	Version int                `json:"version"`
	Links   []eventDTO         `json:"links"`
	History []historyEntryDTO  `json:"history"`
}

type historyEntryDTO struct {
	Action   string `json:"action"`
	ActionID string `json:"action_id,omitempty"`
	Operator string `json:"operator"`
	Content  string `json:"content"`
	Version  int    `json:"version"`
	At       string `json:"at"`
}

func (h *Handler) createIncident(w http.ResponseWriter, r *http.Request) {
	if h.incidents == nil || h.incidents.Poisoned() {
		writeError(w, http.StatusServiceUnavailable, "incident storage is unavailable; restart required")
		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	fields, err := decodeStrictObject(body, allowedIncidentCreateFields)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	var req createIncidentRequest
	if !decodeIncidentString(w, fields, "id", &req.ID) ||
		!decodeIncidentString(w, fields, "title", &req.Title) ||
		!decodeIncidentString(w, fields, "service", &req.Service) ||
		!decodeIncidentString(w, fields, "operator", &req.Operator) {
		return
	}

	inc, err := h.incidents.Create(req.ID, req.Title, req.Service, req.Operator)
	if err != nil {
		writeIncidentError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"id":      inc.ID,
		"status":  inc.Status,
		"version": inc.Version,
	})
}

func (h *Handler) incidentActions(w http.ResponseWriter, r *http.Request) {
	if h.incidents == nil || h.incidents.Poisoned() {
		writeError(w, http.StatusServiceUnavailable, "incident storage is unavailable; restart required")
		return
	}

	id := r.PathValue("id")

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	fields, err := decodeStrictObject(body, allowedIncidentActionFields)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	var req incidentActionRequest
	if !decodeIncidentString(w, fields, "action", &req.Action) ||
		!decodeIncidentString(w, fields, "action_id", &req.ActionID) ||
		!decodeIncidentString(w, fields, "operator", &req.Operator) {
		return
	}
	if !decodeIncidentPositiveInt(w, fields, "expected_version", &req.ExpectedVersion) {
		return
	}
	if !decodeIncidentOptionalString(w, fields, "content", &req.Content) ||
		!decodeIncidentOptionalString(w, fields, "reason", &req.Reason) ||
		!decodeIncidentOptionalString(w, fields, "event_id", &req.EventID) {
		return
	}

	result, err := h.incidents.ApplyAction(
		id, req.ActionID, req.Action, req.Operator, int(req.ExpectedVersion),
		req.Content, req.Reason, req.EventID,
	)
	if err != nil {
		writeIncidentError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"id":        result.Incident.ID,
		"action_id": result.Entry.ActionID,
		"version":   result.Entry.Version,
	})
}

func (h *Handler) getIncident(w http.ResponseWriter, r *http.Request) {
	if h.incidents == nil || h.incidents.Poisoned() {
		writeError(w, http.StatusServiceUnavailable, "incident storage is unavailable; restart required")
		return
	}

	id := r.PathValue("id")
	inc, err := h.incidents.Get(id)
	if err != nil {
		writeIncidentError(w, err)
		return
	}

	// Resolve linked events in link order. EventsByID preserves the requested
	// order and skips missing ids; a missing link is an internal inconsistency.
	linked := h.timeline.EventsByID(inc.Links)
	if len(linked) != len(inc.Links) {
		writeError(w, http.StatusInternalServerError, "incident links are inconsistent")
		return
	}

	links := make([]eventDTO, len(linked))
	for i, event := range linked {
		links[i] = eventDTO{
			ID:       event.ID,
			Service:  event.Service,
			Severity: event.Severity,
			Message:  event.Message,
			At:       event.At,
			Labels:   event.Labels,
		}
	}

	history := make([]historyEntryDTO, len(inc.History))
	for i, entry := range inc.History {
		history[i] = historyEntryDTO{
			Action:   entry.Action,
			ActionID: entry.ActionID,
			Operator: entry.Operator,
			Content:  entry.Content,
			Version:  entry.Version,
			At:       utcRFC3339Nano(entry.At),
		}
	}

	writeJSON(w, http.StatusOK, incidentResponse{
		ID:      inc.ID,
		Title:   inc.Title,
		Service: inc.Service,
		Status:  inc.Status,
		Version: inc.Version,
		Links:   links,
		History: history,
	})
}

// decodeIncidentString decodes one required string member. Missing, null, and
// non-string values are rejected with 400.
func decodeIncidentString(w http.ResponseWriter, fields map[string]json.RawMessage, name string, target *string) bool {
	raw, present := fields[name]
	if !present || bytes.Equal(raw, []byte("null")) {
		writeError(w, http.StatusBadRequest, name+" is required")
		return false
	}
	if err := json.Unmarshal(raw, target); err != nil {
		writeError(w, http.StatusBadRequest, name+" has the wrong type")
		return false
	}
	return true
}

// decodeIncidentOptionalString decodes one optional string member. Absent
// leaves the target unchanged; null and non-string values are rejected.
func decodeIncidentOptionalString(w http.ResponseWriter, fields map[string]json.RawMessage, name string, target *string) bool {
	raw, present := fields[name]
	if !present {
		return true
	}
	if bytes.Equal(raw, []byte("null")) {
		writeError(w, http.StatusBadRequest, name+" has the wrong type")
		return false
	}
	if err := json.Unmarshal(raw, target); err != nil {
		writeError(w, http.StatusBadRequest, name+" has the wrong type")
		return false
	}
	return true
}

// decodeIncidentPositiveInt decodes expected_version as a positive decimal
// integer. Fractions, scientific notation, signs, and quoted numbers are all
// wrong types.
func decodeIncidentPositiveInt(w http.ResponseWriter, fields map[string]json.RawMessage, name string, target *int64) bool {
	raw, present := fields[name]
	if !present || bytes.Equal(raw, []byte("null")) {
		writeError(w, http.StatusBadRequest, name+" is required")
		return false
	}
	if !isDecimal(string(raw)) {
		writeError(w, http.StatusBadRequest, name+" has the wrong type")
		return false
	}
	if err := json.Unmarshal(raw, target); err != nil {
		writeError(w, http.StatusBadRequest, name+" has the wrong type")
		return false
	}
	if *target < 1 {
		writeError(w, http.StatusBadRequest, name+" must be a positive integer")
		return false
	}
	return true
}

func writeIncidentError(w http.ResponseWriter, err error) {
	if verr, ok := err.(*incidents.VersionError); ok {
		writeJSON(w, http.StatusConflict, map[string]any{
			"error":           verr.Error(),
			"current_version": verr.CurrentVersion,
		})
		return
	}
	switch {
	case incidents.IsValidationError(err):
		writeError(w, http.StatusBadRequest, err.Error())
	case incidents.IsNotFoundError(err):
		writeError(w, http.StatusNotFound, err.Error())
	case incidents.IsConflictError(err):
		writeError(w, http.StatusConflict, err.Error())
	default:
		writeError(w, http.StatusServiceUnavailable, "incident storage is unavailable; restart required")
	}
}
