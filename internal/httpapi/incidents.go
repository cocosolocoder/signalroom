package httpapi

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"

	"github.com/cocosolocoder/signalroom/internal/incidents"
)

// IncidentStorage durably persists an incident record before it becomes
// visible and reports whether incident storage has failed.
type IncidentStorage interface {
	AppendRecord(record incidents.Record) error
	Poisoned() bool
}

// Allowed top-level members for the two incident request bodies.
var (
	allowedIncidentCreateFields = map[string]struct{}{
		"id":       {},
		"title":    {},
		"service":  {},
		"operator": {},
	}
	allowedIncidentActionFields = map[string]struct{}{
		"action_id":        {},
		"operator":         {},
		"expected_version": {},
		"action":           {},
		"content":          {},
		"event_id":         {},
		"reason":           {},
	}
)

type createIncidentResponse struct {
	ID      string `json:"id"`
	Status  string `json:"status"`
	Version int    `json:"version"`
}

type actionResponse struct {
	IncidentID string `json:"incident_id"`
	ActionID   string `json:"action_id"`
	Version    int    `json:"version"`
}

func (h *Handler) incidentsRoot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	h.createIncident(w, r)
}

func (h *Handler) incidentItem(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if h.incidentStoreUnavailable(w) {
		return
	}

	id := r.PathValue("id")
	inc, err := h.incidents.Get(id)
	if err != nil {
		writeIncidentError(w, err)
		return
	}
	writeIncident(w, h, inc)
}

func (h *Handler) incidentActions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	h.addAction(w, r)
}

func (h *Handler) createIncident(w http.ResponseWriter, r *http.Request) {
	if h.incidentStoreUnavailable(w) {
		return
	}

	body, err := readJSONObjectBody(w, r, allowedIncidentCreateFields)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	creation := incidents.Creation{
		ID:       stringField(body, "id"),
		Title:    stringField(body, "title"),
		Service:  stringField(body, "service"),
		Operator: stringField(body, "operator"),
	}
	result, aerr := h.incidents.Create(creation)
	if aerr != nil {
		writeIncidentError(w, aerr)
		return
	}
	writeJSON(w, http.StatusOK, createIncidentResponse{
		ID: result.ID, Status: result.Status, Version: result.Version,
	})
}

func (h *Handler) addAction(w http.ResponseWriter, r *http.Request) {
	if h.incidentStoreUnavailable(w) {
		return
	}

	body, err := readJSONObjectBody(w, r, allowedIncidentActionFields)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	req, ok := decodeActionRequest(w, r.PathValue("id"), body)
	if !ok {
		return
	}

	result, aerr := h.incidents.Apply(req)
	if aerr != nil {
		writeIncidentError(w, aerr)
		return
	}
	writeJSON(w, http.StatusOK, actionResponse{
		IncidentID: result.IncidentID, ActionID: result.ActionID, Version: result.Version,
	})
}

// decodeActionRequest validates the common fields and the one payload member
// the named action requires. Any payload member that does not belong to the
// action is rejected, so each action has an exact, unambiguous shape.
func decodeActionRequest(w http.ResponseWriter, incidentID string, fields map[string]json.RawMessage) (incidents.Request, bool) {
	actionID, ok := requiredString(w, fields, "action_id")
	if !ok {
		return incidents.Request{}, false
	}
	operator, ok := requiredString(w, fields, "operator")
	if !ok {
		return incidents.Request{}, false
	}
	action, ok := requiredString(w, fields, "action")
	if !ok {
		return incidents.Request{}, false
	}

	raw, present := fields["expected_version"]
	if !present {
		writeError(w, http.StatusBadRequest, "expected_version is required")
		return incidents.Request{}, false
	}
	var expected int64
	if !decodeStrictInt(w, raw, "expected_version", &expected) {
		return incidents.Request{}, false
	}

	payloadField, valid := actionPayloadField(action)
	if !valid {
		writeError(w, http.StatusBadRequest, "unknown action")
		return incidents.Request{}, false
	}
	content, ok := requiredString(w, fields, payloadField)
	if !ok {
		return incidents.Request{}, false
	}
	for _, extra := range []string{"content", "event_id", "reason"} {
		if extra == payloadField {
			continue
		}
		if _, present := fields[extra]; present {
			writeError(w, http.StatusBadRequest, "unexpected field "+extra+" for action "+action)
			return incidents.Request{}, false
		}
	}

	return incidents.Request{
		IncidentID:      incidentID,
		ActionID:        actionID,
		Operator:        operator,
		ExpectedVersion: int(expected),
		Type:            action,
		Content:         content,
	}, true
}

// actionPayloadField maps an action name to the member carrying its payload.
func actionPayloadField(action string) (string, bool) {
	switch action {
	case incidents.ActionNote:
		return "content", true
	case incidents.ActionLinkEvent:
		return "event_id", true
	case incidents.ActionResolve, incidents.ActionReopen:
		return "reason", true
	default:
		return "", false
	}
}

func (h *Handler) incidentStoreUnavailable(w http.ResponseWriter) bool {
	if h.incidentLogs == nil || h.incidentLogs.Poisoned() {
		writeError(w, http.StatusServiceUnavailable, "incident storage is unavailable; restart required")
		return true
	}
	return false
}

// writeIncidentError maps domain errors onto the documented statuses. A
// version conflict additionally reports the current version.
func writeIncidentError(w http.ResponseWriter, err error) {
	switch {
	case incidents.IsValidationError(err):
		writeError(w, http.StatusBadRequest, err.Error())
	case incidents.IsNotFoundError(err):
		writeError(w, http.StatusNotFound, err.Error())
	case incidents.IsConflictError(err):
		current := incidents.CurrentVersionOf(err)
		if current > 0 {
			writeJSON(w, http.StatusConflict, map[string]any{
				"error":           err.Error(),
				"current_version": current,
			})
			return
		}
		writeError(w, http.StatusConflict, err.Error())
	default:
		writeError(w, http.StatusServiceUnavailable, "incident storage is unavailable; restart required")
	}
}

// readJSONObjectBody reads and strictly decodes a JSON object whose member
// names are all drawn from allowed. Syntax errors, non-objects, trailing
// data, duplicate keys, and unknown fields are reported as an error the
// caller turns into 400.
func readJSONObjectBody(w http.ResponseWriter, r *http.Request, allowed map[string]struct{}) (map[string]json.RawMessage, error) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		return nil, err
	}
	return decodeStrictObject(body, allowed)
}

// stringField returns the raw string member, or "" when absent, null, or not
// a JSON string; the domain layer then reports the empty field as 400.
func stringField(fields map[string]json.RawMessage, name string) string {
	raw, present := fields[name]
	if !present || bytes.Equal(raw, []byte("null")) {
		return ""
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return ""
	}
	return value
}

// requiredString demands a present, non-null JSON string member.
func requiredString(w http.ResponseWriter, fields map[string]json.RawMessage, name string) (string, bool) {
	raw, present := fields[name]
	if !present || bytes.Equal(raw, []byte("null")) {
		writeError(w, http.StatusBadRequest, name+" is required")
		return "", false
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		writeError(w, http.StatusBadRequest, name+" has the wrong type")
		return "", false
	}
	return value, true
}

// decodeStrictInt accepts only a decimal integer literal: fractions (1.0),
// scientific notation (1e2), signs, booleans, and quoted numbers are a wrong
// type.
func decodeStrictInt(w http.ResponseWriter, raw json.RawMessage, name string, target *int64) bool {
	if bytes.Equal(raw, []byte("null")) || !isDecimal(string(raw)) {
		writeError(w, http.StatusBadRequest, name+" must be a positive integer")
		return false
	}
	if err := json.Unmarshal(raw, target); err != nil {
		writeError(w, http.StatusBadRequest, name+" must be a positive integer")
		return false
	}
	return true
}

type historyEntryDTO struct {
	ActionID string `json:"action_id,omitempty"`
	Operator string `json:"operator"`
	Action   string `json:"action"`
	Content  string `json:"content"`
	Version  int    `json:"version"`
	At       string `json:"at"`
}

type incidentResponse struct {
	ID      string            `json:"id"`
	Title   string            `json:"title"`
	Service string            `json:"service"`
	Status  string            `json:"status"`
	Version int               `json:"version"`
	Events  []eventDTO        `json:"events"`
	History []historyEntryDTO `json:"history"`
}

// writeIncident serializes one committed incident snapshot. Linked events
// are embedded in link order with their full stored content; the incident
// never owns or modifies them.
func writeIncident(w http.ResponseWriter, h *Handler, inc incidents.Incident) {
	linked := h.timeline.EventsByID(inc.Links)
	eventDTOs := eventsToDTOs(linked)
	if eventDTOs == nil {
		eventDTOs = []eventDTO{}
	}

	history := make([]historyEntryDTO, len(inc.History))
	for i, entry := range inc.History {
		history[i] = historyEntryDTO{
			ActionID: entry.ActionID,
			Operator: entry.Operator,
			Action:   entry.Action,
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
		Events:  eventDTOs,
		History: history,
	})
}
