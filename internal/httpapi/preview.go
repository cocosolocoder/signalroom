package httpapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/cocosolocoder/signalroom/internal/events"
	"github.com/cocosolocoder/signalroom/internal/strictjson"
)

// maxGroupLabels bounds how many label names a preview request may group by.
const maxGroupLabels = 4

// allowedPreviewFields is the exact set of members POST /alerts/preview
// accepts; anything else is an unknown field.
var allowedPreviewFields = map[string]struct{}{
	"since":           {},
	"until":           {},
	"window_seconds":  {},
	"threshold":       {},
	"trigger_windows": {},
	"recover_windows": {},
	"service":         {},
	"severity":        {},
	"labels":          {},
	"group_labels":    {},
}

type previewResponse struct {
	GroupLabels []string          `json:"group_labels"`
	Groups      []previewGroupDTO `json:"groups"`
}

type previewGroupDTO struct {
	Service     string          `json:"service"`
	LabelValues []*string       `json:"label_values"`
	Windows     []previewWindow `json:"windows"`
	Alerts      []previewAlert  `json:"alerts"`
}

type previewWindow struct {
	Since  string `json:"since"`
	Until  string `json:"until"`
	Count  int    `json:"count"`
	Status string `json:"status"`
}

type previewAlert struct {
	TriggeredAt string  `json:"triggered_at"`
	RecoveredAt *string `json:"recovered_at"`
}

func (h *Handler) previewAlerts(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if h.unavailable() {
		writeError(w, http.StatusServiceUnavailable, "event storage is unavailable; restart required")
		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	fields, err := decodeStrictObject(body, allowedPreviewFields)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	query, ok := buildPreviewQuery(w, fields)
	if !ok {
		return
	}

	result, perr := h.timeline.PreviewAlerts(query)
	if perr != nil {
		writePreviewError(w, perr)
		return
	}

	groups := make([]previewGroupDTO, 0, len(result.Groups))
	for _, group := range result.Groups {
		windows := make([]previewWindow, len(group.Windows))
		for i, window := range group.Windows {
			windows[i] = previewWindow{
				Since:  utcRFC3339Nano(window.Start),
				Until:  utcRFC3339Nano(window.End),
				Count:  window.Count,
				Status: window.Status,
			}
		}
		alerts := make([]previewAlert, len(group.Alerts))
		for i, alert := range group.Alerts {
			dto := previewAlert{TriggeredAt: utcRFC3339Nano(alert.OpenedAt)}
			if alert.RecoveredAt != nil {
				recovered := utcRFC3339Nano(*alert.RecoveredAt)
				dto.RecoveredAt = &recovered
			}
			alerts[i] = dto
		}
		groups = append(groups, previewGroupDTO{
			Service:     group.Service,
			LabelValues: nonNilLabelValues(group.LabelValues),
			Windows:     windows,
			Alerts:      alerts,
		})
	}

	names := result.GroupLabelNames
	if names == nil {
		names = []string{}
	}
	writeJSON(w, http.StatusOK, previewResponse{
		GroupLabels: names,
		Groups:      groups,
	})
}

// nonNilLabelValues keeps a missing-label null entry but turns a nil (no
// group labels) slice into an empty JSON array.
func nonNilLabelValues(values []*string) []*string {
	if values == nil {
		return []*string{}
	}
	return values
}

// buildPreviewQuery validates the decoded object members and returns the
// normalized query. It writes the 400 itself and returns ok=false on any
// problem, so callers never produce a partial result.
func buildPreviewQuery(w http.ResponseWriter, fields map[string]json.RawMessage) (events.AlertPreviewQuery, bool) {
	var query events.AlertPreviewQuery

	parseTime := func(name string) (time.Time, bool) {
		var text string
		if !decodeRequired(w, fields, name, &text) {
			return time.Time{}, false
		}
		parsed, err := time.Parse(time.RFC3339Nano, text)
		if err != nil {
			writeError(w, http.StatusBadRequest, name+" must be an RFC3339Nano timestamp")
			return time.Time{}, false
		}
		return parsed, true
	}

	since, ok := parseTime("since")
	if !ok {
		return query, false
	}
	until, ok := parseTime("until")
	if !ok {
		return query, false
	}
	if !until.After(since) {
		writeError(w, http.StatusBadRequest, "since must be before until")
		return query, false
	}

	var windowSeconds int64
	if !decodeRequiredInt(w, fields, "window_seconds", &windowSeconds) {
		return query, false
	}
	if windowSeconds < 1 || windowSeconds > 86400 {
		writeError(w, http.StatusBadRequest, "window_seconds must be an integer from 1 to 86400")
		return query, false
	}

	var threshold int64
	if !decodeRequiredInt(w, fields, "threshold", &threshold) {
		return query, false
	}
	if threshold < 1 || threshold > 1000000 {
		writeError(w, http.StatusBadRequest, "threshold must be an integer from 1 to 1000000")
		return query, false
	}

	var triggerWindows int64
	if !decodeRequiredInt(w, fields, "trigger_windows", &triggerWindows) {
		return query, false
	}
	if triggerWindows < 1 || triggerWindows > 100 {
		writeError(w, http.StatusBadRequest, "trigger_windows must be an integer from 1 to 100")
		return query, false
	}

	var recoverWindows int64
	if !decodeRequiredInt(w, fields, "recover_windows", &recoverWindows) {
		return query, false
	}
	if recoverWindows < 1 || recoverWindows > 100 {
		writeError(w, http.StatusBadRequest, "recover_windows must be an integer from 1 to 100")
		return query, false
	}
	if raw, present := fields["service"]; present {
		if bytes.Equal(raw, []byte("null")) {
			writeError(w, http.StatusBadRequest, "service has the wrong type")
			return query, false
		}
		var service string
		if !decodeRaw(w, raw, "service", &service) {
			return query, false
		}
		query.Service = service
	}
	if raw, present := fields["severity"]; present {
		if bytes.Equal(raw, []byte("null")) {
			writeError(w, http.StatusBadRequest, "severity has the wrong type")
			return query, false
		}
		var severity string
		if !decodeRaw(w, raw, "severity", &severity) {
			return query, false
		}
		query.Severity = severity
	}

	// labels follows ingestion semantics, where null means "no labels".
	if raw, present := fields["labels"]; present && !bytes.Equal(raw, []byte("null")) {
		decoded, lerr := events.DecodeLabelsObject(raw)
		if lerr != nil {
			writeError(w, http.StatusBadRequest, "labels must be an object of string values")
			return query, false
		}
		conditions, lerr := events.NormalizeLabelConditions(decoded)
		if lerr != nil {
			writeError(w, http.StatusBadRequest, lerr.Error())
			return query, false
		}
		query.Labels = conditions
	}

	if raw, present := fields["group_labels"]; present {
		if bytes.Equal(raw, []byte("null")) {
			writeError(w, http.StatusBadRequest, "group_labels must be an array of label names")
			return query, false
		}
		var requested []string
		if err := json.Unmarshal(raw, &requested); err != nil {
			writeError(w, http.StatusBadRequest, "group_labels must be an array of label names")
			return query, false
		}
		if len(requested) > maxGroupLabels {
			writeError(w, http.StatusBadRequest, "group_labels may contain at most four names")
			return query, false
		}
		seen := make(map[string]struct{}, len(requested))
		names := make([]string, 0, len(requested))
		for _, name := range requested {
			normalized, gerr := events.NormalizeLabelName(name)
			if gerr != nil {
				writeError(w, http.StatusBadRequest, gerr.Error())
				return query, false
			}
			if _, dup := seen[normalized]; dup {
				writeError(w, http.StatusBadRequest, "group_labels names must not repeat after trimming")
				return query, false
			}
			seen[normalized] = struct{}{}
			names = append(names, normalized)
		}
		query.GroupLabels = names
	}

	query.Since = since
	query.Until = until
	query.Window = time.Duration(windowSeconds) * time.Second
	query.Threshold = int(threshold)
	query.TriggerWindows = int(triggerWindows)
	query.RecoverWindows = int(recoverWindows)
	return query, true
}

// decodeRequired unmarshals one present, non-null member into target. A
// missing or null member is reported with the field's name.
func decodeRequired(w http.ResponseWriter, fields map[string]json.RawMessage, name string, target any) bool {
	raw, present := fields[name]
	if !present || bytes.Equal(raw, []byte("null")) {
		writeError(w, http.StatusBadRequest, name+" is required")
		return false
	}
	return decodeRaw(w, raw, name, target)
}

// decodeRequiredInt is decodeRequired for members that must be decimal
// integer literals: scientific notation (1e2), fractions (1.0), signs, and
// quoted numbers are all a wrong type, matching the query-parameter rules.
func decodeRequiredInt(w http.ResponseWriter, fields map[string]json.RawMessage, name string, target *int64) bool {
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
	return true
}

// decodeRaw unmarshals one member, reporting a typed 400 on failure.
func decodeRaw(w http.ResponseWriter, raw json.RawMessage, name string, target any) bool {
	if err := json.Unmarshal(raw, target); err != nil {
		writeError(w, http.StatusBadRequest, name+" has the wrong type")
		return false
	}
	return true
}

// decodeStrictObject decodes the outer JSON value, requiring an object with
// unique member names drawn from allowed. Syntax errors, non-objects,
// trailing tokens, duplicate keys, and unknown fields are all rejected. It
// is the request-body form of the strict object scan the batch decoders
// share in internal/strictjson.
func decodeStrictObject(body []byte, allowed map[string]struct{}) (map[string]json.RawMessage, error) {
	return strictjson.DecodeObject(body, "request body", allowed)
}

func writePreviewError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, events.ErrAlertRange):
		writeError(w, http.StatusBadRequest, "since must be before until")
	case errors.Is(err, events.ErrAlertWindow):
		writeError(w, http.StatusBadRequest, "window_seconds must be an integer from 1 to 86400")
	case errors.Is(err, events.ErrAlertThreshold):
		writeError(w, http.StatusBadRequest, "threshold must be an integer from 1 to 1000000")
	case errors.Is(err, events.ErrAlertConsecutive):
		writeError(w, http.StatusBadRequest, "trigger_windows and recover_windows must be integers from 1 to 100")
	case errors.Is(err, events.ErrAlertGroupLabels):
		writeError(w, http.StatusBadRequest, "group_labels may contain at most four names")
	case errors.Is(err, events.ErrAlertTooManyWindows):
		writeError(w, http.StatusBadRequest, "range must not require more than 10000 windows")
	case errors.Is(err, events.ErrAlertTooManyGroups):
		writeError(w, http.StatusBadRequest, "preview must not require more than 100000 group windows")
	default:
		writeError(w, http.StatusBadRequest, "invalid alert preview request")
	}
}
