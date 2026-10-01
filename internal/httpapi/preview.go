package httpapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/cocosolocoder/signalroom/internal/events"
)

// preview bounds for the integer request fields.
const (
	minPreviewWindowSeconds = 1
	maxPreviewWindowSeconds = 86400
	minPreviewThreshold     = 1
	maxPreviewThreshold     = 1000000
	minPreviewStreak        = 1
	maxPreviewStreak        = 100
	maxPreviewGroupBy       = 4
)

// previewRequest is the POST /alerts/preview body. All fields except the
// filters are required; the strict decoder rejects unknown fields and the
// duplicate-key walk rejects repeated JSON fields.
type previewRequest struct {
	Since          time.Time         `json:"since"`
	Until          time.Time         `json:"until"`
	WindowSeconds  int               `json:"window_seconds"`
	Threshold      int               `json:"threshold"`
	TriggerWindows int               `json:"trigger_windows"`
	RecoverWindows int               `json:"recover_windows"`
	Service        string            `json:"service"`
	Severity       string            `json:"severity"`
	Labels         map[string]string `json:"labels"`
	GroupBy        []string          `json:"group_by"`
}

// previewResponse is the 200 body. Groups carry their per-window boundaries,
// counts, and end-of-window states; Alerts lists every alert in trigger-time
// order across all groups.
type previewResponse struct {
	Groups []previewGroupDTO `json:"groups"`
	Alerts []previewAlertDTO `json:"alerts"`
}

type previewGroupDTO struct {
	Service string             `json:"service"`
	Labels  map[string]*string `json:"labels,omitempty"`
	Windows []previewWindowDTO `json:"windows"`
}

type previewWindowDTO struct {
	Since string `json:"since"`
	Until string `json:"until"`
	Count int    `json:"count"`
	State string `json:"state"`
}

type previewAlertDTO struct {
	Service     string             `json:"service"`
	Labels      map[string]*string `json:"labels,omitempty"`
	TriggeredAt string             `json:"triggered_at"`
	RecoveredAt *string            `json:"recovered_at"`
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
	if err := rejectDuplicateJSONFields(body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}

	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	var req previewRequest
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid preview request")
		return
	}
	var extra json.RawMessage
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	if err := validatePreviewRequest(req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	labels, err := events.NormalizeLabels(req.Labels)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	groupBy, err := normalizePreviewGroupBy(req.GroupBy)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	query := events.AlertPreviewQuery{
		Service:        req.Service,
		Severity:       req.Severity,
		Labels:         labels,
		Since:          req.Since,
		Until:          req.Until,
		Window:         time.Duration(req.WindowSeconds) * time.Second,
		Threshold:      req.Threshold,
		TriggerWindows: req.TriggerWindows,
		RecoverWindows: req.RecoverWindows,
		GroupBy:        groupBy,
	}
	preview, err := h.timeline.PreviewAlerts(query)
	if err != nil {
		switch {
		case errors.Is(err, events.ErrPreviewTooManyWindows):
			writeError(w, http.StatusBadRequest, "preview range requires more than 10000 windows")
		case errors.Is(err, events.ErrPreviewTooManyGroups):
			writeError(w, http.StatusBadRequest, "preview requires more than 100000 group-window cells")
		default:
			writeError(w, http.StatusBadRequest, err.Error())
		}
		return
	}

	writeJSON(w, http.StatusOK, previewResponseFromResult(preview))
}

// validatePreviewRequest checks required presence, the range order, and the
// integer bounds. Missing timestamps surface as zero instants, which fail the
// range check with a specific message.
func validatePreviewRequest(req previewRequest) error {
	if req.Since.IsZero() {
		return errors.New("since is required and must be an RFC3339Nano timestamp")
	}
	if req.Until.IsZero() {
		return errors.New("until is required and must be an RFC3339Nano timestamp")
	}
	if !req.Since.Before(req.Until) {
		return errors.New("since must be before until")
	}
	if req.WindowSeconds < minPreviewWindowSeconds || req.WindowSeconds > maxPreviewWindowSeconds {
		return errors.New("window_seconds must be an integer from 1 to 86400")
	}
	if req.Threshold < minPreviewThreshold || req.Threshold > maxPreviewThreshold {
		return errors.New("threshold must be an integer from 1 to 1000000")
	}
	if req.TriggerWindows < minPreviewStreak || req.TriggerWindows > maxPreviewStreak {
		return errors.New("trigger_windows must be an integer from 1 to 100")
	}
	if req.RecoverWindows < minPreviewStreak || req.RecoverWindows > maxPreviewStreak {
		return errors.New("recover_windows must be an integer from 1 to 100")
	}
	return nil
}

// normalizePreviewGroupBy trims and validates the group-by label names using
// the same name rules as ingestion: non-empty, at most 64 Unicode code points,
// and no two names colliding after trimming. At most four names are allowed.
func normalizePreviewGroupBy(raw []string) ([]string, error) {
	if len(raw) > maxPreviewGroupBy {
		return nil, fmt.Errorf("group_by may contain at most %d label names", maxPreviewGroupBy)
	}
	out := make([]string, 0, len(raw))
	seen := make(map[string]struct{}, len(raw))
	for _, name := range raw {
		name = strings.TrimSpace(name)
		if name == "" {
			return nil, errors.New("group_by label name must not be empty")
		}
		if utf8.RuneCountInString(name) > events.MaxLabelNameRunes {
			return nil, fmt.Errorf("group_by label name must not exceed %d characters", events.MaxLabelNameRunes)
		}
		if _, dup := seen[name]; dup {
			return nil, fmt.Errorf("group_by label name %q repeats after trimming", name)
		}
		seen[name] = struct{}{}
		out = append(out, name)
	}
	return out, nil
}

// previewResponseFromResult maps the domain result to its wire form, rendering
// every instant in UTC with nanosecond precision.
func previewResponseFromResult(preview events.AlertPreview) previewResponse {
	groups := make([]previewGroupDTO, len(preview.Groups))
	for i, group := range preview.Groups {
		windows := make([]previewWindowDTO, len(group.Windows))
		for j, window := range group.Windows {
			windows[j] = previewWindowDTO{
				Since: utcRFC3339Nano(window.Since),
				Until: utcRFC3339Nano(window.Until),
				Count: window.Count,
				State: window.State,
			}
		}
		groups[i] = previewGroupDTO{
			Service: group.Service,
			Labels:  group.Labels,
			Windows: windows,
		}
	}

	alerts := make([]previewAlertDTO, len(preview.Alerts))
	for i, alert := range preview.Alerts {
		dto := previewAlertDTO{
			Service:     alert.Service,
			Labels:      alert.Labels,
			TriggeredAt: utcRFC3339Nano(alert.TriggeredAt),
			RecoveredAt: nil,
		}
		if alert.RecoveredAt != nil {
			recovered := utcRFC3339Nano(*alert.RecoveredAt)
			dto.RecoveredAt = &recovered
		}
		alerts[i] = dto
	}
	return previewResponse{Groups: groups, Alerts: alerts}
}

// rejectDuplicateJSONFields walks the JSON document and rejects any object
// with a repeated key, including nested label objects. encoding/json silently
// overwrites duplicate keys on decode, so the walk is the only guard.
func rejectDuplicateJSONFields(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	type frame struct {
		object    bool
		expectKey bool
		keys      map[string]struct{}
	}
	var stack []*frame
	for {
		tok, err := dec.Token()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if delim, ok := tok.(json.Delim); ok {
			switch delim {
			case '{':
				stack = append(stack, &frame{object: true, expectKey: true, keys: map[string]struct{}{}})
			case '[':
				stack = append(stack, &frame{object: false})
			case '}', ']':
				if len(stack) == 0 {
					return errors.New("unexpected JSON delimiter")
				}
				stack = stack[:len(stack)-1]
			}
			continue
		}
		if len(stack) == 0 || !stack[len(stack)-1].object {
			continue
		}
		f := stack[len(stack)-1]
		if f.expectKey {
			key, ok := tok.(string)
			if !ok {
				return errors.New("invalid JSON object key")
			}
			if _, dup := f.keys[key]; dup {
				return fmt.Errorf("duplicate JSON field %q", key)
			}
			f.keys[key] = struct{}{}
			f.expectKey = false
		} else {
			f.expectKey = true
		}
	}
}
