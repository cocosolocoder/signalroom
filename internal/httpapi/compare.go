package httpapi

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/cocosolocoder/signalroom/internal/events"
)

// step bounds for the required integer step parameter, in seconds.
const (
	minStepSeconds = 1
	maxStepSeconds = 86400
)

type compareResponse struct {
	BaselineTotal    int                 `json:"baseline_total"`
	ObservationTotal int                 `json:"observation_total"`
	Difference       int                 `json:"difference"`
	Ratio            *float64            `json:"ratio"`
	Segments         []compareSegmentDTO `json:"segments"`
}

type compareSegmentDTO struct {
	BaselineSince    string `json:"baseline_since"`
	BaselineUntil    string `json:"baseline_until"`
	Since            string `json:"since"`
	Until            string `json:"until"`
	BaselineCount    int    `json:"baseline_count"`
	ObservationCount int    `json:"observation_count"`
	Difference       int    `json:"difference"`
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

	// Each timestamp must be present exactly once and parse as RFC3339Nano;
	// presence for a parameter is validated before moving to the next one.
	parseParam := func(name string) (time.Time, bool) {
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
	baselineStart, ok := parseParam("baseline_since")
	if !ok {
		return
	}
	baselineEnd, ok := parseParam("baseline_until")
	if !ok {
		return
	}
	observedStart, ok := parseParam("since")
	if !ok {
		return
	}
	observedEnd, ok := parseParam("until")
	if !ok {
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

	labels, lerr := events.ParseLabelConditions(values["label"])
	if lerr != nil {
		writeError(w, http.StatusBadRequest, lerr.Error())
		return
	}

	query := events.CompareQuery{
		Service:       values.Get("service"),
		Severity:      values.Get("severity"),
		Labels:        labels,
		BaselineSince: baselineStart,
		BaselineUntil: baselineEnd,
		Since:         observedStart,
		Until:         observedEnd,
		Step:          time.Duration(stepSeconds) * time.Second,
	}
	result, cerr := h.timeline.Compare(query)
	if cerr != nil {
		switch {
		case errors.Is(cerr, events.ErrBaselineWindow):
			writeError(w, http.StatusBadRequest, "baseline_since must be before baseline_until")
		case errors.Is(cerr, events.ErrObservationWindow):
			writeError(w, http.StatusBadRequest, "since must be before until")
		case errors.Is(cerr, events.ErrWindowLength):
			writeError(w, http.StatusBadRequest, "baseline and observation windows must have equal length")
		case errors.Is(cerr, events.ErrTooManySegments):
			writeError(w, http.StatusBadRequest, "windows must not require more than 10000 segments")
		default:
			writeError(w, http.StatusBadRequest, "step must be an integer from 1 to 86400 seconds")
		}
		return
	}

	segments := make([]compareSegmentDTO, len(result.Segments))
	for i, segment := range result.Segments {
		segments[i] = compareSegmentDTO{
			BaselineSince:    utcRFC3339Nano(segment.BaselineStart),
			BaselineUntil:    utcRFC3339Nano(segment.BaselineEnd),
			Since:            utcRFC3339Nano(segment.ObservationStart),
			Until:            utcRFC3339Nano(segment.ObservationEnd),
			BaselineCount:    segment.BaselineCount,
			ObservationCount: segment.ObservationCount,
			Difference:       segment.ObservationCount - segment.BaselineCount,
		}
	}
	writeJSON(w, http.StatusOK, compareResponse{
		BaselineTotal:    result.BaselineTotal,
		ObservationTotal: result.ObservationTotal,
		Difference:       result.Difference,
		Ratio:            result.Ratio,
		Segments:         segments,
	})
}

// singleValue returns a parameter only when it appears exactly once. A
// missing key and a repeated key both fail; callers distinguish emptiness
// separately to keep the "required" wording.
func singleValue(values map[string][]string, name string) (string, bool) {
	vs, present := values[name]
	if !present || len(vs) != 1 {
		return "", false
	}
	return vs[0], true
}

// isDecimal accepts a non-empty run of ASCII digits, rejecting signs, spaces,
// fractions, hex notation, and other forms a permissive integer parser might
// accept. Leading zeros (e.g. "01") are allowed, matching the accepted input.
func isDecimal(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func utcRFC3339Nano(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}
