package httpapi

import (
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/cocosolocoder/signalroom/internal/events"
)

// Pagination limit rules for GET /events/page. A missing limit defaults to
// defaultPageLimit; anything that is not a single decimal integer inside
// [1, events.MaxPageLimit] is a 400.
const defaultPageLimit = 100

// pageResponse is one slice of a frozen result set plus the cursor for the
// next slice. NextCursor is null at the end; Events is always an array.
type pageResponse struct {
	Events     []eventDTO `json:"events"`
	NextCursor *string    `json:"next_cursor"`
}

func (h *Handler) pageEvents(w http.ResponseWriter, r *http.Request) {
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

	limit, ok := parsePageLimit(w, values)
	if !ok {
		return
	}

	rawCursor, cursorPresent, ok := singleCursor(w, values)
	if !ok {
		return
	}

	if !cursorPresent {
		h.firstPage(w, values, limit)
		return
	}
	h.nextPage(w, values, rawCursor, limit)
}

// firstPage freezes the set of events matching the current filters, exactly
// the same filters GET /events accepts, and returns its first slice.
func (h *Handler) firstPage(w http.ResponseWriter, values url.Values, limit int) {
	query, err := parseEventFilter(values)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	snapshot := h.timeline.SnapshotRead(events.PrepareQuery(query))
	page, nextOffset, _ := snapshot.Page(0, limit)
	h.writePage(w, snapshot.Query(), snapshot.IDs(), page, nextOffset)
}

// nextPage resumes a frozen set from its cursor. Any filter parameter carried
// alongside the cursor must spell out the complete normalized filter set from
// the first page; a mismatch (including one changed condition) is a 400.
func (h *Handler) nextPage(w http.ResponseWriter, values url.Values, token string, limit int) {
	cursor, err := decodeCursor(h.log, token)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid or expired pagination cursor")
		return
	}

	if hasFilterParams(values) {
		query, ferr := parseEventFilter(values)
		if ferr != nil {
			writeError(w, http.StatusBadRequest, ferr.Error())
			return
		}
		if !events.PrepareQuery(query).EqualTo(cursor.preparedQuery()) {
			writeError(w, http.StatusBadRequest, "query filters do not match the cursor's original filters")
			return
		}
	}

	page, nextOffset, err := h.timeline.ResumePage(cursor.IDs, cursor.Offset, limit)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid or expired pagination cursor")
		return
	}
	h.writePage(w, cursor.preparedQuery(), cursor.IDs, page, nextOffset)
}

// writePage mints the next cursor (or null at the end) and emits the page.
func (h *Handler) writePage(w http.ResponseWriter, query events.PreparedQuery, ids []string, page []events.Event, nextOffset int) {
	resp := pageResponse{Events: eventsToDTOs(page)}
	if nextOffset < len(ids) {
		token, err := encodeCursor(h.log, newCursorData(query, ids, nextOffset))
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, "event storage is unavailable; restart required")
			return
		}
		resp.NextCursor = &token
	}
	writeJSON(w, http.StatusOK, resp)
}

// singleCursor accepts the cursor parameter only when it is absent or appears
// exactly once with a non-empty value; a missing cursor starts a first page.
func singleCursor(w http.ResponseWriter, values url.Values) (string, bool, bool) {
	raws, present := values["cursor"]
	if !present {
		return "", false, true
	}
	if len(raws) != 1 || raws[0] == "" {
		writeError(w, http.StatusBadRequest, "cursor must be a single non-empty value")
		return "", false, false
	}
	return raws[0], true, true
}

// parsePageLimit enforces the single decimal integer rule and the 1..1000
// range, defaulting to defaultPageLimit when the parameter is absent.
func parsePageLimit(w http.ResponseWriter, values url.Values) (int, bool) {
	raws, present := values["limit"]
	if !present {
		return defaultPageLimit, true
	}
	if len(raws) != 1 || raws[0] == "" || !isDecimal(raws[0]) {
		writeError(w, http.StatusBadRequest, "limit must be a single decimal integer from 1 to 1000")
		return 0, false
	}
	value, err := strconv.Atoi(raws[0])
	if err != nil || value < 1 || value > events.MaxPageLimit {
		writeError(w, http.StatusBadRequest, "limit must be a single decimal integer from 1 to 1000")
		return 0, false
	}
	return value, true
}

// filterParamNames are the GET /events filter keys, all of which must match a
// cursor's frozen set when any one of them accompanies a continuation.
var filterParamNames = [...]string{"service", "severity", "since", "until", "label"}

func hasFilterParams(values url.Values) bool {
	for _, name := range filterParamNames {
		if _, present := values[name]; present {
			return true
		}
	}
	return false
}

// parseEventFilter parses and validates the GET /events filter parameters,
// sharing their exact rules: trimmed service, case-insensitive severity,
// inclusive RFC3339Nano time bounds, and repeatable name=value labels.
func parseEventFilter(values url.Values) (events.Query, error) {
	labels, err := events.ParseLabelConditions(values["label"])
	if err != nil {
		return events.Query{}, err
	}
	query := events.Query{
		Service:  values.Get("service"),
		Severity: values.Get("severity"),
		Labels:   labels,
	}
	if raw := values.Get("since"); raw != "" {
		parsed, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			return events.Query{}, errors.New("since must be an RFC3339Nano timestamp")
		}
		query.Since = parsed
	}
	if raw := values.Get("until"); raw != "" {
		parsed, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			return events.Query{}, errors.New("until must be an RFC3339Nano timestamp")
		}
		query.Until = parsed
	}
	if !query.Since.IsZero() && !query.Until.IsZero() && query.Since.After(query.Until) {
		return events.Query{}, errors.New("since must not be after until")
	}
	return query, nil
}
