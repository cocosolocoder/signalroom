package httpapi

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/cocosolocoder/signalroom/internal/events"
)

// Pagination defaults and bounds.
const (
	defaultPageLimit = 100
	maxPageLimit     = 1000
	cursorVersion    = 1
)

// CursorStore persists the per-directory signing key and query snapshots for
// one data directory. Cursors are only valid against the directory whose key
// signed them.
type CursorStore interface {
	CursorKey() []byte
	SaveSnapshot(id string, payload []byte) error
	LoadSnapshot(id string) ([]byte, error)
}

// pageResponse is the GET /events/page body. next_cursor is null when the
// snapshot has no more records.
type pageResponse struct {
	Events     []eventDTO `json:"events"`
	NextCursor *string    `json:"next_cursor"`
}

// snapshotQuery is the normalized, comparable form of a page request's
// filters. Bounds are pointers so a missing end and the zero instant are
// distinguishable.
type snapshotQuery struct {
	Service  string            `json:"service"`
	Severity string            `json:"severity"`
	Since    *time.Time        `json:"since,omitempty"`
	Until    *time.Time        `json:"until,omitempty"`
	Labels   map[string]string `json:"labels,omitempty"`
}

func snapshotQueryFromQuery(q events.Query) snapshotQuery {
	sq := snapshotQuery{
		Service:  strings.TrimSpace(q.Service),
		Severity: strings.ToLower(strings.TrimSpace(q.Severity)),
		Labels:   q.Labels,
	}
	if !q.Since.IsZero() {
		t := q.Since
		sq.Since = &t
	}
	if !q.Until.IsZero() {
		t := q.Until
		sq.Until = &t
	}
	return sq
}

// matches reports whether q, after the same normalization, is this filter
// set. Time zones and label order are irrelevant; severity case and
// surrounding whitespace are normalized away.
func (sq snapshotQuery) matches(q events.Query) bool {
	if sq.Service != strings.TrimSpace(q.Service) {
		return false
	}
	if sq.Severity != strings.ToLower(strings.TrimSpace(q.Severity)) {
		return false
	}
	if !timePtrEqual(sq.Since, q.Since) || !timePtrEqual(sq.Until, q.Until) {
		return false
	}
	return maps.Equal(sq.Labels, q.Labels)
}

// equal compares two stored filter sets, used to cross-check a cursor
// against its snapshot.
func (sq snapshotQuery) equal(other snapshotQuery) bool {
	if sq.Service != other.Service || sq.Severity != other.Severity {
		return false
	}
	if !timePtrPtrEqual(sq.Since, other.Since) || !timePtrPtrEqual(sq.Until, other.Until) {
		return false
	}
	return maps.Equal(sq.Labels, other.Labels)
}

func timePtrEqual(p *time.Time, t time.Time) bool {
	if p == nil {
		return t.IsZero()
	}
	return !t.IsZero() && p.Equal(t)
}

func timePtrPtrEqual(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Equal(*b)
}

// snapshotPayload is the durable record of one first-page query: the ordered
// id set captured at that moment and the normalized filters that produced it.
type snapshotPayload struct {
	IDs   []string      `json:"ids"`
	Query snapshotQuery `json:"query"`
}

// cursorPayload is the signed, opaque continuation token. It carries the
// snapshot id, the next offset, the filter set (cross-checked against the
// snapshot), and a fingerprint of the signing key so a cursor from another
// data directory is rejected even before the HMAC is considered.
type cursorPayload struct {
	V int           `json:"v"`
	S string        `json:"s"`
	O int           `json:"o"`
	Q snapshotQuery `json:"q"`
	D string        `json:"d"`
}

func fingerprint(key []byte) string {
	sum := sha256.Sum256(key)
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

func encodeCursor(cp cursorPayload, key []byte) string {
	payload, _ := json.Marshal(cp)
	mac := hmac.New(sha256.New, key)
	mac.Write(payload)
	return base64.RawURLEncoding.EncodeToString(payload) + "." +
		base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func decodeCursor(encoded string, key []byte) (cursorPayload, error) {
	parts := strings.Split(encoded, ".")
	if len(parts) != 2 {
		return cursorPayload{}, errors.New("invalid cursor")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return cursorPayload{}, errors.New("invalid cursor")
	}
	gotMAC, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return cursorPayload{}, errors.New("invalid cursor")
	}
	mac := hmac.New(sha256.New, key)
	mac.Write(payload)
	if !hmac.Equal(gotMAC, mac.Sum(nil)) {
		return cursorPayload{}, errors.New("invalid cursor")
	}
	var cp cursorPayload
	if err := json.Unmarshal(payload, &cp); err != nil {
		return cursorPayload{}, errors.New("invalid cursor")
	}
	if cp.V != cursorVersion {
		return cursorPayload{}, errors.New("invalid cursor")
	}
	return cp, nil
}

// pageEvents serves GET /events/page. A request without a cursor starts a
// fresh snapshot; a request with one continues the snapshot it names, with
// the same filters and a possibly changed limit.
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
	key := h.cursors.CursorKey()

	var snap snapshotPayload
	var snapshotID string
	offset := 0

	cursorValues, hasCursor := values["cursor"]
	if hasCursor {
		if len(cursorValues) != 1 {
			writeError(w, http.StatusBadRequest, "cursor must not be empty")
			return
		}
		rawCursor := cursorValues[0]
		if rawCursor == "" {
			writeError(w, http.StatusBadRequest, "cursor must not be empty")
			return
		}
		cp, err := decodeCursor(rawCursor, key)
		if err != nil {
			writeError(w, http.StatusBadRequest, "cursor is invalid")
			return
		}
		if cp.D != fingerprint(key) {
			writeError(w, http.StatusBadRequest, "cursor is invalid")
			return
		}
		raw, err := h.cursors.LoadSnapshot(cp.S)
		if err != nil {
			writeError(w, http.StatusBadRequest, "cursor is invalid")
			return
		}
		if err := json.Unmarshal(raw, &snap); err != nil {
			writeError(w, http.StatusBadRequest, "cursor is invalid")
			return
		}
		if !cp.Q.equal(snap.Query) {
			writeError(w, http.StatusBadRequest, "cursor is invalid")
			return
		}
		offset = cp.O
		if offset < 0 || offset > len(snap.IDs) {
			writeError(w, http.StatusBadRequest, "cursor is invalid")
			return
		}
		if anyFilterPresent(values) {
			q, err := buildPageQuery(values)
			if err != nil {
				writeError(w, http.StatusBadRequest, err.Error())
				return
			}
			if !snap.Query.matches(q) {
				writeError(w, http.StatusBadRequest, "cursor does not match the query filters")
				return
			}
		}
		snapshotID = cp.S
	} else {
		q, err := buildPageQuery(values)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		snapshotID, err = newSnapshotID()
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, "event storage is unavailable; restart required")
			return
		}
		snap = snapshotPayload{
			IDs:   h.timeline.SnapshotIDs(q),
			Query: snapshotQueryFromQuery(q),
		}
		raw, err := json.Marshal(snap)
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, "event storage is unavailable; restart required")
			return
		}
		if err := h.cursors.SaveSnapshot(snapshotID, raw); err != nil {
			writeError(w, http.StatusServiceUnavailable, "event storage is unavailable; restart required")
			return
		}
	}

	limit, err := parsePageLimit(values)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	end := offset + limit
	if end > len(snap.IDs) {
		end = len(snap.IDs)
	}
	pageIDs := snap.IDs[offset:end]
	page := h.timeline.EventsByID(pageIDs)
	if len(page) != len(pageIDs) {
		writeError(w, http.StatusBadRequest, "cursor is invalid")
		return
	}

	var nextCursor *string
	if end < len(snap.IDs) {
		cp := cursorPayload{
			V: cursorVersion, S: snapshotID, O: end, Q: snap.Query, D: fingerprint(key),
		}
		encoded := encodeCursor(cp, key)
		nextCursor = &encoded
	}

	writeJSON(w, http.StatusOK, pageResponse{Events: eventsToDTOs(page), NextCursor: nextCursor})
}

// buildPageQuery parses and normalizes the same filters GET /events accepts.
func buildPageQuery(values url.Values) (events.Query, error) {
	labels, err := events.ParseLabelConditions(values["label"])
	if err != nil {
		return events.Query{}, err
	}
	q := events.Query{
		Service:  values.Get("service"),
		Severity: values.Get("severity"),
		Labels:   labels,
	}
	if raw := values.Get("since"); raw != "" {
		parsed, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			return events.Query{}, errors.New("since must be an RFC3339Nano timestamp")
		}
		q.Since = parsed
	}
	if raw := values.Get("until"); raw != "" {
		parsed, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			return events.Query{}, errors.New("until must be an RFC3339Nano timestamp")
		}
		q.Until = parsed
	}
	if !q.Since.IsZero() && !q.Until.IsZero() && q.Since.After(q.Until) {
		return events.Query{}, errors.New("since must not be after until")
	}
	return q, nil
}

// anyFilterPresent reports whether the request carries any filter parameter,
// which switches a continuation from "reuse the first page's filters" to
// "check the full filter set".
func anyFilterPresent(values url.Values) bool {
	for _, name := range []string{"service", "severity", "since", "until", "label"} {
		if _, ok := values[name]; ok {
			return true
		}
	}
	return false
}

// parsePageLimit returns the limit, defaulting to defaultPageLimit when
// omitted. Empty, repeated, or out-of-range values are rejected.
func parsePageLimit(values url.Values) (int, error) {
	vs, present := values["limit"]
	if !present {
		return defaultPageLimit, nil
	}
	if len(vs) != 1 {
		return 0, errors.New("limit must be an integer from 1 to 1000")
	}
	raw := vs[0]
	if raw == "" || !isDecimal(raw) {
		return 0, errors.New("limit must be an integer from 1 to 1000")
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 || n > maxPageLimit {
		return 0, errors.New("limit must be an integer from 1 to 1000")
	}
	return n, nil
}

// newSnapshotID returns a random hex id for one query snapshot.
func newSnapshotID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
