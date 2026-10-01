package httpapi

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/cocosolocoder/signalroom/internal/events"
)

// CursorSigner mints and verifies the opaque pagination tokens. The durable
// log implements it with the data directory's secret; tests may stub it.
type CursorSigner interface {
	SignCursor(payload []byte) (string, error)
	VerifyCursor(token string) ([]byte, error)
}

// cursorVersion is the only cursor payload format this server mints and
// accepts.
const cursorVersion = 1

// cursorData is the authenticated, opaque state of one paginated query. It
// freezes the full normalized filter set together with the complete ordered
// id list of the matching event set and the reader's offset inside it. Because
// every id is embedded, a resumed page never needs to consult "events added
// since" — later ingests simply are not on the list.
type cursorData struct {
	Version  int               `json:"v"`
	Service  string            `json:"s,omitempty"`
	Severity string            `json:"lvl,omitempty"`
	Since    *time.Time        `json:"since,omitempty"`
	Until    *time.Time        `json:"until,omitempty"`
	Labels   map[string]string `json:"lab,omitempty"`
	IDs      []string          `json:"ids"`
	Offset   int               `json:"off"`
}

func newCursorData(query events.PreparedQuery, ids []string, offset int) cursorData {
	data := cursorData{
		Version:  cursorVersion,
		Service:  query.Service,
		Severity: query.Severity,
		Labels:   query.Labels,
		IDs:      ids,
		Offset:   offset,
	}
	if !query.Since.IsZero() {
		since := query.Since
		data.Since = &since
	}
	if !query.Until.IsZero() {
		until := query.Until
		data.Until = &until
	}
	return data
}

// preparedQuery rebuilds the normalized filter frozen in the cursor.
func (c cursorData) preparedQuery() events.PreparedQuery {
	query := events.PreparedQuery{
		Service:  c.Service,
		Severity: c.Severity,
		Labels:   c.Labels,
	}
	if c.Since != nil {
		query.Since = *c.Since
	}
	if c.Until != nil {
		query.Until = *c.Until
	}
	return query
}

// valid checks the authenticated payload's internal consistency. The HMAC
// already proves origin and integrity; this guards shape: only the current
// version, a non-negative offset within the id list, and a present id list.
func (c cursorData) valid() bool {
	if c.Version != cursorVersion || c.IDs == nil {
		return false
	}
	return c.Offset >= 0 && c.Offset <= len(c.IDs)
}

func encodeCursor(signer CursorSigner, data cursorData) (string, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return "", err
	}
	return signer.SignCursor(raw)
}

func decodeCursor(signer CursorSigner, token string) (cursorData, error) {
	raw, err := signer.VerifyCursor(token)
	if err != nil {
		return cursorData{}, err
	}
	var data cursorData
	if err := json.Unmarshal(raw, &data); err != nil {
		return cursorData{}, errors.New("cursor payload is unreadable")
	}
	if !data.valid() {
		return cursorData{}, errors.New("cursor payload is out of range")
	}
	return data, nil
}
