package logfile

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"strings"
)

// ErrCursorSignature means a cursor token was malformed or not signed with
// this data directory's key. Callers turn it into a 400.
var ErrCursorSignature = errors.New("cursor signature is invalid")

// SignCursor binds payload to the data directory with HMAC-SHA256 and returns
// an opaque token of the form base64url(payload).base64url(mac). The payload
// is readable only as untrusted input after VerifyCursor succeeds.
func (l *Log) SignCursor(payload []byte) (string, error) {
	key := l.cursorKey // immutable after Open, so no lock is needed
	if len(key) == 0 {
		return "", errors.New("cursor key is not initialized")
	}
	mac := hmac.New(sha256.New, key)
	mac.Write(payload)
	enc := base64.RawURLEncoding
	return enc.EncodeToString(payload) + "." + enc.EncodeToString(mac.Sum(nil)), nil
}

// VerifyCursor checks the token's shape and its HMAC against the directory
// key, returning the authenticated payload. A token from another data
// directory, a truncated token, or a tampered payload fails here.
func (l *Log) VerifyCursor(token string) ([]byte, error) {
	key := l.cursorKey // immutable after Open, so no lock is needed
	if len(key) == 0 {
		return nil, errors.New("cursor key is not initialized")
	}

	payloadPart, macPart, found := strings.Cut(token, ".")
	if !found || payloadPart == "" || macPart == "" || strings.Contains(macPart, ".") {
		return nil, ErrCursorSignature
	}
	enc := base64.RawURLEncoding
	payload, err := enc.DecodeString(payloadPart)
	if err != nil || len(payload) == 0 {
		return nil, ErrCursorSignature
	}
	wantMAC, err := enc.DecodeString(macPart)
	if err != nil {
		return nil, ErrCursorSignature
	}
	mac := hmac.New(sha256.New, key)
	mac.Write(payload)
	if !hmac.Equal(wantMAC, mac.Sum(nil)) {
		return nil, ErrCursorSignature
	}
	return payload, nil
}
