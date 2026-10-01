package httpapi

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"strings"
)

var errBadTestCursor = errors.New("invalid test cursor")

func signTestCursor(payload []byte) string {
	mac := hmac.New(sha256.New, testCursorSecret)
	mac.Write(payload)
	enc := base64.RawURLEncoding
	return enc.EncodeToString(payload) + "." + enc.EncodeToString(mac.Sum(nil))
}

func verifyTestCursor(token string) ([]byte, error) {
	payloadPart, macPart, found := strings.Cut(token, ".")
	if !found || payloadPart == "" || macPart == "" {
		return nil, errBadTestCursor
	}
	enc := base64.RawURLEncoding
	payload, err := enc.DecodeString(payloadPart)
	if err != nil {
		return nil, errBadTestCursor
	}
	wantMAC, err := enc.DecodeString(macPart)
	if err != nil {
		return nil, errBadTestCursor
	}
	mac := hmac.New(sha256.New, testCursorSecret)
	mac.Write(payload)
	if !hmac.Equal(wantMAC, mac.Sum(nil)) {
		return nil, errBadTestCursor
	}
	return payload, nil
}
