package logfile

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// cursorKeyName holds the per-directory HMAC secret for pagination cursors.
// The file is created on first open and never changes afterwards; a cursor
// signed with another directory's key fails verification here.
const cursorKeyName = "cursor.key"

// cursorKeyBytes is the width of the random HMAC secret.
const cursorKeyBytes = 32

// ensureCursorKey returns the data directory's pagination secret, creating a
// fresh random one on first use and fsyncing it before it is trusted. An
// existing but malformed key is fatal rather than rotated: silently replacing
// it would invalidate outstanding cursors without explaining why.
func ensureCursorKey(dir string) ([]byte, error) {
	path := filepath.Join(dir, cursorKeyName)
	raw, err := os.ReadFile(path)
	switch {
	case err == nil:
		key, perr := parseCursorKey(raw)
		if perr != nil {
			return nil, fmt.Errorf("read cursor key: %w", perr)
		}
		return key, nil
	case errors.Is(err, os.ErrNotExist):
		// Fall through to creation below.
	default:
		return nil, fmt.Errorf("read cursor key: %w", err)
	}

	key := make([]byte, cursorKeyBytes)
	if _, err := rand.Read(key); err != nil {
		return nil, fmt.Errorf("generate cursor key: %w", err)
	}
	encoded := []byte(hex.EncodeToString(key) + "\n")

	// O_EXCL: another process holding the lock is impossible, and without the
	// lock this refuses to clobber an existing key.
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create cursor key: %w", err)
	}
	if _, err := file.Write(encoded); err != nil {
		file.Close()
		return nil, fmt.Errorf("write cursor key: %w", err)
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return nil, fmt.Errorf("sync cursor key: %w", err)
	}
	if err := file.Close(); err != nil {
		return nil, fmt.Errorf("close cursor key: %w", err)
	}
	if err := syncDir(dir); err != nil {
		return nil, fmt.Errorf("sync data directory: %w", err)
	}
	return key, nil
}

// parseCursorKey decodes the persisted hex secret, tolerating a trailing
// newline but nothing else.
func parseCursorKey(raw []byte) ([]byte, error) {
	encoded := strings.TrimSpace(string(raw))
	if encoded == "" {
		return nil, errors.New("cursor key is empty")
	}
	key, err := hex.DecodeString(encoded)
	if err != nil {
		return nil, errors.New("cursor key is not valid hexadecimal")
	}
	if len(key) != cursorKeyBytes {
		return nil, fmt.Errorf("cursor key must be %d bytes, got %d", cursorKeyBytes, len(key))
	}
	return key, nil
}
