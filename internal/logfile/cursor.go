package logfile

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// Cursor signing material and query snapshots live alongside the event log.
// The key is generated once per data directory and never changes, so cursors
// stay valid across restarts of that directory and are rejected by any other.
const (
	keyName      = "cursor.key"
	snapshotsDir = "snapshots"
	keySize      = 32
	snapshotIDLen = 32 // 16 random bytes encoded as hex
)

// loadOrCreateKey reads the data directory's signing key, generating and
// durably persisting one on first use.
func loadOrCreateKey(dir string) ([]byte, error) {
	path := filepath.Join(dir, keyName)
	data, err := os.ReadFile(path)
	if err == nil {
		if len(data) != keySize {
			return nil, corruption("cursor key has wrong size")
		}
		return data, nil
	}
	if !os.IsNotExist(err) {
		return nil, fmt.Errorf("read cursor key: %w", err)
	}

	key := make([]byte, keySize)
	if _, err := rand.Read(key); err != nil {
		return nil, fmt.Errorf("generate cursor key: %w", err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, key, 0o600); err != nil {
		return nil, fmt.Errorf("write cursor key: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return nil, fmt.Errorf("write cursor key: %w", err)
	}
	if err := syncDir(dir); err != nil {
		return nil, fmt.Errorf("sync data directory: %w", err)
	}
	return key, nil
}

// CursorKey returns the stable per-directory key used to sign cursors.
func (l *Log) CursorKey() []byte {
	return l.key
}

// SaveSnapshot durably writes one query snapshot under its random id. The
// write is atomic (temp file, rename, directory fsync), so a crash leaves
// either the previous state or the complete new file.
func (l *Log) SaveSnapshot(id string, payload []byte) error {
	if !isSnapshotID(id) {
		return errors.New("invalid snapshot id")
	}
	dir := filepath.Join(l.dir, snapshotsDir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create snapshots directory: %w", err)
	}
	final := filepath.Join(dir, id+".json")
	tmp := filepath.Join(dir, id+".tmp")
	if err := os.WriteFile(tmp, payload, 0o644); err != nil {
		return fmt.Errorf("write snapshot: %w", err)
	}
	if err := os.Rename(tmp, final); err != nil {
		return fmt.Errorf("write snapshot: %w", err)
	}
	if err := syncDir(dir); err != nil {
		return fmt.Errorf("sync snapshots directory: %w", err)
	}
	return nil
}

// LoadSnapshot reads a previously saved query snapshot.
func (l *Log) LoadSnapshot(id string) ([]byte, error) {
	if !isSnapshotID(id) {
		return nil, errors.New("invalid snapshot id")
	}
	data, err := os.ReadFile(filepath.Join(l.dir, snapshotsDir, id+".json"))
	if err != nil {
		return nil, fmt.Errorf("read snapshot: %w", err)
	}
	return data, nil
}

// isSnapshotID reports whether id is the hex form of a 16-byte random value.
func isSnapshotID(id string) bool {
	if len(id) != snapshotIDLen {
		return false
	}
	_, err := hex.DecodeString(id)
	return err == nil
}
