package logfile

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCursorKeyCreatedOnceAndStable(t *testing.T) {
	dir := t.TempDir()

	log, _, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	keyPath := filepath.Join(dir, cursorKeyName)
	raw, err := os.ReadFile(keyPath)
	if err != nil {
		t.Fatalf("key file: %v", err)
	}
	if len(strings.TrimSpace(string(raw))) != cursorKeyBytes*2 { // hex
		t.Fatalf("key file length: %q", raw)
	}
	if fi, err := os.Stat(keyPath); err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("key file permissions: %v %v", fi, err)
	}

	token, err := log.SignCursor([]byte(`{"hello":"world"}`))
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	payload, err := log.VerifyCursor(token)
	if err != nil || string(payload) != `{"hello":"world"}` {
		t.Fatalf("verify: %q %v", payload, err)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen: the same key must persist, so a cursor minted before the restart
	// verifies afterwards.
	log2, _, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer log2.Close()
	if payload, err := log2.VerifyCursor(token); err != nil {
		t.Fatalf("cursor must survive restart: %v", err)
	} else if string(payload) != `{"hello":"world"}` {
		t.Fatalf("payload after restart: %q", payload)
	}
}

func TestCursorRejectedAcrossDirectories(t *testing.T) {
	dirA := t.TempDir()
	dirB := t.TempDir()
	logA, _, err := Open(dirA)
	if err != nil {
		t.Fatalf("open A: %v", err)
	}
	defer logA.Close()
	logB, _, err := Open(dirB)
	if err != nil {
		t.Fatalf("open B: %v", err)
	}
	defer logB.Close()

	token, err := logA.SignCursor([]byte("payload"))
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	if _, err := logB.VerifyCursor(token); err == nil {
		t.Fatal("cursor from another data directory must be rejected")
	}
}

func TestVerifyCursorRejectsGarbage(t *testing.T) {
	dir := t.TempDir()
	log, _, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer log.Close()

	token, err := log.SignCursor([]byte("payload"))
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	for _, bad := range []string{
		"",
		"nodot",
		".",
		"abc.",
		".sig",
		"a.b.c",
		token + "x",
		"x" + token[1:],
	} {
		if _, err := log.VerifyCursor(bad); err == nil {
			t.Fatalf("garbage cursor %q must be rejected", bad)
		}
	}
}
