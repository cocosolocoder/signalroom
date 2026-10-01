package logfile

import (
	"bytes"
	"testing"
)

func TestCursorKeyStableAndDirBound(t *testing.T) {
	dir := t.TempDir()
	log1, _, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	key1 := log1.CursorKey()
	if len(key1) != keySize {
		t.Fatalf("key length: %d", len(key1))
	}
	log1.Close()

	// Reopening the same directory yields the same key.
	log2, _, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	key2 := log2.CursorKey()
	if !bytes.Equal(key1, key2) {
		t.Fatal("key must be stable across restart")
	}
	log2.Close()

	// A different directory gets a different key.
	dir2 := t.TempDir()
	log3, _, err := Open(dir2)
	if err != nil {
		t.Fatal(err)
	}
	key3 := log3.CursorKey()
	if bytes.Equal(key1, key3) {
		t.Fatal("different directories must have different keys")
	}
	log3.Close()
}

func TestSnapshotSaveLoad(t *testing.T) {
	dir := t.TempDir()
	log, _, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()

	id := "0123456789abcdef0123456789abcdef"
	payload := []byte(`{"ids":["e1","e2"],"query":{}}`)
	if err := log.SaveSnapshot(id, payload); err != nil {
		t.Fatal(err)
	}
	got, err := log.LoadSnapshot(id)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("snapshot mismatch: %s", got)
	}

	// Invalid ids are rejected.
	if err := log.SaveSnapshot("short", payload); err == nil {
		t.Fatal("short id should be rejected")
	}
	if _, err := log.LoadSnapshot("short"); err == nil {
		t.Fatal("short id should be rejected")
	}
	if err := log.SaveSnapshot("0123456789abcdef0123456789abcde!", payload); err == nil {
		t.Fatal("non-hex id should be rejected")
	}
}

func TestSnapshotMissingReturnsError(t *testing.T) {
	dir := t.TempDir()
	log, _, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()

	if _, err := log.LoadSnapshot("0123456789abcdef0123456789abcdef"); err == nil {
		t.Fatal("loading a missing snapshot should fail")
	}
}
