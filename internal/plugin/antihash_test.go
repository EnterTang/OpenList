package plugin

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestModifyHashAppendsTrailerAndIsDetected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "a.mkv")
	if err := os.WriteFile(path, []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	changed, err := ModifyHash(path)
	if err != nil || !changed {
		t.Fatalf("ModifyHash: changed=%v err=%v", changed, err)
	}
	ok, err := IsModified(path)
	if err != nil || !ok {
		t.Fatalf("IsModified: ok=%v err=%v", ok, err)
	}
	data, _ := os.ReadFile(path)
	if !bytes.HasSuffix(data, append(append([]byte{}, Padding...), MagicTag...)) {
		t.Fatalf("missing trailer: %q", data)
	}
	if len(data) != 5+TrailerSize {
		t.Fatalf("size=%d want=%d", len(data), 5+TrailerSize)
	}
}

func TestModifyHashSkipsWhenAlreadyModified(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "a.mkv")
	_ = os.WriteFile(path, []byte("hello"), 0o644)
	_, _ = ModifyHash(path)
	info1, _ := os.Stat(path)
	changed, err := ModifyHash(path)
	if err != nil || changed {
		t.Fatalf("second ModifyHash: changed=%v err=%v", changed, err)
	}
	info2, _ := os.Stat(path)
	if info1.Size() != info2.Size() {
		t.Fatalf("size grew on second modify")
	}
}

func TestRestoreHashTruncatesTrailer(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "a.mkv")
	_ = os.WriteFile(path, []byte("hello"), 0o644)
	_, _ = ModifyHash(path)
	changed, err := RestoreHash(path)
	if err != nil || !changed {
		t.Fatalf("RestoreHash: changed=%v err=%v", changed, err)
	}
	data, _ := os.ReadFile(path)
	if string(data) != "hello" {
		t.Fatalf("got %q", data)
	}
	ok, _ := IsModified(path)
	if ok {
		t.Fatal("still marked modified")
	}
}
