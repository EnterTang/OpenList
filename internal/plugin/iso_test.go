package plugin

import (
	"os"
	"path/filepath"
	"testing"
)

func TestISOTargetName(t *testing.T) {
	got, ok := ISOTargetName("movie.mkv")
	if !ok || got != "movie.mkv.iso" {
		t.Fatalf("got %q ok=%v", got, ok)
	}
	if _, ok := ISOTargetName("movie.mkv.iso"); ok {
		t.Fatal("already iso should skip")
	}
	if _, ok := ISOTargetName("movie.ISO"); ok {
		t.Fatal("already ISO should skip")
	}
}

func TestRenameToISO(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "movie.mkv")
	_ = os.WriteFile(src, []byte("x"), 0o644)
	newPath, changed, err := RenameToISO(src)
	if err != nil || !changed || filepath.Base(newPath) != "movie.mkv.iso" {
		t.Fatalf("new=%s changed=%v err=%v", newPath, changed, err)
	}
	if _, err := os.Stat(newPath); err != nil {
		t.Fatal(err)
	}
	src2 := filepath.Join(dir, "other.mkv")
	_ = os.WriteFile(src2, []byte("y"), 0o644)
	_ = os.WriteFile(filepath.Join(dir, "other.mkv.iso"), []byte("z"), 0o644)
	_, changed, err = RenameToISO(src2)
	if err != nil || changed {
		t.Fatalf("conflict should skip: changed=%v err=%v", changed, err)
	}
}
