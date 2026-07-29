package plugin

import (
	"os"
	"path/filepath"
	"testing"
)

func TestProcessAbsolutePathHashThenISO(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "movie.mkv")
	_ = os.WriteFile(path, []byte("hello"), 0o644)
	out, err := ProcessAbsolutePath(path, ProcessOptions{
		AntiHash: true, ISORename: true, Whitelist: "mkv",
	})
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Base(out) != "movie.mkv.iso" {
		t.Fatalf("out=%s", out)
	}
	ok, err := IsModified(out)
	if err != nil || !ok {
		t.Fatalf("expected antihash on renamed file: ok=%v err=%v", ok, err)
	}
}

func TestProcessAbsolutePathRespectsWhitelistAndTemp(t *testing.T) {
	dir := t.TempDir()
	txt := filepath.Join(dir, "a.txt")
	_ = os.WriteFile(txt, []byte("x"), 0o644)
	out, err := ProcessAbsolutePath(txt, ProcessOptions{AntiHash: true, Whitelist: "mkv"})
	if err != nil || out != txt {
		t.Fatalf("txt should skip: out=%s err=%v", out, err)
	}
	tmp := filepath.Join(dir, "a.mkv.tmp")
	_ = os.WriteFile(tmp, []byte("x"), 0o644)
	out, err = ProcessAbsolutePath(tmp, ProcessOptions{AntiHash: true, Whitelist: "tmp,mkv"})
	if err != nil {
		t.Fatal(err)
	}
	ok, _ := IsModified(tmp)
	if ok {
		t.Fatal("temp file must not be modified")
	}
	_ = out
}

func TestProcessAbsolutePathEmptyWhitelistDoesNothing(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "a.mkv")
	_ = os.WriteFile(path, []byte("x"), 0o644)
	out, err := ProcessAbsolutePath(path, ProcessOptions{AntiHash: true, ISORename: true, Whitelist: ""})
	if err != nil || out != path {
		t.Fatalf("out=%s err=%v", out, err)
	}
	ok, _ := IsModified(path)
	if ok {
		t.Fatal("should not modify")
	}
}
