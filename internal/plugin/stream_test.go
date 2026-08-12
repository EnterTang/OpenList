package plugin

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/stream"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
)

func TestProcessStreamerHashThenISO(t *testing.T) {
	payload := []byte("hello-media")
	in := &stream.FileStream{
		Obj: &model.Object{
			Name:     "movie.mkv",
			Size:     int64(len(payload)),
			Modified: time.Now(),
			HashInfo: utils.NewHashInfo(utils.SHA256, strings.Repeat("a", 64)),
		},
		Reader:   bytes.NewReader(payload),
		Mimetype: "video/x-matroska",
	}
	out, err := ProcessStreamer(in, ProcessOptions{AntiHash: true, ISORename: true, Whitelist: "mkv"})
	if err != nil {
		t.Fatal(err)
	}
	if out.GetName() != "movie.mkv.iso" {
		t.Fatalf("name = %q", out.GetName())
	}
	if out.GetSize() != int64(len(payload)+TrailerSize) {
		t.Fatalf("size = %d", out.GetSize())
	}
	if out.GetHash().GetHash(utils.SHA256) != "" {
		t.Fatal("hash must be cleared")
	}
	if out.GetFile() != nil {
		t.Fatal("GetFile must be nil")
	}
	got, err := io.ReadAll(out)
	if err != nil {
		t.Fatal(err)
	}
	want := append(append([]byte{}, payload...), TrailerBytes()...)
	if !bytes.Equal(got, want) {
		t.Fatalf("body mismatch")
	}
	sum := sha256.Sum256(want)
	if hex.EncodeToString(sum[:]) == "" {
		t.Fatal("unexpected empty digest")
	}
}

func TestProcessStreamerSkipsWhitelistAndTemp(t *testing.T) {
	in := &stream.FileStream{
		Obj:    &model.Object{Name: "a.txt", Size: 1},
		Reader: bytes.NewReader([]byte("x")),
	}
	out, err := ProcessStreamer(in, ProcessOptions{AntiHash: true, ISORename: true, Whitelist: "mkv"})
	if err != nil || out != in {
		t.Fatalf("txt should skip: err=%v same=%v", err, out == in)
	}
	tmp := &stream.FileStream{
		Obj:    &model.Object{Name: "a.mkv.tmp", Size: 1},
		Reader: bytes.NewReader([]byte("x")),
	}
	out, err = ProcessStreamer(tmp, ProcessOptions{AntiHash: true, Whitelist: "tmp,mkv"})
	if err != nil || out != tmp {
		t.Fatalf("temp should skip: err=%v", err)
	}
}

func TestProcessStreamerSkipsAlreadyAntiHashedFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "movie.mkv")
	body := append([]byte("abc"), TrailerBytes()...)
	if err := os.WriteFile(path, body, 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	in := &stream.FileStream{
		Obj:    &model.Object{Name: "movie.mkv", Size: int64(len(body))},
		Reader: f,
	}
	out, err := ProcessStreamer(in, ProcessOptions{AntiHash: true, ISORename: true, Whitelist: "mkv"})
	if err != nil {
		t.Fatal(err)
	}
	if out.GetName() != "movie.mkv.iso" {
		t.Fatalf("iso still applied: %q", out.GetName())
	}
	if out.GetSize() != int64(len(body)) {
		t.Fatalf("size should not grow again: %d", out.GetSize())
	}
	got, err := io.ReadAll(out)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("should not double-append trailer")
	}
}

func TestProcessStreamerEmptyWhitelist(t *testing.T) {
	in := &stream.FileStream{
		Obj:    &model.Object{Name: "a.mkv", Size: 1},
		Reader: bytes.NewReader([]byte("x")),
	}
	out, err := ProcessStreamer(in, ProcessOptions{AntiHash: true, ISORename: true, Whitelist: ""})
	if err != nil || out != in {
		t.Fatalf("empty whitelist must no-op")
	}
}

func TestProcessStreamerISOOnlyPreservesHash(t *testing.T) {
	hash := strings.Repeat("b", 64)
	payload := []byte("iso-only")
	in := &stream.FileStream{
		Obj: &model.Object{
			Name:     "movie.mkv",
			Size:     int64(len(payload)),
			HashInfo: utils.NewHashInfo(utils.SHA256, hash),
		},
		Reader: bytes.NewReader(payload),
	}
	out, err := ProcessStreamer(in, ProcessOptions{AntiHash: false, ISORename: true, Whitelist: "mkv"})
	if err != nil {
		t.Fatal(err)
	}
	if out.GetName() != "movie.mkv.iso" {
		t.Fatalf("name = %q", out.GetName())
	}
	if out.GetSize() != int64(len(payload)) {
		t.Fatalf("size = %d", out.GetSize())
	}
	if out.GetHash().GetHash(utils.SHA256) != hash {
		t.Fatalf("hash should be preserved, got %q", out.GetHash().GetHash(utils.SHA256))
	}
	got, err := io.ReadAll(out)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("body should be unchanged")
	}
}

func TestProcessStreamerAntiHashTrailerCheckError(t *testing.T) {
	f := &errReadAtFile{err: io.ErrUnexpectedEOF, size: 32}
	in := &stream.FileStream{
		Obj:    &model.Object{Name: "movie.mkv", Size: 32},
		Reader: f,
	}
	_, err := ProcessStreamer(in, ProcessOptions{AntiHash: true, Whitelist: "mkv"})
	if err == nil {
		t.Fatal("expected trailer check error")
	}
}

type errReadAtFile struct {
	err  error
	size int64
	pos  int64
}

func (f *errReadAtFile) Read(p []byte) (int, error) { return 0, io.EOF }
func (f *errReadAtFile) ReadAt(p []byte, off int64) (int, error) {
	return 0, f.err
}
func (f *errReadAtFile) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		f.pos = offset
	case io.SeekCurrent:
		f.pos += offset
	case io.SeekEnd:
		f.pos = f.size + offset
	}
	return f.pos, nil
}

