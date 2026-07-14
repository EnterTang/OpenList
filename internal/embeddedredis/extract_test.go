package embeddedredis

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestExtractPayloadInstallsAndReusesValidRuntime(t *testing.T) {
	payload := zipPayload(t, nil)
	dataDir := t.TempDir()
	got, err := ExtractPayload(dataDir, payload)
	if err != nil {
		t.Fatal(err)
	}
	wantDir := filepath.Join(dataDir, "runtime", "redis", Version)
	if got.Dir != wantDir || got.ServerPath != filepath.Join(wantDir, "redis-server.exe") {
		t.Fatalf("unexpected runtime: %+v", got)
	}
	wantSHA := fmt.Sprintf("%x", sha256.Sum256(payload))
	if got.PayloadSHA256 != wantSHA {
		t.Fatalf("sha = %q, want %q", got.PayloadSHA256, wantSHA)
	}
	marker, err := os.ReadFile(filepath.Join(wantDir, ".payload-sha256"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(marker)) != wantSHA {
		t.Fatalf("marker = %q", marker)
	}
	for _, name := range requiredPayloadFiles {
		info, err := os.Stat(filepath.Join(wantDir, name))
		if err != nil || !info.Mode().IsRegular() {
			t.Fatalf("%s not regular: %v", name, err)
		}
	}

	sentinel := filepath.Join(wantDir, "sentinel")
	if err := os.WriteFile(sentinel, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	old := time.Unix(123, 0)
	if err := os.Chtimes(sentinel, old, old); err != nil {
		t.Fatal(err)
	}
	if _, err := ExtractPayload(dataDir, payload); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(sentinel)
	if err != nil {
		t.Fatalf("valid runtime was replaced: %v", err)
	}
	if !info.ModTime().Equal(old) {
		t.Fatalf("sentinel mtime changed: %v", info.ModTime())
	}
}

func TestExtractPayloadConcurrentCallsShareInstallation(t *testing.T) {
	payload := zipPayload(t, nil)
	dataDir := t.TempDir()
	const callers = 8
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := ExtractPayload(dataDir, payload)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent extraction failed: %v", err)
		}
	}
	sha := fmt.Sprintf("%x", sha256.Sum256(payload))
	files, err := validatePayload(payload)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := payloadManifest(files)
	if err != nil {
		t.Fatal(err)
	}
	if !runtimeValid(filepath.Join(dataDir, "runtime", "redis", Version), sha, manifest) {
		t.Fatal("final runtime is invalid")
	}
}

func TestExtractPayloadRejectsOversizedEntries(t *testing.T) {
	t.Run("per file", func(t *testing.T) {
		payload := zipPayload(t, map[string]zipEntry{
			"redis-server.exe": {size: maxPayloadFileSize + 1},
		})
		assertRejected(t, payload)
	})
	t.Run("total", func(t *testing.T) {
		overrides := make(map[string]zipEntry, len(requiredPayloadFiles))
		for _, name := range requiredPayloadFiles {
			overrides[name] = zipEntry{size: maxPayloadTotalSize/int64(len(requiredPayloadFiles)) + 1}
		}
		assertRejected(t, zipPayload(t, overrides))
	})
}

func TestExtractPayloadRejectsEmptyRequiredBinary(t *testing.T) {
	assertRejected(t, zipPayload(t, map[string]zipEntry{"msys-2.0.dll": {empty: true}}))
}

func TestExtractPayloadReplacesInvalidRuntime(t *testing.T) {
	for _, setup := range []struct {
		name   string
		marker string
		omit   string
	}{
		{"marker mismatch", "wrong", ""}, {"missing required file", "matching", "msys-ssl-3.dll"},
	} {
		t.Run(setup.name, func(t *testing.T) {
			payload := zipPayload(t, nil)
			dataDir := t.TempDir()
			target := filepath.Join(dataDir, "runtime", "redis", Version)
			if err := os.MkdirAll(target, 0700); err != nil {
				t.Fatal(err)
			}
			sha := fmt.Sprintf("%x", sha256.Sum256(payload))
			marker := setup.marker
			if marker == "matching" {
				marker = sha
			}
			if err := os.WriteFile(filepath.Join(target, ".payload-sha256"), []byte(marker), 0600); err != nil {
				t.Fatal(err)
			}
			for _, name := range requiredPayloadFiles {
				if name != setup.omit {
					if err := os.WriteFile(filepath.Join(target, name), []byte("old"), 0600); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := os.WriteFile(filepath.Join(target, "stale"), []byte("old"), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := ExtractPayload(dataDir, payload); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(filepath.Join(target, "stale")); !os.IsNotExist(err) {
				t.Fatalf("stale file remains: %v", err)
			}
		})
	}
}

func TestExtractPayloadReplacesRuntimeWithModifiedFiles(t *testing.T) {
	for _, test := range []struct {
		name    string
		file    string
		content []byte
	}{
		{name: "tampered executable", file: "redis-server.exe", content: []byte("tampered")},
		{name: "truncated DLL", file: "msys-ssl-3.dll", content: nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			payload := zipPayload(t, nil)
			dataDir := t.TempDir()
			runtime, err := ExtractPayload(dataDir, payload)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(runtime.Dir, test.file), test.content, 0600); err != nil {
				t.Fatal(err)
			}
			sentinel := filepath.Join(runtime.Dir, "stale")
			if err := os.WriteFile(sentinel, []byte("old"), 0600); err != nil {
				t.Fatal(err)
			}

			if _, err := ExtractPayload(dataDir, payload); err != nil {
				t.Fatal(err)
			}
			want := "contents:" + test.file
			got, err := os.ReadFile(filepath.Join(runtime.Dir, test.file))
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != want {
				t.Fatalf("restored file = %q, want %q", got, want)
			}
			if _, err := os.Stat(sentinel); !os.IsNotExist(err) {
				t.Fatalf("invalid runtime was not replaced: %v", err)
			}
		})
	}
}

func TestExtractPayloadRejectsInvalidArchivesWithoutPartialInstall(t *testing.T) {
	for _, missing := range requiredPayloadFiles {
		t.Run("missing "+missing, func(t *testing.T) { assertRejected(t, zipPayload(t, map[string]zipEntry{missing: {omit: true}})) })
	}
	cases := map[string][]zipEntry{
		"absolute": {{name: "/redis-server.exe"}}, "traversal": {{name: "../redis-server.exe"}},
		"nested": {{name: "nested/redis-server.exe"}}, "unexpected": {{name: "surprise.txt"}},
		"duplicate": {{name: "redis-server.exe"}, {name: "redis-server.exe"}},
		"symlink":   {{name: "redis-server.exe", mode: os.ModeSymlink | 0777}},
		"directory": {{name: "redis-server.exe", mode: os.ModeDir | 0755}},
	}
	for name, entries := range cases {
		t.Run(name, func(t *testing.T) {
			assertRejected(t, zipPayload(t, map[string]zipEntry{"redis-server.exe": {omit: true}}, entries...))
		})
	}
}

func TestExtractPayloadDoesNotEscapeDataDir(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	outside := filepath.Join(root, "redis-server.exe")
	_, err := ExtractPayload(dataDir, zipPayload(t, map[string]zipEntry{"redis-server.exe": {omit: true}}, zipEntry{name: "../redis-server.exe", body: "escape"}))
	if err == nil {
		t.Fatal("expected error")
	}
	if _, err := os.Stat(outside); !os.IsNotExist(err) {
		t.Fatalf("outside file created: %v", err)
	}
}

type zipEntry struct {
	name, body string
	mode       os.FileMode
	omit       bool
	empty      bool
	size       int64
}

func zipPayload(t *testing.T, overrides map[string]zipEntry, extras ...zipEntry) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for _, name := range requiredPayloadFiles {
		e := zipEntry{name: name, body: "contents:" + name, mode: 0644}
		if o, ok := overrides[name]; ok {
			if o.omit {
				continue
			}
			if o.name != "" {
				e.name = o.name
			}
			if o.body != "" {
				e.body = o.body
			}
			if o.empty {
				e.body = ""
			}
			if o.size != 0 {
				e.body = strings.Repeat("x", int(o.size))
			}
			if o.mode != 0 {
				e.mode = o.mode
			}
		}
		addZipEntry(t, zw, e)
	}
	for _, e := range extras {
		addZipEntry(t, zw, e)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func addZipEntry(t *testing.T, zw *zip.Writer, e zipEntry) {
	t.Helper()
	h := &zip.FileHeader{Name: e.name, Method: zip.Store}
	h.SetMode(e.mode)
	w, err := zw.CreateHeader(h)
	if err != nil {
		t.Fatal(err)
	}
	if !e.mode.IsDir() {
		if _, err = w.Write([]byte(e.body)); err != nil {
			t.Fatal(err)
		}
	}
}
func assertRejected(t *testing.T, payload []byte) {
	t.Helper()
	dataDir := t.TempDir()
	if _, err := ExtractPayload(dataDir, payload); err == nil {
		t.Fatal("expected error")
	}
	if _, err := os.Stat(filepath.Join(dataDir, "runtime", "redis", Version)); !os.IsNotExist(err) {
		t.Fatalf("partial target installed: %v", err)
	}
}
