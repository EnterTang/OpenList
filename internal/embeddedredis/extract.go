package embeddedredis

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

var ErrPayloadUnavailable = errors.New("embedded Redis payload unavailable")

const (
	maxPayloadFileSize  = int64(32 << 20)
	maxPayloadTotalSize = int64(128 << 20)
)

var requiredPayloadFiles = []string{
	"redis-server.exe",
	"msys-2.0.dll",
	"msys-crypto-3.dll",
	"msys-gcc_s-seh-1.dll",
	"msys-ssl-3.dll",
	"msys-stdc++-6.dll",
	"COPYING.redis",
	"LICENSE.redis-windows",
}

type ExtractedRuntime struct {
	Dir           string
	ServerPath    string
	PayloadSHA256 string
}

func ExtractPayload(dataDir string, payload []byte) (result ExtractedRuntime, err error) {
	sha := fmt.Sprintf("%x", sha256.Sum256(payload))
	files, err := validatePayload(payload)
	if err != nil {
		return ExtractedRuntime{}, err
	}
	manifest, err := payloadManifest(files)
	if err != nil {
		return ExtractedRuntime{}, err
	}
	target := filepath.Join(dataDir, "runtime", "redis", Version)
	runtime := ExtractedRuntime{Dir: target, ServerPath: filepath.Join(target, "redis-server.exe"), PayloadSHA256: sha}
	parent := filepath.Dir(target)
	if err := os.MkdirAll(parent, 0700); err != nil {
		return ExtractedRuntime{}, fmt.Errorf("create runtime parent: %w", err)
	}
	lock, err := acquireInstallLock(filepath.Join(parent, ".install.lock"))
	if err != nil {
		return ExtractedRuntime{}, err
	}
	defer func() { err = errors.Join(err, lock.release()) }()
	if runtimeValid(target, sha, manifest) {
		return runtime, nil
	}
	temp, err := os.MkdirTemp(parent, ".redis-"+Version+"-tmp-")
	if err != nil {
		return ExtractedRuntime{}, fmt.Errorf("create extraction directory: %w", err)
	}
	defer os.RemoveAll(temp)
	for _, name := range requiredPayloadFiles {
		if err := extractFile(files[name], filepath.Join(temp, name)); err != nil {
			return ExtractedRuntime{}, err
		}
	}
	if err := os.WriteFile(filepath.Join(temp, ".payload-sha256"), []byte(sha+"\n"), 0600); err != nil {
		return ExtractedRuntime{}, fmt.Errorf("write payload marker: %w", err)
	}
	if !runtimeValid(temp, sha, manifest) {
		return ExtractedRuntime{}, errors.New("extracted Redis runtime failed verification")
	}
	if err := replaceDirectory(target, temp); err != nil {
		return ExtractedRuntime{}, err
	}
	return runtime, nil
}

func payloadManifest(files map[string]*zip.File) (map[string][sha256.Size]byte, error) {
	manifest := make(map[string][sha256.Size]byte, len(requiredPayloadFiles))
	for _, name := range requiredPayloadFiles {
		file := files[name]
		r, err := file.Open()
		if err != nil {
			return nil, fmt.Errorf("open payload entry %q for verification: %w", name, err)
		}
		h := sha256.New()
		written, copyErr := io.Copy(h, io.LimitReader(r, maxPayloadFileSize+1))
		closeErr := r.Close()
		if copyErr != nil {
			return nil, fmt.Errorf("hash payload entry %q: %w", name, copyErr)
		}
		if closeErr != nil {
			return nil, fmt.Errorf("close payload entry %q after verification: %w", name, closeErr)
		}
		if written > maxPayloadFileSize || uint64(written) != file.UncompressedSize64 {
			return nil, fmt.Errorf("payload entry %q size does not match its metadata", name)
		}
		var digest [sha256.Size]byte
		copy(digest[:], h.Sum(nil))
		manifest[name] = digest
	}
	return manifest, nil
}

func validatePayload(payload []byte) (map[string]*zip.File, error) {
	zr, err := zip.NewReader(bytes.NewReader(payload), int64(len(payload)))
	if err != nil {
		return nil, fmt.Errorf("open Redis payload: %w", err)
	}
	allowed := make(map[string]bool, len(requiredPayloadFiles))
	for _, name := range requiredPayloadFiles {
		allowed[name] = true
	}
	files := make(map[string]*zip.File, len(requiredPayloadFiles))
	var totalSize int64
	for _, file := range zr.File {
		name := file.Name
		if name == "" || filepath.IsAbs(name) || strings.Contains(name, "\\") || strings.Contains(name, "/") || name == "." || name == ".." {
			return nil, fmt.Errorf("unsafe payload path %q", name)
		}
		if !allowed[name] {
			return nil, fmt.Errorf("unexpected payload entry %q", name)
		}
		if _, exists := files[name]; exists {
			return nil, fmt.Errorf("duplicate payload entry %q", name)
		}
		mode := file.Mode()
		if mode&os.ModeSymlink != 0 || mode.IsDir() || !mode.IsRegular() {
			return nil, fmt.Errorf("payload entry %q is not a regular file", name)
		}
		if file.UncompressedSize64 > uint64(maxPayloadFileSize) {
			return nil, fmt.Errorf("payload entry %q exceeds the size limit", name)
		}
		size := int64(file.UncompressedSize64)
		if size > maxPayloadTotalSize-totalSize {
			return nil, errors.New("Redis payload exceeds the total size limit")
		}
		if (name == "redis-server.exe" || strings.HasSuffix(name, ".dll")) && size == 0 {
			return nil, fmt.Errorf("required payload binary %q is empty", name)
		}
		totalSize += size
		files[name] = file
	}
	for _, name := range requiredPayloadFiles {
		if files[name] == nil {
			return nil, fmt.Errorf("payload is missing required file %q", name)
		}
	}
	return files, nil
}

func extractFile(file *zip.File, destination string) error {
	r, err := file.Open()
	if err != nil {
		return fmt.Errorf("open payload entry %q: %w", file.Name, err)
	}
	defer r.Close()
	w, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return fmt.Errorf("create payload file %q: %w", file.Name, err)
	}
	written, copyErr := io.Copy(w, io.LimitReader(r, maxPayloadFileSize+1))
	closeErr := w.Close()
	if copyErr != nil {
		return fmt.Errorf("extract payload file %q: %w", file.Name, copyErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close payload file %q: %w", file.Name, closeErr)
	}
	if written > maxPayloadFileSize {
		return fmt.Errorf("payload entry %q exceeds the size limit", file.Name)
	}
	if uint64(written) != file.UncompressedSize64 {
		return fmt.Errorf("payload entry %q size does not match its metadata", file.Name)
	}
	return nil
}

func runtimeValid(dir, sha string, manifest map[string][sha256.Size]byte) bool {
	marker, err := os.ReadFile(filepath.Join(dir, ".payload-sha256"))
	if err != nil || strings.TrimSpace(string(marker)) != sha {
		return false
	}
	for _, name := range requiredPayloadFiles {
		path := filepath.Join(dir, name)
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() {
			return false
		}
		file, err := os.Open(path)
		if err != nil {
			return false
		}
		h := sha256.New()
		written, copyErr := io.Copy(h, io.LimitReader(file, maxPayloadFileSize+1))
		closeErr := file.Close()
		expected := manifest[name]
		if copyErr != nil || closeErr != nil || written > maxPayloadFileSize || !bytes.Equal(h.Sum(nil), expected[:]) {
			return false
		}
	}
	return true
}

func replaceDirectory(target, prepared string) error {
	backup := target + ".backup"
	if err := os.RemoveAll(backup); err != nil {
		return fmt.Errorf("clean runtime backup: %w", err)
	}
	hadTarget := false
	if _, err := os.Lstat(target); err == nil {
		hadTarget = true
		if err := os.Rename(target, backup); err != nil {
			return fmt.Errorf("back up existing runtime: %w", err)
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("inspect existing runtime: %w", err)
	}
	if err := os.Rename(prepared, target); err != nil {
		if hadTarget {
			if restoreErr := os.Rename(backup, target); restoreErr != nil {
				return errors.Join(fmt.Errorf("install Redis runtime: %w", err), fmt.Errorf("restore previous runtime: %w", restoreErr))
			}
		}
		return fmt.Errorf("install Redis runtime: %w", err)
	}
	if hadTarget {
		if err := os.RemoveAll(backup); err != nil {
			return fmt.Errorf("installed Redis runtime but failed to remove backup: %w", err)
		}
	}
	return nil
}
