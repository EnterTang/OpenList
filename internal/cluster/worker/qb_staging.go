package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/shirou/gopsutil/v4/disk"
)

const maxQBStagingFileBytes int64 = 150 * 1024 * 1024 * 1024

var ErrQBStagingInsufficientSpace = errors.New("qB staging free space is insufficient")

// QBSource is a verified local source selected from qB's file listing.
type QBSource struct {
	WorkerPath   string
	DownloadRoot string
	Name         string
	Size         int64
}

// QBStagingAdmission contains Worker-local staging policy. It is deliberately
// separate from qB configuration so the copy function is straightforward to
// test and cannot accidentally use a Coordinator path.
type QBStagingAdmission struct {
	StagingRoot        string
	DownloadRoot       string
	MaxFileBytes       int64
	ExtensionWhitelist []string
}

// CopyQBFileToStaging copies a completed qB file into a unique Worker-local
// staging file. It never mutates, renames, hashes, or deletes the qB source.
func CopyQBFileToStaging(ctx context.Context, source QBSource, admission QBStagingAdmission) (stagedPath string, err error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	workerPath := filepath.Clean(strings.TrimSpace(source.WorkerPath))
	if workerPath == "." || !filepath.IsAbs(workerPath) {
		return "", errors.New("qB source path must be absolute")
	}
	downloadRoot, err := validateLocalRoot(admission.DownloadRoot, "qB download root")
	if err != nil {
		return "", err
	}
	if downloadRoot == "" {
		if strings.Contains(filepath.ToSlash(strings.TrimSpace(source.WorkerPath)), "/../") || strings.HasSuffix(filepath.ToSlash(strings.TrimSpace(source.WorkerPath)), "/..") {
			return "", errors.New("qB source path escapes declared download root")
		}
	} else if !pathWithin(downloadRoot, workerPath) {
		return "", errors.New("qB source path escapes declared download root")
	}
	stagingRoot, err := validateLocalRoot(admission.StagingRoot, "qB staging root")
	if err != nil {
		return "", err
	}
	if stagingRoot == "" {
		return "", errors.New("qB staging root is required")
	}
	maxBytes := admission.MaxFileBytes
	if maxBytes <= 0 {
		maxBytes = maxQBStagingFileBytes
	}
	if maxBytes > maxQBStagingFileBytes {
		return "", fmt.Errorf("qB staging max file size must not exceed %d bytes", maxQBStagingFileBytes)
	}
	if !torrentExtensionAllowed(sourceName(source), admission.ExtensionWhitelist) {
		return "", fmt.Errorf("qB source file %q has a disallowed extension", sourceName(source))
	}
	info, err := os.Lstat(workerPath)
	if err != nil {
		return "", fmt.Errorf("stat qB source file: %w", err)
	}
	if !info.Mode().IsRegular() {
		return "", errors.New("qB source path must be a regular file")
	}
	if info.Size() <= 0 {
		return "", errors.New("qB source file must not be empty")
	}
	if info.Size() > maxBytes {
		return "", fmt.Errorf("qB source file exceeds staging limit of %d bytes", maxBytes)
	}
	if err := os.MkdirAll(stagingRoot, 0o750); err != nil {
		return "", fmt.Errorf("create qB staging root: %w", err)
	}
	usage, err := disk.UsageWithContext(ctx, stagingRoot)
	if err != nil {
		return "", fmt.Errorf("inspect qB staging free space: %w", err)
	}
	if usage.Free < uint64(info.Size()) {
		return "", fmt.Errorf("%w: free=%d required=%d", ErrQBStagingInsufficientSpace, usage.Free, info.Size())
	}
	if source.Size > 0 && info.Size() != source.Size {
		return "", fmt.Errorf("qB source size changed: qB=%d local=%d", source.Size, info.Size())
	}
	name := sourceName(source)
	if name == "" || name == "." || name == ".." {
		return "", errors.New("qB source file name must be a regular file name")
	}
	// Keep the qB basename as the normal staging filename so downstream
	// processing sees the same media name and extension. A unique fallback is
	// only needed when concurrent copies use the same basename.
	tempPath := filepath.Join(stagingRoot, name)
	temp, err := os.OpenFile(tempPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o640)
	if errors.Is(err, os.ErrExist) {
		extension := filepath.Ext(name)
		stem := strings.TrimSuffix(name, extension)
		temp, err = os.CreateTemp(stagingRoot, "."+stem+".openlist-*"+extension)
	}
	if err != nil {
		return "", fmt.Errorf("create qB staging file: %w", err)
	}
	tempPath = temp.Name()
	defer func() {
		_ = temp.Close()
		if err != nil {
			_ = os.Remove(tempPath)
		}
	}()
	input, err := os.Open(workerPath)
	if err != nil {
		return "", fmt.Errorf("open qB source file: %w", err)
	}
	defer input.Close()
	written, err := copyContext(ctx, temp, input)
	if err != nil {
		return "", fmt.Errorf("copy qB source file to staging: %w", err)
	}
	if written != info.Size() {
		return "", fmt.Errorf("staged qB file size mismatch: copied=%d source=%d", written, info.Size())
	}
	if err := temp.Sync(); err != nil {
		return "", fmt.Errorf("sync qB staging file: %w", err)
	}
	if err := temp.Close(); err != nil {
		return "", fmt.Errorf("close qB staging file: %w", err)
	}
	if err := syncDirectory(stagingRoot); err != nil {
		return "", err
	}
	return tempPath, nil
}

func sourceName(source QBSource) string {
	if name := strings.TrimSpace(source.Name); name != "" {
		return filepath.Base(filepath.FromSlash(name))
	}
	return filepath.Base(source.WorkerPath)
}

func validateLocalRoot(raw, label string) (string, error) {
	value := filepath.Clean(strings.TrimSpace(raw))
	if value == "." || value == "" {
		return "", nil
	}
	if !filepath.IsAbs(value) || value == string(filepath.Separator) {
		return "", fmt.Errorf("%s must be an absolute non-root path", label)
	}
	return value, nil
}

func pathWithin(root, candidate string) bool {
	relative, err := filepath.Rel(root, candidate)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return false
	}
	return !filepath.IsAbs(relative)
}

func copyContext(ctx context.Context, dst io.Writer, src io.Reader) (int64, error) {
	buffer := make([]byte, 1024*1024)
	var written int64
	for {
		if err := ctx.Err(); err != nil {
			return written, err
		}
		read, readErr := src.Read(buffer)
		if read > 0 {
			count, writeErr := dst.Write(buffer[:read])
			written += int64(count)
			if writeErr != nil {
				return written, writeErr
			}
			if count != read {
				return written, io.ErrShortWrite
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return written, nil
			}
			return written, readErr
		}
	}
}

func syncDirectory(root string) error {
	// Windows does not support syncing a directory handle and returns
	// ERROR_ACCESS_DENIED. The staged file has already been flushed with
	// temp.Sync() before this point, so there is no directory metadata sync
	// equivalent to perform on Windows.
	if runtime.GOOS == "windows" {
		return nil
	}
	directory, err := os.Open(root)
	if err != nil {
		return fmt.Errorf("open qB staging directory for sync: %w", err)
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync qB staging directory: %w", err)
	}
	return nil
}
