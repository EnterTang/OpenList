package worker

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestCopyQBFileToStagingKeepsSourceUnchangedAndCleansByCaller(t *testing.T) {
	downloadRoot := t.TempDir()
	stagingRoot := t.TempDir()
	sourcePath := filepath.Join(downloadRoot, "Show.S01E01.mkv")
	original := []byte("original-qb-content")
	if err := os.WriteFile(sourcePath, original, 0o640); err != nil {
		t.Fatal(err)
	}

	stagedPath, err := CopyQBFileToStaging(context.Background(), QBSource{
		WorkerPath: sourcePath, DownloadRoot: downloadRoot, Name: "Show.S01E01.mkv", Size: int64(len(original)),
	}, QBStagingAdmission{StagingRoot: stagingRoot, DownloadRoot: downloadRoot, ExtensionWhitelist: []string{".mkv"}})
	if err != nil {
		t.Fatalf("copy qB file: %v", err)
	}
	staged, err := os.ReadFile(stagedPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(staged) != string(original) {
		t.Fatalf("staged content = %q, want %q", staged, original)
	}
	if filepath.Base(stagedPath) != "Show.S01E01.mkv" {
		t.Fatalf("staged filename = %q, want original qB filename", filepath.Base(stagedPath))
	}
	unchanged, err := os.ReadFile(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if string(unchanged) != string(original) {
		t.Fatalf("qB source changed to %q", unchanged)
	}
	if err := os.Remove(stagedPath); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(stagedPath); !os.IsNotExist(err) {
		t.Fatalf("staged file still exists, stat error=%v", err)
	}
}

func TestCopyQBFileToStagingUsesInjectedCapacityProbe(t *testing.T) {
	downloadRoot := t.TempDir()
	stagingRoot := t.TempDir()
	sourcePath := filepath.Join(downloadRoot, "episode.mkv")
	if err := os.WriteFile(sourcePath, []byte("episode"), 0o640); err != nil {
		t.Fatal(err)
	}
	called := false
	_, err := CopyQBFileToStaging(context.Background(), QBSource{
		WorkerPath: sourcePath, DownloadRoot: downloadRoot, Name: "episode.mkv", Size: 7,
	}, QBStagingAdmission{
		StagingRoot: stagingRoot, DownloadRoot: downloadRoot, ExtensionWhitelist: []string{".mkv"},
		FreeSpace: func(context.Context, string) (uint64, error) {
			called = true
			return 0, nil
		},
	})
	if !called {
		t.Fatal("injected staging capacity probe was not called")
	}
	if err == nil || !strings.Contains(err.Error(), "qB staging free space is insufficient") {
		t.Fatalf("capacity error = %v, want injected qB capacity rejection", err)
	}
}

func TestCopyQBFileToStagingRejectsPathEscape(t *testing.T) {
	_, err := CopyQBFileToStaging(context.Background(), QBSource{WorkerPath: "/mnt/downloads/../../etc/passwd", Name: "passwd"}, QBStagingAdmission{StagingRoot: "/mnt/staging", DownloadRoot: "/mnt/downloads"})
	if err == nil || err.Error() != "qB source path escapes declared download root" {
		t.Fatalf("error = %v, want path escape rejection", err)
	}
}

func TestCopyQBFileToStagingRejects150GiBBoundary(t *testing.T) {
	downloadRoot := t.TempDir()
	stagingRoot := t.TempDir()
	sourcePath := filepath.Join(downloadRoot, "large.mkv")
	if err := os.WriteFile(sourcePath, []byte("small fixture"), 0o640); err != nil {
		t.Fatal(err)
	}
	_, err := CopyQBFileToStaging(context.Background(), QBSource{WorkerPath: sourcePath, DownloadRoot: downloadRoot, Name: "large.mkv", Size: 1}, QBStagingAdmission{StagingRoot: stagingRoot, DownloadRoot: downloadRoot, MaxFileBytes: 150*1024*1024*1024 + 1, ExtensionWhitelist: []string{".mkv"}})
	if err == nil || !strings.Contains(err.Error(), "must not exceed") {
		t.Fatalf("error = %v, want 150 GiB admission rejection", err)
	}
}

func TestCopyQBFileToStagingCreatesUniqueFilesForConcurrentCopies(t *testing.T) {
	downloadRoot := t.TempDir()
	stagingRoot := t.TempDir()
	sourcePath := filepath.Join(downloadRoot, "episode.mkv")
	content := []byte(strings.Repeat("x", 1024))
	if err := os.WriteFile(sourcePath, content, 0o640); err != nil {
		t.Fatal(err)
	}
	admission := QBStagingAdmission{StagingRoot: stagingRoot, DownloadRoot: downloadRoot, ExtensionWhitelist: []string{".mkv"}}
	paths := make(chan string, 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			staged, err := CopyQBFileToStaging(context.Background(), QBSource{WorkerPath: sourcePath, DownloadRoot: downloadRoot, Name: "episode.mkv", Size: int64(len(content))}, admission)
			if err != nil {
				t.Errorf("copy qB file: %v", err)
				return
			}
			paths <- staged
		}()
	}
	wg.Wait()
	close(paths)
	var copied []string
	for staged := range paths {
		copied = append(copied, staged)
	}
	if len(copied) != 2 || copied[0] == copied[1] {
		t.Fatalf("concurrent staging paths = %#v", copied)
	}
	if filepath.Base(copied[0]) != "episode.mkv" && filepath.Base(copied[1]) != "episode.mkv" {
		t.Fatalf("concurrent staging did not retain the original filename: %#v", copied)
	}
	for _, staged := range copied {
		_ = os.Remove(staged)
	}
}
