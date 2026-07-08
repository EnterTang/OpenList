package subscription

import (
	"context"
	"reflect"
	"testing"
)

func TestSaveImportedFilesToTempCreatesDirectoriesAndGroupsByFolder(t *testing.T) {
	provider := &fakeShareSaver{dstDirID: "dir:/tmp/pan123"}
	files := []pan123ImportedFile{
		{Etag: "bc18e4ea5fb89ec5778d1f38c9772f5f", Size: 1024, Path: "Season 1/Episode 01.mkv", Name: "Episode 01.mkv"},
		{Etag: "bc18e4ea5fb89ec5778d1f38c9772f5f", Size: 2048, Path: "Season 1/Episode 02.mkv", Name: "Episode 02.mkv"},
		{Etag: "11111111111111111111111111111111", Size: 512, Path: "Extras/Featurette.mkv", Name: "Featurette.mkv"},
	}
	entries, err := SaveImportedFilesToTemp(context.Background(), provider, "manual_import://pan123", files, SaveShareOptions{
		TempRoot: "/tmp/pan123",
		Match:    func(entry TreeEntry) bool { return entry.Name != "Featurette.mkv" },
	})
	if err != nil {
		t.Fatalf("save imported files: %v", err)
	}
	if got, want := len(entries), 2; got != want {
		t.Fatalf("len(entries) = %d, want %d", got, want)
	}
	wantDirs := []string{"/tmp/pan123", "/tmp/pan123/Season 1"}
	if !reflect.DeepEqual(provider.ensureDirCalls[:2], wantDirs) {
		t.Fatalf("ensureDirCalls = %#v, want prefix %#v", provider.ensureDirCalls, wantDirs)
	}
	seasonItems := provider.saved["dir:/tmp/pan123/Season 1"]
	if len(seasonItems) != 2 {
		t.Fatalf("season items = %#v", seasonItems)
	}
	if seasonItems[0].Raw.(map[string]any)["file_name"] == "" {
		t.Fatalf("raw payload = %#v", seasonItems[0].Raw)
	}
	if _, exists := provider.saved["dir:/tmp/pan123/Extras"]; exists {
		t.Fatalf("extras items = %#v, want none", provider.saved["dir:/tmp/pan123/Extras"])
	}
}

func TestSaveImportedFilesToTempRequiresTempRootAndFiles(t *testing.T) {
	provider := &fakeShareSaver{}
	if _, err := SaveImportedFilesToTemp(context.Background(), provider, "manual_import://pan123", nil, SaveShareOptions{TempRoot: "/tmp/pan123"}); err == nil {
		t.Fatal("expected empty files error")
	}
	if _, err := SaveImportedFilesToTemp(context.Background(), provider, "manual_import://pan123", []pan123ImportedFile{{Path: "Movie.mkv", Name: "Movie.mkv", Etag: "bc18e4ea5fb89ec5778d1f38c9772f5f", Size: 1}}, SaveShareOptions{}); err == nil {
		t.Fatal("expected temp root error")
	}
}
