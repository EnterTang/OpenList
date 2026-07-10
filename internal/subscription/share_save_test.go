package subscription

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type fakeShareSaver struct {
	fakeShareTreeProvider
	ensuredPath    string
	ensureDirCalls []string
	ensureDirIDs   map[string]string
	dstDirID       string
	saved          map[string][]ShareItem
	waitedTasks    []string
}

func (p *fakeShareSaver) EnsureDir(ctx context.Context, path string) (string, error) {
	if p.ensuredPath == "" {
		p.ensuredPath = path
	}
	p.ensureDirCalls = append(p.ensureDirCalls, path)
	if p.ensureDirIDs == nil {
		p.ensureDirIDs = map[string]string{}
	}
	if id, ok := p.ensureDirIDs[path]; ok {
		return id, nil
	}
	if p.dstDirID == "" {
		p.dstDirID = "dir:" + path
	}
	id := "dir:" + path
	if path == p.ensuredPath {
		id = p.dstDirID
	}
	p.ensureDirIDs[path] = id
	return id, nil
}

func (p *fakeShareSaver) SaveShareItems(ctx context.Context, ref ShareRef, parentID string, items []ShareItem, dstDirID string) ([]string, error) {
	if p.saved == nil {
		p.saved = map[string][]ShareItem{}
	}
	p.saved[parentID] = append(p.saved[parentID], items...)
	if dstDirID != "" {
		p.saved[dstDirID] = append(p.saved[dstDirID], items...)
	}
	p.dstDirID = dstDirID
	return []string{"task-1"}, nil
}

func (p *fakeShareSaver) WaitSaveComplete(ctx context.Context, taskIDs []string) error {
	p.waitedTasks = append(p.waitedTasks, taskIDs...)
	return nil
}

func TestSaveShareToTempSavesMatchedFiles(t *testing.T) {
	modified := time.Unix(1700000000, 0)
	provider := &fakeShareSaver{
		fakeShareTreeProvider: fakeShareTreeProvider{
			name: ShareProviderQuark,
			children: map[string][]ShareItem{
				"": {
					{ID: "dir-1", Name: "Season 1", IsDir: true, Modified: modified},
					{ID: "file-1", Name: "Movie.mkv", Size: 1024, Modified: modified},
					{ID: "file-2", Name: "Poster.jpg", Size: 512, Modified: modified},
				},
				"dir-1": {
					{ID: "file-3", ParentID: "dir-1", Name: "Show.S01E01.mkv", Size: 2048, Modified: modified},
				},
			},
		},
		dstDirID: "tmp-dir-id",
	}
	ref := ShareRef{Provider: ShareProviderQuark, RawURL: "https://pan.quark.cn/s/bc18e4ea5fb8"}

	entries, err := SaveShareToTemp(context.Background(), provider, ref, SaveShareOptions{
		TempRoot: "/tmp/quark",
		Match: func(entry TreeEntry) bool {
			return !entry.IsDir && strings.ToLower(filepath.Ext(entry.Name)) == ".mkv"
		},
	})
	if err != nil {
		t.Fatalf("save share: %v", err)
	}
	if provider.ensuredPath != "/tmp/quark" {
		t.Fatalf("ensured path = %q, want /tmp/quark", provider.ensuredPath)
	}
	if got := idsFromShareItems(provider.saved[""]); !stringSlicesEqual(got, []string{"file-1"}) {
		t.Fatalf("root saved items = %#v, want file-1", got)
	}
	if got := idsFromShareItems(provider.saved["dir-1"]); !stringSlicesEqual(got, []string{"file-3"}) {
		t.Fatalf("folder saved items = %#v, want file-3", got)
	}
	if got, want := provider.waitedTasks, []string{"task-1", "task-1"}; !stringSlicesEqual(got, want) {
		t.Fatalf("waited tasks = %#v, want %#v", got, want)
	}
	if got, want := entryPaths(entries), []string{"/Season 1/Show.S01E01.mkv", "/Movie.mkv"}; !stringSlicesEqual(got, want) {
		t.Fatalf("entries = %#v, want %#v", got, want)
	}
}

func TestSaveShareToTempPreservesMatchedParentDirectories(t *testing.T) {
	modified := time.Unix(1700000000, 0)
	provider := &fakeShareSaver{
		fakeShareTreeProvider: fakeShareTreeProvider{
			name: ShareProviderAliyunDrive,
			children: map[string][]ShareItem{
				"": {
					{ID: "dir-season-2", Name: "第 2季", IsDir: true, Modified: modified},
				},
				"dir-season-2": {
					{ID: "file-2", ParentID: "dir-season-2", Name: "02 4k.mp4", Size: 2048, Modified: modified},
				},
			},
		},
		dstDirID: "tmp-dir-id",
	}
	ref := ShareRef{Provider: ShareProviderAliyunDrive, RawURL: "https://www.alipan.com/s/odeXVKsEKxr"}

	_, err := SaveShareToTemp(context.Background(), provider, ref, SaveShareOptions{
		TempRoot: "/tmp/aliyun",
		Match: func(entry TreeEntry) bool {
			return entry.Name == "02 4k.mp4"
		},
	})
	if err != nil {
		t.Fatalf("save share: %v", err)
	}
	if got, want := provider.ensureDirCalls, []string{"/tmp/aliyun", "/tmp/aliyun/第 2季"}; !stringSlicesEqual(got, want) {
		t.Fatalf("ensure dir calls = %#v, want %#v", got, want)
	}
	if got := idsFromShareItems(provider.saved["dir:/tmp/aliyun/第 2季"]); !stringSlicesEqual(got, []string{"file-2"}) {
		t.Fatalf("saved items in season dir = %#v, want file-2", got)
	}
}

func TestSaveShareToTempRequiresTempRoot(t *testing.T) {
	_, err := SaveShareToTemp(context.Background(), &fakeShareSaver{}, ShareRef{}, SaveShareOptions{})
	if err == nil {
		t.Fatal("expected temp root error")
	}
}

func idsFromShareItems(items []ShareItem) []string {
	ids := make([]string, 0, len(items))
	for _, item := range items {
		ids = append(ids, item.ID)
	}
	return ids
}

func entryPaths(entries []TreeEntry) []string {
	paths := make([]string, 0, len(entries))
	for _, entry := range entries {
		paths = append(paths, entry.Path)
	}
	return paths
}
