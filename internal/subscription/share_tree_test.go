package subscription

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeShareTreeProvider struct {
	name     ShareProviderName
	children map[string][]ShareItem
	err      error
}

func (p *fakeShareTreeProvider) Name() ShareProviderName {
	return p.name
}

func (p *fakeShareTreeProvider) ParseURL(raw string) (ShareRef, error) {
	return ParseShareURL(raw)
}

func (p *fakeShareTreeProvider) ListShareChildren(ctx context.Context, ref ShareRef, parentID string) ([]ShareItem, error) {
	if p.err != nil {
		return nil, p.err
	}
	return p.children[parentID], nil
}

func TestListShareTreeReturnsRecursiveEntries(t *testing.T) {
	modified := time.Unix(1700000000, 0)
	provider := &fakeShareTreeProvider{
		name: ShareProviderQuark,
		children: map[string][]ShareItem{
			"": {
				{ID: "dir-1", Name: "Season 1", IsDir: true, Modified: modified},
				{ID: "file-1", Name: "Movie.mkv", Size: 1024, Modified: modified},
			},
			"dir-1": {
				{ID: "file-2", ParentID: "dir-1", Name: "Show.S01E01.mkv", Size: 2048, Modified: modified},
			},
		},
	}
	ref := ShareRef{Provider: ShareProviderQuark, RawURL: "https://pan.quark.cn/s/bc18e4ea5fb8"}

	entries, err := ListShareTree(context.Background(), provider, ref)
	if err != nil {
		t.Fatalf("list share tree: %v", err)
	}
	wantPaths := []string{"/Season 1", "/Season 1/Show.S01E01.mkv", "/Movie.mkv"}
	if len(entries) != len(wantPaths) {
		t.Fatalf("entry count = %d, want %d: %#v", len(entries), len(wantPaths), entries)
	}
	for i, want := range wantPaths {
		if entries[i].Path != want {
			t.Fatalf("entry[%d].Path = %q, want %q", i, entries[i].Path, want)
		}
		if entries[i].RootPath != ref.RawURL {
			t.Fatalf("entry[%d].RootPath = %q, want %q", i, entries[i].RootPath, ref.RawURL)
		}
	}
	if !entries[0].IsDir || entries[1].IsDir || entries[2].IsDir {
		t.Fatalf("unexpected dir flags: %#v", entries)
	}
}

func TestListShareTreePropagatesContextCancellation(t *testing.T) {
	provider := &fakeShareTreeProvider{name: ShareProviderQuark, err: context.Canceled}
	_, err := ListShareTree(context.Background(), provider, ShareRef{RawURL: "https://pan.quark.cn/s/bc18e4ea5fb8"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}
