package subscription

import (
	"context"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestFilterLargestSharePairsPerSlotKeepsBiggestEpisode(t *testing.T) {
	sub := &model.Subscription{
		ID:         8,
		TMDBName:   "飞常日志",
		TMDBYear:   2024,
		TMDBID:     243236,
		MediaType:  "tv",
		Category:   "港台剧",
		TargetRoot: "/139_60t/上传中转",
		Seasons:    []int{2},
	}
	modified := time.Unix(1700000000, 0)
	pairs := []shareTreePair{
		{
			entry: TreeEntry{Path: "/Show.S02E05.small.mkv", Name: "Show.S02E05.small.mkv", Size: 600, Modified: modified},
			item:  ShareItem{ID: "small", Name: "Show.S02E05.small.mkv", Size: 600, Modified: modified},
		},
		{
			entry: TreeEntry{Path: "/Show.S02E05.large.mkv", Name: "Show.S02E05.large.mkv", Size: 900, Modified: modified},
			item:  ShareItem{ID: "large", Name: "Show.S02E05.large.mkv", Size: 900, Modified: modified},
		},
		{
			entry: TreeEntry{Path: "/Show.S02E06.mkv", Name: "Show.S02E06.mkv", Size: 700, Modified: modified},
			item:  ShareItem{ID: "e06", Name: "Show.S02E06.mkv", Size: 700, Modified: modified},
		},
	}

	filtered := filterLargestSharePairsPerSlot(sub, pairs)
	if got, want := len(filtered), 2; got != want {
		t.Fatalf("filtered count = %d, want %d: %#v", got, want, filtered)
	}
	ids := make([]string, 0, len(filtered))
	for _, pair := range filtered {
		ids = append(ids, pair.item.ID)
	}
	if !stringSlicesEqual(ids, []string{"large", "e06"}) {
		t.Fatalf("filtered ids = %#v, want large and e06", ids)
	}
}

func TestRemoveUnselectedTempCandidatesDeletesSameEpisodeLosers(t *testing.T) {
	sub := &model.Subscription{
		ID:         8,
		TMDBName:   "飞常日志",
		TMDBYear:   2024,
		TMDBID:     243236,
		MediaType:  "tv",
		Category:   "港台剧",
		TargetRoot: "/139_60t/上传中转",
		Seasons:    []int{2},
	}
	seenAt := time.Now()
	candidates := []telegramTempCandidate{
		testTelegramTempCandidate(sub, "pan123", TreeEntry{
			RootPath: "/123/转存至移动",
			Path:     "/Show.S02E05.small.mkv",
			Name:     "Show.S02E05.small.mkv",
			ID:       "small",
			Size:     600,
		}, seenAt),
		testTelegramTempCandidate(sub, "pan123", TreeEntry{
			RootPath: "/123/转存至移动",
			Path:     "/Show.S02E05.large.mkv",
			Name:     "Show.S02E05.large.mkv",
			ID:       "large",
			Size:     900,
		}, seenAt),
	}
	selected := selectTelegramTempTransferCandidates(sub, candidates, []string{"pan123"})

	removed := make([]string, 0)
	originalRemove := removeTempFile
	removeTempFile = func(ctx context.Context, path string) error {
		removed = append(removed, path)
		return nil
	}
	t.Cleanup(func() { removeTempFile = originalRemove })

	if err := removeUnselectedTempCandidates(context.Background(), sub, candidates, selected); err != nil {
		t.Fatalf("remove unselected: %v", err)
	}
	if got, want := removed, []string{"/123/转存至移动/Show.S02E05.small.mkv"}; !stringSlicesEqual(got, want) {
		t.Fatalf("removed = %#v, want %#v", got, want)
	}
}

func TestSaveShareToTempKeepsLargestPerEpisode(t *testing.T) {
	sub := &model.Subscription{
		ID:         8,
		TMDBName:   "飞常日志",
		TMDBYear:   2024,
		TMDBID:     243236,
		MediaType:  "tv",
		Category:   "港台剧",
		TargetRoot: "/139_60t/上传中转",
		Seasons:    []int{2},
	}
	modified := time.Unix(1700000000, 0)
	provider := &fakeShareSaver{
		fakeShareTreeProvider: fakeShareTreeProvider{
			name: ShareProviderPan123,
			children: map[string][]ShareItem{
				"": {
					{ID: "small", Name: "飞常日志.2024.S02E05.第5集.small.mkv", Size: 600, Modified: modified},
					{ID: "large", Name: "飞常日志.2024.S02E05.第5集.large.mkv", Size: 900, Modified: modified},
					{ID: "e06", Name: "飞常日志.2024.S02E06.第6集.mkv", Size: 700, Modified: modified},
				},
			},
		},
		dstDirID: "tmp-dir-id",
	}
	ref := ShareRef{Provider: ShareProviderPan123, RawURL: "https://123pan.com/s/example"}

	entries, err := SaveShareToTemp(context.Background(), provider, ref, SaveShareOptions{
		TempRoot:     "/123/转存至移动",
		Subscription: sub,
		Match: func(entry TreeEntry) bool {
			return true
		},
	})
	if err != nil {
		t.Fatalf("save share: %v", err)
	}
	if got := idsFromShareItems(provider.saved[""]); !stringSlicesEqual(got, []string{"large", "e06"}) {
		t.Fatalf("saved items = %#v, want large and e06", got)
	}
	if got, want := len(entries), 2; got != want {
		t.Fatalf("entries count = %d, want %d", got, want)
	}
}
