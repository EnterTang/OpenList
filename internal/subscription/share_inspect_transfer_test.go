package subscription

import (
	"context"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestSelectShareTransferCandidatesKeepsLargestAcrossShares(t *testing.T) {
	sub := &model.Subscription{ID: 1, MediaType: "tv", TargetRoot: "/tv", TMDBName: "Example"}
	seenAt := time.Now()
	smallEntry := TreeEntry{RootPath: "https://pan.quark.cn/s/small", Path: "/Example.S01E01.small.mkv", Name: "Example.S01E01.small.mkv", ID: "small", Size: 600}
	largeEntry := TreeEntry{RootPath: "https://www.123pan.com/s/large", Path: "/Example.S01E01.large.mkv", Name: "Example.S01E01.large.mkv", ID: "large", Size: 900}
	smallItem := itemFromEntry(sub, smallEntry, seenAt)
	smallItem.SourceProvider = string(ShareProviderQuark)
	largeItem := itemFromEntry(sub, largeEntry, seenAt)
	largeItem.SourceProvider = string(ShareProviderPan123)
	selected := selectShareTransferCandidates(sub, []shareTransferCandidate{
		{
			Source: telegramPanSubscriptionSource{Name: string(ShareProviderQuark)},
			Entry:  smallEntry,
			Item:   smallItem,
			Pair:   shareTreePair{entry: smallEntry, item: ShareItem{ID: "small", Size: 600}},
		},
		{
			Source: telegramPanSubscriptionSource{Name: string(ShareProviderPan123)},
			Entry:  largeEntry,
			Item:   largeItem,
			Pair:   shareTreePair{entry: largeEntry, item: ShareItem{ID: "large", Size: 900}},
		},
	}, nil)
	if len(selected) != 1 || selected[0].Entry.ID != "large" {
		t.Fatalf("selected = %#v, want largest pan123 file", selected)
	}
}

func TestSaveShareTransferCandidatesPersistsOnlySelectedPairs(t *testing.T) {
	oldSave := saveSharePairsToTempFn
	defer func() { saveSharePairsToTempFn = oldSave }()

	var savedPairs []shareTreePair
	saveSharePairsToTempFn = func(ctx context.Context, provider ShareSaver, ref ShareRef, pairs []shareTreePair, opts SaveShareOptions) ([]TreeEntry, error) {
		savedPairs = append([]shareTreePair(nil), pairs...)
		entries := make([]TreeEntry, 0, len(pairs))
		for _, pair := range pairs {
			entry := pair.entry
			entry.RootPath = opts.TempRoot
			entries = append(entries, entry)
		}
		return entries, nil
	}
	oldFactory := newShareSaverForProvider
	defer func() { newShareSaverForProvider = oldFactory }()
	newShareSaverForProvider = func(ShareProviderName, model.SubscriptionTelegramPanConfig) (ShareSaver, error) {
		return &fakeShareSaver{}, nil
	}

	entry := TreeEntry{Path: "/Example.S01E01.mkv", Name: "Example.S01E01.mkv", ID: "large", Size: 900}
	selected, err := saveShareTransferCandidates(context.Background(), []shareTransferCandidate{{
		Source: telegramPanSubscriptionSource{
			Name:   string(ShareProviderPan123),
			Config: model.SubscriptionTelegramPanConfig{TempTransferRoot: "/tmp/pan123"},
		},
		Ref:   ShareRef{Provider: ShareProviderPan123, RawURL: "https://www.123pan.com/s/large"},
		Pair:  shareTreePair{entry: entry, item: ShareItem{ID: "large", Name: entry.Name, Size: entry.Size}},
		Entry: entry,
		Item:  &model.SubscriptionItem{SourceKey: "k1", FileSize: 900},
	}})
	if err != nil {
		t.Fatalf("save candidates: %v", err)
	}
	if len(savedPairs) != 1 || savedPairs[0].item.ID != "large" {
		t.Fatalf("saved pairs = %#v", savedPairs)
	}
	if selected[0].Entry.RootPath != "/tmp/pan123" {
		t.Fatalf("entry root = %q, want temp root", selected[0].Entry.RootPath)
	}
}
