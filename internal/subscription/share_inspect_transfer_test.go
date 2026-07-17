package subscription

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestTransferSelectedShareCandidatesSkipsAcceptedEpisodeBeforeSaving(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	sub := &model.Subscription{Name: "Example", TMDBName: "Example", MediaType: "tv", TargetRoot: "/tv"}
	if err := db.CreateSubscription(sub); err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	accepted := &model.SubscriptionItem{
		SubscriptionID: sub.ID, SourceKey: "accepted", SourceProvider: string(ShareProviderQuark),
		FileID: "accepted", FilePath: "/Example.S01E01.quark.mkv", FileName: "Example.S01E01.quark.mkv",
		FileHash: "accepted-hash", Season: 1, Episode: 1, TargetPath: "/tv/Example.S01E01.mkv",
		Status: model.SubscriptionItemStatusTransferred, ClusterJobID: "accepted-job",
	}
	if _, _, err := db.UpsertSubscriptionItem(accepted); err != nil {
		t.Fatalf("seed accepted item: %v", err)
	}

	entry := TreeEntry{ID: "new", Path: "/Example.S01E01.pan123.mkv", Name: "Example.S01E01.pan123.mkv", Size: 900}
	candidate := shareTransferCandidate{
		Source: telegramPanSubscriptionSource{Name: string(ShareProviderPan123)},
		Entry:  entry,
		Item:   itemFromEntry(sub, entry, time.Now()),
	}
	saveCalls := 0
	originalSave := saveShareTransferCandidatesFn
	saveShareTransferCandidatesFn = func(_ context.Context, selected []shareTransferCandidate) ([]shareTransferCandidate, error) {
		saveCalls++
		return selected, nil
	}
	t.Cleanup(func() { saveShareTransferCandidatesFn = originalSave })

	items, _, _, _, _, err := transferSelectedShareCandidates(context.Background(), sub, []shareTransferCandidate{candidate}, false, time.Now(), "hash")
	if err != nil {
		t.Fatalf("transfer candidates: %v", err)
	}
	if saveCalls != 0 || len(items) != 1 || items[0].Status != model.SubscriptionItemStatusSkipped {
		t.Fatalf("save calls=%d items=%#v, want skipped item without temp save", saveCalls, items)
	}
	persisted, err := db.GetSubscriptionItem(sub.ID, candidate.Item.SourceKey)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.Status != model.SubscriptionItemStatusSkipped || !strings.Contains(persisted.LastError, "accepted") {
		t.Fatalf("persisted skipped item = %#v", persisted)
	}

	updatedEntry := entry
	updatedEntry.Size = 1200
	updatedCandidate := candidate
	updatedCandidate.Entry = updatedEntry
	updatedCandidate.Item = itemFromEntry(sub, updatedEntry, time.Now())
	items, _, _, _, _, err = transferSelectedShareCandidates(context.Background(), sub, []shareTransferCandidate{updatedCandidate}, false, time.Now(), "updated-hash")
	if err != nil {
		t.Fatalf("transfer updated candidate: %v", err)
	}
	if saveCalls != 0 || len(items) != 1 || items[0].Status != model.SubscriptionItemStatusSkipped {
		t.Fatalf("updated loser save calls=%d items=%#v, want skipped", saveCalls, items)
	}
	persisted, err = db.GetSubscriptionItem(sub.ID, candidate.Item.SourceKey)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.Status != model.SubscriptionItemStatusSkipped || persisted.FileHash == candidate.Item.FileHash {
		t.Fatalf("updated loser = %#v, want new hash with skipped status", persisted)
	}
}

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
