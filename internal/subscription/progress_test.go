package subscription

import (
	"reflect"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestCalculateSubscriptionProgressReportsLatestEpisodeAndMissingEarlierEpisodes(t *testing.T) {
	now := time.Date(2026, time.July, 15, 12, 0, 0, 0, time.UTC)
	sub := &model.Subscription{
		CreatedAt:                now.Add(-40 * 24 * time.Hour),
		MediaType:                "tv",
		Seasons:                  []int{1},
		LatestSeasonEpisodeStart: 1,
		LatestSeasonEpisodeEnd:   8,
	}
	progress := CalculateSubscriptionProgress(sub, []model.SubscriptionItem{
		{Season: 1, Episode: 1, Status: model.SubscriptionItemStatusTransferred, CreatedAt: now.Add(-20 * 24 * time.Hour)},
		{Season: 1, Episode: 2, Status: model.SubscriptionItemStatusTransferred, CreatedAt: now.Add(-19 * 24 * time.Hour)},
		{Season: 1, Episode: 3, Status: model.SubscriptionItemStatusPending, CreatedAt: now.Add(-18 * 24 * time.Hour)},
		{Season: 1, Episode: 5, Status: model.SubscriptionItemStatusTransferred, CreatedAt: now.Add(-2 * 24 * time.Hour)},
		{Season: 1, Episode: 8, Status: model.SubscriptionItemStatusSkipped, CreatedAt: now.Add(-time.Hour)},
	}, now)

	if progress.ArchiveStatus != model.SubscriptionArchiveStatusOngoing {
		t.Fatalf("archive status = %q, want ongoing", progress.ArchiveStatus)
	}
	if progress.LatestEpisode != 5 {
		t.Fatalf("latest episode = %d, want 5", progress.LatestEpisode)
	}
	if progress.CompletedEpisodes != 3 || progress.ExpectedEpisodes != 8 {
		t.Fatalf("completed/expected = %d/%d, want 3/8", progress.CompletedEpisodes, progress.ExpectedEpisodes)
	}
	if want := []int{4}; !reflect.DeepEqual(progress.MissingEpisodes, want) {
		t.Fatalf("missing episodes = %#v, want %#v", progress.MissingEpisodes, want)
	}
	if progress.LastEpisodeAddedAt == nil || !progress.LastEpisodeAddedAt.Equal(now.Add(-2*24*time.Hour)) {
		t.Fatalf("last episode added at = %v, want %v", progress.LastEpisodeAddedAt, now.Add(-2*24*time.Hour))
	}
}

func TestCalculateSubscriptionProgressArchivesCompletedRange(t *testing.T) {
	now := time.Date(2026, time.July, 15, 12, 0, 0, 0, time.UTC)
	progress := CalculateSubscriptionProgress(&model.Subscription{
		MediaType:                "tv",
		Seasons:                  []int{1},
		LatestSeasonEpisodeStart: 3,
		LatestSeasonEpisodeEnd:   5,
	}, []model.SubscriptionItem{
		{Season: 1, Episode: 3, Status: model.SubscriptionItemStatusTransferred, CreatedAt: now.Add(-3 * time.Hour)},
		{Season: 1, Episode: 4, Status: model.SubscriptionItemStatusTransferred, CreatedAt: now.Add(-2 * time.Hour)},
		{Season: 1, Episode: 5, Status: model.SubscriptionItemStatusTransferred, CreatedAt: now.Add(-time.Hour)},
	}, now)

	if progress.ArchiveStatus != model.SubscriptionArchiveStatusCompleted {
		t.Fatalf("archive status = %q, want completed", progress.ArchiveStatus)
	}
	if len(progress.MissingEpisodes) != 0 {
		t.Fatalf("missing episodes = %#v, want none", progress.MissingEpisodes)
	}
}

func TestCalculateSubscriptionProgressCountsFailedEpisodes(t *testing.T) {
	now := time.Date(2026, time.July, 15, 12, 0, 0, 0, time.UTC)
	progress := CalculateSubscriptionProgress(&model.Subscription{
		MediaType:                "tv",
		Seasons:                  []int{1},
		LatestSeasonEpisodeStart: 1,
		LatestSeasonEpisodeEnd:   4,
	}, []model.SubscriptionItem{
		{Season: 1, Episode: 1, Status: model.SubscriptionItemStatusTransferred, CreatedAt: now.Add(-4 * time.Hour)},
		{Season: 1, Episode: 2, Status: model.SubscriptionItemStatusFailed, CreatedAt: now.Add(-3 * time.Hour)},
		{Season: 1, Episode: 3, Status: model.SubscriptionItemStatusTransferring, CreatedAt: now.Add(-2 * time.Hour)},
		{Season: 1, Episode: 4, Status: model.SubscriptionItemStatusTransferred, CreatedAt: now.Add(-time.Hour)},
	}, now)

	if progress.CompletedEpisodes != 2 || progress.ExpectedEpisodes != 4 || progress.FailedEpisodes != 1 {
		t.Fatalf("progress counts = completed:%d expected:%d failed:%d, want 2/4/1", progress.CompletedEpisodes, progress.ExpectedEpisodes, progress.FailedEpisodes)
	}
}

func TestCalculateSubscriptionProgressAggregatesSelectedSeasonEpisodeCounts(t *testing.T) {
	now := time.Date(2026, time.July, 15, 12, 0, 0, 0, time.UTC)
	progress := CalculateSubscriptionProgress(&model.Subscription{
		MediaType:           "tv",
		Seasons:             []int{1, 2},
		SeasonEpisodeCounts: map[int]int{1: 10, 2: 8},
	}, []model.SubscriptionItem{
		{Season: 1, Episode: 1, Status: model.SubscriptionItemStatusTransferred, CreatedAt: now.Add(-2 * time.Hour)},
		{Season: 2, Episode: 1, Status: model.SubscriptionItemStatusTransferred, CreatedAt: now.Add(-time.Hour)},
		{Season: 2, Episode: 2, Status: model.SubscriptionItemStatusFailed, CreatedAt: now},
	}, now)

	if progress.CompletedEpisodes != 2 || progress.ExpectedEpisodes != 18 || progress.FailedEpisodes != 1 {
		t.Fatalf("progress counts = completed:%d expected:%d failed:%d, want 2/18/1", progress.CompletedEpisodes, progress.ExpectedEpisodes, progress.FailedEpisodes)
	}
	if progress.ArchiveStatus != model.SubscriptionArchiveStatusOngoing {
		t.Fatalf("archive status = %q, want ongoing", progress.ArchiveStatus)
	}
}

func TestCalculateSubscriptionProgressArchivesStalledRangeAfterThirtyDaysWithoutANewerEpisode(t *testing.T) {
	now := time.Date(2026, time.July, 15, 12, 0, 0, 0, time.UTC)
	progress := CalculateSubscriptionProgress(&model.Subscription{
		CreatedAt:                now.Add(-60 * 24 * time.Hour),
		MediaType:                "tv",
		Seasons:                  []int{2},
		LatestSeasonEpisodeStart: 1,
		LatestSeasonEpisodeEnd:   12,
	}, []model.SubscriptionItem{
		{Season: 2, Episode: 1, Status: model.SubscriptionItemStatusTransferred, CreatedAt: now.Add(-31 * 24 * time.Hour)},
		{Season: 2, Episode: 3, Status: model.SubscriptionItemStatusPending, CreatedAt: now.Add(-30*24*time.Hour - time.Nanosecond)},
		{Season: 2, Episode: 2, Status: model.SubscriptionItemStatusSkipped, CreatedAt: now.Add(-time.Hour)},
	}, now)

	if progress.ArchiveStatus != model.SubscriptionArchiveStatusStalled {
		t.Fatalf("archive status = %q, want stalled", progress.ArchiveStatus)
	}
}

func TestCalculateSubscriptionProgressUsesLatestSelectedSeasonOnly(t *testing.T) {
	now := time.Date(2026, time.July, 15, 12, 0, 0, 0, time.UTC)
	progress := CalculateSubscriptionProgress(&model.Subscription{
		MediaType:                "tv",
		Seasons:                  []int{1, 2},
		LatestSeasonEpisodeStart: 1,
		LatestSeasonEpisodeEnd:   2,
	}, []model.SubscriptionItem{
		{Season: 1, Episode: 1, Status: model.SubscriptionItemStatusTransferred, CreatedAt: now.Add(-3 * time.Hour)},
		{Season: 2, Episode: 1, Status: model.SubscriptionItemStatusTransferred, CreatedAt: now.Add(-2 * time.Hour)},
		{Season: 2, Episode: 2, Status: model.SubscriptionItemStatusTransferred, CreatedAt: now.Add(-time.Hour)},
	}, now)

	if progress.ArchiveStatus != model.SubscriptionArchiveStatusCompleted {
		t.Fatalf("archive status = %q, want completed", progress.ArchiveStatus)
	}
	if progress.LatestSeason != 2 || progress.LatestEpisode != 2 {
		t.Fatalf("latest season/episode = %d/%d, want 2/2", progress.LatestSeason, progress.LatestEpisode)
	}
}
