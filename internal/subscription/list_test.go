package subscription

import (
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestListSubscriptionsWithProgressFiltersArchivedSubscriptionsBeforePagination(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	now := time.Date(2026, time.July, 15, 12, 0, 0, 0, time.UTC)

	ongoing := &model.Subscription{
		Name: "ongoing", CreatedAt: now, MediaType: "tv", Seasons: []int{1},
		LatestSeasonEpisodeStart: 1, LatestSeasonEpisodeEnd: 2,
	}
	completed := &model.Subscription{
		Name: "completed", CreatedAt: now, MediaType: "tv", Seasons: []int{1},
		LatestSeasonEpisodeStart: 1, LatestSeasonEpisodeEnd: 2,
	}
	stalled := &model.Subscription{
		Name: "stalled", CreatedAt: now.Add(-31 * 24 * time.Hour), MediaType: "tv", Seasons: []int{1},
		LatestSeasonEpisodeStart: 1, LatestSeasonEpisodeEnd: 2,
	}
	for _, sub := range []*model.Subscription{ongoing, completed, stalled} {
		if err := db.CreateSubscription(sub); err != nil {
			t.Fatalf("create %s subscription: %v", sub.Name, err)
		}
	}
	for _, item := range []model.SubscriptionItem{
		{SubscriptionID: completed.ID, SourceKey: "completed-1", Season: 1, Episode: 1, Status: model.SubscriptionItemStatusTransferred, CreatedAt: now},
		{SubscriptionID: completed.ID, SourceKey: "completed-2", Season: 1, Episode: 2, Status: model.SubscriptionItemStatusTransferred, CreatedAt: now},
		{SubscriptionID: stalled.ID, SourceKey: "stalled-1", Season: 1, Episode: 1, Status: model.SubscriptionItemStatusPending, CreatedAt: now.Add(-31 * 24 * time.Hour)},
	} {
		if _, _, err := db.UpsertSubscriptionItem(&item); err != nil {
			t.Fatalf("upsert %s: %v", item.SourceKey, err)
		}
	}

	items, total, err := ListSubscriptionsWithProgress(db.SubscriptionFilter{Page: 1, PerPage: 1}, model.SubscriptionArchiveStatusOngoing, now)
	if err != nil {
		t.Fatalf("list ongoing subscriptions: %v", err)
	}
	if total != 1 || len(items) != 1 || items[0].Name != "ongoing" {
		t.Fatalf("ongoing subscriptions = total %d, items %#v", total, items)
	}
	if items[0].Progress.ArchiveStatus != model.SubscriptionArchiveStatusOngoing {
		t.Fatalf("ongoing progress status = %q", items[0].Progress.ArchiveStatus)
	}

	items, total, err = ListSubscriptionsWithProgress(db.SubscriptionFilter{Page: 1, PerPage: 1}, model.SubscriptionArchiveStatusCompleted, now)
	if err != nil {
		t.Fatalf("list completed subscriptions: %v", err)
	}
	if total != 1 || len(items) != 1 || items[0].Name != "completed" {
		t.Fatalf("completed subscriptions = total %d, items %#v", total, items)
	}

	items, total, err = ListSubscriptionsWithProgress(db.SubscriptionFilter{Page: 1, PerPage: 1}, model.SubscriptionArchiveStatusStalled, now)
	if err != nil {
		t.Fatalf("list stalled subscriptions: %v", err)
	}
	if total != 1 || len(items) != 1 || items[0].Name != "stalled" {
		t.Fatalf("stalled subscriptions = total %d, items %#v", total, items)
	}
}
