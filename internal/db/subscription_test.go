package db

import (
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestUpsertSubscriptionItemPreservesTransferredStatusOnUnchangedScan(t *testing.T) {
	setupETFArchiveDB(t)

	item, isNew, err := UpsertSubscriptionItem(&model.SubscriptionItem{
		SubscriptionID: 1,
		SourceKey:      "source-a",
		FileHash:       "hash-a",
		FileName:       "01.iso",
		Status:         model.SubscriptionItemStatusPending,
		LastSeenAt:     time.Now(),
	})
	if err != nil {
		t.Fatalf("upsert initial item: %v", err)
	}
	if !isNew || item.Status != model.SubscriptionItemStatusPending {
		t.Fatalf("initial item = %#v isNew=%v", item, isNew)
	}

	item.Status = model.SubscriptionItemStatusTransferred
	item.LastError = ""
	if _, _, err := UpsertSubscriptionItem(item); err != nil {
		t.Fatalf("mark transferred: %v", err)
	}

	scanned, isNew, err := UpsertSubscriptionItem(&model.SubscriptionItem{
		SubscriptionID: 1,
		SourceKey:      "source-a",
		FileHash:       "hash-a",
		FileName:       "01.iso",
		Status:         model.SubscriptionItemStatusPending,
		LastSeenAt:     time.Now(),
	})
	if err != nil {
		t.Fatalf("upsert unchanged scan: %v", err)
	}
	if isNew {
		t.Fatal("unchanged scan reported new item")
	}
	if scanned.Status != model.SubscriptionItemStatusTransferred {
		t.Fatalf("status = %q, want transferred", scanned.Status)
	}

	changed, _, err := UpsertSubscriptionItem(&model.SubscriptionItem{
		SubscriptionID: 1,
		SourceKey:      "source-a",
		FileHash:       "hash-b",
		FileName:       "01.iso",
		Status:         model.SubscriptionItemStatusPending,
		LastSeenAt:     time.Now(),
	})
	if err != nil {
		t.Fatalf("upsert changed scan: %v", err)
	}
	if changed.Status != model.SubscriptionItemStatusPending {
		t.Fatalf("changed status = %q, want pending", changed.Status)
	}
}

func TestUpsertSubscriptionItemResetsTransferredStatusWhenTargetPathChanges(t *testing.T) {
	setupETFArchiveDB(t)

	item, _, err := UpsertSubscriptionItem(&model.SubscriptionItem{
		SubscriptionID: 1,
		SourceKey:      "source-target-change",
		FileHash:       "hash-a",
		FileName:       "01.mp4",
		TargetPath:     "/media/Season 1/Show.S01E01.mp4",
		Status:         model.SubscriptionItemStatusPending,
		LastSeenAt:     time.Now(),
	})
	if err != nil {
		t.Fatalf("upsert initial item: %v", err)
	}
	item.Status = model.SubscriptionItemStatusTransferred
	if _, _, err := UpsertSubscriptionItem(item); err != nil {
		t.Fatalf("mark transferred: %v", err)
	}

	rescanned, isNew, err := UpsertSubscriptionItem(&model.SubscriptionItem{
		SubscriptionID: 1,
		SourceKey:      "source-target-change",
		FileHash:       "hash-a",
		FileName:       "01.mp4",
		TargetPath:     "/media/Season 2/Show.S02E01.mp4",
		Status:         model.SubscriptionItemStatusPending,
		LastSeenAt:     time.Now(),
	})
	if err != nil {
		t.Fatalf("upsert target-changed item: %v", err)
	}
	if isNew {
		t.Fatal("target-changed scan reported new item")
	}
	if rescanned.Status != model.SubscriptionItemStatusPending {
		t.Fatalf("status = %q, want pending after target path changed", rescanned.Status)
	}
	if rescanned.LastError != "" {
		t.Fatalf("last error = %q, want cleared", rescanned.LastError)
	}
}

func TestUpsertSubscriptionItemPersistsSourceProvider(t *testing.T) {
	setupETFArchiveDB(t)

	item, _, err := UpsertSubscriptionItem(&model.SubscriptionItem{
		SubscriptionID: 2,
		SourceKey:      "provider-source",
		SourceProvider: "pan123",
		FileHash:       "hash-provider",
		Status:         model.SubscriptionItemStatusPending,
		LastSeenAt:     time.Now(),
	})
	if err != nil {
		t.Fatalf("upsert provider item: %v", err)
	}
	if item.SourceProvider != "pan123" {
		t.Fatalf("source provider = %q, want pan123", item.SourceProvider)
	}

	item.SourceProvider = "quark"
	updated, _, err := UpsertSubscriptionItem(item)
	if err != nil {
		t.Fatalf("update provider item: %v", err)
	}
	if updated.SourceProvider != "quark" {
		t.Fatalf("updated source provider = %q, want quark", updated.SourceProvider)
	}
}

func TestUpdateAndDeleteSubscriptionEditableFields(t *testing.T) {
	setupETFArchiveDB(t)

	sub := &model.Subscription{
		Name:                     "Some Show",
		SourceType:               model.SubscriptionSourceManual,
		SourceConfig:             `{"links":["https://pan.quark.cn/s/first"]}`,
		CheckIntervalMinutes:     60,
		MediaType:                "tv",
		Seasons:                  []int{2},
		LatestSeasonEpisodeStart: 5,
	}
	if err := CreateSubscription(sub); err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	sub.SourceConfig = `{"links":["https://115cdn.com/s/second"]}`
	sub.CheckIntervalMinutes = 180
	sub.LatestSeasonEpisodeStart = 9
	sub.LatestSeasonEpisodeEnd = 12
	if err := UpdateSubscription(sub); err != nil {
		t.Fatalf("update subscription: %v", err)
	}
	got, err := GetSubscriptionByID(sub.ID)
	if err != nil {
		t.Fatalf("get updated subscription: %v", err)
	}
	if got.SourceConfig != sub.SourceConfig || got.CheckIntervalMinutes != 180 || got.LatestSeasonEpisodeStart != 9 || got.LatestSeasonEpisodeEnd != 12 {
		t.Fatalf("updated editable fields = %#v", got)
	}
	if _, _, err := UpsertSubscriptionItem(&model.SubscriptionItem{SubscriptionID: sub.ID, SourceKey: "item", LastSeenAt: time.Now()}); err != nil {
		t.Fatalf("create child item: %v", err)
	}
	if err := CreateSubscriptionRun(&model.SubscriptionRun{SubscriptionID: sub.ID, StartedAt: time.Now(), Status: model.SubscriptionStatusFailed}); err != nil {
		t.Fatalf("create child run: %v", err)
	}
	if err := DeleteSubscription(sub.ID); err != nil {
		t.Fatalf("delete subscription: %v", err)
	}
	if _, err := GetSubscriptionByID(sub.ID); err == nil {
		t.Fatal("deleted subscription still exists")
	}
	items, err := ListSubscriptionItems(sub.ID)
	if err != nil || len(items) != 0 {
		t.Fatalf("deleted subscription items = %#v err=%v", items, err)
	}
	runs, total, err := ListSubscriptionRuns(SubscriptionRunFilter{SubscriptionID: sub.ID, Page: 1, PerPage: 10})
	if err != nil || total != 0 || len(runs) != 0 {
		t.Fatalf("deleted subscription runs = total %d items %#v err=%v", total, runs, err)
	}
}

func TestListSubscriptionRunsFiltersSuccessfulNoopRuns(t *testing.T) {
	setupETFArchiveDB(t)

	now := time.Now()
	runs := []model.SubscriptionRun{
		{
			SubscriptionID: 1,
			StartedAt:      now.Add(-3 * time.Minute),
			Status:         model.SubscriptionStatusSuccess,
		},
		{
			SubscriptionID: 1,
			StartedAt:      now.Add(-2 * time.Minute),
			Status:         model.SubscriptionStatusSuccess,
			AddedCount:     1,
		},
		{
			SubscriptionID: 1,
			StartedAt:      now.Add(-1 * time.Minute),
			Status:         model.SubscriptionStatusFailed,
			Error:          "temporary failure",
		},
	}
	for i := range runs {
		if err := CreateSubscriptionRun(&runs[i]); err != nil {
			t.Fatalf("create run %d: %v", i, err)
		}
	}

	items, total, err := ListSubscriptionRuns(SubscriptionRunFilter{Page: 1, PerPage: 10})
	if err != nil {
		t.Fatalf("list runs: %v", err)
	}
	if total != 2 || len(items) != 2 {
		t.Fatalf("runs total/len = %d/%d, want 2/2: %#v", total, len(items), items)
	}
	for _, item := range items {
		if item.Status == model.SubscriptionStatusSuccess && item.AddedCount == 0 && item.ChangedCount == 0 && item.TransferredCount == 0 {
			t.Fatalf("successful noop run was returned: %#v", item)
		}
	}

	items, total, err = ListSubscriptionRuns(SubscriptionRunFilter{Status: model.SubscriptionStatusSuccess, Page: 1, PerPage: 10})
	if err != nil {
		t.Fatalf("list success runs: %v", err)
	}
	if total != 1 || len(items) != 1 || items[0].AddedCount != 1 {
		t.Fatalf("success runs = total %d items %#v, want only changed success run", total, items)
	}
}

func TestDeleteAndClearFailedSubscriptionRuns(t *testing.T) {
	setupETFArchiveDB(t)

	runs := []model.SubscriptionRun{
		{SubscriptionID: 1, StartedAt: time.Now().Add(-3 * time.Minute), Status: model.SubscriptionStatusFailed, Error: "first failure"},
		{SubscriptionID: 1, StartedAt: time.Now().Add(-2 * time.Minute), Status: model.SubscriptionStatusSuccess, AddedCount: 1},
		{SubscriptionID: 2, StartedAt: time.Now().Add(-time.Minute), Status: model.SubscriptionStatusSuccess, Error: "completed with an error"},
	}
	for i := range runs {
		if err := CreateSubscriptionRun(&runs[i]); err != nil {
			t.Fatalf("create run %d: %v", i, err)
		}
	}

	if err := DeleteSubscriptionRun(runs[0].ID); err != nil {
		t.Fatalf("delete failed run: %v", err)
	}
	deleted, err := ClearFailedSubscriptionRuns()
	if err != nil {
		t.Fatalf("clear failed runs: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("cleared runs = %d, want 1", deleted)
	}

	items, total, err := ListSubscriptionRuns(SubscriptionRunFilter{Page: 1, PerPage: 10})
	if err != nil {
		t.Fatalf("list remaining runs: %v", err)
	}
	if total != 1 || len(items) != 1 || items[0].ID != runs[1].ID {
		t.Fatalf("remaining runs = total %d items %#v, want successful run %d", total, items, runs[1].ID)
	}
}
