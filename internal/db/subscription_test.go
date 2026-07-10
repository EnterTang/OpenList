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
