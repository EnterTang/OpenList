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
