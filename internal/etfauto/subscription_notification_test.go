package etfauto

import (
	"encoding/json"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"gorm.io/gorm"
)

func TestNotificationSuccessCompletesLinkedSubscriptionItem(t *testing.T) {
	setupETFSubscriptionDB(t)
	item := model.SubscriptionItem{
		SubscriptionID: 7, SourceKey: "source-1", FileHash: "hash-1",
		Status: model.SubscriptionItemStatusNotifying, ClusterJobID: "cluster-1",
	}
	if _, _, err := db.UpsertSubscriptionItem(&item); err != nil {
		t.Fatal(err)
	}
	source := model.SubscriptionEpisodeSource{
		SubscriptionID: item.SubscriptionID, Season: 1, Episode: 1, SourceItemID: item.ID,
		FileHash: item.FileHash, Status: model.SubscriptionItemStatusNotifying, ClusterJobID: item.ClusterJobID,
	}
	if err := db.GetDb().Create(&source).Error; err != nil {
		t.Fatal(err)
	}
	jobIDs, err := json.Marshal([]string{item.ClusterJobID})
	if err != nil {
		t.Fatal(err)
	}
	job := model.ClusterJob{ID: item.ClusterJobID, IdempotencyKey: item.ClusterJobID, SubscriptionID: item.SubscriptionID, SubscriptionItemID: item.ID}
	if err := db.GetDb().Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.GetDb().Transaction(func(tx *gorm.DB) error {
		return updateClusterJobNotificationStatus(tx, string(jobIDs), model.ClusterNotificationStatusSucceeded)
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.GetDb().First(&item, item.ID).Error; err != nil {
		t.Fatal(err)
	}
	if item.Status != model.SubscriptionItemStatusTransferred {
		t.Fatalf("item status = %q, want transferred", item.Status)
	}
	if err := db.GetDb().First(&source, source.ID).Error; err != nil {
		t.Fatal(err)
	}
	if source.Status != model.SubscriptionItemStatusTransferred {
		t.Fatalf("source status = %q, want transferred", source.Status)
	}
}
