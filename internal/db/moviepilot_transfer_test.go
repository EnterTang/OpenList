package db

import (
	"context"
	"strings"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func TestBindTorrentTxRejectsASecondWorkerForTheSameHash(t *testing.T) {
	database, err := gorm.Open(sqlite.Open("file:moviepilot_transfer_binding_test?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	sqlDB, err := database.DB()
	if err != nil {
		t.Fatalf("get sqlite db: %v", err)
	}
	defer sqlDB.Close()
	if err := database.AutoMigrate(
		&model.MoviePilotDownloadIntent{},
		&model.MoviePilotTorrentBinding{},
	); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	intent := &model.MoviePilotDownloadIntent{
		ID:        "intent-1",
		RequestID: "request-1",
		Status:    model.MoviePilotIntentStatusAccepted,
	}
	if err := database.Create(intent).Error; err != nil {
		t.Fatalf("create intent: %v", err)
	}
	hash := strings.Repeat("b", 40)
	if _, err := BindTorrentTx(context.Background(), database, intent, "mp-main", "qb-hk", "worker-a", "qb-a", hash, "/downloads/show"); err != nil {
		t.Fatalf("bind first worker: %v", err)
	}
	if _, err := BindTorrentTx(context.Background(), database, intent, "mp-main", "qb-hk", "worker-b", "qb-b", hash, "/downloads/show"); err == nil || !strings.Contains(err.Error(), "torrent hash is already bound to worker-a") {
		t.Fatalf("second worker error = %v", err)
	}
}

func TestListMoviePilotTransferViewsIncludesWorkerRoute(t *testing.T) {
	database, err := gorm.Open(sqlite.Open("file:moviepilot_transfer_view_test?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	sqlDB, err := database.DB()
	if err != nil {
		t.Fatalf("get sqlite db: %v", err)
	}
	defer sqlDB.Close()
	if err := database.AutoMigrate(&model.Subscription{}, &model.MoviePilotDownloadIntent{}, &model.MoviePilotTorrentBinding{}, &model.MoviePilotDeliveryFile{}); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	sub := &model.Subscription{ID: 7, Name: "Show"}
	intent := &model.MoviePilotDownloadIntent{ID: "intent-view", RequestID: "request-view", SubscriptionID: sub.ID, Status: model.MoviePilotIntentStatusBound}
	binding := &model.MoviePilotTorrentBinding{ID: "binding-view", IntentID: intent.ID, WorkerNodeID: "worker-1", DownloaderAlias: "qb-main", QBClientID: "qb-1", TorrentHash: strings.Repeat("c", 40), Status: model.MoviePilotTorrentStatusTransferring, RetentionStatus: model.MoviePilotRetentionStatusPending}
	delivery := &model.MoviePilotDeliveryFile{ID: "delivery-view", TorrentBindingID: binding.ID, RelativePath: "Season 1/E01.mkv", FileName: "E01.mkv", Status: model.MoviePilotDeliveryStatusUploading, UploadProgress: .5, Required: true}
	for _, value := range []any{sub, intent, binding, delivery} {
		if err := database.Create(value).Error; err != nil {
			t.Fatalf("create fixture: %v", err)
		}
	}
	old := db
	db = database
	t.Cleanup(func() { db = old })
	views, err := ListMoviePilotTransferViews(context.Background(), sub.ID, "")
	if err != nil {
		t.Fatalf("list transfer views: %v", err)
	}
	if len(views) != 1 || views[0].WorkerNodeID != "worker-1" || views[0].QBClientID != "qb-1" || views[0].Status != model.MoviePilotDeliveryStatusUploading || views[0].UploadProgress != .5 {
		t.Fatalf("views = %#v", views)
	}
}
