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
