package db

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func TestCreateIntentTxRejectsRequestIDReuseForDifferentIntent(t *testing.T) {
	database, err := gorm.Open(sqlite.Open("file:moviepilot_intent_idempotency_test?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.AutoMigrate(&model.MoviePilotDownloadIntent{}); err != nil {
		t.Fatal(err)
	}
	policy, _ := json.Marshal(model.TorrentRetentionPolicy{MinSeedSeconds: 3600})
	first := &model.MoviePilotDownloadIntent{
		ID: "intent-first", RequestID: "request-shared", BridgeInstanceID: "mp-main",
		SubscriptionID: 1, SubscriptionItemID: 2, MediaSource: "tmdb", MediaID: "123",
		ResourceRef: "resource-a", TorrentFingerprint: "fingerprint-a", RetentionPolicyJSON: string(policy),
	}
	if err := CreateIntentTx(context.Background(), database, first); err != nil {
		t.Fatal(err)
	}
	changed := *first
	changed.ID = "intent-second"
	changed.SubscriptionID = 9
	if err := CreateIntentTx(context.Background(), database, &changed); err == nil || !strings.Contains(err.Error(), "different intent") {
		t.Fatalf("request ID reuse error = %v", err)
	}
}

func TestCreateIntentTxRepairsMissingSubscriptionItemAssociation(t *testing.T) {
	database, err := gorm.Open(sqlite.Open("file:moviepilot_intent_item_repair_test?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.AutoMigrate(&model.MoviePilotDownloadIntent{}); err != nil {
		t.Fatal(err)
	}
	first := &model.MoviePilotDownloadIntent{
		ID: "intent-item-repair", RequestID: "request-item-repair", BridgeInstanceID: "mp-main",
		SubscriptionID: 1, MediaSource: "tmdb", MediaID: "123", ResourceRef: "resource-a",
		TorrentFingerprint: "fingerprint-a", RetentionPolicyJSON: "{}",
	}
	if err := CreateIntentTx(context.Background(), database, first); err != nil {
		t.Fatal(err)
	}
	second := *first
	second.ID = "intent-item-repair-retry"
	second.SubscriptionItemID = 42
	if err := CreateIntentTx(context.Background(), database, &second); err != nil {
		t.Fatalf("retry should repair the local item association: %v", err)
	}
	if second.ID != first.ID || second.SubscriptionItemID != 42 {
		t.Fatalf("reused intent = %#v, want existing ID with repaired item", second)
	}
	var stored model.MoviePilotDownloadIntent
	if err := database.First(&stored, "request_id = ?", first.RequestID).Error; err != nil {
		t.Fatal(err)
	}
	if stored.SubscriptionItemID != 42 {
		t.Fatalf("stored subscription item ID = %d, want 42", stored.SubscriptionItemID)
	}
}

func TestCreateIntentTxRefreshesSchedulingProjectionOnRetry(t *testing.T) {
	database, err := gorm.Open(sqlite.Open("file:moviepilot_intent_schedule_retry_test?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.AutoMigrate(&model.MoviePilotDownloadIntent{}); err != nil {
		t.Fatal(err)
	}
	first := &model.MoviePilotDownloadIntent{
		ID: "intent-schedule", RequestID: "request-schedule", BridgeInstanceID: "mp-main",
		SubscriptionID: 1, MediaSource: "tmdb", MediaID: "123", ResourceRef: "resource-a",
		TorrentFingerprint: "fingerprint-a", RetentionPolicyJSON: "{}",
		DownloaderPolicyJSON: `{"mode":"coordinator_select"}`, DownloaderPolicyMode: "coordinator_select",
		SelectedDownloader: "qb-a", SelectedRouteID: "route-a", ReservationID: "reservation-a",
		Status: model.MoviePilotIntentStatusWaitingCapacity, LastErrorCode: "downloader_capacity_unavailable", LastError: "route is full",
	}
	if err := CreateIntentTx(context.Background(), database, first); err != nil {
		t.Fatal(err)
	}
	retry := *first
	retry.ID = "intent-schedule-retry"
	retry.DownloaderPolicyJSON = `{"mode":"moviepilot_select"}`
	retry.DownloaderPolicyMode = "moviepilot_select"
	retry.SelectedDownloader, retry.SelectedRouteID, retry.ReservationID = "", "", ""
	retry.Status = model.MoviePilotIntentStatusPending
	retry.LastErrorCode, retry.LastError = "", ""
	if err := CreateIntentTx(context.Background(), database, &retry); err != nil {
		t.Fatalf("refresh scheduling projection: %v", err)
	}
	var stored model.MoviePilotDownloadIntent
	if err := database.First(&stored, "request_id = ?", first.RequestID).Error; err != nil {
		t.Fatal(err)
	}
	if stored.Status != model.MoviePilotIntentStatusPending || stored.DownloaderPolicyMode != "moviepilot_select" || stored.SelectedDownloader != "" || stored.SelectedRouteID != "" || stored.ReservationID != "" || stored.LastErrorCode != "" || stored.LastError != "" {
		t.Fatalf("stale scheduling projection = %#v", stored)
	}
}

func TestMoviePilotBridgeOutboxEnforcesOneRowPerBridgeRequest(t *testing.T) {
	database, err := gorm.Open(sqlite.Open("file:moviepilot_outbox_unique_test?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.AutoMigrate(&model.MoviePilotBridgeOutbox{}); err != nil {
		t.Fatal(err)
	}
	first := model.MoviePilotBridgeOutbox{ID: "outbox-1", BridgeID: "mp-main", RequestID: "request-1", EventID: "event-1"}
	if err := database.Create(&first).Error; err != nil {
		t.Fatal(err)
	}
	duplicate := model.MoviePilotBridgeOutbox{ID: "outbox-2", BridgeID: first.BridgeID, RequestID: first.RequestID, EventID: "event-2"}
	if err := database.Create(&duplicate).Error; err == nil {
		t.Fatal("duplicate bridge/request outbox row was accepted")
	}
}

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

func TestBindTorrentTxRejectsChangedImmutableBinding(t *testing.T) {
	database, err := gorm.Open(sqlite.Open("file:moviepilot_binding_immutable_test?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.AutoMigrate(&model.MoviePilotDownloadIntent{}, &model.MoviePilotTorrentBinding{}); err != nil {
		t.Fatal(err)
	}
	intent := &model.MoviePilotDownloadIntent{
		ID: "intent-immutable", RequestID: "request-immutable", BridgeInstanceID: "mp-main",
		Status: model.MoviePilotIntentStatusAccepted,
	}
	if err := database.Create(intent).Error; err != nil {
		t.Fatal(err)
	}
	hash := strings.Repeat("f", 40)
	if _, err := BindTorrentTx(context.Background(), database, intent, "mp-main", "qb-main", "worker-1", "qb-1", hash, "/downloads/show"); err != nil {
		t.Fatal(err)
	}
	if _, err := BindTorrentTx(context.Background(), database, intent, "mp-main", "qb-other", "worker-1", "qb-1", hash, "/downloads/show"); err == nil || !strings.Contains(err.Error(), "immutable torrent binding") {
		t.Fatalf("changed downloader error = %v", err)
	}
	if _, err := BindTorrentTx(context.Background(), database, intent, "mp-main", "qb-main", "worker-1", "qb-1", hash, "/downloads/other"); err == nil || !strings.Contains(err.Error(), "immutable torrent binding") {
		t.Fatalf("changed content path error = %v", err)
	}
}

func TestBindTorrentTxScopesTorrentHashToBridgeInstance(t *testing.T) {
	database, err := gorm.Open(sqlite.Open("file:moviepilot_transfer_bridge_scope_test?mode=memory&cache=shared"), &gorm.Config{})
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
	intents := []*model.MoviePilotDownloadIntent{
		{ID: "intent-bridge-a", RequestID: "request-bridge-a", BridgeInstanceID: "mp-a", Status: model.MoviePilotIntentStatusAccepted},
		{ID: "intent-bridge-b", RequestID: "request-bridge-b", BridgeInstanceID: "mp-b", Status: model.MoviePilotIntentStatusAccepted},
	}
	for _, intent := range intents {
		if err := database.Create(intent).Error; err != nil {
			t.Fatalf("create intent %s: %v", intent.ID, err)
		}
	}
	hash := strings.Repeat("d", 40)
	if _, err := BindTorrentTx(context.Background(), database, intents[0], "mp-a", "qb-a", "worker-a", "qb-a", hash, "/downloads/a"); err != nil {
		t.Fatalf("bind bridge A: %v", err)
	}
	if _, err := BindTorrentTx(context.Background(), database, intents[1], "mp-b", "qb-b", "worker-b", "qb-b", hash, "/downloads/b"); err != nil {
		t.Fatalf("bind same hash on bridge B: %v", err)
	}
}

func TestNormalizeMoviePilotTorrentBindingIndexReplacesLegacyGlobalUniqueness(t *testing.T) {
	database, err := gorm.Open(sqlite.Open("file:moviepilot_transfer_index_migration_test?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Exec(`CREATE TABLE movie_pilot_torrent_bindings (id TEXT PRIMARY KEY, bridge_instance_id TEXT, torrent_hash TEXT)`).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Exec(`CREATE UNIQUE INDEX idx_moviepilot_torrent_hash ON movie_pilot_torrent_bindings(torrent_hash)`).Error; err != nil {
		t.Fatal(err)
	}
	if err := NormalizeMoviePilotTorrentBindingIndex(database); err != nil {
		t.Fatal(err)
	}
	if database.Migrator().HasIndex(&model.MoviePilotTorrentBinding{}, "idx_moviepilot_torrent_hash") || !database.Migrator().HasIndex(&model.MoviePilotTorrentBinding{}, "idx_moviepilot_bridge_torrent_hash") {
		t.Fatal("legacy global index was not replaced by the bridge-scoped index")
	}
	hash := strings.Repeat("e", 40)
	if err := database.Exec(`INSERT INTO movie_pilot_torrent_bindings(id, bridge_instance_id, torrent_hash) VALUES (?, ?, ?), (?, ?, ?)`, "a", "mp-a", hash, "b", "mp-b", hash).Error; err != nil {
		t.Fatalf("same hash across bridge instances should be accepted after migration: %v", err)
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

func TestListMoviePilotProgressUsesLatestBindingDeliveryCounts(t *testing.T) {
	database, err := gorm.Open(sqlite.Open("file:moviepilot_progress_latest_binding_test?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	sqlDB, err := database.DB()
	if err != nil {
		t.Fatalf("get sqlite db: %v", err)
	}
	defer sqlDB.Close()
	if err := database.AutoMigrate(&model.MoviePilotDownloadIntent{}, &model.MoviePilotTorrentBinding{}, &model.MoviePilotDeliveryFile{}); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	now := time.Now().UTC()
	intents := []model.MoviePilotDownloadIntent{
		{ID: "intent-old", RequestID: "request-old", SubscriptionID: 9, Status: model.MoviePilotIntentStatusCompleted},
		{ID: "intent-new", RequestID: "request-new", SubscriptionID: 9, Status: model.MoviePilotIntentStatusBound},
	}
	bindings := []model.MoviePilotTorrentBinding{
		{ID: "binding-old", IntentID: "intent-old", BridgeInstanceID: "mp-main", TorrentHash: strings.Repeat("a", 40), UpdatedAt: now.Add(-time.Hour), Status: model.MoviePilotTorrentStatusDeleted, RetentionStatus: model.MoviePilotRetentionStatusDeleted},
		{ID: "binding-new", IntentID: "intent-new", BridgeInstanceID: "mp-main", TorrentHash: strings.Repeat("b", 40), UpdatedAt: now, Status: model.MoviePilotTorrentStatusTransferring, RetentionStatus: model.MoviePilotRetentionStatusPending, LastQBProgress: 1},
	}
	deliveries := []model.MoviePilotDeliveryFile{
		{ID: "delivery-1", TorrentBindingID: "binding-new", RelativePath: "S01E01.mkv", Required: true, Status: model.MoviePilotDeliveryStatusMaterialized, UploadProgress: 1},
		{ID: "delivery-2", TorrentBindingID: "binding-new", RelativePath: "S01E02.mkv", Required: true, Status: model.MoviePilotDeliveryStatusUploading, UploadProgress: .5},
	}
	for i := range intents {
		if err := database.Create(&intents[i]).Error; err != nil {
			t.Fatalf("create intent: %v", err)
		}
	}
	for i := range bindings {
		if err := database.Create(&bindings[i]).Error; err != nil {
			t.Fatalf("create binding: %v", err)
		}
	}
	for i := range deliveries {
		if err := database.Create(&deliveries[i]).Error; err != nil {
			t.Fatalf("create delivery: %v", err)
		}
	}
	old := db
	db = database
	t.Cleanup(func() { db = old })

	progress, err := ListMoviePilotProgressBySubscriptionIDs([]uint{9})
	if err != nil {
		t.Fatalf("list progress: %v", err)
	}
	got := progress[9]
	if got.ExpectedFiles != 2 || got.TransferredFiles != 1 || got.UploadProgress != .75 || got.TorrentStatus != model.MoviePilotTorrentStatusTransferring {
		t.Fatalf("latest binding progress = %#v", got)
	}
}
