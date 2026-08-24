package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/moviepilotbridge"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

type torrentDispatchRecorder struct {
	requests []TorrentJobDispatchRequest
}

type moviePilotTorrentControlRecorder struct {
	paused  []string
	resumed []string
}

func (r *moviePilotTorrentControlRecorder) PauseTorrent(_ context.Context, _, requestID, _, _, _ string) error {
	r.paused = append(r.paused, requestID)
	return nil
}

func (r *moviePilotTorrentControlRecorder) ResumeTorrent(_ context.Context, _, requestID, _, _, _ string) error {
	r.resumed = append(r.resumed, requestID)
	return nil
}

func (r *torrentDispatchRecorder) DispatchTorrentJob(_ context.Context, request TorrentJobDispatchRequest) (*model.ClusterJob, error) {
	r.requests = append(r.requests, request)
	return &model.ClusterJob{ID: fmt.Sprintf("job-%d", len(r.requests)), IdempotencyKey: request.IdempotencyKey}, nil
}

func openTorrentTransferTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	database, err := gorm.Open(sqlite.Open(fmt.Sprintf("file:%s?mode=memory&cache=shared", strings.ReplaceAll(t.Name(), "/", "_"))), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.AutoMigrate(
		&model.Subscription{}, &model.SubscriptionItem{}, &model.MoviePilotBridgeInstance{},
		&model.MoviePilotDownloadIntent{}, &model.MoviePilotTorrentBinding{}, &model.MoviePilotDeliveryFile{},
		&model.ClusterNode{}, &model.ClusterNodeSession{}, &model.ClusterNodeInventory{}, &model.ClusterJob{}, &model.ClusterJobAttempt{}, &model.ClusterJobStage{},
	); err != nil {
		t.Fatal(err)
	}
	return database
}

func TestTorrentBoundCreatesPinnedObserveRequest(t *testing.T) {
	database := openTorrentTransferTestDB(t)
	subscription := model.Subscription{ID: 7, Name: "Show", SourceType: model.SubscriptionSourceMoviePilot, MediaType: "tv", TMDBID: 123, TargetRoot: "/media/tv", DeliveryTarget: model.SubscriptionStorageTarget{Provider: "yidong139", Folder: "library"}}
	item := model.SubscriptionItem{ID: 8, SubscriptionID: subscription.ID, SourceKey: "moviepilot:resource", SourceProvider: model.SubscriptionSourceMoviePilot, FileName: "Show.S01E01.mkv", Season: 1, Episode: 1, TargetPath: "/media/tv/Show/Season 1/Show.S01E01.mkv", Status: model.SubscriptionItemStatusTransferring}
	intent := model.MoviePilotDownloadIntent{ID: "intent-1", RequestID: "request-1", BridgeInstanceID: "bridge-1", SubscriptionID: subscription.ID, SubscriptionItemID: item.ID, MediaSource: "tmdb", MediaID: "123", RetentionPolicyJSON: `{"min_seed_seconds":3600}`, Status: model.MoviePilotIntentStatusAccepted}
	for _, value := range []any{&subscription, &item, &intent} {
		if err := database.Create(value).Error; err != nil {
			t.Fatal(err)
		}
	}
	routes := protocol.NodeCapabilities{RedisDurabilityReady: true, MoviePilotRoutes: []protocol.MoviePilotRouteInventory{{BridgeInstanceID: "bridge-1", Downloader: "qb-main", QBClientID: "qb-1", QBHealth: "healthy", UploadConcurrency: 2}}}
	raw, _ := json.Marshal(routes)
	if err := database.Create(&model.ClusterNode{ID: "worker-1", Status: model.ClusterNodeStatusOnline, LastSessionID: "session-worker-1"}).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&model.ClusterNodeSession{ID: "session-worker-1", NodeID: "worker-1", Status: model.ClusterSessionStatusConnected}).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&model.ClusterNodeInventory{NodeID: "worker-1", Revision: 1, CapabilitiesJSON: string(raw)}).Error; err != nil {
		t.Fatal(err)
	}
	recorder := &torrentDispatchRecorder{}
	service := New(database, "")
	service.SetTorrentJobDispatcher(recorder)
	event := moviepilotbridge.BridgeEvent{EventID: "event-1", RequestID: intent.RequestID, Type: moviepilotbridge.EventTorrentBound, OccurredAt: time.Now().UTC(), Torrent: &moviepilotbridge.TorrentBoundPayload{Downloader: "qb-main", TorrentHash: strings.Repeat("a", 40), ContentPath: "/downloads/Show", Media: moviepilotbridge.MediaIdentity{MediaType: "tv", Season: 1, Episode: 1}}}
	if err := service.HandleMoviePilotEvent(context.Background(), "bridge-1", event); err != nil {
		t.Fatal(err)
	}
	var binding model.MoviePilotTorrentBinding
	if err := database.First(&binding, "intent_id = ?", intent.ID).Error; err != nil {
		t.Fatal(err)
	}
	if binding.WorkerNodeID != "worker-1" || binding.QBClientID != "qb-1" || binding.RetentionPolicyJSON != intent.RetentionPolicyJSON {
		t.Fatalf("binding = %#v", binding)
	}
	if len(recorder.requests) != 1 || recorder.requests[0].JobType != model.ClusterJobTypeTorrentObserve || recorder.requests[0].TaskContext.Torrent.WorkerNodeID != "worker-1" {
		t.Fatalf("observe requests = %#v", recorder.requests)
	}
}

func TestResolveMoviePilotWorkerRouteRejectsOfflineWorkerInventory(t *testing.T) {
	database := openTorrentTransferTestDB(t)
	routes := protocol.NodeCapabilities{MoviePilotRoutes: []protocol.MoviePilotRouteInventory{{
		BridgeInstanceID: "bridge-offline", Downloader: "qb-main", QBClientID: "qb-offline", QBHealth: "healthy",
	}}}
	raw, _ := json.Marshal(routes)
	for _, value := range []any{
		&model.ClusterNode{ID: "worker-offline", Status: model.ClusterNodeStatusOffline, LastSessionID: "session-offline"},
		&model.ClusterNodeSession{ID: "session-offline", NodeID: "worker-offline", Status: model.ClusterSessionStatusDisconnected},
		&model.ClusterNodeInventory{NodeID: "worker-offline", Revision: 1, CapabilitiesJSON: string(raw)},
	} {
		if err := database.Create(value).Error; err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := New(database, "").resolveMoviePilotWorkerRoute(context.Background(), "bridge-offline", "qb-main"); err == nil || !strings.Contains(err.Error(), "no healthy Worker route") {
		t.Fatalf("offline route error = %v", err)
	}
}

func TestReconcileWorkerOfflineTorrentControlPausesAndResumesExactBinding(t *testing.T) {
	database := openTorrentTransferTestDB(t)
	intent := model.MoviePilotDownloadIntent{ID: "intent-offline-control", RequestID: "request-offline-control", BridgeInstanceID: "bridge-1"}
	binding := model.MoviePilotTorrentBinding{
		ID: "binding-offline-control", IntentID: intent.ID, BridgeInstanceID: intent.BridgeInstanceID,
		DownloaderAlias: "qb-main", WorkerNodeID: "worker-offline-control", QBClientID: "qb-1",
		TorrentHash: strings.Repeat("4", 40), Status: model.MoviePilotTorrentStatusDownloading,
	}
	node := model.ClusterNode{ID: binding.WorkerNodeID, Status: model.ClusterNodeStatusOffline}
	for _, value := range []any{&intent, &binding, &node} {
		if err := database.Create(value).Error; err != nil {
			t.Fatal(err)
		}
	}
	recorder := &moviePilotTorrentControlRecorder{}
	service := New(database, "")
	service.SetMoviePilotTorrentController(recorder)
	if _, err := service.ReconcileWorkerOfflineTorrentControl(context.Background(), 10); err != nil {
		t.Fatal(err)
	}
	if len(recorder.paused) != 1 || recorder.paused[0] != intent.RequestID {
		t.Fatalf("pause calls = %#v", recorder.paused)
	}
	if err := database.First(&binding, "id = ?", binding.ID).Error; err != nil {
		t.Fatal(err)
	}
	if !binding.PausedForWorkerOffline || binding.WorkerOfflinePausedAt == nil {
		t.Fatalf("binding after offline pause = %#v", binding)
	}
	if err := database.Model(&model.ClusterNode{}).Where("id = ?", node.ID).Updates(map[string]any{"status": model.ClusterNodeStatusOnline}).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := service.ReconcileWorkerOfflineTorrentControl(context.Background(), 10); err != nil {
		t.Fatal(err)
	}
	if len(recorder.resumed) != 1 || recorder.resumed[0] != intent.RequestID {
		t.Fatalf("resume calls = %#v", recorder.resumed)
	}
	binding = model.MoviePilotTorrentBinding{}
	if err := database.First(&binding, "id = ?", "binding-offline-control").Error; err != nil {
		t.Fatal(err)
	}
	if binding.PausedForWorkerOffline || binding.WorkerOfflinePausedAt != nil {
		t.Fatalf("binding after worker recovery = %#v", binding)
	}
}

func TestObserveTorrentCreatesOneDeliveryPerFileAndDispatchesRecognizedEpisodes(t *testing.T) {
	database := openTorrentTransferTestDB(t)
	subscription := model.Subscription{ID: 7, Name: "Show", SourceType: model.SubscriptionSourceMoviePilot, MediaType: "tv", TMDBID: 123, TargetRoot: "/media/tv", DeliveryTarget: model.SubscriptionStorageTarget{Provider: "yidong139", Folder: "library"}}
	items := []model.SubscriptionItem{
		{ID: 8, SubscriptionID: subscription.ID, SourceKey: "episode-1", SourceProvider: model.SubscriptionSourceMoviePilot, FileName: "Show.S01E01.mkv", Season: 1, Episode: 1, TargetPath: "/media/tv/Show/Season 1/Show.S01E01.mkv", Status: model.SubscriptionItemStatusTransferring},
		{ID: 9, SubscriptionID: subscription.ID, SourceKey: "episode-2", SourceProvider: model.SubscriptionSourceMoviePilot, FileName: "Show.S01E02.mkv", Season: 1, Episode: 2, TargetPath: "/media/tv/Show/Season 1/Show.S01E02.mkv", Status: model.SubscriptionItemStatusPending},
	}
	intent := model.MoviePilotDownloadIntent{ID: "intent-1", RequestID: "request-1", BridgeInstanceID: "bridge-1", SubscriptionID: subscription.ID, SubscriptionItemID: items[0].ID, MediaSource: "tmdb", MediaID: "123", Status: model.MoviePilotIntentStatusBound}
	binding := model.MoviePilotTorrentBinding{ID: "binding-1", IntentID: intent.ID, BridgeInstanceID: "bridge-1", DownloaderAlias: "qb-main", WorkerNodeID: "worker-1", QBClientID: "qb-1", TorrentHash: strings.Repeat("b", 40), ContentPath: "/downloads/Show", ObserveJobID: "parent-1", Status: model.MoviePilotTorrentStatusBound}
	for _, value := range []any{&subscription, &items[0], &items[1], &intent, &binding} {
		if err := database.Create(value).Error; err != nil {
			t.Fatal(err)
		}
	}
	task := moviePilotTorrentTaskContext(&subscription, &items[0], &binding, binding.ID, moviepilotbridge.MediaIdentity{MediaType: "tv", Season: 1, Episode: 1})
	task.ParentBatchID = "parent-1"
	task.Torrent.RelativePath = ""
	rawTask, _ := json.Marshal(task)
	parent := model.ClusterJob{ID: "parent-1", Type: model.ClusterJobTypeTorrentObserve, Status: model.ClusterJobStatusPlanning, IdempotencyKey: "parent-key", ExpectedItems: 1}
	observe := model.ClusterJob{ID: "observe-1", ParentJobID: parent.ID, Type: model.ClusterJobTypeTorrentObserve, Status: model.ClusterJobStatusSucceeded, IdempotencyKey: "observe-key", TaskContextJSON: string(rawTask)}
	if err := database.Create(&parent).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&observe).Error; err != nil {
		t.Fatal(err)
	}
	recorder := &torrentDispatchRecorder{}
	service := New(database, "")
	service.SetTorrentJobDispatcher(recorder)
	result := map[string]any{"qb_state": "uploading", "progress": 1.0, "ratio": 1.25, "seeding_seconds": int64(7200), "files": []map[string]any{
		{"name": "Show.S01E01.mkv", "size": int64(100)},
		{"name": "Show.S01E02.mkv", "size": int64(200)},
		{"name": "sample.txt", "size": int64(20)},
	}}
	if err := service.ObserveTorrent(context.Background(), observe.ID, result); err != nil {
		t.Fatal(err)
	}
	var deliveries []model.MoviePilotDeliveryFile
	if err := database.Order("relative_path ASC").Find(&deliveries).Error; err != nil {
		t.Fatal(err)
	}
	if len(deliveries) != 3 || deliveries[0].Status != model.MoviePilotDeliveryStatusUploading || deliveries[1].Status != model.MoviePilotDeliveryStatusUploading || deliveries[2].Status != model.MoviePilotDeliveryStatusSkipped {
		t.Fatalf("deliveries = %#v", deliveries)
	}
	if len(recorder.requests) != 2 || recorder.requests[0].TaskContext.Torrent.RelativePath == recorder.requests[1].TaskContext.Torrent.RelativePath {
		t.Fatalf("transfer requests = %#v", recorder.requests)
	}
	var updatedParent model.ClusterJob
	if err := database.First(&updatedParent, "id = ?", parent.ID).Error; err != nil {
		t.Fatal(err)
	}
	if updatedParent.ExpectedItems != 3 {
		t.Fatalf("parent expected_items = %d, want 3", updatedParent.ExpectedItems)
	}
	var updatedBinding model.MoviePilotTorrentBinding
	if err := database.First(&updatedBinding, "id = ?", binding.ID).Error; err != nil {
		t.Fatal(err)
	}
	if updatedBinding.Status != model.MoviePilotTorrentStatusTransferring || updatedBinding.LastQBState != "uploading" || updatedBinding.LastQBProgress != 1 || updatedBinding.LastQBRatio != 1.25 || updatedBinding.LastQBSeedingSeconds != 7200 {
		t.Fatalf("binding qB observation = %#v", updatedBinding)
	}
}

func TestObserveTorrentCreatesMissingSubscriptionItemForMultiEpisodeTorrent(t *testing.T) {
	database := openTorrentTransferTestDB(t)
	subscription := model.Subscription{ID: 17, Name: "Show", TMDBName: "Show", SourceType: model.SubscriptionSourceMoviePilot, MediaType: "tv", TMDBID: 123, TargetRoot: "/media", Category: "剧集", Seasons: []int{1}, DeliveryTarget: model.SubscriptionStorageTarget{Provider: "yidong139", Folder: "library"}}
	item := model.SubscriptionItem{ID: 18, SubscriptionID: subscription.ID, SourceKey: "episode-1", SourceProvider: model.SubscriptionSourceMoviePilot, FileName: "Show.S01E01.mkv", Season: 1, Episode: 1, TargetPath: "/media/tv/剧集/Show {tmdb-123}/Season 1/Show.S01E01.mkv", Status: model.SubscriptionItemStatusTransferring}
	intent := model.MoviePilotDownloadIntent{ID: "intent-multi-episode", RequestID: "request-multi-episode", BridgeInstanceID: "bridge-1", SubscriptionID: subscription.ID, SubscriptionItemID: item.ID, MediaSource: "tmdb", MediaID: "123", ResourceRef: "resource://torrent", Status: model.MoviePilotIntentStatusBound}
	binding := model.MoviePilotTorrentBinding{ID: "binding-multi-episode", IntentID: intent.ID, BridgeInstanceID: "bridge-1", DownloaderAlias: "qb-main", WorkerNodeID: "worker-1", QBClientID: "qb-1", TorrentHash: strings.Repeat("a", 40), ContentPath: "/downloads/Show", ObserveJobID: "parent-multi-episode", Status: model.MoviePilotTorrentStatusBound}
	for _, value := range []any{&subscription, &item, &intent, &binding} {
		if err := database.Create(value).Error; err != nil {
			t.Fatal(err)
		}
	}
	task := moviePilotTorrentTaskContext(&subscription, &item, &binding, binding.ID, moviepilotbridge.MediaIdentity{MediaType: "tv", Season: 1, Episode: 1})
	task.ParentBatchID = "parent-multi-episode"
	rawTask, _ := json.Marshal(task)
	parent := model.ClusterJob{ID: "parent-multi-episode", Type: model.ClusterJobTypeTorrentObserve, Status: model.ClusterJobStatusPlanning, IdempotencyKey: "parent-multi-episode-key", ExpectedItems: 1}
	observe := model.ClusterJob{ID: "observe-multi-episode", ParentJobID: parent.ID, Type: model.ClusterJobTypeTorrentObserve, Status: model.ClusterJobStatusSucceeded, IdempotencyKey: "observe-multi-episode-key", TaskContextJSON: string(rawTask)}
	if err := database.Create(&parent).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&observe).Error; err != nil {
		t.Fatal(err)
	}
	recorder := &torrentDispatchRecorder{}
	service := New(database, "")
	service.SetTorrentJobDispatcher(recorder)
	result := map[string]any{"files": []map[string]any{
		{"name": "Show.S01E01.mkv", "size": int64(100)},
		{"name": "Show.S01E02.mkv", "size": int64(200)},
	}}
	if err := service.ObserveTorrent(context.Background(), observe.ID, result); err != nil {
		t.Fatal(err)
	}
	var deliveries []model.MoviePilotDeliveryFile
	if err := database.Order("relative_path ASC").Find(&deliveries).Error; err != nil {
		t.Fatal(err)
	}
	if len(deliveries) != 2 || !deliveries[0].Required || !deliveries[1].Required {
		t.Fatalf("deliveries = %#v", deliveries)
	}
	var episodeTwo model.SubscriptionItem
	if err := database.Where("subscription_id = ? AND season = ? AND episode = ?", subscription.ID, 1, 2).First(&episodeTwo).Error; err != nil {
		t.Fatalf("find created episode 2 item: %v", err)
	}
	if episodeTwo.SourceProvider != model.SubscriptionSourceMoviePilot || episodeTwo.TargetPath == "" || episodeTwo.Status != model.SubscriptionItemStatusTransferring {
		t.Fatalf("created episode 2 item = %#v", episodeTwo)
	}
	if len(recorder.requests) != 2 {
		t.Fatalf("transfer requests = %#v", recorder.requests)
	}
}

func TestReconcileTorrentTransfersDoesNotResubmitTerminalFailedDelivery(t *testing.T) {
	database := openTorrentTransferTestDB(t)
	delivery := model.MoviePilotDeliveryFile{
		ID: "delivery-terminal-failure", TorrentBindingID: "binding-terminal-failure",
		RelativePath: "Show.S01E01.mkv", Required: true,
		Status: model.MoviePilotDeliveryStatusFailed, LastErrorCode: "upload_rejected", LastError: "manual review required",
	}
	if err := database.Create(&delivery).Error; err != nil {
		t.Fatal(err)
	}
	recorder := &torrentDispatchRecorder{}
	service := New(database, "")
	service.SetTorrentJobDispatcher(recorder)

	if err := service.ReconcileTorrentTransfers(context.Background(), "", 100); err != nil {
		t.Fatal(err)
	}
	if err := database.First(&delivery, "id = ?", delivery.ID).Error; err != nil {
		t.Fatal(err)
	}
	if len(recorder.requests) != 0 || delivery.Status != model.MoviePilotDeliveryStatusFailed || delivery.LastErrorCode != "upload_rejected" {
		t.Fatalf("terminal failed delivery was resubmitted: requests=%#v delivery=%#v", recorder.requests, delivery)
	}
}

func TestTorrentStateChangedGatesRetentionOnRatioSeedTimeAndHNR(t *testing.T) {
	database := openTorrentTransferTestDB(t)
	intent := model.MoviePilotDownloadIntent{ID: "intent-retention", RequestID: "request-retention", BridgeInstanceID: "bridge-1", SubscriptionID: 7, Status: model.MoviePilotIntentStatusBound}
	binding := model.MoviePilotTorrentBinding{ID: "binding-retention", IntentID: intent.ID, BridgeInstanceID: "bridge-1", DownloaderAlias: "qb-main", WorkerNodeID: "worker-1", QBClientID: "qb-1", TorrentHash: strings.Repeat("c", 40), Status: model.MoviePilotTorrentStatusDownloadCompleted, RetentionStatus: model.MoviePilotRetentionStatusPending, RetentionPolicyJSON: `{"min_seed_seconds":3600,"min_ratio":1.5,"site_rule_id":"site-rule"}`}
	if err := database.Create(&intent).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&binding).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&model.MoviePilotDeliveryFile{ID: "delivery-retention", TorrentBindingID: binding.ID, RelativePath: "Show.S01E01.mkv", Required: true, Status: model.MoviePilotDeliveryStatusMaterialized, ManifestID: "manifest-retention"}).Error; err != nil {
		t.Fatal(err)
	}
	passed := true
	service := New(database, "")
	event := moviepilotbridge.BridgeEvent{EventID: "event-retention", RequestID: intent.RequestID, Type: moviepilotbridge.EventTorrentStateChanged, OccurredAt: time.Now().UTC(), State: &moviepilotbridge.TorrentStatePayload{State: "stalledUP", Progress: 1, Ratio: 2, SeedingSeconds: 3600, HNRPassed: &passed}}
	if err := service.HandleMoviePilotEvent(context.Background(), "bridge-1", event); err != nil {
		t.Fatal(err)
	}
	var updated model.MoviePilotTorrentBinding
	if err := database.First(&updated, "id = ?", binding.ID).Error; err != nil {
		t.Fatal(err)
	}
	if updated.Status != model.MoviePilotTorrentStatusSeeding || updated.RetentionStatus != model.MoviePilotRetentionStatusEligible || updated.RetentionEligibleAt == nil || !updated.LastQBHNRPassed {
		t.Fatalf("binding after eligible state = %#v", updated)
	}
}

func TestTorrentStateChangedDoesNotEnableRetentionBeforeDeliveryMaterialized(t *testing.T) {
	database := openTorrentTransferTestDB(t)
	intent := model.MoviePilotDownloadIntent{ID: "intent-retention-unmaterialized", RequestID: "request-retention-unmaterialized", BridgeInstanceID: "bridge-1", SubscriptionID: 7, Status: model.MoviePilotIntentStatusBound}
	binding := model.MoviePilotTorrentBinding{ID: "binding-retention-unmaterialized", IntentID: intent.ID, BridgeInstanceID: "bridge-1", DownloaderAlias: "qb-main", WorkerNodeID: "worker-1", QBClientID: "qb-1", TorrentHash: strings.Repeat("f", 40), Status: model.MoviePilotTorrentStatusDownloadCompleted, RetentionStatus: model.MoviePilotRetentionStatusPending, RetentionPolicyJSON: `{"min_seed_seconds":3600,"min_ratio":1.5,"site_rule_id":"site-rule"}`}
	if err := database.Create(&intent).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&binding).Error; err != nil {
		t.Fatal(err)
	}
	passed := true
	service := New(database, "")
	event := moviepilotbridge.BridgeEvent{EventID: "event-retention-unmaterialized", RequestID: intent.RequestID, Type: moviepilotbridge.EventTorrentStateChanged, OccurredAt: time.Now().UTC(), State: &moviepilotbridge.TorrentStatePayload{State: "seeding", Progress: 1, Ratio: 2, SeedingSeconds: 3600, HNRPassed: &passed}}
	if err := service.HandleMoviePilotEvent(context.Background(), "bridge-1", event); err != nil {
		t.Fatal(err)
	}
	var updated model.MoviePilotTorrentBinding
	if err := database.First(&updated, "id = ?", binding.ID).Error; err != nil {
		t.Fatal(err)
	}
	if updated.RetentionStatus == model.MoviePilotRetentionStatusEligible || updated.RetentionEligibleAt != nil {
		t.Fatalf("retention became eligible before delivery materialization: %#v", updated)
	}
}

func TestDelayedTorrentStateDoesNotRegressTerminalLifecycle(t *testing.T) {
	database := openTorrentTransferTestDB(t)
	intent := model.MoviePilotDownloadIntent{
		ID: "intent-delayed-state", RequestID: "request-delayed-state", BridgeInstanceID: "bridge-1",
		Status: model.MoviePilotIntentStatusCompleted,
	}
	binding := model.MoviePilotTorrentBinding{
		ID: "binding-delayed-state", IntentID: intent.ID, BridgeInstanceID: "bridge-1",
		DownloaderAlias: "qb-main", WorkerNodeID: "worker-1", QBClientID: "qb-1",
		TorrentHash: strings.Repeat("8", 40), Status: model.MoviePilotTorrentStatusDeleted,
		RetentionStatus: model.MoviePilotRetentionStatusDeleted,
	}
	for _, value := range []any{&intent, &binding} {
		if err := database.Create(value).Error; err != nil {
			t.Fatal(err)
		}
	}
	event := moviepilotbridge.BridgeEvent{
		EventID: "event-delayed-state", RequestID: intent.RequestID,
		Type: moviepilotbridge.EventTorrentStateChanged, OccurredAt: time.Now().UTC(),
		State: &moviepilotbridge.TorrentStatePayload{State: "downloading", Progress: .5, Ratio: .25},
	}
	if err := New(database, "").HandleMoviePilotEvent(context.Background(), "bridge-1", event); err != nil {
		t.Fatal(err)
	}
	if err := database.First(&binding, "id = ?", binding.ID).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.First(&intent, "id = ?", intent.ID).Error; err != nil {
		t.Fatal(err)
	}
	if binding.Status != model.MoviePilotTorrentStatusDeleted || binding.RetentionStatus != model.MoviePilotRetentionStatusDeleted {
		t.Fatalf("terminal binding regressed: %#v", binding)
	}
	if intent.Status != model.MoviePilotIntentStatusCompleted {
		t.Fatalf("terminal intent regressed: %#v", intent)
	}
	if binding.LastQBProgress != .5 || binding.LastQBState != "downloading" {
		t.Fatalf("delayed telemetry was not retained: %#v", binding)
	}
}

func TestDelayedAcceptedAndFailedEventsDoNotRegressCompletedLifecycle(t *testing.T) {
	database := openTorrentTransferTestDB(t)
	intent := model.MoviePilotDownloadIntent{
		ID: "intent-delayed-terminal-events", RequestID: "request-delayed-terminal-events",
		BridgeInstanceID: "bridge-1", Status: model.MoviePilotIntentStatusCompleted,
		LastErrorCode: "", LastError: "",
	}
	binding := model.MoviePilotTorrentBinding{
		ID: "binding-delayed-terminal-events", IntentID: intent.ID, BridgeInstanceID: "bridge-1",
		DownloaderAlias: "qb-main", WorkerNodeID: "worker-1", QBClientID: "qb-1",
		TorrentHash: strings.Repeat("9", 40), Status: model.MoviePilotTorrentStatusDeleted,
		RetentionStatus: model.MoviePilotRetentionStatusDeleted,
	}
	for _, value := range []any{&intent, &binding} {
		if err := database.Create(value).Error; err != nil {
			t.Fatal(err)
		}
	}
	service := New(database, "")
	accepted := moviepilotbridge.BridgeEvent{
		EventID: "event-delayed-accepted", RequestID: intent.RequestID,
		Type: moviepilotbridge.EventIntentAccepted, OccurredAt: time.Now().UTC(),
	}
	if err := service.HandleMoviePilotEvent(context.Background(), "bridge-1", accepted); err != nil {
		t.Fatal(err)
	}
	failed := moviepilotbridge.BridgeEvent{
		EventID: "event-delayed-failed", RequestID: intent.RequestID,
		Type: moviepilotbridge.EventTorrentFailed, OccurredAt: time.Now().UTC(),
		Failure: &moviepilotbridge.TorrentFailure{Code: "late_failure", Message: "late failure"},
	}
	if err := service.HandleMoviePilotEvent(context.Background(), "bridge-1", failed); err != nil {
		t.Fatal(err)
	}
	if err := database.First(&intent, "id = ?", intent.ID).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.First(&binding, "id = ?", binding.ID).Error; err != nil {
		t.Fatal(err)
	}
	if intent.Status != model.MoviePilotIntentStatusCompleted || intent.LastErrorCode != "" || intent.LastError != "" {
		t.Fatalf("completed intent regressed: %#v", intent)
	}
	if binding.Status != model.MoviePilotTorrentStatusDeleted || binding.RetentionStatus != model.MoviePilotRetentionStatusDeleted || binding.LastErrorCode != "" {
		t.Fatalf("deleted binding regressed: %#v", binding)
	}
}

func TestDuplicateTorrentBoundDoesNotRegressAdvancedLifecycle(t *testing.T) {
	database := openTorrentTransferTestDB(t)
	subscription := model.Subscription{ID: 27, Name: "Show", SourceType: model.SubscriptionSourceMoviePilot, MediaType: "tv", TMDBID: 123, TargetRoot: "/media", DeliveryTarget: model.SubscriptionStorageTarget{Provider: "yidong139", Folder: "library"}}
	item := model.SubscriptionItem{ID: 28, SubscriptionID: subscription.ID, SourceProvider: model.SubscriptionSourceMoviePilot, FileName: "Show.S01E01.mkv", Season: 1, Episode: 1, TargetPath: "/media/Show.S01E01.mkv"}
	intent := model.MoviePilotDownloadIntent{ID: "intent-duplicate-bound", RequestID: "request-duplicate-bound", BridgeInstanceID: "bridge-1", SubscriptionID: subscription.ID, SubscriptionItemID: item.ID, MediaSource: "tmdb", MediaID: "123", Status: model.MoviePilotIntentStatusCompleted}
	binding := model.MoviePilotTorrentBinding{ID: "binding-duplicate-bound", IntentID: intent.ID, BridgeInstanceID: "bridge-1", DownloaderAlias: "qb-main", WorkerNodeID: "worker-1", QBClientID: "qb-1", TorrentHash: strings.Repeat("a", 40), ContentPath: "/downloads/Show", Status: model.MoviePilotTorrentStatusSeeding, RetentionStatus: model.MoviePilotRetentionStatusHeld}
	routes := protocol.NodeCapabilities{MoviePilotRoutes: []protocol.MoviePilotRouteInventory{{BridgeInstanceID: "bridge-1", Downloader: "qb-main", QBClientID: "qb-1", QBHealth: "healthy", UploadConcurrency: 2}}}
	rawRoutes, _ := json.Marshal(routes)
	for _, value := range []any{
		&subscription, &item, &intent, &binding,
		&model.ClusterNode{ID: "worker-1", Status: model.ClusterNodeStatusOnline, LastSessionID: "session-duplicate-bound"},
		&model.ClusterNodeSession{ID: "session-duplicate-bound", NodeID: "worker-1", Status: model.ClusterSessionStatusConnected},
		&model.ClusterNodeInventory{NodeID: "worker-1", Revision: 1, CapabilitiesJSON: string(rawRoutes)},
	} {
		if err := database.Create(value).Error; err != nil {
			t.Fatal(err)
		}
	}
	recorder := &torrentDispatchRecorder{}
	service := New(database, "")
	service.SetTorrentJobDispatcher(recorder)
	event := moviepilotbridge.BridgeEvent{EventID: "event-duplicate-bound", RequestID: intent.RequestID, Type: moviepilotbridge.EventTorrentBound, OccurredAt: time.Now().UTC(), Torrent: &moviepilotbridge.TorrentBoundPayload{Downloader: "qb-main", TorrentHash: binding.TorrentHash, ContentPath: binding.ContentPath}}
	if err := service.HandleMoviePilotEvent(context.Background(), "bridge-1", event); err != nil {
		t.Fatal(err)
	}
	if err := database.First(&intent, "id = ?", intent.ID).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.First(&binding, "id = ?", binding.ID).Error; err != nil {
		t.Fatal(err)
	}
	if intent.Status != model.MoviePilotIntentStatusCompleted || binding.Status != model.MoviePilotTorrentStatusSeeding || binding.RetentionStatus != model.MoviePilotRetentionStatusHeld {
		t.Fatalf("duplicate bound regressed lifecycle: intent=%#v binding=%#v", intent, binding)
	}
}

func TestRefreshRetentionRequiresKnownHNRResultForConfiguredSiteRule(t *testing.T) {
	database := openTorrentTransferTestDB(t)
	binding := model.MoviePilotTorrentBinding{
		ID: "binding-hnr-unknown", IntentID: "intent-hnr-unknown", BridgeInstanceID: "bridge-1",
		DownloaderAlias: "qb-main", WorkerNodeID: "worker-1", QBClientID: "qb-1", TorrentHash: strings.Repeat("7", 40),
		Status: model.MoviePilotTorrentStatusSeeding, RetentionStatus: model.MoviePilotRetentionStatusPending,
		RetentionPolicyJSON: `{"min_seed_seconds":3600,"min_ratio":1.5,"site_rule_id":"site-rule"}`,
		LastQBRatio:         2, LastQBSeedingSeconds: 7200,
	}
	if err := database.Create(&binding).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&model.MoviePilotDeliveryFile{ID: "delivery-hnr-unknown", TorrentBindingID: binding.ID, RelativePath: "Show.S01E01.mkv", Required: true, Status: model.MoviePilotDeliveryStatusMaterialized, ManifestID: "manifest-hnr-unknown"}).Error; err != nil {
		t.Fatal(err)
	}
	if err := New(database, "").refreshRetentionEligibility(context.Background(), 10); err != nil {
		t.Fatal(err)
	}
	var updated model.MoviePilotTorrentBinding
	if err := database.First(&updated, "id = ?", binding.ID).Error; err != nil {
		t.Fatal(err)
	}
	if updated.RetentionStatus != model.MoviePilotRetentionStatusManualReview || updated.RetentionEligibleAt != nil {
		t.Fatalf("unknown H&R binding = %#v", updated)
	}
}

func TestReconcileTorrentRetentionDispatchesPeriodicQBInspection(t *testing.T) {
	database := openTorrentTransferTestDB(t)
	binding := model.MoviePilotTorrentBinding{
		ID: "binding-inspect", IntentID: "intent-inspect", BridgeInstanceID: "bridge-1",
		DownloaderAlias: "qb-main", WorkerNodeID: "worker-1", QBClientID: "qb-1", TorrentHash: strings.Repeat("6", 40),
		ObserveJobID: "observe-inspect", Status: model.MoviePilotTorrentStatusFilesDiscovered,
		RetentionStatus: model.MoviePilotRetentionStatusPending, RetentionPolicyJSON: `{"min_seed_seconds":3600}`,
	}
	task := protocol.TaskContext{Torrent: &protocol.TorrentTaskContext{
		BindingID: binding.ID, BridgeInstanceID: binding.BridgeInstanceID, Downloader: binding.DownloaderAlias,
		QBClientID: binding.QBClientID, WorkerNodeID: binding.WorkerNodeID, TorrentHash: binding.TorrentHash,
	}}
	rawTask, _ := json.Marshal(task)
	parent := model.ClusterJob{ID: binding.ObserveJobID, Type: model.ClusterJobTypeTorrentObserve, IdempotencyKey: "observe-inspect", TaskContextJSON: string(rawTask)}
	for _, value := range []any{&binding, &parent} {
		if err := database.Create(value).Error; err != nil {
			t.Fatal(err)
		}
	}
	recorder := &torrentDispatchRecorder{}
	service := New(database, "")
	service.SetTorrentJobDispatcher(recorder)
	if err := service.ReconcileTorrentRetention(context.Background(), 10); err != nil {
		t.Fatal(err)
	}
	if len(recorder.requests) != 1 || recorder.requests[0].JobType != model.ClusterJobTypeTorrentRetention || recorder.requests[0].TaskContext.Torrent.Action != "inspect" {
		t.Fatalf("retention requests = %#v", recorder.requests)
	}
}

func TestCompleteTorrentRetentionInspectionPersistsAuthoritativeQBState(t *testing.T) {
	database := openTorrentTransferTestDB(t)
	binding := model.MoviePilotTorrentBinding{
		ID: "binding-inspect-result", IntentID: "intent-inspect-result", BridgeInstanceID: "bridge-1",
		DownloaderAlias: "qb-main", WorkerNodeID: "worker-1", QBClientID: "qb-1", TorrentHash: strings.Repeat("5", 40),
		Status: model.MoviePilotTorrentStatusFilesDiscovered, RetentionStatus: model.MoviePilotRetentionStatusPending,
		RetentionPolicyJSON: `{"min_seed_seconds":7200,"min_ratio":2.0}`,
	}
	task := protocol.TaskContext{Torrent: &protocol.TorrentTaskContext{BindingID: binding.ID, TorrentHash: binding.TorrentHash, Action: "inspect"}}
	rawTask, _ := json.Marshal(task)
	job := model.ClusterJob{ID: "retention-inspect-result", Type: model.ClusterJobTypeTorrentRetention, IdempotencyKey: "retention-inspect-result", TaskContextJSON: string(rawTask)}
	for _, value := range []any{&binding, &job} {
		if err := database.Create(value).Error; err != nil {
			t.Fatal(err)
		}
	}
	result := protocol.JobResult{Status: "succeeded", Result: map[string]any{
		"action": "inspect", "qb_state": "stalledUP", "progress": 1.0, "ratio": 1.75, "seeding_seconds": int64(6400),
	}}
	if err := New(database, "").completeTorrentRetention(context.Background(), job.ID, result); err != nil {
		t.Fatal(err)
	}
	var updated model.MoviePilotTorrentBinding
	if err := database.First(&updated, "id = ?", binding.ID).Error; err != nil {
		t.Fatal(err)
	}
	if updated.Status != model.MoviePilotTorrentStatusSeeding || updated.RetentionStatus != model.MoviePilotRetentionStatusPending || updated.LastQBState != "stalledUP" || updated.LastQBRatio != 1.75 || updated.LastQBSeedingSeconds != 6400 {
		t.Fatalf("binding after inspection = %#v", updated)
	}
}

func TestLateRetentionResultsDoNotRegressDeletedBinding(t *testing.T) {
	database := openTorrentTransferTestDB(t)
	binding := model.MoviePilotTorrentBinding{
		ID: "binding-late-retention", IntentID: "intent-late-retention", BridgeInstanceID: "bridge-1",
		DownloaderAlias: "qb-main", WorkerNodeID: "worker-1", QBClientID: "qb-1",
		TorrentHash: strings.Repeat("3", 40), Status: model.MoviePilotTorrentStatusDeleted,
		RetentionStatus: model.MoviePilotRetentionStatusDeleted,
	}
	inspectTask := protocol.TaskContext{Torrent: &protocol.TorrentTaskContext{BindingID: binding.ID, TorrentHash: binding.TorrentHash, Action: "inspect"}}
	deleteTask := protocol.TaskContext{Torrent: &protocol.TorrentTaskContext{BindingID: binding.ID, TorrentHash: binding.TorrentHash, Action: "delete"}}
	inspectRaw, _ := json.Marshal(inspectTask)
	deleteRaw, _ := json.Marshal(deleteTask)
	for _, value := range []any{
		&binding,
		&model.ClusterJob{ID: "late-retention-inspect", Type: model.ClusterJobTypeTorrentRetention, IdempotencyKey: "late-retention-inspect", TaskContextJSON: string(inspectRaw)},
		&model.ClusterJob{ID: "late-retention-delete", Type: model.ClusterJobTypeTorrentRetention, IdempotencyKey: "late-retention-delete", TaskContextJSON: string(deleteRaw)},
	} {
		if err := database.Create(value).Error; err != nil {
			t.Fatal(err)
		}
	}
	service := New(database, "")
	inspectResult := protocol.JobResult{Status: "succeeded", Result: map[string]any{"qb_state": "downloading", "progress": .5}}
	if err := service.completeTorrentRetention(context.Background(), "late-retention-inspect", inspectResult); err != nil {
		t.Fatal(err)
	}
	deleteFailure := protocol.JobResult{Status: "failed", ErrorCode: "late_delete_failure", Error: "late delete failure"}
	if err := service.completeTorrentRetention(context.Background(), "late-retention-delete", deleteFailure); err != nil {
		t.Fatal(err)
	}
	if err := database.First(&binding, "id = ?", binding.ID).Error; err != nil {
		t.Fatal(err)
	}
	if binding.Status != model.MoviePilotTorrentStatusDeleted || binding.RetentionStatus != model.MoviePilotRetentionStatusDeleted || binding.LastErrorCode != "" {
		t.Fatalf("late retention result regressed deleted binding: %#v", binding)
	}
}

func TestReconcileTorrentRetentionKeysDeletionToEligibilityDecision(t *testing.T) {
	database := openTorrentTransferTestDB(t)
	eligibleAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
	binding := model.MoviePilotTorrentBinding{
		ID: "binding-delete-key", IntentID: "intent-delete-key", BridgeInstanceID: "bridge-1",
		DownloaderAlias: "qb-main", WorkerNodeID: "worker-1", QBClientID: "qb-1", TorrentHash: strings.Repeat("4", 40),
		ObserveJobID: "observe-delete-key", Status: model.MoviePilotTorrentStatusSeeding,
		RetentionStatus: model.MoviePilotRetentionStatusEligible, RetentionEligibleAt: &eligibleAt,
		RetentionPolicyJSON: `{"min_seed_seconds":3600}`,
	}
	task := protocol.TaskContext{Torrent: &protocol.TorrentTaskContext{
		BindingID: binding.ID, BridgeInstanceID: binding.BridgeInstanceID, Downloader: binding.DownloaderAlias,
		QBClientID: binding.QBClientID, WorkerNodeID: binding.WorkerNodeID, TorrentHash: binding.TorrentHash,
	}}
	rawTask, _ := json.Marshal(task)
	parent := model.ClusterJob{ID: binding.ObserveJobID, Type: model.ClusterJobTypeTorrentObserve, IdempotencyKey: "observe-delete-key", TaskContextJSON: string(rawTask)}
	delivery := model.MoviePilotDeliveryFile{
		ID: "delivery-delete-key", TorrentBindingID: binding.ID, RelativePath: "Show.S01E01.mkv",
		Required: true, Status: model.MoviePilotDeliveryStatusMaterialized, ManifestID: "manifest-delete-key",
	}
	for _, value := range []any{&binding, &parent, &delivery} {
		if err := database.Create(value).Error; err != nil {
			t.Fatal(err)
		}
	}
	recorder := &torrentDispatchRecorder{}
	service := New(database, "")
	service.SetTorrentJobDispatcher(recorder)
	if err := service.ReconcileTorrentRetention(context.Background(), 10); err != nil {
		t.Fatal(err)
	}
	if len(recorder.requests) != 1 {
		t.Fatalf("retention requests = %#v", recorder.requests)
	}
	wantKey := fmt.Sprintf("%sretention:%s:%d", moviePilotTorrentDeliveryPrefix, binding.ID, eligibleAt.UnixNano())
	if recorder.requests[0].IdempotencyKey != wantKey {
		t.Fatalf("idempotency key = %q, want %q", recorder.requests[0].IdempotencyKey, wantKey)
	}
}
