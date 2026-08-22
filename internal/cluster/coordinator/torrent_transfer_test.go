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
		&model.ClusterNodeInventory{}, &model.ClusterJob{}, &model.ClusterJobAttempt{}, &model.ClusterJobStage{},
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
	result := map[string]any{"files": []map[string]any{
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
	passed := true
	service := New(database, "")
	event := moviepilotbridge.BridgeEvent{EventID: "event-retention", RequestID: intent.RequestID, Type: moviepilotbridge.EventTorrentStateChanged, OccurredAt: time.Now().UTC(), State: &moviepilotbridge.TorrentStatePayload{State: "seeding", Progress: 1, Ratio: 2, SeedingSeconds: 3600, HNRPassed: &passed}}
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
