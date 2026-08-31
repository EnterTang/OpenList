package db

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func TestListMoviePilotTaskStatusesIncludesUnboundAndDeliveryProgress(t *testing.T) {
	database, err := gorm.Open(sqlite.Open("file:moviepilot_task_status_test?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := database.AutoMigrate(
		&model.Subscription{}, &model.SubscriptionItem{}, &model.MoviePilotDownloadIntent{},
		&model.MoviePilotTorrentBinding{}, &model.MoviePilotDeliveryFile{}, &model.ClusterJob{},
		&model.ClusterJobStage{}, &model.ClusterNode{},
	); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	now := time.Now().UTC()
	subscription := model.Subscription{ID: 21, Name: "赌王之王", SourceType: model.SubscriptionSourceMoviePilot}
	item := model.SubscriptionItem{ID: 22, SubscriptionID: subscription.ID, SourceKey: "episode-1", FileName: "Episode 1.mkv", Status: model.SubscriptionItemStatusTransferring}
	unboundItem := model.SubscriptionItem{ID: 23, SubscriptionID: subscription.ID, SourceKey: "episode-2", FileName: "Episode 2.mkv", Status: model.SubscriptionItemStatusPending}
	if err := database.Create(&subscription).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&item).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&unboundItem).Error; err != nil {
		t.Fatal(err)
	}
	boundIntent := model.MoviePilotDownloadIntent{
		ID: "intent-bound", RequestID: "request-bound", BridgeInstanceID: "mp-main",
		SubscriptionID: subscription.ID, SubscriptionItemID: item.ID, Status: model.MoviePilotIntentStatusBound,
		UpdatedAt: now,
	}
	unboundIntent := model.MoviePilotDownloadIntent{
		ID: "intent-unbound", RequestID: "request-unbound", BridgeInstanceID: "mp-main",
		SubscriptionID: subscription.ID, SubscriptionItemID: unboundItem.ID, Status: model.MoviePilotIntentStatusAccepted,
		LastError: "", UpdatedAt: now.Add(-time.Minute),
	}
	if err := database.Create(&boundIntent).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&unboundIntent).Error; err != nil {
		t.Fatal(err)
	}
	binding := model.MoviePilotTorrentBinding{
		ID: "binding-1", IntentID: boundIntent.ID, BridgeInstanceID: "mp-main", WorkerNodeID: "worker-1",
		DownloaderAlias: "qb-main", QBClientID: "qb-main", TorrentHash: strings.Repeat("a", 40),
		Status: model.MoviePilotTorrentStatusTransferring, LastQBProgress: 1, UpdatedAt: now,
	}
	if err := database.Create(&binding).Error; err != nil {
		t.Fatal(err)
	}
	for _, delivery := range []model.MoviePilotDeliveryFile{
		{ID: "delivery-1", TorrentBindingID: binding.ID, RelativePath: "Episode 1.mkv", SourceSize: 100, Required: true, Status: model.MoviePilotDeliveryStatusMaterialized, UploadProgress: 1, UpdatedAt: now},
		{ID: "delivery-2", TorrentBindingID: binding.ID, RelativePath: "Episode 2.mkv", SourceSize: 100, Required: true, Status: model.MoviePilotDeliveryStatusUploading, UploadProgress: .5, UpdatedAt: now},
	} {
		if err := database.Create(&delivery).Error; err != nil {
			t.Fatal(err)
		}
	}
	job := model.ClusterJob{ID: "job-1", SubscriptionID: subscription.ID, SubscriptionItemID: item.ID, Status: model.ClusterJobStatusRunning, UpdatedAt: now}
	stage := model.ClusterJobStage{ID: "stage-1", JobID: job.ID, AttemptID: "attempt-1", Name: model.ClusterStageUploadingMobile, Status: model.ClusterStageStatusRunning, UpdatedAt: now}
	node := model.ClusterNode{ID: "worker-1", Status: model.ClusterNodeStatusOnline}
	if err := database.Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&stage).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&node).Error; err != nil {
		t.Fatal(err)
	}

	previous := db
	db = database
	t.Cleanup(func() { db = previous })
	statuses, err := ListMoviePilotTaskStatuses(context.Background(), subscription.ID, "mp-main", 20)
	if err != nil {
		t.Fatalf("list task statuses: %v", err)
	}
	if len(statuses) != 2 {
		t.Fatalf("got %d statuses, want 2: %#v", len(statuses), statuses)
	}
	if statuses[0].RequestID != boundIntent.RequestID || statuses[0].Phase != model.MoviePilotDeliveryStatusUploading {
		t.Fatalf("bound status = %#v", statuses[0])
	}
	if statuses[0].SubscriptionName != subscription.Name || statuses[0].WorkerStatus != model.ClusterNodeStatusOnline || statuses[0].UploadProgress != .75 {
		t.Fatalf("bound metadata/progress = %#v", statuses[0])
	}
	if statuses[1].RequestID != unboundIntent.RequestID || statuses[1].Phase != "waiting_binding" {
		t.Fatalf("unbound status = %#v", statuses[1])
	}
}

func TestMoviePilotTaskPhaseUsesSemanticLifecycleStages(t *testing.T) {
	cases := []struct {
		name       string
		status     model.MoviePilotTaskStatus
		deliveries []model.MoviePilotDeliveryFile
		hasBinding bool
		want       string
	}{
		{name: "qB downloading", status: model.MoviePilotTaskStatus{ClusterJobStage: model.ClusterStageQBObserving, ClusterJobStatus: model.ClusterJobStatusRunning}, hasBinding: true, want: model.MoviePilotTaskPhaseDownloading},
		{name: "qB completed", status: model.MoviePilotTaskStatus{ClusterJobStage: model.ClusterStageQBObserving, ClusterJobStageStatus: model.ClusterStageStatusSucceeded, ClusterJobStatus: model.ClusterJobStatusRunning, DownloadProgress: 1}, hasBinding: true, want: model.MoviePilotTaskPhaseDownloadComplete},
		{name: "staging", status: model.MoviePilotTaskStatus{ClusterJobStage: model.ClusterStageQBCopying, ClusterJobStatus: model.ClusterJobStatusRunning}, hasBinding: true, want: model.MoviePilotTaskPhaseStaging},
		{name: "uploading", status: model.MoviePilotTaskStatus{ClusterJobStage: model.ClusterStageUploadingMobile, ClusterJobStatus: model.ClusterJobStatusRunning}, hasBinding: true, want: model.MoviePilotTaskPhaseUploading},
		{name: "delivery staging", status: model.MoviePilotTaskStatus{}, deliveries: []model.MoviePilotDeliveryFile{{Status: model.MoviePilotDeliveryStatusStaging}}, hasBinding: true, want: model.MoviePilotTaskPhaseStaging},
		{name: "delivery uploading", status: model.MoviePilotTaskStatus{}, deliveries: []model.MoviePilotDeliveryFile{{Status: model.MoviePilotDeliveryStatusUploading}}, hasBinding: true, want: model.MoviePilotTaskPhaseUploading},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			if got := moviePilotTaskPhase(tt.status, tt.hasBinding, tt.deliveries); got != tt.want {
				t.Fatalf("phase = %q, want %q", got, tt.want)
			}
		})
	}
}
