package coordinator

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func TestSafeRelativeMediaRoot(t *testing.T) {
	got, err := safeRelativeMediaRoot("/TV/Example/Season 01/episode.mkv")
	if err != nil {
		t.Fatal(err)
	}
	if got != "TV/Example/Season 01" {
		t.Fatalf("root = %q", got)
	}
	if _, err := safeRelativeMediaRoot("episode.mkv"); err == nil {
		t.Fatal("rootless path should be rejected")
	}
}

func TestPrepareArchiveRecordUpdatesExistingPathWithoutCreatingConflict(t *testing.T) {
	database := openMaterializerTestDB(t, "archive_replace", &model.ETFArchiveRecord{})
	existing := model.ETFArchiveRecord{
		StorageID:        1,
		StorageMountPath: "/etf",
		SourceName:       "episode-old.mkv",
		ArchiveETFPath:   "/etf/TV/episode.etf",
		SourceSize:       100,
		SourceSHA256:     strings.Repeat("A", 64),
		TMDBName:         "Preserved metadata",
		Status:           model.ETFArchiveStatusArchived,
	}
	if err := database.Create(&existing).Error; err != nil {
		t.Fatal(err)
	}
	candidate := &model.ETFArchiveRecord{
		StorageID:        1,
		StorageMountPath: " /etf ",
		SourceName:       "episode-new.mkv",
		SourcePath:       "/TV/episode-new.mkv",
		ArchiveETFPath:   " /etf/TV/episode.etf ",
		SourceSize:       200,
		SourceSHA256:     strings.Repeat("b", 64),
		TMDBID:           123,
		Status:           model.ETFArchiveStatusArchived,
	}

	service := New(database, "token")
	prepared, writeETF, err := service.prepareArchiveRecord(context.Background(), candidate)
	if err != nil {
		t.Fatal(err)
	}
	if !writeETF {
		t.Fatal("changed hash should rewrite the ETF")
	}
	if prepared.ID != existing.ID {
		t.Fatalf("record ID = %d, want existing ID %d", prepared.ID, existing.ID)
	}
	if prepared.TMDBName != existing.TMDBName {
		t.Fatalf("preserved metadata = %q, want %q", prepared.TMDBName, existing.TMDBName)
	}
	if err := service.persistArchiveRecord(context.Background(), prepared); err != nil {
		t.Fatal(err)
	}

	var records []model.ETFArchiveRecord
	if err := database.Find(&records).Error; err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("archive record count = %d, want 1", len(records))
	}
	if records[0].SourceSHA256 != strings.Repeat("B", 64) || records[0].SourceSize != 200 || records[0].SourceName != candidate.SourceName {
		t.Fatalf("updated archive record = %#v", records[0])
	}

	sameHash := *candidate
	sameHash.SourceName = "should-not-replace-idempotent-record.mkv"
	prepared, writeETF, err = service.prepareArchiveRecord(context.Background(), &sameHash)
	if err != nil {
		t.Fatal(err)
	}
	if writeETF {
		t.Fatal("same path and hash should be idempotent")
	}
	if prepared.SourceName != candidate.SourceName {
		t.Fatalf("idempotent record source name = %q, want %q", prepared.SourceName, candidate.SourceName)
	}
}

func TestCompleteManifestMaterializationMarksJobSucceeded(t *testing.T) {
	database := openMaterializerTestDB(t, "job_complete",
		&model.ClusterUploadManifest{},
		&model.ClusterJobStage{},
		&model.ClusterJob{},
		&model.SubscriptionItem{},
	)
	manifest := model.ClusterUploadManifest{ID: "manifest-1", JobID: "job-1", MediaItemID: "media-1", PayloadHash: "payload-1", Status: model.ClusterUploadManifestStatusAccepted, LastError: "old error"}
	stage := model.ClusterJobStage{ID: "stage-1", JobID: "job-1", AttemptID: "attempt-1", Name: model.ClusterStageETFMaterializing, Status: model.ClusterStageStatusRunning}
	item := model.SubscriptionItem{ID: 9, SubscriptionID: 7, SourceKey: "source-1", Status: model.SubscriptionItemStatusTransferring, ClusterJobID: "job-1"}
	job := model.ClusterJob{ID: "job-1", IdempotencyKey: "job-1", Status: model.ClusterJobStatusRunning, NotificationStatus: model.ClusterNotificationStatusUnknown, SubscriptionID: 7, SubscriptionItemID: item.ID}
	for _, value := range []any{&manifest, &stage, &item, &job} {
		if err := database.Create(value).Error; err != nil {
			t.Fatal(err)
		}
	}

	finishedAt := time.Date(2026, 7, 12, 12, 30, 0, 0, time.UTC)
	service := New(database, "token")
	if err := service.completeManifestMaterialization(context.Background(), manifest.ID, job.ID, model.ClusterNotificationStatusPending, finishedAt); err != nil {
		t.Fatal(err)
	}
	if err := database.First(&manifest, "id = ?", manifest.ID).Error; err != nil {
		t.Fatal(err)
	}
	if manifest.Status != model.ClusterUploadManifestStatusConsumed || manifest.ConsumedAt == nil || !manifest.ConsumedAt.Equal(finishedAt) || manifest.LastError != "" {
		t.Fatalf("completed manifest = %#v", manifest)
	}
	if err := database.First(&stage, "id = ?", stage.ID).Error; err != nil {
		t.Fatal(err)
	}
	if stage.Status != model.ClusterStageStatusSucceeded || stage.FinishedAt == nil || !stage.FinishedAt.Equal(finishedAt) {
		t.Fatalf("completed stage = %#v", stage)
	}
	if err := database.First(&job, "id = ?", job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if job.Status != model.ClusterJobStatusSucceeded || job.FinishedAt == nil || !job.FinishedAt.Equal(finishedAt) || job.NotificationStatus != model.ClusterNotificationStatusPending {
		t.Fatalf("completed job = %#v", job)
	}
	if err := database.First(&item, item.ID).Error; err != nil {
		t.Fatal(err)
	}
	if item.Status != model.SubscriptionItemStatusTransferred {
		t.Fatalf("subscription item status = %q", item.Status)
	}
}

func TestClusterTargetNotificationStatus(t *testing.T) {
	if got := clusterTargetNotificationStatus("  "); got != model.ClusterNotificationStatusNotRequired {
		t.Fatalf("empty target status = %q, want not_required", got)
	}
	if got := clusterTargetNotificationStatus("https://target.example/api/v1"); got != model.ClusterNotificationStatusPending {
		t.Fatalf("configured target status = %q, want pending", got)
	}
}

func openMaterializerTestDB(t *testing.T, name string, models ...any) *gorm.DB {
	t.Helper()
	database, err := gorm.Open(sqlite.Open("file:"+name+"?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.AutoMigrate(models...); err != nil {
		t.Fatal(err)
	}
	return database
}
