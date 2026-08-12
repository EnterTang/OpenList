package repair

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	appdb "github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

func TestRepairSubscriptionStatusesDryRunAndApply(t *testing.T) {
	database, err := gorm.Open(sqlite.Open(fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name())), &gorm.Config{
		NamingStrategy: schema.NamingStrategy{TablePrefix: "x_"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.AutoMigrate(
		&model.Subscription{}, &model.SubscriptionItem{}, &model.SubscriptionEpisodeSource{},
		&model.ClusterJob{}, &model.ClusterJobAttempt{}, &model.ClusterJobStage{}, &model.ClusterUploadManifest{},
		&model.ETFArchiveRecord{}, &model.ETFMediaRoot{}, &model.ETFMediaRootBatch{}, &model.ETFSubscriptionJob{},
	); err != nil {
		t.Fatal(err)
	}
	appdb.UseConnection(database)
	t.Cleanup(func() { appdb.UseConnection(nil) })

	subscription := &model.Subscription{
		Name: "少女怪兽焦糖味", TMDBID: 308874, MediaType: "tv", SourceType: model.SubscriptionSourceTelegram,
	}
	if err := database.Create(subscription).Error; err != nil {
		t.Fatal(err)
	}
	succeededItem := &model.SubscriptionItem{
		SubscriptionID: subscription.ID, SourceKey: "successful-source", SourceProvider: "pan123",
		SourceURL: "https://example.invalid/success", FileName: "S01E02.mkv", FileHash: "successful-hash",
		Season: 1, Episode: 2, Status: model.SubscriptionItemStatusTransferred, ClusterJobID: "successful-job",
	}
	failedItem := &model.SubscriptionItem{
		SubscriptionID: subscription.ID, SourceKey: "failed-source", SourceProvider: "pan123",
		SourceURL: "https://example.invalid/failed", FileName: "S01E02-later.mkv", FileHash: "failed-hash",
		Season: 1, Episode: 2, Status: model.SubscriptionItemStatusFailed, ClusterJobID: "failed-job", LastError: "unexpected EOF",
	}
	if err := database.Create(succeededItem).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(failedItem).Error; err != nil {
		t.Fatal(err)
	}
	finishedAt := time.Now().UTC()
	for _, job := range []*model.ClusterJob{
		{ID: "successful-job", IdempotencyKey: "successful-job", Type: model.ClusterJobTypeMediaTransfer, Status: model.ClusterJobStatusSucceeded, NotificationStatus: model.ClusterNotificationStatusSucceeded, CurrentAttemptID: "successful-attempt", SubscriptionItemID: succeededItem.ID, FinishedAt: &finishedAt},
		{ID: "failed-job", IdempotencyKey: "failed-job", Type: model.ClusterJobTypeMediaTransfer, Status: model.ClusterJobStatusFailed, NotificationStatus: model.ClusterNotificationStatusPending, CurrentAttemptID: "failed-attempt", SubscriptionItemID: failedItem.ID, LastError: failedItem.LastError, FinishedAt: &finishedAt},
	} {
		if err := database.Create(job).Error; err != nil {
			t.Fatal(err)
		}
	}
	stage := &model.ClusterJobStage{
		ID: "failed-stage", JobID: "failed-job", AttemptID: "failed-attempt",
		Name: model.ClusterStageUploadingMobile, Status: model.ClusterStageStatusPermitted,
	}
	if err := database.Create(stage).Error; err != nil {
		t.Fatal(err)
	}
	materializedJob := &model.ClusterJob{ID: "materialized-running", IdempotencyKey: "materialized-running", Status: model.ClusterJobStatusRunning}
	if err := database.Create(materializedJob).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&model.ClusterUploadManifest{ID: "materialized-running-manifest", JobID: materializedJob.ID, MediaItemID: "materialized-running-media", Status: model.ClusterUploadManifestStatusConsumed}).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&model.ClusterUploadManifest{
		ID: "successful-manifest", JobID: "successful-job", SubscriptionID: subscription.ID,
		SubscriptionItemID: succeededItem.ID, Season: 1, Episode: 2, Status: model.ClusterUploadManifestStatusConsumed,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&model.ETFArchiveRecord{
		StorageID: 1, StorageMountPath: "/139", SourceName: "S01E02.mkv", ArchiveETFPath: "/139/S01E02.mkv.etf",
		TMDBMatched: true, TMDBID: subscription.TMDBID, MediaType: subscription.MediaType, Season: 1, Episode: 2,
		SourceSize: 1024, SourceSHA256: strings.Repeat("A", 64), Status: model.ETFArchiveStatusArchived,
	}).Error; err != nil {
		t.Fatal(err)
	}
	snapshot := &model.SubscriptionEpisodeSource{
		SubscriptionID: subscription.ID, Season: 1, Episode: 2, SourceItemID: failedItem.ID,
		SourceType: subscription.SourceType, SourceProvider: failedItem.SourceProvider, ShareURL: failedItem.SourceURL,
		FileName: failedItem.FileName, FileHash: failedItem.FileHash, Status: failedItem.Status, ClusterJobID: failedItem.ClusterJobID,
	}
	if err := database.Create(snapshot).Error; err != nil {
		t.Fatal(err)
	}
	capabilityRoot := &model.ETFMediaRoot{RootKey: "capability-root", TargetBaseURL: "https://target.example/api/v1"}
	if err := database.Create(capabilityRoot).Error; err != nil {
		t.Fatal(err)
	}
	capabilityJob := &model.ETFSubscriptionJob{
		JobKey: "capability-job", MediaRootID: capabilityRoot.ID, Type: model.ETFSubscriptionJobTypeCreate,
		Status: model.ETFSubscriptionJobStatusPending, TargetBaseURL: capabilityRoot.TargetBaseURL,
	}
	if err := database.Create(capabilityJob).Error; err != nil {
		t.Fatal(err)
	}

	dryRun, err := RepairSubscriptionStatuses(context.Background(), database, SubscriptionStatusOptions{DeclareTargetIdempotent: true})
	if err != nil {
		t.Fatal(err)
	}
	if dryRun.Applied || dryRun.NotificationStatusesConverged != 1 || dryRun.StagesConverged != 1 || dryRun.EpisodeSnapshotsRestored != 1 || dryRun.IdempotencyCapabilitiesDeclared != 2 || dryRun.MaterializedJobsConverged != 1 {
		t.Fatalf("dry-run report = %#v", dryRun)
	}
	assertRepairState(t, database, snapshot.ID, failedItem.ID, model.ClusterNotificationStatusPending, model.ClusterStageStatusPermitted)
	assertTargetIdempotency(t, database, capabilityRoot.ID, capabilityJob.ID, false)
	assertClusterJobStatus(t, database, materializedJob.ID, model.ClusterJobStatusRunning)

	applied, err := RepairSubscriptionStatuses(context.Background(), database, SubscriptionStatusOptions{Apply: true, DeclareTargetIdempotent: true})
	if err != nil {
		t.Fatal(err)
	}
	if !applied.Applied || applied.EpisodeSnapshotsRestored != 1 {
		t.Fatalf("apply report = %#v", applied)
	}
	assertRepairState(t, database, snapshot.ID, succeededItem.ID, model.ClusterNotificationStatusNotStarted, model.ClusterStageStatusFailed)
	assertTargetIdempotency(t, database, capabilityRoot.ID, capabilityJob.ID, true)
	assertClusterJobStatus(t, database, materializedJob.ID, model.ClusterJobStatusSucceeded)
}

func assertClusterJobStatus(t *testing.T, database *gorm.DB, jobID, want string) {
	t.Helper()
	var job model.ClusterJob
	if err := database.First(&job, "id = ?", jobID).Error; err != nil {
		t.Fatal(err)
	}
	if job.Status != want {
		t.Fatalf("job %s status=%q, want %q", jobID, job.Status, want)
	}
}

func assertRepairState(t *testing.T, database *gorm.DB, snapshotID, wantItemID uint, wantNotification, wantStage string) {
	t.Helper()
	var snapshot model.SubscriptionEpisodeSource
	if err := database.First(&snapshot, snapshotID).Error; err != nil {
		t.Fatal(err)
	}
	if snapshot.SourceItemID != wantItemID {
		t.Fatalf("snapshot source item = %d, want %d", snapshot.SourceItemID, wantItemID)
	}
	var job model.ClusterJob
	if err := database.First(&job, "id = ?", "failed-job").Error; err != nil {
		t.Fatal(err)
	}
	if job.NotificationStatus != wantNotification {
		t.Fatalf("notification = %q, want %q", job.NotificationStatus, wantNotification)
	}
	var stage model.ClusterJobStage
	if err := database.First(&stage, "id = ?", "failed-stage").Error; err != nil {
		t.Fatal(err)
	}
	if stage.Status != wantStage {
		t.Fatalf("stage = %q, want %q", stage.Status, wantStage)
	}
}

func assertTargetIdempotency(t *testing.T, database *gorm.DB, rootID, jobID uint, want bool) {
	t.Helper()
	var root model.ETFMediaRoot
	if err := database.First(&root, rootID).Error; err != nil {
		t.Fatal(err)
	}
	var job model.ETFSubscriptionJob
	if err := database.First(&job, jobID).Error; err != nil {
		t.Fatal(err)
	}
	if root.TargetSupportsIdempotency != want || job.TargetSupportsIdempotency != want {
		t.Fatalf("idempotency root=%v job=%v, want %v", root.TargetSupportsIdempotency, job.TargetSupportsIdempotency, want)
	}
}
