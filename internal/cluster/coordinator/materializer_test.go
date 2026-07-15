package coordinator

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/etfauto"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func TestProcessPendingManifestsCompletesWithoutETFRootPath(t *testing.T) {
	originalConfig := conf.Conf
	conf.Conf = conf.DefaultConfig(t.TempDir())
	t.Cleanup(func() { conf.Conf = originalConfig })
	database := openMaterializerTestDB(t, "without_etf_root",
		&model.ClusterUploadManifest{},
		&model.ClusterJobStage{},
		&model.ClusterJob{},
		&model.SubscriptionItem{},
	)
	conf.Conf.Cluster.ETFRootPath = ""
	conf.Conf.Cluster.TargetBaseURL = ""

	manifest := model.ClusterUploadManifest{ID: "manifest-no-root", JobID: "job-no-root", MediaItemID: "media-no-root", PayloadHash: "payload-no-root", Status: model.ClusterUploadManifestStatusAccepted, ReceivedAt: time.Now().UTC()}
	stage := model.ClusterJobStage{ID: "stage-no-root", JobID: manifest.JobID, AttemptID: "attempt-no-root", Name: model.ClusterStageETFMaterializing, Status: model.ClusterStageStatusPending}
	item := model.SubscriptionItem{ID: 11, SubscriptionID: 8, SourceKey: "source-no-root", Status: model.SubscriptionItemStatusTransferring, ClusterJobID: manifest.JobID}
	job := model.ClusterJob{ID: manifest.JobID, IdempotencyKey: manifest.JobID, Status: model.ClusterJobStatusRunning, SubscriptionID: item.SubscriptionID, SubscriptionItemID: item.ID}
	for _, value := range []any{&manifest, &stage, &item, &job} {
		if err := database.Create(value).Error; err != nil {
			t.Fatal(err)
		}
	}

	processed, err := New(database, "token").ProcessPendingManifests(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if processed != 1 {
		t.Fatalf("processed = %d, want 1", processed)
	}
	if err := database.First(&manifest, "id = ?", manifest.ID).Error; err != nil {
		t.Fatal(err)
	}
	if manifest.Status != model.ClusterUploadManifestStatusConsumed || manifest.ConsumedAt == nil {
		t.Fatalf("manifest = %#v, want consumed", manifest)
	}
	if err := database.First(&job, "id = ?", job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if job.Status != model.ClusterJobStatusSucceeded || job.NotificationStatus != model.ClusterNotificationStatusNotRequired {
		t.Fatalf("job = %#v, want succeeded without notification", job)
	}
}

func TestSafeRelativeMediaRoot(t *testing.T) {
	for _, test := range []struct {
		path string
		want string
	}{
		{path: "/TV/Example/Season 1/episode.mkv", want: "TV/Example"},
		{path: "/TV/Example/Season 01/episode.mkv", want: "TV/Example"},
		{path: "/movie/Example/movie.mkv", want: "movie/Example"},
	} {
		got, err := safeRelativeMediaRoot(test.path)
		if err != nil {
			t.Fatal(err)
		}
		if got != test.want {
			t.Fatalf("root = %q, want %q", got, test.want)
		}
	}
	if _, err := safeRelativeMediaRoot("episode.mkv"); err == nil {
		t.Fatal("rootless path should be rejected")
	}
}

func TestSafeRelativeArchiveDirectoryPreservesSeasonDirectory(t *testing.T) {
	for _, test := range []struct {
		path string
		want string
	}{
		{path: "/TV/Example/Season 1/episode.mkv", want: "TV/Example/Season 1"},
		{path: "/TV/Example/Season 01/episode.mkv", want: "TV/Example/Season 01"},
		{path: "/movie/Example/movie.mkv", want: "movie/Example"},
	} {
		got, err := safeRelativeArchiveDirectory(test.path)
		if err != nil {
			t.Fatal(err)
		}
		if got != test.want {
			t.Fatalf("archive directory = %q, want %q", got, test.want)
		}
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

func TestMergeClusterMaterializationSettingsUsesDriverArchiveRootAtMount(t *testing.T) {
	root, notification := mergeClusterMaterializationSettings(
		"/139_60t",
		"/139_60t",
		etfauto.Config{},
		driver.ETFArchiveSettings{RelativeRoot: "ETF转存归档"},
	)
	if root != "/139_60t/ETF转存归档" {
		t.Fatalf("root = %q, want driver archive root", root)
	}
	if notification.Enabled || notification.TargetBaseURL != "" {
		t.Fatalf("notification = %#v, want disabled", notification)
	}
}

func TestMergeClusterMaterializationSettingsPreservesExplicitDeeperRoot(t *testing.T) {
	root, _ := mergeClusterMaterializationSettings(
		"/139_60t/custom-archive",
		"/139_60t",
		etfauto.Config{},
		driver.ETFArchiveSettings{RelativeRoot: "ETF转存归档"},
	)
	if root != "/139_60t/custom-archive" {
		t.Fatalf("root = %q, want explicit cluster root", root)
	}
}

func TestMergeClusterMaterializationSettingsFallsBackToDriverTarget(t *testing.T) {
	_, notification := mergeClusterMaterializationSettings(
		"/139_60t",
		"/139_60t",
		etfauto.Config{},
		driver.ETFArchiveSettings{
			AutoSubscriptionEnabled:   true,
			TargetBaseURL:             "https://target.example/api/v1/",
			TargetAPIToken:            " token ",
			TargetSupportsIdempotency: true,
			QuietWindowSeconds:        45,
			SharePeriodUnit:           2,
			ShareType:                 "etf",
		},
	)
	if !notification.Enabled || notification.TargetBaseURL != "https://target.example/api/v1" || notification.TargetAPIToken != "token" {
		t.Fatalf("notification = %#v, want driver target", notification)
	}
	if !notification.TargetSupportsIdempotency || notification.QuietWindow != 45*time.Second || notification.SharePeriodUnit != 2 || notification.ShareType != "etf" {
		t.Fatalf("notification policy = %#v", notification)
	}
	if status := clusterTargetNotificationStatus(notification.TargetBaseURL); status != model.ClusterNotificationStatusPending {
		t.Fatalf("notification status = %q, want pending", status)
	}
}

func TestMergeClusterMaterializationSettingsKeepsExplicitClusterTarget(t *testing.T) {
	explicit := etfauto.Config{
		Enabled: true, TargetBaseURL: "https://cluster.example/api/v1", TargetAPIToken: "cluster-token",
		QuietWindow: 10 * time.Second, SharePeriodUnit: 3, ShareType: "regular",
	}
	_, notification := mergeClusterMaterializationSettings(
		"/139_60t",
		"/139_60t",
		explicit,
		driver.ETFArchiveSettings{AutoSubscriptionEnabled: true, TargetBaseURL: "https://driver.example/api/v1"},
	)
	if notification.TargetBaseURL != explicit.TargetBaseURL || notification.TargetAPIToken != explicit.TargetAPIToken || notification.ShareType != explicit.ShareType {
		t.Fatalf("notification = %#v, want explicit cluster target %#v", notification, explicit)
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
