package etfauto

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func setupETFSubscriptionDB(t *testing.T) {
	t.Helper()
	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name())
	database, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	conf.Conf = conf.DefaultConfig("data")
	db.Init(database)
	t.Cleanup(func() {
		sqlDB, err := database.DB()
		if err == nil {
			_ = sqlDB.Close()
		}
	})
}

func archivedETFRecord(episode int, hash string) *model.ETFArchiveRecord {
	return &model.ETFArchiveRecord{
		StorageID:        1,
		StorageMountPath: "/139_60t",
		SourceName:       fmt.Sprintf("婚姻攻略.S01E%02d.mkv", episode),
		ArchiveETFPath:   fmt.Sprintf("/139_60t/ETF管理/tv/国产剧/婚姻攻略 (2024) {tmdb-260868}/Season 1/婚姻攻略.S01E%02d.mkv.etf", episode),
		TMDBMatched:      true,
		TMDBID:           260868,
		TMDBName:         "婚姻攻略",
		TMDBYear:         2024,
		MediaType:        "tv",
		Category:         "国产剧",
		Season:           1,
		Episode:          episode,
		SourceSize:       int64(1000 + episode),
		SourceSHA256:     hash,
		Status:           model.ETFArchiveStatusArchived,
	}
}

func TestRecordArchiveEventCoalescesETFUploadsIntoOneDueCreateJob(t *testing.T) {
	setupETFSubscriptionDB(t)
	ctx := context.Background()
	now := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	cfg := Config{
		Enabled:         true,
		TargetBaseURL:   "http://localhost:8080/api/v1",
		QuietWindow:     30 * time.Second,
		SharePeriodUnit: 1,
		ShareType:       "etf",
	}

	first, err := RecordArchiveEvent(ctx, ArchiveEvent{
		Record:           archivedETFRecord(1, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
		ClusterJobID:     "cluster-job-1",
		MediaRootFileID:  "folder-media-root",
		MediaRootPath:    "/139_60t/ETF管理/tv/国产剧/婚姻攻略 (2024) {tmdb-260868}",
		MediaRootCreated: true,
		OccurredAt:       now,
	}, cfg)
	if err != nil {
		t.Fatalf("record first archive event: %v", err)
	}
	if !first.MediaRootCreated {
		t.Fatalf("first event should mark media root as created")
	}

	second, err := RecordArchiveEvent(ctx, ArchiveEvent{
		Record:           archivedETFRecord(2, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
		ClusterJobID:     "cluster-job-2",
		MediaRootFileID:  "folder-media-root",
		MediaRootPath:    "/139_60t/ETF管理/tv/国产剧/婚姻攻略 (2024) {tmdb-260868}",
		MediaRootCreated: false,
		OccurredAt:       now.Add(10 * time.Second),
	}, cfg)
	if err != nil {
		t.Fatalf("record second archive event: %v", err)
	}
	if second.MediaRootID != first.MediaRootID {
		t.Fatalf("second event media root id = %d, want %d", second.MediaRootID, first.MediaRootID)
	}
	if second.ETFCount != 2 {
		t.Fatalf("coalesced batch etf count = %d, want 2", second.ETFCount)
	}
	batchClusterJobs, err := decodeClusterJobIDs(second.ClusterJobIDsJSON)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(batchClusterJobs) != "[cluster-job-1 cluster-job-2]" {
		t.Fatalf("batch cluster jobs = %v", batchClusterJobs)
	}

	closed, err := CloseDueBatches(ctx, now.Add(39*time.Second))
	if err != nil {
		t.Fatalf("close early batches: %v", err)
	}
	if closed != 0 {
		t.Fatalf("closed early batches = %d, want 0", closed)
	}

	closed, err = CloseDueBatches(ctx, now.Add(41*time.Second))
	if err != nil {
		t.Fatalf("close due batches: %v", err)
	}
	if closed != 1 {
		t.Fatalf("closed due batches = %d, want 1", closed)
	}

	jobs, err := ListJobs(ctx, JobFilter{Type: model.ETFSubscriptionJobTypeCreate})
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("job count = %d, want 1", len(jobs))
	}
	if jobs[0].TargetBaseURL != cfg.TargetBaseURL || jobs[0].ShareType != "etf" {
		t.Fatalf("job target/share type = %q/%q, want configured target and etf", jobs[0].TargetBaseURL, jobs[0].ShareType)
	}
	jobClusterJobs, err := decodeClusterJobIDs(jobs[0].ClusterJobIDsJSON)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(jobClusterJobs) != "[cluster-job-1 cluster-job-2]" {
		t.Fatalf("notification job cluster jobs = %v", jobClusterJobs)
	}
}

func TestRecordArchiveEventQueuesCreateJobForFirstTrackedRootEvenWithoutNewDirectory(t *testing.T) {
	setupETFSubscriptionDB(t)
	ctx := context.Background()
	now := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	cfg := Config{
		Enabled:         true,
		TargetBaseURL:   "http://localhost:8080/api/v1",
		QuietWindow:     time.Second,
		SharePeriodUnit: 1,
		ShareType:       "etf",
	}

	batch, err := RecordArchiveEvent(ctx, ArchiveEvent{
		Record:           archivedETFRecord(1, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
		MediaRootFileID:  "folder-media-root",
		MediaRootPath:    "/139_60t/ETF管理/tv/国产剧/婚姻攻略 (2024) {tmdb-260868}",
		MediaRootCreated: false,
		OccurredAt:       now,
	}, cfg)
	if err != nil {
		t.Fatalf("record archive event: %v", err)
	}
	if !batch.MediaRootCreated {
		t.Fatalf("first tracked media root should be treated as created")
	}
	if batch.Reason != model.ETFMediaRootBatchReasonInitialCreate {
		t.Fatalf("batch reason = %q, want %q", batch.Reason, model.ETFMediaRootBatchReasonInitialCreate)
	}

	if _, err := CloseDueBatches(ctx, now.Add(2*time.Second)); err != nil {
		t.Fatalf("close due batches: %v", err)
	}
	jobs, err := ListJobs(ctx, JobFilter{Type: model.ETFSubscriptionJobTypeCreate})
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("job count = %d, want 1", len(jobs))
	}
}

func TestCloseContentChangedBatchQueuesOneManualCheckJob(t *testing.T) {
	setupETFSubscriptionDB(t)
	ctx := context.Background()
	now := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	cfg := Config{
		Enabled:         true,
		TargetBaseURL:   "http://localhost:8080/api/v1",
		TargetAPIToken:  "target-token",
		QuietWindow:     time.Second,
		SharePeriodUnit: 1,
		ShareType:       "etf",
	}

	initialBatch, err := RecordArchiveEvent(ctx, ArchiveEvent{
		Record:           archivedETFRecord(1, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
		MediaRootFileID:  "folder-media-root",
		MediaRootPath:    "/139_60t/ETF管理/tv/国产剧/婚姻攻略 (2024) {tmdb-260868}",
		MediaRootCreated: true,
		OccurredAt:       now,
	}, cfg)
	if err != nil {
		t.Fatalf("record initial archive event: %v", err)
	}
	if _, err := CloseDueBatches(ctx, now.Add(2*time.Second)); err != nil {
		t.Fatalf("close initial batch: %v", err)
	}
	createJobs, err := ListJobs(ctx, JobFilter{Type: model.ETFSubscriptionJobTypeCreate})
	if err != nil {
		t.Fatalf("list create jobs: %v", err)
	}
	if len(createJobs) != 1 {
		t.Fatalf("create job count = %d, want 1", len(createJobs))
	}
	initialFingerprint, err := ComputeMediaRootFingerprint(ctx, initialBatch.MediaRootID)
	if err != nil {
		t.Fatalf("compute initial fingerprint: %v", err)
	}
	if err := MarkCreateSubscriptionSucceeded(ctx, createJobs[0].ID, CreateSubscriptionResult{
		SubscriptionID: 77,
		TaskID:         "task_create",
		Fingerprint:    initialFingerprint,
	}); err != nil {
		t.Fatalf("mark create subscription succeeded: %v", err)
	}

	changedBatch, err := RecordArchiveEvent(ctx, ArchiveEvent{
		Record:           archivedETFRecord(2, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
		MediaRootFileID:  "folder-media-root",
		MediaRootPath:    "/139_60t/ETF管理/tv/国产剧/婚姻攻略 (2024) {tmdb-260868}",
		MediaRootCreated: false,
		OccurredAt:       now.Add(3 * time.Second),
	}, cfg)
	if err != nil {
		t.Fatalf("record changed archive event: %v", err)
	}
	if changedBatch.Reason != model.ETFMediaRootBatchReasonContentChanged {
		t.Fatalf("changed batch reason = %q, want %q", changedBatch.Reason, model.ETFMediaRootBatchReasonContentChanged)
	}
	if _, err := CloseDueBatches(ctx, now.Add(5*time.Second)); err != nil {
		t.Fatalf("close changed batch: %v", err)
	}

	changedFingerprint, err := ComputeMediaRootFingerprint(ctx, initialBatch.MediaRootID)
	if err != nil {
		t.Fatalf("compute changed fingerprint: %v", err)
	}
	if changedFingerprint == initialFingerprint {
		t.Fatalf("changed fingerprint = initial fingerprint %q", initialFingerprint)
	}
	jobs, err := ListJobs(ctx, JobFilter{Type: model.ETFSubscriptionJobTypeManualCheck})
	if err != nil {
		t.Fatalf("list manual check jobs: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("manual check job count = %d, want 1", len(jobs))
	}
	if jobs[0].BatchID != changedBatch.ID || jobs[0].TargetSubscriptionID != 77 || jobs[0].Fingerprint != changedFingerprint {
		t.Fatalf("manual check job = batch %d subscription %d fingerprint %q, want %d/77/%q",
			jobs[0].BatchID, jobs[0].TargetSubscriptionID, jobs[0].Fingerprint, changedBatch.ID, changedFingerprint)
	}
	if jobs[0].TargetAPIToken != cfg.TargetAPIToken {
		t.Fatalf("manual check token = %q, want configured token", jobs[0].TargetAPIToken)
	}

	if err := closeBatch(ctx, changedBatch); err != nil {
		t.Fatalf("close changed batch again: %v", err)
	}
	jobs, err = ListJobs(ctx, JobFilter{Type: model.ETFSubscriptionJobTypeManualCheck})
	if err != nil {
		t.Fatalf("list manual check jobs after repeated close: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("manual check job count after repeated close = %d, want 1", len(jobs))
	}
}

func TestCloseNoChangeBatchCompletesLinkedNotification(t *testing.T) {
	setupETFSubscriptionDB(t)
	ctx := context.Background()
	now := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	cfg := Config{Enabled: true, TargetBaseURL: "http://localhost:8080/api/v1", QuietWindow: time.Second, SharePeriodUnit: 1, ShareType: "etf"}
	for _, id := range []string{"cluster-no-change-initial", "cluster-no-change-repeat"} {
		if err := db.GetDb().Create(&model.ClusterJob{ID: id, IdempotencyKey: id, NotificationStatus: model.ClusterNotificationStatusPending}).Error; err != nil {
			t.Fatal(err)
		}
	}
	batch, err := RecordArchiveEvent(ctx, ArchiveEvent{
		Record: archivedETFRecord(1, strings.Repeat("a", 64)), ClusterJobID: "cluster-no-change-initial",
		MediaRootFileID: "folder-media-root", MediaRootPath: "/139_60t/ETF管理/tv/国产剧/婚姻攻略 (2024) {tmdb-260868}",
		MediaRootCreated: true, OccurredAt: now,
	}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := CloseDueBatches(ctx, now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	createJobs, err := ListJobs(ctx, JobFilter{Type: model.ETFSubscriptionJobTypeCreate})
	if err != nil || len(createJobs) != 1 {
		t.Fatalf("create jobs=%#v err=%v", createJobs, err)
	}
	if err := MarkCreateSubscriptionSucceeded(ctx, createJobs[0].ID, CreateSubscriptionResult{
		SubscriptionID: 77, Fingerprint: createJobs[0].Fingerprint,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := RecordArchiveEvent(ctx, ArchiveEvent{
		Record: archivedETFRecord(1, strings.Repeat("a", 64)), ClusterJobID: "cluster-no-change-repeat",
		MediaRootFileID: "folder-media-root", MediaRootPath: "/139_60t/ETF管理/tv/国产剧/婚姻攻略 (2024) {tmdb-260868}",
		OccurredAt: now.Add(3 * time.Second),
	}, cfg); err != nil {
		t.Fatal(err)
	}
	if _, err := CloseDueBatches(ctx, now.Add(5*time.Second)); err != nil {
		t.Fatal(err)
	}
	checks, err := ListJobs(ctx, JobFilter{Type: model.ETFSubscriptionJobTypeManualCheck})
	if err != nil || len(checks) != 0 {
		t.Fatalf("manual checks=%#v err=%v", checks, err)
	}
	var clusterJob model.ClusterJob
	if err := db.GetDb().First(&clusterJob, "id = ?", "cluster-no-change-repeat").Error; err != nil {
		t.Fatal(err)
	}
	if clusterJob.NotificationStatus != model.ClusterNotificationStatusSucceeded {
		t.Fatalf("no-change notification=%q", clusterJob.NotificationStatus)
	}
	root, err := getMediaRoot(ctx, batch.MediaRootID)
	if err != nil {
		t.Fatal(err)
	}
	if root.Status != model.ETFMediaRootStatusSubscribed || root.PendingChangeCount != 0 {
		t.Fatalf("no-change root=%#v", root)
	}
}

func TestReconcileNoChangeBatchNotificationsRepairsHistoricalPending(t *testing.T) {
	setupETFSubscriptionDB(t)
	ctx := context.Background()
	root := &model.ETFMediaRoot{
		RootKey: "historical-no-change", TargetSubscriptionID: 65,
		CurrentFingerprint: "same", LastNotifiedFingerprint: "same",
		PendingChangeCount: 1, Status: model.ETFMediaRootStatusDirty,
	}
	if err := db.GetDb().Create(root).Error; err != nil {
		t.Fatal(err)
	}
	job := &model.ClusterJob{ID: "historical-no-change-job", IdempotencyKey: "historical-no-change-job", NotificationStatus: model.ClusterNotificationStatusPending}
	if err := db.GetDb().Create(job).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.GetDb().Create(&model.ETFSubscriptionJob{
		JobKey: "historical-no-change-succeeded", MediaRootID: root.ID,
		Type: model.ETFSubscriptionJobTypeManualCheck, Status: model.ETFSubscriptionJobStatusSucceeded,
		TargetSubscriptionID: root.TargetSubscriptionID, Fingerprint: "same",
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.GetDb().Create(&model.ETFMediaRootBatch{
		BatchKey: "historical-no-change-batch", MediaRootID: root.ID, Status: model.ETFMediaRootBatchStatusClosed,
		ETFCount: 1, FingerprintAfterBatch: "same", ClusterJobIDsJSON: `["historical-no-change-job"]`,
	}).Error; err != nil {
		t.Fatal(err)
	}
	completed, err := ReconcileNoChangeBatchNotifications(ctx)
	if err != nil || completed != 1 {
		t.Fatalf("completed=%d err=%v", completed, err)
	}
	if err := db.GetDb().First(job, "id = ?", job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.GetDb().First(root, root.ID).Error; err != nil {
		t.Fatal(err)
	}
	if job.NotificationStatus != model.ClusterNotificationStatusSucceeded || root.PendingChangeCount != 0 || root.Status != model.ETFMediaRootStatusSubscribed {
		t.Fatalf("job=%#v root=%#v", job, root)
	}
	completed, err = ReconcileNoChangeBatchNotifications(ctx)
	if err != nil || completed != 0 {
		t.Fatalf("second completed=%d err=%v", completed, err)
	}
}

func TestReconcileNoChangeBatchNotificationsUsesSucceededBatchCoverage(t *testing.T) {
	setupETFSubscriptionDB(t)
	ctx := context.Background()
	root := &model.ETFMediaRoot{RootKey: "covered-root", TargetSubscriptionID: 65, CurrentFingerprint: "latest", LastNotifiedFingerprint: "latest", Status: model.ETFMediaRootStatusSubscribed}
	if err := db.GetDb().Create(root).Error; err != nil {
		t.Fatal(err)
	}
	job := &model.ClusterJob{ID: "covered-intermediate-job", IdempotencyKey: "covered-intermediate-job", NotificationStatus: model.ClusterNotificationStatusPending}
	if err := db.GetDb().Create(job).Error; err != nil {
		t.Fatal(err)
	}
	batch := &model.ETFMediaRootBatch{BatchKey: "covered-intermediate-batch", MediaRootID: root.ID, Status: model.ETFMediaRootBatchStatusClosed, ETFCount: 1, FingerprintAfterBatch: "intermediate", ClusterJobIDsJSON: `["covered-intermediate-job"]`}
	if err := db.GetDb().Create(batch).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.GetDb().Create(&model.ETFSubscriptionJob{JobKey: "covered-latest-check", MediaRootID: root.ID, BatchID: batch.ID + 1, Type: model.ETFSubscriptionJobTypeManualCheck, Status: model.ETFSubscriptionJobStatusSucceeded, Fingerprint: "latest"}).Error; err != nil {
		t.Fatal(err)
	}
	completed, err := ReconcileNoChangeBatchNotifications(ctx)
	if err != nil || completed != 1 {
		t.Fatalf("completed=%d err=%v", completed, err)
	}
	if err := db.GetDb().First(job, "id = ?", job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if job.NotificationStatus != model.ClusterNotificationStatusSucceeded {
		t.Fatalf("covered notification=%q", job.NotificationStatus)
	}
}

func TestQueueOrphanBatchNotificationsCreatesCatchUpJob(t *testing.T) {
	setupETFSubscriptionDB(t)
	ctx := context.Background()
	root := &model.ETFMediaRoot{RootKey: "orphan-root", CurrentFingerprint: "current", PendingChangeCount: 1, Status: model.ETFMediaRootStatusDirty, TargetBaseURL: "https://target.example/api/v1", TargetSupportsIdempotency: true}
	if err := db.GetDb().Create(root).Error; err != nil {
		t.Fatal(err)
	}
	clusterJob := &model.ClusterJob{ID: "orphan-cluster-job", IdempotencyKey: "orphan-cluster-job", Status: model.ClusterJobStatusSucceeded, NotificationStatus: model.ClusterNotificationStatusPending}
	if err := db.GetDb().Create(clusterJob).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.GetDb().Create(&model.ETFMediaRootBatch{BatchKey: "orphan-batch", MediaRootID: root.ID, Status: model.ETFMediaRootBatchStatusClosed, ETFCount: 1, FingerprintAfterBatch: "current", ClusterJobIDsJSON: `["orphan-cluster-job"]`}).Error; err != nil {
		t.Fatal(err)
	}
	queued, linked, err := QueueOrphanBatchNotifications(ctx)
	if err != nil || queued != 1 || linked != 1 {
		t.Fatalf("queued=%d linked=%d err=%v", queued, linked, err)
	}
	var job model.ETFSubscriptionJob
	if err := db.GetDb().Where("media_root_id = ?", root.ID).First(&job).Error; err != nil {
		t.Fatal(err)
	}
	if job.Type != model.ETFSubscriptionJobTypeCreate || job.Status != model.ETFSubscriptionJobStatusPending || job.ClusterJobIDsJSON != `["orphan-cluster-job"]` {
		t.Fatalf("orphan catch-up=%#v", job)
	}
}

func TestLateCreateSuccessPreservesNewFingerprintAndQueuesCatchUp(t *testing.T) {
	setupETFSubscriptionDB(t)
	ctx := context.Background()
	now := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	cfg := Config{
		Enabled: true, TargetBaseURL: "http://localhost:8080/api/v1", TargetAPIToken: "target-token",
		QuietWindow: time.Second, SharePeriodUnit: 1, ShareType: "etf",
	}
	for _, clusterID := range []string{"cluster-initial", "cluster-later", "cluster-later-repeat"} {
		if err := db.GetDb().Create(&model.ClusterJob{
			ID: clusterID, IdempotencyKey: clusterID, NotificationStatus: model.ClusterNotificationStatusPending,
		}).Error; err != nil {
			t.Fatal(err)
		}
	}
	initialBatch, err := RecordArchiveEvent(ctx, ArchiveEvent{
		Record:       archivedETFRecord(1, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
		ClusterJobID: "cluster-initial", MediaRootFileID: "folder-media-root",
		MediaRootPath:    "/139_60t/ETF管理/tv/国产剧/婚姻攻略 (2024) {tmdb-260868}",
		MediaRootCreated: true, OccurredAt: now,
	}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := CloseDueBatches(ctx, now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	createJobs, err := ListJobs(ctx, JobFilter{Type: model.ETFSubscriptionJobTypeCreate})
	if err != nil || len(createJobs) != 1 {
		t.Fatalf("create jobs=%#v err=%v", createJobs, err)
	}
	initialFingerprint := createJobs[0].Fingerprint

	if _, err := RecordArchiveEvent(ctx, ArchiveEvent{
		Record:       archivedETFRecord(2, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
		ClusterJobID: "cluster-later", MediaRootFileID: "folder-media-root",
		MediaRootPath: "/139_60t/ETF管理/tv/国产剧/婚姻攻略 (2024) {tmdb-260868}",
		OccurredAt:    now.Add(3 * time.Second),
	}, cfg); err != nil {
		t.Fatal(err)
	}
	if _, err := CloseDueBatches(ctx, now.Add(5*time.Second)); err != nil {
		t.Fatal(err)
	}
	changedFingerprint, err := ComputeMediaRootFingerprint(ctx, initialBatch.MediaRootID)
	if err != nil || changedFingerprint == initialFingerprint {
		t.Fatalf("changed fingerprint=%q initial=%q err=%v", changedFingerprint, initialFingerprint, err)
	}
	if err := MarkCreateSubscriptionSucceeded(ctx, createJobs[0].ID, CreateSubscriptionResult{
		SubscriptionID: 77, TaskID: "task-create", Fingerprint: initialFingerprint,
	}); err != nil {
		t.Fatal(err)
	}
	root, err := getMediaRoot(ctx, initialBatch.MediaRootID)
	if err != nil {
		t.Fatal(err)
	}
	if root.CurrentFingerprint != changedFingerprint || root.LastNotifiedFingerprint != initialFingerprint || root.Status != model.ETFMediaRootStatusDirty || root.TargetSubscriptionID != 77 {
		t.Fatalf("root after late create = %#v", root)
	}
	checks, err := ListJobs(ctx, JobFilter{Type: model.ETFSubscriptionJobTypeManualCheck})
	if err != nil || len(checks) != 1 {
		t.Fatalf("catch-up jobs=%#v err=%v", checks, err)
	}
	if checks[0].Fingerprint != changedFingerprint || checks[0].TargetSubscriptionID != 77 || checks[0].ClusterJobIDsJSON != `["cluster-later"]` {
		t.Fatalf("catch-up job = %#v", checks[0])
	}

	if _, err := RecordArchiveEvent(ctx, ArchiveEvent{
		Record:       archivedETFRecord(2, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
		ClusterJobID: "cluster-later-repeat", MediaRootFileID: "folder-media-root",
		MediaRootPath: "/139_60t/ETF管理/tv/国产剧/婚姻攻略 (2024) {tmdb-260868}",
		OccurredAt:    now.Add(6 * time.Second),
	}, cfg); err != nil {
		t.Fatal(err)
	}
	if _, err := CloseDueBatches(ctx, now.Add(8*time.Second)); err != nil {
		t.Fatal(err)
	}
	checks, err = ListJobs(ctx, JobFilter{Type: model.ETFSubscriptionJobTypeManualCheck})
	if err != nil || len(checks) != 1 {
		t.Fatalf("merged catch-up jobs=%#v err=%v", checks, err)
	}
	if checks[0].ClusterJobIDsJSON != `["cluster-later","cluster-later-repeat"]` {
		t.Fatalf("merged cluster ids = %s", checks[0].ClusterJobIDsJSON)
	}
	var initialCluster, laterCluster model.ClusterJob
	if err := db.GetDb().First(&initialCluster, "id = ?", "cluster-initial").Error; err != nil {
		t.Fatal(err)
	}
	if err := db.GetDb().First(&laterCluster, "id = ?", "cluster-later").Error; err != nil {
		t.Fatal(err)
	}
	if initialCluster.NotificationStatus != model.ClusterNotificationStatusSucceeded || laterCluster.NotificationStatus != model.ClusterNotificationStatusPending {
		t.Fatalf("notification statuses initial=%q later=%q", initialCluster.NotificationStatus, laterCluster.NotificationStatus)
	}
}

func TestRequestManualCheckSkipsUnchangedRootAndDeduplicatesChangedFingerprint(t *testing.T) {
	setupETFSubscriptionDB(t)
	ctx := context.Background()
	now := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	cfg := Config{Enabled: true, TargetBaseURL: "http://localhost:8080/api/v1", QuietWindow: time.Second, SharePeriodUnit: 1, ShareType: "etf"}

	batch, err := RecordArchiveEvent(ctx, ArchiveEvent{
		Record:           archivedETFRecord(1, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
		MediaRootFileID:  "folder-media-root",
		MediaRootPath:    "/139_60t/ETF管理/tv/国产剧/婚姻攻略 (2024) {tmdb-260868}",
		MediaRootCreated: true,
		OccurredAt:       now,
	}, cfg)
	if err != nil {
		t.Fatalf("record archive event: %v", err)
	}
	if _, err := CloseDueBatches(ctx, now.Add(2*time.Second)); err != nil {
		t.Fatalf("close due batches: %v", err)
	}
	createJobs, err := ListJobs(ctx, JobFilter{Type: model.ETFSubscriptionJobTypeCreate})
	if err != nil {
		t.Fatalf("list create jobs: %v", err)
	}
	if len(createJobs) != 1 {
		t.Fatalf("create job count = %d, want 1", len(createJobs))
	}
	fingerprint, err := ComputeMediaRootFingerprint(ctx, batch.MediaRootID)
	if err != nil {
		t.Fatalf("compute fingerprint: %v", err)
	}
	if err := MarkCreateSubscriptionSucceeded(ctx, createJobs[0].ID, CreateSubscriptionResult{
		SubscriptionID: 77,
		TaskID:         "task_create",
		Fingerprint:    fingerprint,
	}); err != nil {
		t.Fatalf("mark create subscription succeeded: %v", err)
	}

	unchanged, err := RequestManualCheck(ctx, batch.MediaRootID, now.Add(3*time.Second))
	if err != nil {
		t.Fatalf("request unchanged manual check: %v", err)
	}
	if unchanged.Status != ManualCheckNoChange {
		t.Fatalf("unchanged manual check status = %q, want %q", unchanged.Status, ManualCheckNoChange)
	}

	if _, err := RecordArchiveEvent(ctx, ArchiveEvent{
		Record:           archivedETFRecord(2, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
		MediaRootFileID:  "folder-media-root",
		MediaRootPath:    "/139_60t/ETF管理/tv/国产剧/婚姻攻略 (2024) {tmdb-260868}",
		MediaRootCreated: false,
		OccurredAt:       now.Add(4 * time.Second),
	}, cfg); err != nil {
		t.Fatalf("record changed archive event: %v", err)
	}
	if _, err := CloseDueBatches(ctx, now.Add(6*time.Second)); err != nil {
		t.Fatalf("close changed batch: %v", err)
	}

	changed, err := RequestManualCheck(ctx, batch.MediaRootID, now.Add(7*time.Second))
	if err != nil {
		t.Fatalf("request changed manual check: %v", err)
	}
	if changed.Status != ManualCheckAlreadyQueued || changed.Job == nil {
		t.Fatalf("changed manual check = status %q job %#v, want automatically queued job", changed.Status, changed.Job)
	}
	again, err := RequestManualCheck(ctx, batch.MediaRootID, now.Add(8*time.Second))
	if err != nil {
		t.Fatalf("request duplicate manual check: %v", err)
	}
	if again.Status != ManualCheckAlreadyQueued || again.Job == nil || again.Job.ID != changed.Job.ID {
		t.Fatalf("duplicate manual check = status %q job %#v, want existing job %d", again.Status, again.Job, changed.Job.ID)
	}
}
