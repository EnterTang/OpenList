package etfauto

import (
	"context"
	"fmt"
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
	if changed.Status != ManualCheckQueued || changed.Job == nil {
		t.Fatalf("changed manual check = status %q job %#v, want queued job", changed.Status, changed.Job)
	}
	again, err := RequestManualCheck(ctx, batch.MediaRootID, now.Add(8*time.Second))
	if err != nil {
		t.Fatalf("request duplicate manual check: %v", err)
	}
	if again.Status != ManualCheckAlreadyQueued || again.Job == nil || again.Job.ID != changed.Job.ID {
		t.Fatalf("duplicate manual check = status %q job %#v, want existing job %d", again.Status, again.Job, changed.Job.ID)
	}
}
