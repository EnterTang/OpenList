package etfauto

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

type fakeShareProvider struct {
	record *model.MobileShareRecord
	calls  int
}

func (f *fakeShareProvider) CreateOrReuseShare(ctx context.Context, root *model.ETFMediaRoot) (*model.MobileShareRecord, error) {
	f.calls++
	return f.record, nil
}

func TestRunPendingJobsCreatesShareAndTargetSubscriptionOnce(t *testing.T) {
	setupETFSubscriptionDB(t)
	ctx := context.Background()
	now := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	var createRequests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/subscriptions" {
			t.Fatalf("path = %s, want /api/v1/subscriptions", r.URL.Path)
		}
		createRequests++
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		if body["share_type"] != "etf" || body["share_url"] != "https://yun.139.com/w/i/root" {
			t.Fatalf("body = %#v, want etf share", body)
		}
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"subscription": map[string]any{"id": 99},
			"task_id":      "task_create",
			"type":         "subscription:check_updates",
			"status":       "pending",
		})
	}))
	defer server.Close()

	cfg := Config{Enabled: true, TargetBaseURL: server.URL + "/api/v1", QuietWindow: time.Second, SharePeriodUnit: 1, ShareType: "etf"}
	clusterJob := model.ClusterJob{ID: "cluster-create-success", IdempotencyKey: "cluster-create-success", NotificationStatus: model.ClusterNotificationStatusPending}
	if err := db.GetDb().Create(&clusterJob).Error; err != nil {
		t.Fatal(err)
	}
	batch, err := RecordArchiveEvent(ctx, ArchiveEvent{
		Record:           archivedETFRecord(1, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
		ClusterJobID:     clusterJob.ID,
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
	share := &fakeShareProvider{record: &model.MobileShareRecord{
		ID:          10,
		ShareURL:    "https://yun.139.com/w/i/root",
		ExtractCode: "abcd",
	}}
	processed, err := RunPendingJobs(ctx, RunnerOptions{
		ShareProvider: share,
		HTTPClient:    server.Client(),
		Timeout:       time.Second,
	})
	if err != nil {
		t.Fatalf("run pending jobs: %v", err)
	}
	if processed != 1 || share.calls != 1 || createRequests != 1 {
		t.Fatalf("processed/share calls/create requests = %d/%d/%d, want 1/1/1", processed, share.calls, createRequests)
	}

	jobs, err := ListJobs(ctx, JobFilter{Type: model.ETFSubscriptionJobTypeCreate})
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(jobs) != 1 || jobs[0].Status != model.ETFSubscriptionJobStatusSucceeded {
		t.Fatalf("job = %#v, want one succeeded create job", jobs)
	}
	root, err := getMediaRoot(ctx, batch.MediaRootID)
	if err != nil {
		t.Fatalf("get media root: %v", err)
	}
	if root.TargetSubscriptionID != 99 || root.LastCreateTaskID != "task_create" || root.MobileShareRecordID != 10 {
		t.Fatalf("root target state = subscription %d task %q share %d, want 99/task_create/10", root.TargetSubscriptionID, root.LastCreateTaskID, root.MobileShareRecordID)
	}
	if err := db.GetDb().First(&clusterJob, "id = ?", clusterJob.ID).Error; err != nil {
		t.Fatal(err)
	}
	if clusterJob.NotificationStatus != model.ClusterNotificationStatusSucceeded {
		t.Fatalf("cluster notification status = %q, want succeeded", clusterJob.NotificationStatus)
	}
}

func TestProcessOnceClosesDueBatchesBeforeRunningJobs(t *testing.T) {
	setupETFSubscriptionDB(t)
	ctx := context.Background()
	now := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"subscription": map[string]any{"id": 100},
			"task_id":      "task_create",
		})
	}))
	defer server.Close()
	cfg := Config{Enabled: true, TargetBaseURL: server.URL + "/api/v1", QuietWindow: time.Second, SharePeriodUnit: 1, ShareType: "etf"}
	if _, err := RecordArchiveEvent(ctx, ArchiveEvent{
		Record:           archivedETFRecord(1, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
		MediaRootFileID:  "folder-media-root",
		MediaRootPath:    "/139_60t/ETF管理/tv/国产剧/婚姻攻略 (2024) {tmdb-260868}",
		MediaRootCreated: true,
		OccurredAt:       now,
	}, cfg); err != nil {
		t.Fatalf("record archive event: %v", err)
	}
	result, err := ProcessOnce(ctx, RunnerOptions{
		ShareProvider: &fakeShareProvider{record: &model.MobileShareRecord{ID: 10, ShareURL: "https://yun.139.com/w/i/root"}},
		HTTPClient:    server.Client(),
		Timeout:       time.Second,
		Now:           now.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatalf("process once: %v", err)
	}
	if result.ClosedBatches != 1 || result.ProcessedJobs != 1 {
		t.Fatalf("process result = %#v, want one closed batch and one processed job", result)
	}
}

func TestRunPendingJobsMarksFailedCreateJobRetryable(t *testing.T) {
	setupETFSubscriptionDB(t)
	ctx := context.Background()
	now := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "target busy", http.StatusTooManyRequests)
	}))
	defer server.Close()
	cfg := Config{Enabled: true, TargetBaseURL: server.URL + "/api/v1", QuietWindow: time.Second, SharePeriodUnit: 1, ShareType: "etf"}
	clusterJob := model.ClusterJob{ID: "cluster-create-failure", IdempotencyKey: "cluster-create-failure", NotificationStatus: model.ClusterNotificationStatusPending}
	if err := db.GetDb().Create(&clusterJob).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := RecordArchiveEvent(ctx, ArchiveEvent{
		Record:           archivedETFRecord(1, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
		ClusterJobID:     clusterJob.ID,
		MediaRootFileID:  "folder-media-root",
		MediaRootPath:    "/139_60t/ETF管理/tv/国产剧/婚姻攻略 (2024) {tmdb-260868}",
		MediaRootCreated: true,
		OccurredAt:       now,
	}, cfg); err != nil {
		t.Fatalf("record archive event: %v", err)
	}
	if _, err := CloseDueBatches(ctx, now.Add(2*time.Second)); err != nil {
		t.Fatalf("close due batches: %v", err)
	}
	processed, err := RunPendingJobs(ctx, RunnerOptions{
		ShareProvider: &fakeShareProvider{record: &model.MobileShareRecord{ID: 10, ShareURL: "https://yun.139.com/w/i/root"}},
		HTTPClient:    server.Client(),
		Timeout:       time.Second,
		MaxRetries:    2,
		RetryDelay:    time.Second,
		Now:           now.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatalf("run pending jobs: %v", err)
	}
	if processed != 1 {
		t.Fatalf("processed = %d, want 1", processed)
	}
	jobs, err := ListJobs(ctx, JobFilter{Type: model.ETFSubscriptionJobTypeCreate})
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("job count = %d, want 1", len(jobs))
	}
	if jobs[0].Status != model.ETFSubscriptionJobStatusFailed || jobs[0].Attempts != 1 || jobs[0].NextRetryAt == nil {
		t.Fatalf("failed job = status %q attempts %d next %v, want failed attempt with retry", jobs[0].Status, jobs[0].Attempts, jobs[0].NextRetryAt)
	}
	if jobs[0].LastError == "" || jobs[0].LastError == fmt.Sprint(nil) {
		t.Fatalf("failed job last error should be recorded")
	}
	if err := db.GetDb().First(&clusterJob, "id = ?", clusterJob.ID).Error; err != nil {
		t.Fatal(err)
	}
	if clusterJob.NotificationStatus != model.ClusterNotificationStatusPending {
		t.Fatalf("retrying cluster notification status = %q, want pending", clusterJob.NotificationStatus)
	}
	processed, err = RunPendingJobs(ctx, RunnerOptions{
		ShareProvider: &fakeShareProvider{record: &model.MobileShareRecord{ID: 10, ShareURL: "https://yun.139.com/w/i/root"}},
		HTTPClient:    server.Client(),
		Timeout:       time.Second,
		MaxRetries:    2,
		RetryDelay:    time.Second,
		Now:           now.Add(4 * time.Second),
	})
	if err != nil || processed != 1 {
		t.Fatalf("run terminal retry = processed %d error %v", processed, err)
	}
	if err := db.GetDb().First(&jobs[0], jobs[0].ID).Error; err != nil {
		t.Fatal(err)
	}
	if jobs[0].Status != model.ETFSubscriptionJobStatusDeadLetter {
		t.Fatalf("terminal notification job status = %q, want dead_letter", jobs[0].Status)
	}
	if err := db.GetDb().First(&clusterJob, "id = ?", clusterJob.ID).Error; err != nil {
		t.Fatal(err)
	}
	if clusterJob.NotificationStatus != model.ClusterNotificationStatusFailed {
		t.Fatalf("terminal cluster notification status = %q, want failed", clusterJob.NotificationStatus)
	}
}

func TestRunPendingJobsMarksUncertainDeliveryUnknown(t *testing.T) {
	setupETFSubscriptionDB(t)
	ctx := context.Background()
	root := model.ETFMediaRoot{RootKey: "unknown-root", Status: model.ETFMediaRootStatusCollecting, TargetBaseURL: "http://target.invalid"}
	if err := db.GetDb().Create(&root).Error; err != nil {
		t.Fatal(err)
	}
	clusterJob := model.ClusterJob{ID: "cluster-unknown", IdempotencyKey: "cluster-unknown", NotificationStatus: model.ClusterNotificationStatusPending}
	if err := db.GetDb().Create(&clusterJob).Error; err != nil {
		t.Fatal(err)
	}
	job := model.ETFSubscriptionJob{
		JobKey: "notification-unknown", MediaRootID: root.ID, Type: model.ETFSubscriptionJobTypeCreate,
		Status: model.ETFSubscriptionJobStatusPending, TargetBaseURL: "http://target.invalid",
		ClusterJobIDsJSON: `["cluster-unknown"]`,
	}
	if err := db.GetDb().Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, context.DeadlineExceeded
	})}
	processed, err := RunPendingJobs(ctx, RunnerOptions{
		ShareProvider: &fakeShareProvider{record: &model.MobileShareRecord{ID: 10, ShareURL: "https://yun.139.com/w/i/root"}},
		HTTPClient:    client, Now: time.Now().UTC(),
	})
	if err != nil || processed != 1 {
		t.Fatalf("processed=%d err=%v", processed, err)
	}
	if err := db.GetDb().First(&job, job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if job.Status != model.ETFSubscriptionJobStatusUnknown || job.NextRetryAt != nil {
		t.Fatalf("job=%#v, want unknown without retry", job)
	}
	if err := db.GetDb().First(&clusterJob, "id = ?", clusterJob.ID).Error; err != nil {
		t.Fatal(err)
	}
	if clusterJob.NotificationStatus != model.ClusterNotificationStatusUnknown {
		t.Fatalf("cluster notification=%q, want unknown", clusterJob.NotificationStatus)
	}
	if err := RetryUnknownJob(ctx, job.ID); err != nil {
		t.Fatal(err)
	}
	if err := db.GetDb().First(&job, job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if job.Status != model.ETFSubscriptionJobStatusPending {
		t.Fatalf("retried unknown job status=%q", job.Status)
	}
	if err := db.GetDb().First(&clusterJob, "id = ?", clusterJob.ID).Error; err != nil {
		t.Fatal(err)
	}
	if clusterJob.NotificationStatus != model.ClusterNotificationStatusPending {
		t.Fatalf("retried cluster notification=%q, want pending", clusterJob.NotificationStatus)
	}
}

func TestUncertainDeliveryRetriesWhenTargetDeclaresIdempotency(t *testing.T) {
	setupETFSubscriptionDB(t)
	ctx := context.Background()
	root := model.ETFMediaRoot{RootKey: "idempotent-root", Status: model.ETFMediaRootStatusCollecting}
	if err := db.GetDb().Create(&root).Error; err != nil {
		t.Fatal(err)
	}
	job := model.ETFSubscriptionJob{
		JobKey: "notification-idempotent", MediaRootID: root.ID, Type: model.ETFSubscriptionJobTypeCreate,
		Status: model.ETFSubscriptionJobStatusPending, TargetBaseURL: "http://target.invalid",
		TargetSupportsIdempotency: true,
	}
	if err := db.GetDb().Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, context.DeadlineExceeded
	})}
	processed, err := RunPendingJobs(ctx, RunnerOptions{
		ShareProvider: &fakeShareProvider{record: &model.MobileShareRecord{ID: 10, ShareURL: "https://yun.139.com/w/i/root"}},
		HTTPClient:    client, RetryDelay: time.Minute, Now: time.Now().UTC(),
	})
	if err != nil || processed != 1 {
		t.Fatalf("processed=%d err=%v", processed, err)
	}
	if err := db.GetDb().First(&job, job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if job.Status != model.ETFSubscriptionJobStatusFailed || job.NextRetryAt == nil {
		t.Fatalf("idempotent target job=%#v, want retryable failure", job)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

func TestManualCheckSuccessUpdatesLinkedClusterJobs(t *testing.T) {
	setupETFSubscriptionDB(t)
	ctx := context.Background()
	root := model.ETFMediaRoot{RootKey: "manual-root", Status: model.ETFMediaRootStatusDirty}
	if err := db.GetDb().Create(&root).Error; err != nil {
		t.Fatal(err)
	}
	clusterJob := model.ClusterJob{ID: "cluster-manual-success", IdempotencyKey: "cluster-manual-success", NotificationStatus: model.ClusterNotificationStatusPending}
	if err := db.GetDb().Create(&clusterJob).Error; err != nil {
		t.Fatal(err)
	}
	job := model.ETFSubscriptionJob{
		JobKey:            "manual-linked",
		MediaRootID:       root.ID,
		Type:              model.ETFSubscriptionJobTypeManualCheck,
		Status:            model.ETFSubscriptionJobStatusPending,
		Fingerprint:       "fingerprint-1",
		ClusterJobIDsJSON: `["cluster-manual-success"]`,
	}
	if err := db.GetDb().Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	if err := markManualCheckSucceeded(ctx, job.ID, &TargetTaskResult{TaskID: "task-check", RawJSON: `{}`}); err != nil {
		t.Fatal(err)
	}
	if err := db.GetDb().First(&clusterJob, "id = ?", clusterJob.ID).Error; err != nil {
		t.Fatal(err)
	}
	if clusterJob.NotificationStatus != model.ClusterNotificationStatusSucceeded {
		t.Fatalf("manual-check cluster notification status = %q, want succeeded", clusterJob.NotificationStatus)
	}
}
