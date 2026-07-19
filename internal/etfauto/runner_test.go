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

func TestProcessOnceReconcilesUnknownCreateByLookupWithoutAnotherPost(t *testing.T) {
	setupETFSubscriptionDB(t)
	ctx := context.Background()
	var lookupRequests, createRequests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/subscriptions/lookup":
			lookupRequests++
			_ = json.NewEncoder(w).Encode(map[string]any{
				"exists": true, "subscription_id": 27, "task_id": "task-observed", "status": "completed",
			})
		case r.Method == http.MethodPost:
			createRequests++
			http.Error(w, "duplicate create must not be attempted", http.StatusConflict)
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	root := model.ETFMediaRoot{
		RootKey: "unknown-lookup-root", MediaType: "tv", TMDBID: 308874,
		Status: model.ETFMediaRootStatusCollecting, TargetBaseURL: server.URL + "/api/v1", TargetAPIToken: "token",
	}
	if err := db.GetDb().Create(&root).Error; err != nil {
		t.Fatal(err)
	}
	clusterJob := model.ClusterJob{ID: "cluster-unknown-lookup", IdempotencyKey: "cluster-unknown-lookup", NotificationStatus: model.ClusterNotificationStatusUnknown}
	if err := db.GetDb().Create(&clusterJob).Error; err != nil {
		t.Fatal(err)
	}
	job := model.ETFSubscriptionJob{
		JobKey: "notification-unknown-lookup", MediaRootID: root.ID, Type: model.ETFSubscriptionJobTypeCreate,
		Status: model.ETFSubscriptionJobStatusUnknown, TargetBaseURL: root.TargetBaseURL, TargetAPIToken: root.TargetAPIToken,
		Fingerprint: "fingerprint-observed", ClusterJobIDsJSON: `["cluster-unknown-lookup"]`,
	}
	if err := db.GetDb().Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	result, err := ProcessOnce(ctx, RunnerOptions{HTTPClient: server.Client(), Timeout: time.Second, Now: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	if result.ReconciledUnknown != 1 || result.ProcessedJobs != 0 || lookupRequests != 1 || createRequests != 0 {
		t.Fatalf("result=%#v lookup=%d create=%d", result, lookupRequests, createRequests)
	}
	if err := db.GetDb().First(&job, job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if job.Status != model.ETFSubscriptionJobStatusSucceeded || job.TargetSubscriptionID != 27 {
		t.Fatalf("reconciled job = %#v", job)
	}
	if err := db.GetDb().First(&root, root.ID).Error; err != nil {
		t.Fatal(err)
	}
	if root.TargetSubscriptionID != 27 || root.LastCreateTaskID != "task-observed" {
		t.Fatalf("reconciled root = %#v", root)
	}
	if err := db.GetDb().First(&clusterJob, "id = ?", clusterJob.ID).Error; err != nil {
		t.Fatal(err)
	}
	if clusterJob.NotificationStatus != model.ClusterNotificationStatusSucceeded {
		t.Fatalf("cluster notification = %q", clusterJob.NotificationStatus)
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

func TestManualCheckRecreatesMissingTargetSubscription(t *testing.T) {
	setupETFSubscriptionDB(t)
	ctx := context.Background()
	var createRequests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/subscriptions/77", "/api/v1/subscriptions/77/check":
			http.Error(w, `{"code":"NOT_FOUND","message":"subscription not found"}`, http.StatusNotFound)
		case "/api/v1/subscriptions":
			createRequests++
			_ = json.NewEncoder(w).Encode(map[string]any{"subscription": map[string]any{"id": 88}, "task_id": "task-recreated"})
		default:
			t.Fatalf("unexpected request %s", r.URL.Path)
		}
	}))
	defer server.Close()

	root := model.ETFMediaRoot{RootKey: "missing-target", Status: model.ETFMediaRootStatusDirty, TargetBaseURL: server.URL + "/api/v1", TargetSubscriptionID: 77}
	if err := db.GetDb().Create(&root).Error; err != nil {
		t.Fatal(err)
	}
	manualCheck := model.ETFSubscriptionJob{
		JobKey: "check:missing-target:fingerprint-1", MediaRootID: root.ID, Type: model.ETFSubscriptionJobTypeManualCheck,
		Status: model.ETFSubscriptionJobStatusPending, TargetBaseURL: root.TargetBaseURL, TargetSubscriptionID: 77, Fingerprint: "fingerprint-1",
	}
	if err := db.GetDb().Create(&manualCheck).Error; err != nil {
		t.Fatal(err)
	}
	share := &fakeShareProvider{record: &model.MobileShareRecord{ID: 10, ShareURL: "https://yun.139.com/w/i/root"}}
	if processed, err := RunPendingJobs(ctx, RunnerOptions{ShareProvider: share, HTTPClient: server.Client(), Now: time.Now().UTC()}); err != nil || processed != 1 {
		t.Fatalf("manual check processed=%d err=%v", processed, err)
	}
	if err := db.GetDb().First(&manualCheck, manualCheck.ID).Error; err != nil {
		t.Fatal(err)
	}
	if manualCheck.Status != model.ETFSubscriptionJobStatusDeadLetter {
		t.Fatalf("manual check status = %q, want dead_letter", manualCheck.Status)
	}
	if err := db.GetDb().First(&root, root.ID).Error; err != nil {
		t.Fatal(err)
	}
	if root.TargetSubscriptionID != 0 || root.Status != model.ETFMediaRootStatusDirty {
		t.Fatalf("root after missing target = %#v", root)
	}
	var replacement model.ETFSubscriptionJob
	if err := db.GetDb().Where("type = ?", model.ETFSubscriptionJobTypeCreate).First(&replacement).Error; err != nil {
		t.Fatal(err)
	}
	if replacement.Status != model.ETFSubscriptionJobStatusPending || replacement.JobKey != "recreate:missing-target:fingerprint-1" {
		t.Fatalf("replacement = %#v", replacement)
	}
	if processed, err := RunPendingJobs(ctx, RunnerOptions{ShareProvider: share, HTTPClient: server.Client(), Now: time.Now().UTC()}); err != nil || processed != 1 {
		t.Fatalf("replacement processed=%d err=%v", processed, err)
	}
	if createRequests != 1 {
		t.Fatalf("create requests = %d, want 1", createRequests)
	}
	if err := db.GetDb().First(&root, root.ID).Error; err != nil {
		t.Fatal(err)
	}
	if root.TargetSubscriptionID != 88 || root.LastCreateTaskID != "task-recreated" {
		t.Fatalf("recreated root = %#v", root)
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
