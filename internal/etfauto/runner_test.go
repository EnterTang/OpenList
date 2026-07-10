package etfauto

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

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
		http.Error(w, "target down", http.StatusBadGateway)
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
	if _, err := CloseDueBatches(ctx, now.Add(2*time.Second)); err != nil {
		t.Fatalf("close due batches: %v", err)
	}
	processed, err := RunPendingJobs(ctx, RunnerOptions{
		ShareProvider: &fakeShareProvider{record: &model.MobileShareRecord{ID: 10, ShareURL: "https://yun.139.com/w/i/root"}},
		HTTPClient:    server.Client(),
		Timeout:       time.Second,
		MaxRetries:    2,
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
}
