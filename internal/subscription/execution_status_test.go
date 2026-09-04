package subscription

import (
	"context"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestAggregateSubscriptionStatusRequiresDurableTransferOutcome(t *testing.T) {
	tests := []struct {
		name  string
		items []model.SubscriptionItem
		want  string
	}{
		{name: "dispatch only remains running", items: []model.SubscriptionItem{{Status: model.SubscriptionItemStatusPending}}, want: model.SubscriptionStatusRunning},
		{name: "retry wait remains running", items: []model.SubscriptionItem{{Status: model.SubscriptionItemStatusRetryWait}}, want: model.SubscriptionStatusRunning},
		{name: "blocked remains running", items: []model.SubscriptionItem{{Status: model.SubscriptionItemStatusBlocked}}, want: model.SubscriptionStatusRunning},
		{name: "unknown remains running", items: []model.SubscriptionItem{{Status: model.SubscriptionItemStatusUnknown}}, want: model.SubscriptionStatusRunning},
		{name: "notifying remains running", items: []model.SubscriptionItem{{Status: model.SubscriptionItemStatusNotifying}}, want: model.SubscriptionStatusRunning},
		{name: "transferring remains running", items: []model.SubscriptionItem{{Status: model.SubscriptionItemStatusTransferring}}, want: model.SubscriptionStatusRunning},
		{name: "terminal failure is failed", items: []model.SubscriptionItem{{Status: model.SubscriptionItemStatusFailed}}, want: model.SubscriptionStatusFailed},
		{name: "transferred and skipped is success", items: []model.SubscriptionItem{{Status: model.SubscriptionItemStatusTransferred}, {Status: model.SubscriptionItemStatusSkipped}}, want: model.SubscriptionStatusSuccess},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := aggregateSubscriptionStatus(tc.items); got != tc.want {
				t.Fatalf("status = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestReconcileSubscriptionExecutionRepairsOrphansAndTerminalMismatch(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	sub := &model.Subscription{Name: "reconcile", TMDBName: "reconcile", LastStatus: model.SubscriptionStatusSuccess}
	if err := db.CreateSubscription(sub); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	transferred := &model.SubscriptionItem{
		SubscriptionID: sub.ID, SourceKey: "done", FileHash: "hash-done", Status: model.SubscriptionItemStatusTransferred,
		ClusterJobID: "job-done", LastSeenAt: now,
	}
	orphaned := &model.SubscriptionItem{
		SubscriptionID: sub.ID, SourceKey: "orphan", FileHash: "hash-orphan", Status: model.SubscriptionItemStatusTransferring,
		LastSeenAt: now,
	}
	failedWithSuccessfulJob := &model.SubscriptionItem{
		SubscriptionID: sub.ID, SourceKey: "failed-but-done", FileHash: "hash-failed-but-done", Status: model.SubscriptionItemStatusFailed,
		ClusterJobID: "job-failed-but-done", LastSeenAt: now, LastError: "old transfer failure",
	}
	for _, item := range []*model.SubscriptionItem{transferred, orphaned, failedWithSuccessfulJob} {
		if err := db.GetDb().Create(item).Error; err != nil {
			t.Fatal(err)
		}
	}
	job := &model.ClusterJob{
		ID: "job-done", Type: model.ClusterJobTypeMediaTransfer, Status: model.ClusterJobStatusRunning,
		IdempotencyKey: "idempotent-done", SubscriptionID: sub.ID, SubscriptionItemID: transferred.ID,
		AvailableAt: now,
	}
	if err := db.GetDb().Create(job).Error; err != nil {
		t.Fatal(err)
	}
	successfulJob := &model.ClusterJob{
		ID: "job-failed-but-done", Type: model.ClusterJobTypeMediaTransfer, Status: model.ClusterJobStatusSucceeded,
		IdempotencyKey: "idempotent-failed-but-done", SubscriptionID: sub.ID, SubscriptionItemID: failedWithSuccessfulJob.ID,
		AvailableAt: now, FinishedAt: &now,
	}
	if err := db.GetDb().Create(successfulJob).Error; err != nil {
		t.Fatal(err)
	}

	result, err := ReconcileSubscriptionExecution(context.Background(), sub.ID)
	if err != nil {
		t.Fatal(err)
	}
	if result.Repaired < 2 {
		t.Fatalf("reconcile result = %#v, want at least two repairs", result)
	}
	var gotOrphan model.SubscriptionItem
	if err := db.GetDb().First(&gotOrphan, orphaned.ID).Error; err != nil {
		t.Fatal(err)
	}
	if gotOrphan.Status != model.SubscriptionItemStatusPending || gotOrphan.ClusterJobID != "" {
		t.Fatalf("orphaned item = %#v", gotOrphan)
	}
	var gotJob model.ClusterJob
	if err := db.GetDb().First(&gotJob, "id = ?", job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if gotJob.Status != model.ClusterJobStatusSucceeded || gotJob.FinishedAt == nil {
		t.Fatalf("terminal mismatch job = %#v", gotJob)
	}
	var gotRecovered model.SubscriptionItem
	if err := db.GetDb().First(&gotRecovered, failedWithSuccessfulJob.ID).Error; err != nil {
		t.Fatal(err)
	}
	if gotRecovered.Status != model.SubscriptionItemStatusTransferred || gotRecovered.LastError != "" {
		t.Fatalf("failed item with successful job = %#v", gotRecovered)
	}
	var gotSub model.Subscription
	if err := db.GetDb().First(&gotSub, sub.ID).Error; err != nil {
		t.Fatal(err)
	}
	if gotSub.LastStatus != model.SubscriptionStatusRunning {
		t.Fatalf("subscription status = %q, want running", gotSub.LastStatus)
	}
}

func TestReconcileSubscriptionExecutionFinalizesLatestRunAfterTerminalJobs(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	sub := &model.Subscription{Name: "run-finalize", TMDBName: "run-finalize", LastStatus: model.SubscriptionStatusRunning}
	if err := db.CreateSubscription(sub); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	item := &model.SubscriptionItem{
		SubscriptionID: sub.ID, SourceKey: "terminal-item", FileHash: "terminal-hash",
		Status: model.SubscriptionItemStatusTransferring, ClusterJobID: "terminal-job", LastSeenAt: now,
	}
	if err := db.GetDb().Create(item).Error; err != nil {
		t.Fatal(err)
	}
	job := &model.ClusterJob{
		ID: "terminal-job", Type: model.ClusterJobTypeMediaTransfer, Status: model.ClusterJobStatusSucceeded,
		IdempotencyKey: "terminal-idempotency", SubscriptionID: sub.ID, SubscriptionItemID: item.ID,
		AvailableAt: now, FinishedAt: &now,
	}
	if err := db.GetDb().Create(job).Error; err != nil {
		t.Fatal(err)
	}
	run := &model.SubscriptionRun{
		SubscriptionID: sub.ID, StartedAt: now.Add(-time.Minute), FinishedAt: &now,
		Status: model.SubscriptionStatusRunning, CompletionState: "dispatching", TransferStatus: "running",
		DispatchStatus: model.SubscriptionStatusSuccess, DiscoverStatus: model.SubscriptionStatusSuccess,
	}
	if err := db.GetDb().Create(run).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := ReconcileSubscriptionExecution(context.Background(), sub.ID); err != nil {
		t.Fatal(err)
	}
	var gotItem model.SubscriptionItem
	if err := db.GetDb().First(&gotItem, item.ID).Error; err != nil {
		t.Fatal(err)
	}
	if gotItem.Status != model.SubscriptionItemStatusTransferred || gotItem.StateVersion == 0 {
		t.Fatalf("item = %#v, want transferred with advanced state version", gotItem)
	}
	var gotRun model.SubscriptionRun
	if err := db.GetDb().First(&gotRun, run.ID).Error; err != nil {
		t.Fatal(err)
	}
	if gotRun.Status != model.SubscriptionStatusSuccess || gotRun.CompletionState == "dispatching" || gotRun.CompletionState == "transferring" || gotRun.FinishedAt == nil {
		t.Fatalf("run = %#v, want terminal success", gotRun)
	}
}

func TestSubscriptionNeedsExecutionFollowupDoesNotSpinOnBlockedWorker(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	sub := &model.Subscription{Name: "blocked", TMDBName: "blocked", Active: true, TransferEnabled: true}
	if err := db.CreateSubscription(sub); err != nil {
		t.Fatal(err)
	}
	item := &model.SubscriptionItem{
		SubscriptionID: sub.ID, SourceKey: "blocked-item", Status: model.SubscriptionItemStatusPending,
		LastError: "subscription media task has no compatible cluster worker",
	}
	if err := db.GetDb().Create(item).Error; err != nil {
		t.Fatal(err)
	}
	followup, err := SubscriptionNeedsExecutionFollowup(nil, sub.ID)
	if err != nil {
		t.Fatal(err)
	}
	if followup {
		t.Fatal("blocked worker item should wait for the normal scheduler interval")
	}
}

func TestSubscriptionExecutionFollowupUsesNormalRunForMoviePilotCapacityWait(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	sub := &model.Subscription{Name: "MoviePilot capacity", TMDBName: "MoviePilot capacity", Active: true, TransferEnabled: true}
	if err := db.CreateSubscription(sub); err != nil {
		t.Fatal(err)
	}
	retryAt := time.Now().UTC().Add(-time.Minute)
	item := &model.SubscriptionItem{
		SubscriptionID: sub.ID, SourceKey: "moviepilot:capacity", Status: model.SubscriptionItemStatusRetryWait,
		LastErrorCode: "downloader_capacity_unavailable", LastError: "all qB routes are below the configured download safety margin or concurrency limit",
		RetryAt: &retryAt,
	}
	if err := db.GetDb().Create(item).Error; err != nil {
		t.Fatal(err)
	}
	action, err := subscriptionExecutionFollowupActionFor(context.Background(), sub.ID)
	if err != nil {
		t.Fatal(err)
	}
	if action != subscriptionFollowupNormalRun {
		t.Fatalf("follow-up action = %q, want %q", action, subscriptionFollowupNormalRun)
	}
	followup, err := SubscriptionNeedsExecutionFollowup(context.Background(), sub.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !followup {
		t.Fatal("MoviePilot capacity wait should trigger a follow-up")
	}
}

func TestSubscriptionExecutionFollowupKeepsDurableClusterJobReplay(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	sub := &model.Subscription{Name: "Cluster capacity", TMDBName: "Cluster capacity", Active: true, TransferEnabled: true}
	if err := db.CreateSubscription(sub); err != nil {
		t.Fatal(err)
	}
	retryAt := time.Now().UTC().Add(-time.Minute)
	item := &model.SubscriptionItem{
		SubscriptionID: sub.ID, SourceKey: "cluster:capacity", ClusterJobID: "cluster-job-1", Status: model.SubscriptionItemStatusRetryWait,
		LastErrorCode: "worker_capacity_unavailable", LastError: "worker capacity unavailable", RetryAt: &retryAt,
	}
	if err := db.GetDb().Create(item).Error; err != nil {
		t.Fatal(err)
	}
	action, err := subscriptionExecutionFollowupActionFor(context.Background(), sub.ID)
	if err != nil {
		t.Fatal(err)
	}
	if action != subscriptionFollowupClusterJob {
		t.Fatalf("follow-up action = %q, want %q", action, subscriptionFollowupClusterJob)
	}
}

func TestReconcileSubscriptionExecutionClassifiesRecoverableFailures(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	sub := &model.Subscription{Name: "classify", TMDBName: "classify", LastStatus: model.SubscriptionStatusRunning}
	if err := db.CreateSubscription(sub); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	cases := []struct {
		key      string
		code     string
		message  string
		want     string
		wantCode string
	}{
		{key: "retry", code: "share_save_retryable", want: model.SubscriptionItemStatusRetryWait, wantCode: "share_save_retryable"},
		{key: "capacity", code: "worker_capacity_unavailable", want: model.SubscriptionItemStatusRetryWait, wantCode: "worker_capacity_unavailable"},
		{key: "start-timeout", code: "worker_start_timeout", want: model.SubscriptionItemStatusRetryWait, wantCode: "worker_start_timeout"},
		{key: "unknown", code: "share_save_result_unknown", want: model.SubscriptionItemStatusUnknown, wantCode: "share_save_result_unknown"},
		{key: "blocked", code: "no_compatible_worker", want: model.SubscriptionItemStatusBlocked, wantCode: "no_compatible_worker"},
		{key: "direct-reauthorize", code: "direct_share_reauthorize", want: model.SubscriptionItemStatusBlocked, wantCode: "direct_share_reauthorize"},
		{key: "generic-eof", code: "worker_execution_failed", message: "failed to read all data: unexpected EOF", want: model.SubscriptionItemStatusRetryWait, wantCode: "worker_execution_failed"},
		{key: "generic-confirmation", code: "worker_execution_failed", message: "同名文件，是否继续？", want: model.SubscriptionItemStatusBlocked, wantCode: "worker_execution_failed"},
		{key: "generic-policy", code: "worker_execution_failed", message: "文件涉及违规内容", want: model.SubscriptionItemStatusFailed, wantCode: "worker_execution_failed"},
	}
	for i, tc := range cases {
		item := &model.SubscriptionItem{
			SubscriptionID: sub.ID, SourceKey: tc.key, FileHash: tc.key, Status: model.SubscriptionItemStatusFailed,
			ClusterJobID: "job-" + tc.key, LastSeenAt: now,
		}
		if err := db.GetDb().Create(item).Error; err != nil {
			t.Fatal(err)
		}
		job := &model.ClusterJob{
			ID: "job-" + tc.key, Type: model.ClusterJobTypeMediaTransfer, Status: model.ClusterJobStatusFailed,
			IdempotencyKey: "idem-" + tc.key, SubscriptionID: sub.ID, SubscriptionItemID: item.ID,
			LastErrorCode: tc.code, LastError: tc.message, AvailableAt: now.Add(time.Duration(i) * time.Second), FinishedAt: &now,
		}
		if job.LastError == "" {
			job.LastError = "safe test error"
		}
		if err := db.GetDb().Create(job).Error; err != nil {
			t.Fatal(err)
		}
	}
	if _, err := ReconcileSubscriptionExecution(context.Background(), sub.ID); err != nil {
		t.Fatal(err)
	}
	for _, tc := range cases {
		var item model.SubscriptionItem
		if err := db.GetDb().Where("subscription_id = ? AND source_key = ?", sub.ID, tc.key).First(&item).Error; err != nil {
			t.Fatal(err)
		}
		if item.Status != tc.want || item.LastErrorCode != tc.wantCode {
			t.Fatalf("%s item = %#v, want status=%q code=%q", tc.key, item, tc.want, tc.wantCode)
		}
	}
}
