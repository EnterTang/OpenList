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
