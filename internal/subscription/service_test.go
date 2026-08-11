package subscription

import (
	"context"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestFinishClusterRetryProjectsDetailedRunCounts(t *testing.T) {
	setupSubscriptionRuntimeDB(t)

	sub := &model.Subscription{
		Name:       "retry projection",
		TMDBName:   "retry projection",
		LastStatus: model.SubscriptionStatusRunning,
	}
	if err := db.CreateSubscription(sub); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	statuses := []string{
		model.SubscriptionItemStatusPending,
		model.SubscriptionItemStatusNotifying,
		model.SubscriptionItemStatusTransferring,
		model.SubscriptionItemStatusTransferred,
		model.SubscriptionItemStatusSkipped,
		model.SubscriptionItemStatusRetryWait,
		model.SubscriptionItemStatusBlocked,
		model.SubscriptionItemStatusUnknown,
		model.SubscriptionItemStatusFailed,
	}
	for i, status := range statuses {
		item := &model.SubscriptionItem{
			SubscriptionID: sub.ID,
			SourceKey:      status,
			FileHash:       status,
			Status:         status,
			LastSeenAt:     now.Add(time.Duration(i) * time.Second),
		}
		if err := db.GetDb().Create(item).Error; err != nil {
			t.Fatal(err)
		}
	}

	result, err := finishClusterRetry(sub.ID, 2)
	if err != nil {
		t.Fatal(err)
	}
	if result == nil || result.Run == nil {
		t.Fatalf("result = %#v, want persisted run", result)
	}
	run := result.Run
	if run.QueuedCount != 2 || run.DispatchedCount != 2 {
		t.Fatalf("queued/dispatched = %d/%d, want 2/2", run.QueuedCount, run.DispatchedCount)
	}
	if run.DiscoveredCount != len(statuses) {
		t.Fatalf("discovered = %d, want %d", run.DiscoveredCount, len(statuses))
	}
	if run.SucceededCount != 1 || run.SkippedCount != 1 || run.RetryableCount != 1 || run.BlockedCount != 1 || run.UnknownCount != 1 || run.FailedCount != 1 {
		t.Fatalf("run counts = %#v", run)
	}
	if run.TransferStatus == model.SubscriptionStatusSuccess {
		t.Fatalf("transfer status = %q, want non-success while retry_wait/blocked/unknown/pending work remains", run.TransferStatus)
	}
	if run.CompletionState != "unknown" {
		t.Fatalf("completion state = %q, want unknown", run.CompletionState)
	}
}

func TestRunProjectsPendingDiscoveryWithoutSuccess(t *testing.T) {
	setupSubscriptionRuntimeDB(t)

	oldSnapshot := snapshotPaths
	snapshotPaths = func(context.Context, []string) (*TreeSnapshot, error) {
		return &TreeSnapshot{
			Entries: []TreeEntry{{
				RootPath: "/library",
				Path:     "/Show.S01E01.mkv",
				Name:     "Show.S01E01.mkv",
				ID:       "entry-1",
				Size:     1024,
			}},
			Hash: "hash-1",
		}, nil
	}
	t.Cleanup(func() { snapshotPaths = oldSnapshot })

	sub := &model.Subscription{
		Name:         "Show",
		TMDBName:     "Show",
		SourceType:   model.SubscriptionSourceManual,
		SourceConfig: `{"paths":["/library"]}`,
	}
	if err := db.CreateSubscription(sub); err != nil {
		t.Fatal(err)
	}

	result, err := Run(context.Background(), sub.ID, false)
	if err != nil {
		t.Fatal(err)
	}
	if result == nil || result.Run == nil {
		t.Fatalf("result = %#v, want run summary", result)
	}
	run := result.Run
	if run.DiscoveredCount != 1 {
		t.Fatalf("discovered = %d, want 1", run.DiscoveredCount)
	}
	if run.DispatchedCount != 0 || run.SucceededCount != 0 || run.SkippedCount != 0 || run.RetryableCount != 0 || run.BlockedCount != 0 || run.UnknownCount != 0 || run.FailedCount != 0 {
		t.Fatalf("unexpected counts = %#v", run)
	}
	if run.DiscoverStatus != model.SubscriptionStatusSuccess {
		t.Fatalf("discover status = %q, want success", run.DiscoverStatus)
	}
	if run.TransferStatus == model.SubscriptionStatusSuccess {
		t.Fatalf("transfer status = %q, want non-success for pending-only discovery", run.TransferStatus)
	}
	if run.CompletionState != "scanning" {
		t.Fatalf("completion state = %q, want scanning", run.CompletionState)
	}
	if run.Status != model.SubscriptionStatusRunning {
		t.Fatalf("run status = %q, want running", run.Status)
	}
}
