package subscription

import (
	"context"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestRunSubscriptionRetentionDeletesOnlyExpiredTerminalRecords(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	if !db.GetDb().Migrator().HasIndex(&model.SubscriptionTelegramEvent{}, "idx_subscription_telegram_events_retention_processed") {
		t.Fatal("missing event retention index")
	}
	if !db.GetDb().Migrator().HasIndex(&model.ClusterInbox{}, "idx_cluster_inbox_retention") {
		t.Fatal("missing cluster inbox retention index")
	}
	now := time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC)
	old := now.Add(-30 * 24 * time.Hour)
	recent := now.Add(-time.Hour)
	processedAt := old
	recentProcessedAt := recent

	events := []model.SubscriptionTelegramEvent{
		{SubscriptionID: 1, Channel: "source", MessageID: "old-processed", Status: model.SubscriptionTelegramEventStatusProcessed, CreatedAt: old, UpdatedAt: old, ProcessedAt: &processedAt},
		{SubscriptionID: 1, Channel: "source", MessageID: "old-dead", Status: model.SubscriptionTelegramEventStatusDeadLetter, CreatedAt: old, UpdatedAt: old},
		{SubscriptionID: 1, Channel: "source", MessageID: "old-pending", Status: model.SubscriptionTelegramEventStatusPending, CreatedAt: old, UpdatedAt: old},
		{SubscriptionID: 1, Channel: "source", MessageID: "recent-processed", Status: model.SubscriptionTelegramEventStatusProcessed, CreatedAt: recent, UpdatedAt: recent, ProcessedAt: &recentProcessedAt},
	}
	for i := range events {
		if err := db.GetDb().Create(&events[i]).Error; err != nil {
			t.Fatalf("create event %q: %v", events[i].MessageID, err)
		}
	}

	oldInboxProcessedAt := old
	recentInboxProcessedAt := recent
	inboxes := []model.ClusterInbox{
		{ID: "old-processed", MessageID: "old-processed", PeerNodeID: "node", SessionID: "session", Seq: 1, Status: model.ClusterMessageStatusProcessed, CreatedAt: old, UpdatedAt: old, ReceivedAt: old, ProcessedAt: &oldInboxProcessedAt},
		{ID: "old-failed", MessageID: "old-failed", PeerNodeID: "node", SessionID: "session", Seq: 2, Status: model.ClusterMessageStatusFailed, CreatedAt: old, UpdatedAt: old, ReceivedAt: old},
		{ID: "recent-processed", MessageID: "recent-processed", PeerNodeID: "node", SessionID: "session", Seq: 3, Status: model.ClusterMessageStatusProcessed, CreatedAt: recent, UpdatedAt: recent, ReceivedAt: recent, ProcessedAt: &recentInboxProcessedAt},
	}
	for i := range inboxes {
		if err := db.GetDb().Create(&inboxes[i]).Error; err != nil {
			t.Fatalf("create inbox %q: %v", inboxes[i].MessageID, err)
		}
	}

	report, err := RunSubscriptionRetention(context.Background(), RetentionOptions{
		BatchSize:         2,
		EventTerminalAge:  7 * 24 * time.Hour,
		InboxProcessedAge: 14 * 24 * time.Hour,
		Now:               now,
	})
	if err != nil {
		t.Fatalf("run retention: %v", err)
	}
	if report.EventDeleted != 2 || report.InboxDeleted != 1 {
		t.Fatalf("retention report = %#v, want 2 events and 1 inbox row deleted", report)
	}

	var remainingEvents []model.SubscriptionTelegramEvent
	if err := db.GetDb().Order("message_id ASC").Find(&remainingEvents).Error; err != nil {
		t.Fatalf("list remaining events: %v", err)
	}
	if len(remainingEvents) != 2 || remainingEvents[0].MessageID != "old-pending" || remainingEvents[1].MessageID != "recent-processed" {
		t.Fatalf("remaining events = %#v", remainingEvents)
	}

	var remainingInbox []model.ClusterInbox
	if err := db.GetDb().Order("message_id ASC").Find(&remainingInbox).Error; err != nil {
		t.Fatalf("list remaining inbox rows: %v", err)
	}
	if len(remainingInbox) != 2 || remainingInbox[0].MessageID != "old-failed" || remainingInbox[1].MessageID != "recent-processed" {
		t.Fatalf("remaining inbox rows = %#v", remainingInbox)
	}
}

func TestRunSubscriptionRetentionDryRunIsIdempotent(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	now := time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC)
	processedAt := now.Add(-10 * 24 * time.Hour)
	event := model.SubscriptionTelegramEvent{
		SubscriptionID: 1,
		Channel:        "source",
		MessageID:      "dry-run",
		Status:         model.SubscriptionTelegramEventStatusProcessed,
		CreatedAt:      processedAt,
		UpdatedAt:      processedAt,
		ProcessedAt:    &processedAt,
	}
	if err := db.GetDb().Create(&event).Error; err != nil {
		t.Fatalf("create event: %v", err)
	}

	for i := 0; i < 2; i++ {
		report, err := RunSubscriptionRetention(context.Background(), RetentionOptions{
			DryRun:            true,
			BatchSize:         10,
			EventTerminalAge:  7 * 24 * time.Hour,
			InboxProcessedAge: 14 * 24 * time.Hour,
			Now:               now,
		})
		if err != nil {
			t.Fatalf("dry-run retention: %v", err)
		}
		if report.EventEligible != 1 || report.EventDeleted != 0 {
			t.Fatalf("dry-run report = %#v", report)
		}
	}

	var count int64
	if err := db.GetDb().Model(&model.SubscriptionTelegramEvent{}).Count(&count).Error; err != nil {
		t.Fatalf("count events after dry-run: %v", err)
	}
	if count != 1 {
		t.Fatalf("event count after dry-run = %d, want 1", count)
	}
}

func TestRunSubscriptionRetentionHonorsCancellation(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := RunSubscriptionRetention(ctx, RetentionOptions{}); err != context.Canceled {
		t.Fatalf("canceled retention error = %v, want context.Canceled", err)
	}
}
