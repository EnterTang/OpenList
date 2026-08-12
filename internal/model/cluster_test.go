package model

import (
	"testing"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func TestClusterModelsAutoMigrate(t *testing.T) {
	database, err := gorm.Open(sqlite.Open("file:cluster_models?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() {
		sqlDB, err := database.DB()
		if err == nil {
			_ = sqlDB.Close()
		}
	})

	models := []any{
		new(ClusterNode),
		new(ClusterNodeSession),
		new(ClusterNodeInventory),
		new(ClusterCoordinatorLease),
		new(ClusterSecret),
		new(ClusterNodeDesiredConfig),
		new(ClusterStorageProfile),
		new(ClusterControlAudit),
		new(ClusterWorkerObservedState),
		new(ClusterJob),
		new(ClusterJobAttempt),
		new(ClusterJobStage),
		new(ClusterUploadManifest),
		new(ClusterShareInspectManifest),
		new(ClusterOutbox),
		new(ClusterInbox),
	}
	if err := database.AutoMigrate(models...); err != nil {
		t.Fatalf("auto migrate cluster models: %v", err)
	}
	for _, item := range models {
		if !database.Migrator().HasTable(item) {
			t.Fatalf("missing table for %T", item)
		}
	}

	firstOutbox := &ClusterOutbox{ID: "outbox-1", MessageID: "message-1", PeerNodeID: "node-1", SessionID: "session-1", Seq: 1}
	if err := database.Create(firstOutbox).Error; err != nil {
		t.Fatalf("create first outbox row: %v", err)
	}
	duplicateOutboxSeq := &ClusterOutbox{ID: "outbox-2", MessageID: "message-2", PeerNodeID: "node-1", SessionID: "session-2", Seq: 1}
	if err := database.Create(duplicateOutboxSeq).Error; err == nil {
		t.Fatal("duplicate cross-session outbox sequence was accepted")
	}

	firstInbox := &ClusterInbox{ID: "inbox-1", MessageID: "message-3", PeerNodeID: "node-1", SessionID: "session-1", Seq: 1}
	if err := database.Create(firstInbox).Error; err != nil {
		t.Fatalf("create first inbox row: %v", err)
	}
	duplicateInboxSeq := &ClusterInbox{ID: "inbox-2", MessageID: "message-4", PeerNodeID: "node-1", SessionID: "session-2", Seq: 1}
	if err := database.Create(duplicateInboxSeq).Error; err != nil {
		t.Fatalf("cross-session inbox sequence should be accepted: %v", err)
	}
	duplicateSameSessionInboxSeq := &ClusterInbox{ID: "inbox-3", MessageID: "message-5", PeerNodeID: "node-1", SessionID: "session-2", Seq: 1}
	if err := database.Create(duplicateSameSessionInboxSeq).Error; err == nil {
		t.Fatal("duplicate same-session inbox sequence was accepted")
	}
}
