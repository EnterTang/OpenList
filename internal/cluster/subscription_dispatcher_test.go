package cluster

import (
	"encoding/json"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/subscription"
)

func TestSubscriptionMediaTaskContextCarriesProviderTargets(t *testing.T) {
	task := subscription.ClusterMediaTask{
		SubscriptionID:        11,
		SubscriptionItemID:    22,
		SubscriptionName:      "Example",
		SourceKey:             "source-1",
		SourceMessageID:       "9001",
		ShareProvider:         "pan123",
		ShareURL:              "https://www.123pan.com/s/example",
		SharePasscode:         "1234",
		ShareRefFingerprint:   "share-ref",
		SourceFileID:          "file-1",
		SourceRelativePath:    "Example.S01E01.mkv",
		SourceSize:            7 << 30,
		SourceHash:            "hash-1",
		MediaItemID:           "media-1",
		MediaType:             "tv",
		TMDBID:                123,
		Season:                1,
		Episode:               1,
		TempTarget:            model.SubscriptionStorageTarget{Provider: "pan115", Folder: "任务专属临时目录"},
		DeliveryTarget:        model.SubscriptionStorageTarget{Provider: "yidong139", Folder: "订阅目标/港台剧"},
		LogicalMediaRoot:      "/139_60t/港台剧",
		LogicalTargetPath:     "/139_60t/港台剧/Example/Season 01/Example.S01E01.mkv",
		WorkflowVersion:       "workflow-1",
		SealedManifestVersion: "manifest-1",
	}

	context := subscriptionMediaTaskContext(task, "mobile-primary")
	if context.StagingTarget.Provider != "pan115" || context.StagingTarget.Folder != "任务专属临时目录" {
		t.Fatalf("staging target = %#v", context.StagingTarget)
	}
	if context.DeliveryTarget.Provider != "yidong139" || context.DeliveryTarget.Folder != "订阅目标/港台剧" {
		t.Fatalf("delivery target = %#v", context.DeliveryTarget)
	}
	if !context.StagingTarget.NeedShareSave || !context.DeliveryTarget.NeedUpload {
		t.Fatalf("target flags = staging:%#v delivery:%#v", context.StagingTarget, context.DeliveryTarget)
	}
}

func TestDispatcherWireContextPreservesWorkerProviderAccountBindings(t *testing.T) {
	task := subscription.ClusterMediaTask{
		ShareProvider: "pan123", SourceSize: 4 << 30,
		TempTarget:     model.SubscriptionStorageTarget{Provider: "pan123", Folder: "转存至移动"},
		DeliveryTarget: model.SubscriptionStorageTarget{Provider: "yidong139", Folder: "剧集"},
	}
	context := subscriptionMediaTaskContext(task, "/worker-139")
	bindTaskContextProviderAccounts(&context, nodeProviderAccountMatch{
		Staging:  protocol.ProviderAccountInventory{StorageID: 12, NodeMountID: "mount-123", AccountFingerprint: "account-123"},
		Delivery: protocol.ProviderAccountInventory{StorageID: 39, NodeMountID: "mount-139", AccountFingerprint: "account-139"},
	})
	raw, err := json.Marshal(context)
	if err != nil {
		t.Fatal(err)
	}
	var workerContext protocol.TaskContext
	if err := json.Unmarshal(raw, &workerContext); err != nil {
		t.Fatal(err)
	}
	if workerContext.StagingTarget.StorageID != 12 || workerContext.StagingTarget.NodeMountID != "mount-123" || workerContext.StagingTarget.AccountFingerprint != "account-123" {
		t.Fatalf("worker staging binding = %#v", workerContext.StagingTarget)
	}
	if workerContext.DeliveryTarget.StorageID != 39 || workerContext.DeliveryTarget.NodeMountID != "mount-139" || workerContext.DeliveryTarget.AccountFingerprint != "account-139" {
		t.Fatalf("worker delivery binding = %#v", workerContext.DeliveryTarget)
	}
}
