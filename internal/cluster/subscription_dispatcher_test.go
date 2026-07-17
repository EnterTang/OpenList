package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/coordinator"
	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/subscription"
)

type inspectObservationDispatcher struct {
	tasks []subscription.ClusterMediaTask
}

func (d *inspectObservationDispatcher) DispatchSubscriptionInspect(context.Context, subscription.ClusterInspectTask) (string, error) {
	return "", nil
}

func (d *inspectObservationDispatcher) DispatchSubscriptionMedia(_ context.Context, tasks []subscription.ClusterMediaTask) ([]subscription.ClusterDispatchResult, error) {
	d.tasks = append(d.tasks, tasks...)
	results := make([]subscription.ClusterDispatchResult, 0, len(tasks))
	for _, task := range tasks {
		results = append(results, subscription.ClusterDispatchResult{SourceKey: task.SourceKey, JobID: "job-" + task.SourceKey})
	}
	return results, nil
}

func TestSubscriptionMediaTaskContextUsesWorkerManagedProviderTargets(t *testing.T) {
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
		LogicalMediaRoot:      "/139_60t/港台剧",
		LogicalTargetPath:     "/139_60t/港台剧/Example/Season 01/Example.S01E01.mkv",
		PreferredWorkerNodeID: "worker-139",
		WorkflowVersion:       "workflow-1",
		SealedManifestVersion: "manifest-1",
	}

	context := subscriptionMediaTaskContext(task, "mobile-primary")
	if context.StagingTarget.Provider != "pan123" || context.StagingTarget.Folder != "" {
		t.Fatalf("staging target = %#v", context.StagingTarget)
	}
	if context.DeliveryTarget.Provider != "yidong139" || context.DeliveryTarget.Folder != "" {
		t.Fatalf("delivery target = %#v", context.DeliveryTarget)
	}
	if !context.StagingTarget.NeedShareSave || !context.DeliveryTarget.NeedUpload {
		t.Fatalf("target flags = staging:%#v delivery:%#v", context.StagingTarget, context.DeliveryTarget)
	}
	if context.Subscription.PreferredWorkerNodeID != "worker-139" {
		t.Fatalf("wire preferred worker = %q", context.Subscription.PreferredWorkerNodeID)
	}
}

func TestDispatcherWireContextPreservesWorkerProviderAccountBindings(t *testing.T) {
	task := subscription.ClusterMediaTask{
		ShareProvider: "pan123", SourceSize: 4 << 30,
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

func TestConsumeSubscriptionShareInspectWaitsForObservationAndSelectsLargest(t *testing.T) {
	database := openClusterRuntimeTestDB(t)
	originalConfig := conf.Conf
	conf.Conf = conf.DefaultConfig(t.TempDir())
	t.Cleanup(func() { conf.Conf = originalConfig })
	db.Init(database)
	dispatcher := &inspectObservationDispatcher{}
	subscription.RegisterClusterDispatcher(dispatcher)
	t.Cleanup(func() { subscription.RegisterClusterDispatcher(nil) })

	sub := &model.Subscription{Name: "Example", TMDBName: "Example", SourceType: model.SubscriptionSourceManual, TransferEnabled: true, MediaType: "tv", TargetRoot: "/tv"}
	if err := db.CreateSubscription(sub); err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	const observationKey = "observation-1"
	makeTask := func(provider, shareURL string) protocol.TaskContext {
		return protocol.TaskContext{
			Subscription: protocol.SubscriptionTaskContext{
				SubscriptionID: sub.ID, SubscriptionName: sub.Name,
				ObservationKey: observationKey, ObservationExpected: 2,
			},
			Share: protocol.ShareTaskContext{Provider: provider, URL: shareURL},
		}
	}
	manifestOne := protocol.ShareInspectManifest{
		Objects: []protocol.SourceObject{{Provider: "quark", SourceFileID: "small", SourceRelativePath: "Example.S01E01.small.mkv", Size: 600}},
	}
	manifestTwo := protocol.ShareInspectManifest{
		Objects: []protocol.SourceObject{{Provider: "pan123", SourceFileID: "large", SourceRelativePath: "Example.S01E01.large.mkv", Size: 900}},
	}
	createRecord := func(id, jobID string, task protocol.TaskContext, manifest protocol.ShareInspectManifest) model.ClusterShareInspectManifest {
		taskJSON, _ := json.Marshal(task)
		if err := database.Create(&model.ClusterJob{ID: jobID, IdempotencyKey: jobID, Type: model.ClusterJobTypeShareInspect, SubscriptionID: sub.ID, TaskContextJSON: string(taskJSON)}).Error; err != nil {
			t.Fatalf("create job: %v", err)
		}
		payload, _ := json.Marshal(manifest)
		record := model.ClusterShareInspectManifest{
			ID: id, JobID: jobID, SubscriptionID: sub.ID,
			ObservationKey: observationKey, ObservationExpected: 2,
			PayloadJSON: string(payload), Status: model.ClusterShareInspectStatusPending, InspectedAt: time.Now().UTC(),
		}
		if err := database.Create(&record).Error; err != nil {
			t.Fatalf("create record: %v", err)
		}
		return record
	}
	recordOne := createRecord("record-1", "inspect-1", makeTask("quark", "https://pan.quark.cn/s/bc18e4ea5fb8"), manifestOne)
	if err := consumeSubscriptionShareInspect(context.Background(), recordOne, manifestOne); !errors.Is(err, coordinator.ErrShareInspectObservationIncomplete) {
		t.Fatalf("first manifest error = %v, want incomplete observation", err)
	}
	recordTwo := createRecord("record-2", "inspect-2", makeTask("pan123", "https://www.123pan.com/s/example"), manifestTwo)
	if err := consumeSubscriptionShareInspect(context.Background(), recordTwo, manifestTwo); err != nil {
		t.Fatalf("consume complete observation: %v", err)
	}
	if len(dispatcher.tasks) != 1 || dispatcher.tasks[0].SourceFileID != "large" || dispatcher.tasks[0].SourceSize != 900 {
		t.Fatalf("dispatched tasks = %#v, want largest episode only", dispatcher.tasks)
	}
	if err := database.First(&recordOne, "id = ?", recordOne.ID).Error; err != nil {
		t.Fatal(err)
	}
	if recordOne.Status != model.ClusterShareInspectStatusConsumed || recordOne.ConsumedAt == nil {
		t.Fatalf("first record = %#v, want consumed with batch", recordOne)
	}
}

func TestConsumeSubscriptionShareInspectDefersAndResumesIncompleteObservation(t *testing.T) {
	database := openClusterRuntimeTestDB(t)
	originalConfig := conf.Conf
	conf.Conf = conf.DefaultConfig(t.TempDir())
	t.Cleanup(func() { conf.Conf = originalConfig })
	db.Init(database)
	sub := &model.Subscription{Name: "Example", SourceType: model.SubscriptionSourcePanSou}
	if err := db.CreateSubscription(sub); err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	createJobAndRecord := func(jobID, observationKey string, expected int, createdAt time.Time) model.ClusterShareInspectManifest {
		task := protocol.TaskContext{
			Subscription: protocol.SubscriptionTaskContext{
				SubscriptionID: sub.ID, ObservationKey: observationKey, ObservationExpected: expected,
			},
			Share: protocol.ShareTaskContext{Provider: "quark", URL: "https://pan.quark.cn/s/" + jobID},
		}
		taskJSON, _ := json.Marshal(task)
		if err := database.Create(&model.ClusterJob{
			ID: jobID, IdempotencyKey: jobID, Type: model.ClusterJobTypeShareInspect,
			SubscriptionID: sub.ID, TaskContextJSON: string(taskJSON), CreatedAt: createdAt,
		}).Error; err != nil {
			t.Fatalf("create job: %v", err)
		}
		manifest := protocol.ShareInspectManifest{}
		payload, _ := json.Marshal(manifest)
		record := model.ClusterShareInspectManifest{
			ID: jobID + "-record", JobID: jobID, SubscriptionID: sub.ID,
			ObservationKey: observationKey, ObservationExpected: expected,
			PayloadJSON: string(payload), Status: model.ClusterShareInspectStatusPending,
			InspectedAt: createdAt, CreatedAt: createdAt,
		}
		if err := database.Create(&record).Error; err != nil {
			t.Fatalf("create record: %v", err)
		}
		return record
	}
	now := time.Now().UTC()
	firstRecord := createJobAndRecord("first-job", "observation", 2, now.Add(-time.Minute))

	if err := consumeSubscriptionShareInspect(context.Background(), firstRecord, protocol.ShareInspectManifest{}); !errors.Is(err, coordinator.ErrShareInspectObservationIncomplete) {
		t.Fatalf("consume incomplete observation: %v", err)
	}
	if err := database.First(&firstRecord, "id = ?", firstRecord.ID).Error; err != nil {
		t.Fatal(err)
	}
	if firstRecord.Status != model.ClusterShareInspectStatusIncomplete || firstRecord.ConsumedAt != nil {
		t.Fatalf("first record = %#v, want incomplete and out of pending queue", firstRecord)
	}

	secondRecord := createJobAndRecord("second-job", "observation", 2, now)
	if err := consumeSubscriptionShareInspect(context.Background(), secondRecord, protocol.ShareInspectManifest{}); err != nil {
		t.Fatalf("resume complete observation: %v", err)
	}
	if err := database.First(&firstRecord, "id = ?", firstRecord.ID).Error; err != nil {
		t.Fatal(err)
	}
	if firstRecord.Status != model.ClusterShareInspectStatusConsumed || firstRecord.ConsumedAt == nil {
		t.Fatalf("first record = %#v, want consumed after missing manifest arrived", firstRecord)
	}
}

func TestConsumeSubscriptionShareInspectCompletesWhenInspectJobFailedWithoutManifest(t *testing.T) {
	database := openClusterRuntimeTestDB(t)
	originalConfig := conf.Conf
	conf.Conf = conf.DefaultConfig(t.TempDir())
	t.Cleanup(func() { conf.Conf = originalConfig })
	db.Init(database)
	dispatcher := &inspectObservationDispatcher{}
	subscription.RegisterClusterDispatcher(dispatcher)
	t.Cleanup(func() { subscription.RegisterClusterDispatcher(nil) })

	sub := &model.Subscription{Name: "Example", TMDBName: "Example", SourceType: model.SubscriptionSourceManual, TransferEnabled: true, MediaType: "tv", TargetRoot: "/tv"}
	if err := db.CreateSubscription(sub); err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	const observationKey = "observation-failed"
	makeTask := func(jobID, provider, shareURL string) protocol.TaskContext {
		return protocol.TaskContext{
			Subscription: protocol.SubscriptionTaskContext{
				SubscriptionID: sub.ID, SubscriptionName: sub.Name,
				ObservationKey: observationKey, ObservationExpected: 2,
			},
			Share: protocol.ShareTaskContext{Provider: provider, URL: shareURL},
		}
	}
	createSucceededRecord := func(id, jobID string, task protocol.TaskContext, manifest protocol.ShareInspectManifest) model.ClusterShareInspectManifest {
		taskJSON, _ := json.Marshal(task)
		if err := database.Create(&model.ClusterJob{ID: jobID, IdempotencyKey: jobID, Type: model.ClusterJobTypeShareInspect, SubscriptionID: sub.ID, Status: model.ClusterJobStatusSucceeded, TaskContextJSON: string(taskJSON)}).Error; err != nil {
			t.Fatalf("create job: %v", err)
		}
		payload, _ := json.Marshal(manifest)
		record := model.ClusterShareInspectManifest{
			ID: id, JobID: jobID, SubscriptionID: sub.ID,
			ObservationKey: observationKey, ObservationExpected: 2,
			PayloadJSON: string(payload), Status: model.ClusterShareInspectStatusPending, InspectedAt: time.Now().UTC(),
		}
		if err := database.Create(&record).Error; err != nil {
			t.Fatalf("create record: %v", err)
		}
		return record
	}
	manifest := protocol.ShareInspectManifest{
		Objects: []protocol.SourceObject{{Provider: "pan123", SourceFileID: "ok", SourceRelativePath: "Example.S01E01.mkv", Size: 500}},
	}
	record := createSucceededRecord("record-ok", "inspect-ok", makeTask("inspect-ok", "pan123", "https://www.123pan.com/s/ok"), manifest)
	taskJSON, _ := json.Marshal(makeTask("inspect-failed", "quark", "https://pan.quark.cn/s/dead"))
	if err := database.Create(&model.ClusterJob{
		ID: "inspect-failed", IdempotencyKey: "inspect-failed", Type: model.ClusterJobTypeShareInspect,
		SubscriptionID: sub.ID, Status: model.ClusterJobStatusFailed, TaskContextJSON: string(taskJSON),
		LastError: "分享地址已失效",
	}).Error; err != nil {
		t.Fatalf("create failed job: %v", err)
	}
	if err := consumeSubscriptionShareInspect(context.Background(), record, manifest); err != nil {
		t.Fatalf("consume observation with terminal failed inspect: %v", err)
	}
	if len(dispatcher.tasks) != 1 {
		t.Fatalf("dispatched tasks = %#v, want one media task", dispatcher.tasks)
	}
}
