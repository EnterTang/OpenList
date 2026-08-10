package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
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

func TestDispatchSubscriptionMedia_AttachesSameShareSaveBatchToSiblingTasks(t *testing.T) {
	database := openClusterRuntimeTestDB(t)
	configureProviderPipelineDB(t, database)
	createProviderPipelineInventory(t, database, "worker-a", preferredWorkerTestAccounts(500))

	transport := &providerPipelineTransport{nodes: []string{"worker-a"}}
	dispatcher := subscriptionDispatcher{runtime: &Runtime{dispatchTransport: transport}}

	first := validProviderPipelineTask(20 << 30)
	first.IdempotencyKey = "batch-task-1"
	first.SubscriptionItemID = 201
	first.SourceKey = "source-1"
	first.SourceFileID = "file-1"
	first.SourceRelativePath = "Season 01/Example.S01E01.mkv"
	first.SharePasscode = "2468"
	first.ShareRefFingerprint = "share-ref-1"
	first.PreferredWorkerNodeID = "worker-a"

	second := first
	second.IdempotencyKey = "batch-task-2"
	second.SubscriptionItemID = 202
	second.SourceKey = "source-2"
	second.SourceFileID = "file-2"
	second.SourceRelativePath = "Season 01/Example.S01E02.mkv"
	second.LogicalTargetPath = "/legacy/episode-2.mkv"

	results, err := dispatcher.DispatchSubscriptionMedia(t.Context(), []subscription.ClusterMediaTask{second, first})
	if err != nil {
		t.Fatalf("dispatch sibling tasks: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("dispatch results = %#v", results)
	}
	offers := decodeMediaOffersBySourceFileID(t, transport.sent)
	if len(offers) != 2 {
		t.Fatalf("offers = %#v, want 2", offers)
	}

	offerOne := offers["file-1"]
	offerTwo := offers["file-2"]
	if offerOne.TaskContext.ShareSaveKey == "" || offerOne.TaskContext.ShareSaveKey != offerTwo.TaskContext.ShareSaveKey {
		t.Fatalf("share-save keys = %q and %q, want same non-empty key", offerOne.TaskContext.ShareSaveKey, offerTwo.TaskContext.ShareSaveKey)
	}
	if strings.Contains(offerOne.TaskContext.ShareSaveKey, first.SharePasscode) {
		t.Fatalf("share-save key leaked passcode: %q", offerOne.TaskContext.ShareSaveKey)
	}
	if len(offerOne.TaskContext.SourceObjects) != 1 || offerOne.TaskContext.SourceObjects[0].SourceFileID != "file-1" {
		t.Fatalf("offer one primary source objects = %#v", offerOne.TaskContext.SourceObjects)
	}
	if len(offerTwo.TaskContext.SourceObjects) != 1 || offerTwo.TaskContext.SourceObjects[0].SourceFileID != "file-2" {
		t.Fatalf("offer two primary source objects = %#v", offerTwo.TaskContext.SourceObjects)
	}
	assertShareSaveObjects(t, offerOne.TaskContext.ShareSaveObjects, []string{"file-1", "file-2"})
	assertShareSaveObjects(t, offerTwo.TaskContext.ShareSaveObjects, []string{"file-1", "file-2"})
}

func TestDispatchSubscriptionMedia_PreflightFailsWithoutCoordinatorOrConnectedWorker(t *testing.T) {
	task := validProviderPipelineTask(20 << 30)

	for _, tt := range []struct {
		name      string
		runtime   *Runtime
		wantError string
	}{
		{
			name:      "coordinator disabled",
			runtime:   &Runtime{},
			wantError: "cluster coordinator is disabled",
		},
		{
			name:      "no connected worker",
			runtime:   &Runtime{dispatchTransport: &providerPipelineTransport{}},
			wantError: "no cluster worker is connected",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			database := openClusterRuntimeTestDB(t)
			configureProviderPipelineDB(t, database)

			results, err := (subscriptionDispatcher{runtime: tt.runtime}).DispatchSubscriptionMedia(t.Context(), []subscription.ClusterMediaTask{task})
			if err == nil || err.Error() != tt.wantError {
				t.Fatalf("dispatch error = %v, want %q", err, tt.wantError)
			}
			if results != nil {
				t.Fatalf("dispatch results = %#v, want nil on preflight failure", results)
			}
			assertProviderPipelineCount(t, database, &model.ClusterJob{}, 0)
			assertProviderPipelineCount(t, database, &model.ClusterJobAttempt{}, 0)
			assertProviderPipelineCount(t, database, &model.ClusterOutbox{}, 0)
		})
	}
}

func TestDispatchSubscriptionMedia_PreflightRejectsPartialBatchWhenAnyTaskHasNoCompatibleWorker(t *testing.T) {
	database := openClusterRuntimeTestDB(t)
	configureProviderPipelineDB(t, database)
	createProviderPipelineInventory(t, database, "worker-a", preferredWorkerTestAccounts(500))

	transport := &providerPipelineTransport{nodes: []string{"worker-a"}}
	dispatcher := subscriptionDispatcher{runtime: &Runtime{dispatchTransport: transport}}

	assignable := validProviderPipelineTask(20 << 30)
	assignable.IdempotencyKey = "preflight-ok"
	assignable.SubscriptionItemID = 401
	assignable.SourceKey = "source-ok"
	assignable.SourceFileID = "file-ok"

	incompatible := validProviderPipelineTask(600 << 30)
	incompatible.IdempotencyKey = "preflight-incompatible"
	incompatible.SubscriptionItemID = 402
	incompatible.SourceKey = "source-incompatible"
	incompatible.SourceFileID = "file-incompatible"
	incompatible.LogicalTargetPath = "/legacy/episode-2.mkv"

	results, err := dispatcher.DispatchSubscriptionMedia(t.Context(), []subscription.ClusterMediaTask{assignable, incompatible})
	wantError := `subscription media task "source-incompatible" has no connected compatible cluster worker`
	if err == nil || err.Error() != wantError {
		t.Fatalf("dispatch error = %v, want %q", err, wantError)
	}
	if results != nil {
		t.Fatalf("dispatch results = %#v, want nil when preflight rejects the batch", results)
	}
	if len(transport.sent) != 0 {
		t.Fatalf("transport sent = %#v, want zero worker offers on preflight failure", transport.sent)
	}
	assertProviderPipelineCount(t, database, &model.ClusterJob{}, 0)
	assertProviderPipelineCount(t, database, &model.ClusterJobAttempt{}, 0)
	assertProviderPipelineCount(t, database, &model.ClusterOutbox{}, 0)
}

func TestDispatchSubscriptionMedia_SeparatesShareSaveBatchByShareAndTarget(t *testing.T) {
	database := openClusterRuntimeTestDB(t)
	configureProviderPipelineDB(t, database)
	createProviderPipelineInventory(t, database, "worker-a", preferredWorkerTestAccounts(500))
	createProviderPipelineInventory(t, database, "worker-b", preferredWorkerTestAccounts(500))

	transport := &providerPipelineTransport{nodes: []string{"worker-a", "worker-b"}}
	dispatcher := subscriptionDispatcher{runtime: &Runtime{dispatchTransport: transport}}

	sameShareWorkerA := validProviderPipelineTask(20 << 30)
	sameShareWorkerA.IdempotencyKey = "group-task-a"
	sameShareWorkerA.SubscriptionItemID = 301
	sameShareWorkerA.SourceKey = "source-a"
	sameShareWorkerA.SourceFileID = "file-a"
	sameShareWorkerA.SharePasscode = "2468"
	sameShareWorkerA.ShareRefFingerprint = "share-ref-1"
	sameShareWorkerA.PreferredWorkerNodeID = "worker-a"

	differentShareWorkerA := sameShareWorkerA
	differentShareWorkerA.IdempotencyKey = "group-task-b"
	differentShareWorkerA.SubscriptionItemID = 302
	differentShareWorkerA.SourceKey = "source-b"
	differentShareWorkerA.SourceFileID = "file-b"
	differentShareWorkerA.ShareURL = "https://www.123pan.com/s/other"
	differentShareWorkerA.ShareRefFingerprint = "share-ref-2"
	differentShareWorkerA.LogicalTargetPath = "/legacy/episode-b.mkv"

	sameShareWorkerB := sameShareWorkerA
	sameShareWorkerB.IdempotencyKey = "group-task-c"
	sameShareWorkerB.SubscriptionItemID = 303
	sameShareWorkerB.SourceKey = "source-c"
	sameShareWorkerB.SourceFileID = "file-c"
	sameShareWorkerB.PreferredWorkerNodeID = "worker-b"
	sameShareWorkerB.LogicalTargetPath = "/legacy/episode-c.mkv"

	if _, err := dispatcher.DispatchSubscriptionMedia(t.Context(), []subscription.ClusterMediaTask{
		sameShareWorkerA,
		differentShareWorkerA,
		sameShareWorkerB,
	}); err != nil {
		t.Fatalf("dispatch grouped tasks: %v", err)
	}
	offers := decodeMediaOffersBySourceFileID(t, transport.sent)
	if len(offers) != 3 {
		t.Fatalf("offers = %#v, want 3", offers)
	}

	keyShareA := offers["file-a"].TaskContext.ShareSaveKey
	keyShareB := offers["file-b"].TaskContext.ShareSaveKey
	keyWorkerB := offers["file-c"].TaskContext.ShareSaveKey
	if keyShareA == "" || keyShareB == "" || keyWorkerB == "" {
		t.Fatalf("share-save keys must all be set: a=%q b=%q c=%q", keyShareA, keyShareB, keyWorkerB)
	}
	if keyShareA == keyShareB {
		t.Fatalf("different share should not reuse batch key: %q", keyShareA)
	}
	if keyShareA == keyWorkerB {
		t.Fatalf("different worker target should not reuse batch key: %q", keyShareA)
	}
	assertShareSaveObjects(t, offers["file-a"].TaskContext.ShareSaveObjects, []string{"file-a"})
	assertShareSaveObjects(t, offers["file-b"].TaskContext.ShareSaveObjects, []string{"file-b"})
	assertShareSaveObjects(t, offers["file-c"].TaskContext.ShareSaveObjects, []string{"file-c"})
}

func TestAttachShareSaveBatchContext_SeparatesByDeliveryBinding(t *testing.T) {
	task := validProviderPipelineTask(20 << 30)
	task.SharePasscode = "2468"
	task.ShareRefFingerprint = "share-ref-1"

	firstContext := subscriptionMediaTaskContext(task, "mobile-primary")
	firstContext.StagingTarget.StorageID = 11
	firstContext.StagingTarget.NodeMountID = "staging-a"
	firstContext.StagingTarget.AccountFingerprint = "staging-fp"
	firstContext.DeliveryTarget.StorageID = 21
	firstContext.DeliveryTarget.NodeMountID = "delivery-a"
	firstContext.DeliveryTarget.AccountFingerprint = "delivery-fp-a"

	secondTask := task
	secondTask.SourceFileID = "file-2"
	secondTask.SourceKey = "source-2"
	secondTask.SubscriptionItemID = task.SubscriptionItemID + 1
	secondTask.IdempotencyKey = "delivery-binding-task-2"
	secondTask.LogicalTargetPath = "/legacy/episode-2.mkv"
	secondContext := subscriptionMediaTaskContext(secondTask, "mobile-primary")
	secondContext.StagingTarget.StorageID = 11
	secondContext.StagingTarget.NodeMountID = "staging-a"
	secondContext.StagingTarget.AccountFingerprint = "staging-fp"
	secondContext.DeliveryTarget.StorageID = 22
	secondContext.DeliveryTarget.NodeMountID = "delivery-b"
	secondContext.DeliveryTarget.AccountFingerprint = "delivery-fp-b"

	requests := []DispatchMediaJobRequest{
		{NodeID: "worker-a", TaskContext: firstContext},
		{NodeID: "worker-a", TaskContext: secondContext},
	}
	attachShareSaveBatchContext(requests)

	if requests[0].TaskContext.ShareSaveKey == "" || requests[1].TaskContext.ShareSaveKey == "" {
		t.Fatalf("share-save keys must be set: %#v", requests)
	}
	if requests[0].TaskContext.ShareSaveKey == requests[1].TaskContext.ShareSaveKey {
		t.Fatalf("different delivery bindings should produce different batch keys: %q", requests[0].TaskContext.ShareSaveKey)
	}
	assertShareSaveObjects(t, requests[0].TaskContext.ShareSaveObjects, []string{"file-1"})
	assertShareSaveObjects(t, requests[1].TaskContext.ShareSaveObjects, []string{"file-2"})
}

func TestAttachShareSaveBatchContext_MergesAcrossTargetProfileAliases(t *testing.T) {
	task := validProviderPipelineTask(20 << 30)
	task.SharePasscode = "2468"
	task.ShareRefFingerprint = "share-ref-1"

	firstContext := subscriptionMediaTaskContext(task, "mobile-primary")
	firstContext.StagingTarget.StorageID = 11
	firstContext.StagingTarget.NodeMountID = "staging-a"
	firstContext.StagingTarget.AccountFingerprint = "staging-fp"
	firstContext.DeliveryTarget.StorageID = 21
	firstContext.DeliveryTarget.NodeMountID = "delivery-a"
	firstContext.DeliveryTarget.AccountFingerprint = "delivery-fp-a"

	secondTask := task
	secondTask.SourceFileID = "file-2"
	secondTask.SourceKey = "source-2"
	secondTask.SubscriptionItemID = task.SubscriptionItemID + 1
	secondTask.IdempotencyKey = "target-profile-alias-task-2"
	secondTask.LogicalTargetPath = "/legacy/episode-2.mkv"
	secondContext := subscriptionMediaTaskContext(secondTask, "mobile-secondary-alias")
	secondContext.StagingTarget.StorageID = 11
	secondContext.StagingTarget.NodeMountID = "staging-a"
	secondContext.StagingTarget.AccountFingerprint = "staging-fp"
	secondContext.DeliveryTarget.StorageID = 21
	secondContext.DeliveryTarget.NodeMountID = "delivery-a"
	secondContext.DeliveryTarget.AccountFingerprint = "delivery-fp-a"

	requests := []DispatchMediaJobRequest{
		{NodeID: "worker-a", TaskContext: firstContext},
		{NodeID: "worker-a", TaskContext: secondContext},
	}
	attachShareSaveBatchContext(requests)

	if requests[0].TaskContext.ShareSaveKey == "" || requests[1].TaskContext.ShareSaveKey == "" {
		t.Fatalf("share-save keys must be set: %#v", requests)
	}
	if requests[0].TaskContext.ShareSaveKey != requests[1].TaskContext.ShareSaveKey {
		t.Fatalf("different target profile aliases should share one batch: %q != %q", requests[0].TaskContext.ShareSaveKey, requests[1].TaskContext.ShareSaveKey)
	}
	assertShareSaveObjects(t, requests[0].TaskContext.ShareSaveObjects, []string{"file-1", "file-2"})
	assertShareSaveObjects(t, requests[1].TaskContext.ShareSaveObjects, []string{"file-1", "file-2"})
}

func TestAttachShareSaveBatchContext_CanonicalizesDuplicateIdentityMetadata(t *testing.T) {
	task := validProviderPipelineTask(20 << 30)
	task.SharePasscode = "2468"
	task.ShareRefFingerprint = "share-ref-1"
	task.SourceFileID = "file-1"
	firstContext := subscriptionMediaTaskContext(task, "mobile-primary")
	firstContext.StagingTarget.StorageID = 11
	firstContext.StagingTarget.NodeMountID = "staging-a"
	firstContext.StagingTarget.AccountFingerprint = "staging-fp"
	firstContext.DeliveryTarget.StorageID = 21
	firstContext.DeliveryTarget.NodeMountID = "delivery-a"
	firstContext.DeliveryTarget.AccountFingerprint = "delivery-fp-a"
	firstContext.ParentBatchID = "batch-1"
	firstContext.SourceObjects = []protocol.SourceObject{{Provider: "pan123", SourceFileID: "file-1", SourceRelativePath: "z-path.mkv", Size: 200, Hash: "z"}}

	secondTask := task
	secondTask.SourceKey = "source-2"
	secondTask.SubscriptionItemID = task.SubscriptionItemID + 1
	secondTask.IdempotencyKey = "canonical-identity-task-2"
	secondTask.LogicalTargetPath = "/legacy/episode-2.mkv"
	secondTask.SourceFileID = "file-1"
	secondContext := subscriptionMediaTaskContext(secondTask, "mobile-alias")
	secondContext.StagingTarget.StorageID = 11
	secondContext.StagingTarget.NodeMountID = "staging-a"
	secondContext.StagingTarget.AccountFingerprint = "staging-fp"
	secondContext.DeliveryTarget.StorageID = 21
	secondContext.DeliveryTarget.NodeMountID = "delivery-a"
	secondContext.DeliveryTarget.AccountFingerprint = "delivery-fp-a"
	secondContext.ParentBatchID = "batch-1"
	secondContext.SourceObjects = []protocol.SourceObject{{Provider: "pan123", SourceFileID: "file-1", SourceRelativePath: "a-path.mkv", Size: 100, Hash: "a"}}

	requests := []DispatchMediaJobRequest{
		{NodeID: "worker-a", TaskContext: firstContext},
		{NodeID: "worker-a", TaskContext: secondContext},
	}
	attachShareSaveBatchContext(requests)

	for i, request := range requests {
		if len(request.TaskContext.ShareSaveObjects) != 1 {
			t.Fatalf("request %d share-save objects = %#v, want one canonical object", i, request.TaskContext.ShareSaveObjects)
		}
		if request.TaskContext.ShareSaveObjects[0].SourceRelativePath != "a-path.mkv" {
			t.Fatalf("request %d canonical object = %#v, want lexicographically stable metadata choice", i, request.TaskContext.ShareSaveObjects[0])
		}
		if err := request.TaskContext.Validate(); err != nil {
			t.Fatalf("request %d validate error = %v", i, err)
		}
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
	createJob := func(jobID string, task protocol.TaskContext) {
		taskJSON, _ := json.Marshal(task)
		if err := database.Create(&model.ClusterJob{ID: jobID, IdempotencyKey: jobID, Type: model.ClusterJobTypeShareInspect, SubscriptionID: sub.ID, TaskContextJSON: string(taskJSON)}).Error; err != nil {
			t.Fatalf("create job: %v", err)
		}
	}
	createRecord := func(id, jobID string, manifest protocol.ShareInspectManifest) model.ClusterShareInspectManifest {
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
	// Both child inspects are dispatched up front. When the weaker-priority
	// quark manifest lands first, the higher-priority pan123 sibling is still
	// non-terminal, so the episode slot must keep waiting instead of
	// dispatching the smaller quark file.
	createJob("inspect-1", makeTask("quark", "https://pan.quark.cn/s/bc18e4ea5fb8"))
	createJob("inspect-2", makeTask("pan123", "https://www.123pan.com/s/example"))

	recordOne := createRecord("record-1", "inspect-1", manifestOne)
	if err := consumeSubscriptionShareInspect(context.Background(), recordOne, manifestOne); !errors.Is(err, coordinator.ErrShareInspectObservationIncomplete) {
		t.Fatalf("first manifest error = %v, want incomplete observation", err)
	}
	if len(dispatcher.tasks) != 0 {
		t.Fatalf("dispatched tasks after first manifest = %#v, want none while higher-priority sibling is pending", dispatcher.tasks)
	}
	recordTwo := createRecord("record-2", "inspect-2", manifestTwo)
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

func decodeMediaOffersBySourceFileID(t *testing.T, messages []protocol.Envelope) map[string]protocol.JobOffer {
	t.Helper()
	offers := make(map[string]protocol.JobOffer, len(messages))
	for _, message := range messages {
		if message.Type != protocol.MessageJobOffer {
			continue
		}
		offer, err := protocol.DecodePayload[protocol.JobOffer](message)
		if err != nil {
			t.Fatalf("decode offer: %v", err)
		}
		if len(offer.TaskContext.SourceObjects) != 1 {
			t.Fatalf("offer primary source objects = %#v, want exactly one", offer.TaskContext.SourceObjects)
		}
		offers[offer.TaskContext.SourceObjects[0].SourceFileID] = offer
	}
	return offers
}

func assertShareSaveObjects(t *testing.T, objects []protocol.SourceObject, wantFileIDs []string) {
	t.Helper()
	if len(objects) != len(wantFileIDs) {
		t.Fatalf("share-save objects len = %d, want %d (%#v)", len(objects), len(wantFileIDs), objects)
	}
	for i, want := range wantFileIDs {
		if objects[i].SourceFileID != want {
			t.Fatalf("share-save objects[%d] = %q, want %q; full=%#v", i, objects[i].SourceFileID, want, objects)
		}
	}
}

func TestConsumeSubscriptionShareInspectDispatchesWhenPriorityClosed(t *testing.T) {
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
	const observationKey = "observation-priority"
	makeTask := func(provider, shareURL string) protocol.TaskContext {
		return protocol.TaskContext{
			Subscription: protocol.SubscriptionTaskContext{
				SubscriptionID: sub.ID, SubscriptionName: sub.Name,
				ObservationKey: observationKey, ObservationExpected: 2,
			},
			Share: protocol.ShareTaskContext{Provider: provider, URL: shareURL},
		}
	}
	createJob := func(jobID string, task protocol.TaskContext) {
		taskJSON, _ := json.Marshal(task)
		if err := database.Create(&model.ClusterJob{ID: jobID, IdempotencyKey: jobID, Type: model.ClusterJobTypeShareInspect, SubscriptionID: sub.ID, TaskContextJSON: string(taskJSON)}).Error; err != nil {
			t.Fatalf("create job: %v", err)
		}
	}
	// The pan123 child reports first; the quark sibling job is dispatched but
	// still non-terminal. Since quark can never outrank pan123 in the default
	// priority list, the episode slot must close and dispatch immediately
	// instead of waiting for the whole observation to finish.
	createJob("inspect-quark", makeTask("quark", "https://pan.quark.cn/s/bc18e4ea5fb8"))
	createJob("inspect-pan123", makeTask("pan123", "https://www.123pan.com/s/example"))

	manifest := protocol.ShareInspectManifest{
		Objects: []protocol.SourceObject{{Provider: "pan123", SourceFileID: "pan123-episode", SourceRelativePath: "Example.S01E01.mkv", Size: 500}},
	}
	payload, _ := json.Marshal(manifest)
	record := model.ClusterShareInspectManifest{
		ID: "record-pan123", JobID: "inspect-pan123", SubscriptionID: sub.ID,
		ObservationKey: observationKey, ObservationExpected: 2,
		PayloadJSON: string(payload), Status: model.ClusterShareInspectStatusPending, InspectedAt: time.Now().UTC(),
	}
	if err := database.Create(&record).Error; err != nil {
		t.Fatalf("create record: %v", err)
	}

	if err := consumeSubscriptionShareInspect(context.Background(), record, manifest); !errors.Is(err, coordinator.ErrShareInspectObservationIncomplete) {
		t.Fatalf("consume error = %v, want incomplete observation", err)
	}
	if len(dispatcher.tasks) != 1 || dispatcher.tasks[0].SourceFileID != "pan123-episode" || dispatcher.tasks[0].ShareProvider != "pan123" {
		t.Fatalf("dispatched tasks = %#v, want priority-closed pan123 dispatch", dispatcher.tasks)
	}
	if err := database.First(&record, "id = ?", record.ID).Error; err != nil {
		t.Fatal(err)
	}
	if record.Status != model.ClusterShareInspectStatusIncomplete {
		t.Fatalf("record status = %q, want incomplete while quark sibling is still pending", record.Status)
	}
}

func TestConsumeSubscriptionShareInspectMovieWaitsForHigherPrioritySibling(t *testing.T) {
	database := openClusterRuntimeTestDB(t)
	originalConfig := conf.Conf
	conf.Conf = conf.DefaultConfig(t.TempDir())
	t.Cleanup(func() { conf.Conf = originalConfig })
	db.Init(database)
	dispatcher := &inspectObservationDispatcher{}
	subscription.RegisterClusterDispatcher(dispatcher)
	t.Cleanup(func() { subscription.RegisterClusterDispatcher(nil) })

	sub := &model.Subscription{Name: "Movie", TMDBName: "Movie", SourceType: model.SubscriptionSourceManual, TransferEnabled: true, MediaType: "movie", TargetRoot: "/movies"}
	if err := db.CreateSubscription(sub); err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	const observationKey = "observation-movie-priority"
	makeTask := func(provider, shareURL string) protocol.TaskContext {
		return protocol.TaskContext{
			Subscription: protocol.SubscriptionTaskContext{
				SubscriptionID: sub.ID, SubscriptionName: sub.Name,
				ObservationKey: observationKey, ObservationExpected: 2,
			},
			Share: protocol.ShareTaskContext{Provider: provider, URL: shareURL},
		}
	}
	createJob := func(jobID string, task protocol.TaskContext) {
		taskJSON, _ := json.Marshal(task)
		if err := database.Create(&model.ClusterJob{ID: jobID, IdempotencyKey: jobID, Type: model.ClusterJobTypeShareInspect, SubscriptionID: sub.ID, TaskContextJSON: string(taskJSON)}).Error; err != nil {
			t.Fatalf("create job: %v", err)
		}
	}
	// The aliyun child reports first with a small file, well below the movie
	// size floor. The pan123 sibling job is dispatched but still
	// non-terminal, and pan123 outranks aliyun in the default priority list,
	// so the movie slot must keep waiting instead of dispatching aliyun's file.
	createJob("inspect-aliyun", makeTask("aliyun_drive", "https://www.alipan.com/s/example"))
	createJob("inspect-pan123", makeTask("pan123", "https://www.123pan.com/s/example"))

	manifest := protocol.ShareInspectManifest{
		Objects: []protocol.SourceObject{{Provider: "aliyun_drive", SourceFileID: "aliyun-movie", SourceRelativePath: "Movie.mkv", Size: 2 << 30}},
	}
	payload, _ := json.Marshal(manifest)
	record := model.ClusterShareInspectManifest{
		ID: "record-aliyun", JobID: "inspect-aliyun", SubscriptionID: sub.ID,
		ObservationKey: observationKey, ObservationExpected: 2,
		PayloadJSON: string(payload), Status: model.ClusterShareInspectStatusPending, InspectedAt: time.Now().UTC(),
	}
	if err := database.Create(&record).Error; err != nil {
		t.Fatalf("create record: %v", err)
	}

	if err := consumeSubscriptionShareInspect(context.Background(), record, manifest); !errors.Is(err, coordinator.ErrShareInspectObservationIncomplete) {
		t.Fatalf("consume error = %v, want incomplete observation", err)
	}
	if len(dispatcher.tasks) != 0 {
		t.Fatalf("dispatched tasks = %#v, want none while higher-priority movie sibling is pending", dispatcher.tasks)
	}
}

func TestConsumeSubscriptionShareInspectMovieDispatchesAtSizeFloor(t *testing.T) {
	database := openClusterRuntimeTestDB(t)
	originalConfig := conf.Conf
	conf.Conf = conf.DefaultConfig(t.TempDir())
	t.Cleanup(func() { conf.Conf = originalConfig })
	db.Init(database)
	dispatcher := &inspectObservationDispatcher{}
	subscription.RegisterClusterDispatcher(dispatcher)
	t.Cleanup(func() { subscription.RegisterClusterDispatcher(nil) })

	sub := &model.Subscription{Name: "Movie", TMDBName: "Movie", SourceType: model.SubscriptionSourceManual, TransferEnabled: true, MediaType: "movie", TargetRoot: "/movies"}
	if err := db.CreateSubscription(sub); err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	const observationKey = "observation-movie-floor"
	makeTask := func(provider, shareURL string) protocol.TaskContext {
		return protocol.TaskContext{
			Subscription: protocol.SubscriptionTaskContext{
				SubscriptionID: sub.ID, SubscriptionName: sub.Name,
				ObservationKey: observationKey, ObservationExpected: 2,
			},
			Share: protocol.ShareTaskContext{Provider: provider, URL: shareURL},
		}
	}
	createJob := func(jobID string, task protocol.TaskContext) {
		taskJSON, _ := json.Marshal(task)
		if err := database.Create(&model.ClusterJob{ID: jobID, IdempotencyKey: jobID, Type: model.ClusterJobTypeShareInspect, SubscriptionID: sub.ID, TaskContextJSON: string(taskJSON)}).Error; err != nil {
			t.Fatalf("create job: %v", err)
		}
	}
	// pan123 reports first with a file at (or above) the 20GiB movie floor.
	// The quark sibling job is dispatched but still non-terminal; the size
	// floor must close the slot immediately regardless of that pending
	// sibling.
	createJob("inspect-pan123", makeTask("pan123", "https://www.123pan.com/s/example"))
	createJob("inspect-quark", makeTask("quark", "https://pan.quark.cn/s/bc18e4ea5fb8"))

	manifest := protocol.ShareInspectManifest{
		Objects: []protocol.SourceObject{{Provider: "pan123", SourceFileID: "pan123-movie", SourceRelativePath: "Movie.mkv", Size: 21 << 30}},
	}
	payload, _ := json.Marshal(manifest)
	record := model.ClusterShareInspectManifest{
		ID: "record-pan123", JobID: "inspect-pan123", SubscriptionID: sub.ID,
		ObservationKey: observationKey, ObservationExpected: 2,
		PayloadJSON: string(payload), Status: model.ClusterShareInspectStatusPending, InspectedAt: time.Now().UTC(),
	}
	if err := database.Create(&record).Error; err != nil {
		t.Fatalf("create record: %v", err)
	}

	if err := consumeSubscriptionShareInspect(context.Background(), record, manifest); !errors.Is(err, coordinator.ErrShareInspectObservationIncomplete) {
		t.Fatalf("consume error = %v, want incomplete observation", err)
	}
	if len(dispatcher.tasks) != 1 || dispatcher.tasks[0].SourceFileID != "pan123-movie" || dispatcher.tasks[0].ShareProvider != "pan123" {
		t.Fatalf("dispatched tasks = %#v, want size-floor-closed pan123 movie dispatch", dispatcher.tasks)
	}
}

func TestConsumeSubscriptionShareInspectWaitsSameProviderBelowFloor(t *testing.T) {
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
	const observationKey = "observation-same-provider"
	makeTask := func(provider, shareURL string) protocol.TaskContext {
		return protocol.TaskContext{
			Subscription: protocol.SubscriptionTaskContext{
				SubscriptionID: sub.ID, SubscriptionName: sub.Name,
				ObservationKey: observationKey, ObservationExpected: 2,
			},
			Share: protocol.ShareTaskContext{Provider: provider, URL: shareURL},
		}
	}
	createJob := func(jobID string, task protocol.TaskContext) {
		taskJSON, _ := json.Marshal(task)
		if err := database.Create(&model.ClusterJob{ID: jobID, IdempotencyKey: jobID, Type: model.ClusterJobTypeShareInspect, SubscriptionID: sub.ID, TaskContextJSON: string(taskJSON)}).Error; err != nil {
			t.Fatalf("create job: %v", err)
		}
	}
	// Both children are pan123. The first manifest is below the default 1GiB
	// episode floor, and its sibling (same provider, still non-terminal)
	// could still report a larger file, so the slot must keep waiting.
	createJob("inspect-first", makeTask("pan123", "https://www.123pan.com/s/first"))
	createJob("inspect-second", makeTask("pan123", "https://www.123pan.com/s/second"))

	manifest := protocol.ShareInspectManifest{
		Objects: []protocol.SourceObject{{Provider: "pan123", SourceFileID: "pan123-partial", SourceRelativePath: "Example.S01E01.mkv", Size: 500 << 20}},
	}
	payload, _ := json.Marshal(manifest)
	record := model.ClusterShareInspectManifest{
		ID: "record-first", JobID: "inspect-first", SubscriptionID: sub.ID,
		ObservationKey: observationKey, ObservationExpected: 2,
		PayloadJSON: string(payload), Status: model.ClusterShareInspectStatusPending, InspectedAt: time.Now().UTC(),
	}
	if err := database.Create(&record).Error; err != nil {
		t.Fatalf("create record: %v", err)
	}

	if err := consumeSubscriptionShareInspect(context.Background(), record, manifest); !errors.Is(err, coordinator.ErrShareInspectObservationIncomplete) {
		t.Fatalf("consume error = %v, want incomplete observation", err)
	}
	if len(dispatcher.tasks) != 0 {
		t.Fatalf("dispatched tasks = %#v, want none while same-provider sibling is pending below the size floor", dispatcher.tasks)
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
