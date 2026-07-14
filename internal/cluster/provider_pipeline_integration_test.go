package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/OpenListTeam/OpenList/v4/internal/cluster/transport"
	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/subscription"
	"gorm.io/gorm"
)

func TestHybridDispatcherPersistsAndOffersHighestWeightProviderBindings(t *testing.T) {
	database := openClusterRuntimeTestDB(t)
	configureProviderPipelineDB(t, database)
	accounts := []protocol.ProviderAccountInventory{
		{StorageID: 1231, NodeMountID: "pan123-ordinary", Provider: "pan123", MountPath: "/123-ordinary", AccountFingerprint: "fp-123-ordinary", Status: "work", MembershipWeight: 100, SupportsShareSave: true, SupportsDownload: true, FreeBytes: 900 << 30},
		{StorageID: 1232, NodeMountID: "pan123-svip", Provider: "pan123", MountPath: "/123-svip", AccountFingerprint: "fp-123-svip", Status: "work", MembershipWeight: 300, SupportsShareSave: true, SupportsDownload: true, FreeBytes: 500 << 30},
		{StorageID: 1391, NodeMountID: "mobile-diamond", Provider: "yidong139", MountPath: "/139-diamond", AccountFingerprint: "fp-139", Status: "work", MembershipWeight: 400, MaxSingleUploadBytes: 500 << 30, SupportsUpload: true, SupportsETF: true, FreeBytes: 800 << 30},
	}
	createProviderPipelineInventory(t, database, "hybrid-local", accounts)
	fakeTransport := &providerPipelineTransport{nodes: []string{"hybrid-local"}}
	task := validProviderPipelineTask(20 << 30)
	task.IdempotencyKey = "hybrid-provider-pipeline"
	results, err := (subscriptionDispatcher{runtime: &Runtime{dispatchTransport: fakeTransport}}).DispatchSubscriptionMedia(t.Context(), []subscription.ClusterMediaTask{task})
	if err != nil || len(results) != 1 || results[0].JobID == "" {
		t.Fatalf("dispatch results = %#v, err=%v", results, err)
	}
	assertProviderPipelineCount(t, database, &model.ClusterJob{}, 2)
	assertProviderPipelineCount(t, database, &model.ClusterJobAttempt{}, 1)
	assertProviderPipelineCount(t, database, &model.ClusterOutbox{}, 1)
	if len(fakeTransport.sent) != 1 || fakeTransport.sent[0].Type != protocol.MessageJobOffer {
		t.Fatalf("sent messages = %#v", fakeTransport.sent)
	}
	offer, err := protocol.DecodePayload[protocol.JobOffer](fakeTransport.sent[0])
	if err != nil {
		t.Fatal(err)
	}
	if offer.TaskContext.StagingTarget.StorageID != 1232 || offer.TaskContext.StagingTarget.NodeMountID != "pan123-svip" {
		t.Fatalf("offered staging binding = %#v", offer.TaskContext.StagingTarget)
	}
	if offer.TaskContext.DeliveryTarget.StorageID != 1391 || offer.TaskContext.DeliveryTarget.NodeMountID != "mobile-diamond" {
		t.Fatalf("offered delivery binding = %#v", offer.TaskContext.DeliveryTarget)
	}
}

func TestOversize139DispatchCreatesNoClusterSideEffects(t *testing.T) {
	database := openClusterRuntimeTestDB(t)
	configureProviderPipelineDB(t, database)
	createProviderPipelineInventory(t, database, "worker-oversize", []protocol.ProviderAccountInventory{
		{StorageID: 1231, NodeMountID: "pan123", Provider: "pan123", MountPath: "/123", Status: "work", SupportsShareSave: true, SupportsDownload: true, FreeBytes: 100 << 30},
		{StorageID: 1391, NodeMountID: "mobile-ordinary", Provider: "yidong139", MountPath: "/139", Status: "work", MembershipWeight: 100, MaxSingleUploadBytes: 5 << 30, SupportsUpload: true, SupportsETF: true, FreeBytes: 100 << 30},
	})
	fakeTransport := &providerPipelineTransport{nodes: []string{"worker-oversize"}}
	task := validProviderPipelineTask(6 << 30)
	task.IdempotencyKey = "oversize-provider-pipeline"
	if _, err := (subscriptionDispatcher{runtime: &Runtime{dispatchTransport: fakeTransport}}).DispatchSubscriptionMedia(t.Context(), []subscription.ClusterMediaTask{task}); err == nil {
		t.Fatal("expected oversize dispatch rejection")
	}
	for _, table := range []any{&model.ClusterJob{}, &model.ClusterJobAttempt{}, &model.ClusterOutbox{}, &model.ClusterJobStage{}, &model.ClusterUploadManifest{}} {
		assertProviderPipelineCount(t, database, table, 0)
	}
	if len(fakeTransport.sent) != 0 {
		t.Fatalf("unexpected offer side effects: %#v", fakeTransport.sent)
	}
}

func configureProviderPipelineDB(t *testing.T, database *gorm.DB) {
	t.Helper()
	original := conf.Conf
	conf.Conf = conf.DefaultConfig(t.TempDir())
	t.Cleanup(func() { conf.Conf = original })
	db.Init(database)
}

func validProviderPipelineTask(size int64) subscription.ClusterMediaTask {
	return subscription.ClusterMediaTask{
		SubscriptionID: 1, SubscriptionItemID: 2, SubscriptionName: "pipeline", SourceKey: "source-1", IdempotencyKey: "pipeline-task",
		ShareProvider: "pan123", ShareURL: "https://www.123pan.com/s/test", SourceFileID: "file-1", SourceRelativePath: "episode.mkv", SourceSize: size,
		MediaItemID: "media-1", MediaType: "tv", LogicalMediaRoot: "/legacy", LogicalTargetPath: "/legacy/episode.mkv",
		WorkflowVersion: subscription.ClusterWorkflowVersion, SealedManifestVersion: subscription.ClusterSealedManifestVersion,
	}
}

func createProviderPipelineInventory(t *testing.T, database *gorm.DB, nodeID string, accounts []protocol.ProviderAccountInventory) {
	t.Helper()
	capabilities, _ := json.Marshal(protocol.NodeCapabilities{SupportedProviders: []string{"pan123", "yidong139"}, SupportedOperations: []string{"share.save", "mobile.upload", "result.report"}, RedisDurabilityReady: true})
	providerAccounts, _ := json.Marshal(accounts)
	if err := database.Create(&model.ClusterNode{ID: nodeID, Name: nodeID, Role: model.ClusterRoleHybrid, Status: model.ClusterNodeStatusOnline}).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&model.ClusterNodeInventory{ID: nodeID + "-inventory", NodeID: nodeID, Revision: 1, CollectedAt: time.Now().UTC(), CapabilitiesJSON: string(capabilities), ProviderAccountsJSON: string(providerAccounts), MountsJSON: "[]"}).Error; err != nil {
		t.Fatal(err)
	}
}

func assertProviderPipelineCount(t *testing.T, database *gorm.DB, table any, want int64) {
	t.Helper()
	var got int64
	if err := database.Model(table).Count(&got).Error; err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("count for %T = %d, want %d", table, got, want)
	}
}

type providerPipelinePeer struct{ nodeID string }

func (p providerPipelinePeer) NodeID() string                                { return p.nodeID }
func (p providerPipelinePeer) SessionID() string                             { return "session-" + p.nodeID }
func (p providerPipelinePeer) ConnectionEpoch() uint64                       { return 1 }
func (p providerPipelinePeer) Send(context.Context, protocol.Envelope) error { return nil }

type providerPipelineTransport struct {
	nodes   []string
	sent    []protocol.Envelope
	sendErr error
}

func (t *providerPipelineTransport) ConnectedNodes() []string {
	return append([]string(nil), t.nodes...)
}
func (t *providerPipelineTransport) Session(nodeID string) (transport.Peer, bool) {
	for _, id := range t.nodes {
		if id == nodeID {
			return providerPipelinePeer{nodeID: nodeID}, true
		}
	}
	return nil, false
}
func (t *providerPipelineTransport) Send(_ context.Context, nodeID string, message protocol.Envelope) error {
	if t.sendErr != nil {
		return t.sendErr
	}
	if _, ok := t.Session(nodeID); !ok {
		return errors.New("not connected")
	}
	t.sent = append(t.sent, message)
	return nil
}
