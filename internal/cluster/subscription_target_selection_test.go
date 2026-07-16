package cluster

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/subscription"
)

func TestChooseDispatchTargetIgnoresLegacySubscriptionDeliveryProvider(t *testing.T) {
	database := openClusterRuntimeTestDB(t)
	oldConf := conf.Conf
	conf.Conf = conf.DefaultConfig(t.TempDir())
	t.Cleanup(func() { conf.Conf = oldConf })
	db.Init(database)

	capabilities, err := json.Marshal(protocol.NodeCapabilities{
		SupportedProviders:   []string{"pan123", "yidong139"},
		SupportedOperations:  []string{"share.save", "mobile.upload", "result.report"},
		RedisDurabilityReady: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	mounts, err := json.Marshal([]protocol.MountInventory{{
		Provider:    "yidong139",
		MountPath:   "/139-a",
		CanUpload:   true,
		SupportsETF: true,
		FreeBytes:   100 << 30,
	}})
	if err != nil {
		t.Fatal(err)
	}
	providerAccounts, err := json.Marshal([]protocol.ProviderAccountInventory{{
		Provider:          "pan123",
		MountPath:         "/123-a",
		Status:            "work",
		SupportsShareSave: true,
		SupportsDownload:  true,
		FreeBytes:         100 << 30,
	}, {
		Provider:             "yidong139",
		MountPath:            "/139-a",
		Status:               "work",
		SupportsUpload:       true,
		SupportsETF:          true,
		MaxSingleUploadBytes: 500 << 30,
		FreeBytes:            100 << 30,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&model.ClusterNodeInventory{
		ID:                   "inventory-4",
		NodeID:               "worker-4",
		Revision:             1,
		CollectedAt:          time.Now().UTC(),
		InventoryHash:        "hash-4",
		CapabilitiesJSON:     string(capabilities),
		MountsJSON:           string(mounts),
		ProviderAccountsJSON: string(providerAccounts),
	}).Error; err != nil {
		t.Fatal(err)
	}
	configJSON, err := json.Marshal(protocol.WorkerDesiredConfig{
		TargetBindings: map[string]protocol.TargetBinding{
			"mobile-primary": {MountPath: "/139-a"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&model.ClusterNodeDesiredConfig{
		NodeID:           "worker-4",
		Status:           model.ClusterDesiredStatusApplied,
		Revision:         1,
		ObservedRevision: 1,
		ConfigJSON:       string(configJSON),
	}).Error; err != nil {
		t.Fatal(err)
	}

	runtime := &Runtime{}
	target := runtime.chooseDispatchTarget(context.Background(), []*dispatchTarget{{nodeID: "worker-4", targetProfile: "mobile-primary"}}, subscription.ClusterMediaTask{
		ShareProvider:    "pan123",
		SourceSize:       1 << 30,
		LogicalMediaRoot: "/115/剧集",
	})
	if target == nil {
		t.Fatal("expected target selection to use the worker's yidong139 account")
	}
}

func TestChooseDispatchTargetAppliesPreferredWorkerAfterEligibility(t *testing.T) {
	database := openClusterRuntimeTestDB(t)
	configureProviderPipelineDB(t, database)
	createProviderPipelineInventory(t, database, "worker-auto", preferredWorkerTestAccounts(500))
	createProviderPipelineInventory(t, database, "worker-preferred", preferredWorkerTestAccounts(100))

	runtime := &Runtime{}
	targets := []*dispatchTarget{{nodeID: "worker-auto"}, {nodeID: "worker-preferred"}}
	automaticTask := subscription.ClusterMediaTask{ShareProvider: "pan123", SourceSize: 1 << 30}
	automatic := runtime.chooseDispatchTarget(context.Background(), targets, automaticTask)
	if automatic == nil || automatic.nodeID != "worker-auto" {
		t.Fatalf("automatic target = %#v, want highest-scoring worker-auto", automatic)
	}

	preferredTask := automaticTask
	preferredTask.PreferredWorkerNodeID = "worker-preferred"
	preferred := runtime.chooseDispatchTarget(context.Background(), targets, preferredTask)
	if preferred == nil || preferred.nodeID != "worker-preferred" {
		t.Fatalf("preferred target = %#v, want eligible worker-preferred", preferred)
	}
}

func TestChooseDispatchTargetFallsBackWhenPreferredWorkerIsUnavailableOrIncompatible(t *testing.T) {
	database := openClusterRuntimeTestDB(t)
	configureProviderPipelineDB(t, database)
	createProviderPipelineInventory(t, database, "worker-auto", preferredWorkerTestAccounts(500))
	createProviderPipelineInventory(t, database, "worker-incompatible", []protocol.ProviderAccountInventory{
		{Provider: "pan123", Status: "work", SupportsShareSave: true, SupportsDownload: true, FreeBytes: 100 << 30},
	})

	runtime := &Runtime{}
	task := subscription.ClusterMediaTask{
		ShareProvider: "pan123", SourceSize: 1 << 30, PreferredWorkerNodeID: "worker-offline",
	}
	target := runtime.chooseDispatchTarget(context.Background(), []*dispatchTarget{{nodeID: "worker-auto"}}, task)
	if target == nil || target.nodeID != "worker-auto" {
		t.Fatalf("offline preferred fallback = %#v, want worker-auto", target)
	}

	task.PreferredWorkerNodeID = "worker-incompatible"
	target = runtime.chooseDispatchTarget(context.Background(), []*dispatchTarget{{nodeID: "worker-auto"}, {nodeID: "worker-incompatible"}}, task)
	if target == nil || target.nodeID != "worker-auto" {
		t.Fatalf("incompatible preferred fallback = %#v, want worker-auto", target)
	}
}

func TestSubscriptionDispatchTargetsFallsBackFromDrainingOrDisabledPreferredWorker(t *testing.T) {
	for _, tt := range []struct {
		name    string
		updates map[string]any
	}{
		{name: "draining", updates: map[string]any{"drain": true}},
		{name: "disabled", updates: map[string]any{"disabled": true}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			database := openClusterRuntimeTestDB(t)
			configureProviderPipelineDB(t, database)
			createProviderPipelineInventory(t, database, "worker-auto", preferredWorkerTestAccounts(500))
			createProviderPipelineInventory(t, database, "worker-preferred", preferredWorkerTestAccounts(100))
			if err := database.Model(&model.ClusterNode{}).Where("id = ?", "worker-preferred").Updates(tt.updates).Error; err != nil {
				t.Fatal(err)
			}

			runtime := &Runtime{dispatchTransport: &providerPipelineTransport{nodes: []string{"worker-auto", "worker-preferred"}}}
			targets, err := runtime.subscriptionDispatchTargets(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			target := runtime.chooseDispatchTarget(t.Context(), targets, subscription.ClusterMediaTask{
				ShareProvider: "pan123", SourceSize: 1 << 30, PreferredWorkerNodeID: "worker-preferred",
			})
			if target == nil || target.nodeID != "worker-auto" {
				t.Fatalf("%s preferred fallback = %#v, want worker-auto", tt.name, target)
			}
		})
	}
}

func TestSelectRedispatchNodeIDRetainsSoftPreference(t *testing.T) {
	context := protocol.TaskContext{Subscription: protocol.SubscriptionTaskContext{PreferredWorkerNodeID: "worker-preferred"}}
	if got := selectRedispatchNodeID([]string{"worker-auto", "worker-preferred"}, context, 0); got != "worker-preferred" {
		t.Fatalf("redispatch target = %q, want preferred worker", got)
	}
	if got := selectRedispatchNodeID([]string{"worker-auto", "worker-other"}, context, 1); got != "worker-other" {
		t.Fatalf("fallback redispatch target = %q, want existing offset selection", got)
	}
}

func preferredWorkerTestAccounts(weight int) []protocol.ProviderAccountInventory {
	return []protocol.ProviderAccountInventory{
		{Provider: "pan123", Status: "work", MembershipWeight: weight, SupportsShareSave: true, SupportsDownload: true, FreeBytes: 100 << 30},
		{Provider: "yidong139", Status: "work", MembershipWeight: weight, SupportsUpload: true, SupportsETF: true, MaxSingleUploadBytes: 500 << 30, FreeBytes: 100 << 30},
	}
}
