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
