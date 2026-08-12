package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestHandleInventoryPersistsProviderAccountsJSON(t *testing.T) {
	database := openCoordinatorTestDB(t)
	if err := database.AutoMigrate(&model.ClusterNodeInventory{}); err != nil {
		t.Fatal(err)
	}
	service := New(database, "")
	peer := &testPeer{nodeID: "worker-1"}
	if err := database.Create(&model.ClusterNode{ID: "worker-1", Status: model.ClusterNodeStatusOnline}).Error; err != nil {
		t.Fatal(err)
	}

	report := protocol.InventoryReport{
		Revision:      3,
		CollectedAt:   time.Now().UTC(),
		InventoryHash: "hash-1",
		Capabilities:  protocol.NodeCapabilities{RedisDurabilityReady: true},
		Mounts:        []protocol.MountInventory{{NodeMountID: "mount-1", Provider: "yidong139", MountPath: "/139-a", CanUpload: true, SupportsETF: true}},
		ProviderAccounts: []protocol.ProviderAccountInventory{{
			NodeMountID:          "mount-1",
			Provider:             "yidong139",
			MountPath:            "/139-a",
			AccountAlias:         "mobile-a",
			MaxSingleUploadBytes: 500 << 30,
		}},
	}

	if err := service.handleInventory(context.Background(), peer, report); err != nil {
		t.Fatal(err)
	}

	var inventory model.ClusterNodeInventory
	if err := database.First(&inventory, "node_id = ?", "worker-1").Error; err != nil {
		t.Fatal(err)
	}
	if inventory.ProviderAccountsJSON == "" {
		t.Fatal("expected provider accounts json to be persisted")
	}
}
