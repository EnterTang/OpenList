package worker

import (
	"context"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestBuildInventoryIncludesProviderAccounts(t *testing.T) {
	oldList := listInventoryStorages
	oldHydrate := hydrateInventoryStorage
	defer func() {
		listInventoryStorages = oldList
		hydrateInventoryStorage = oldHydrate
	}()

	listInventoryStorages = func() ([]model.Storage, error) {
		return []model.Storage{{
			ID:        7,
			MountPath: "/139-a",
			Driver:    "139Yun",
			Remark:    "mobile-a",
			Status:    "work",
		}}, nil
	}
	hydrateInventoryStorage = func(ctx context.Context, nodeID string, storage model.Storage) (inventoryStorageSnapshot, error) {
		return inventoryStorageSnapshot{
			Mount:   providerMountInventory(nodeID, storage),
			Account: providerAccountInventory(nodeID, storage, 1024, 2048),
		}, nil
	}

	report, err := BuildInventory(context.Background(), "node-1", true)
	if err != nil {
		t.Fatalf("build inventory: %v", err)
	}
	if len(report.ProviderAccounts) != 1 {
		t.Fatalf("provider accounts = %d, want 1", len(report.ProviderAccounts))
	}
	if report.ProviderAccounts[0].Provider != "yidong139" {
		t.Fatalf("provider = %q, want yidong139", report.ProviderAccounts[0].Provider)
	}
}
