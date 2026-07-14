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
			Addition:  `{"type":"personal_new","cluster_dedicated_account":true,"membership_tier":"diamond","user_domain_id":"account-a"}`,
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
	account := report.ProviderAccounts[0]
	if account.StorageID != 7 || account.AccountAlias != "mobile-a" || account.AccountFingerprint == "" {
		t.Fatalf("provider account identity = %#v", account)
	}
	if account.MembershipTier != "diamond" || account.MembershipWeight != 400 || account.MaxSingleUploadBytes != 500<<30 {
		t.Fatalf("provider account membership = %#v", account)
	}
}

func TestProviderAccountInventoryKeepsUnknown139MembershipWithoutUploadLimit(t *testing.T) {
	storage := model.Storage{ID: 9, MountPath: "/139-b", Driver: "139Yun", Status: "work"}
	account := providerAccountInventory("node-1", storage, 10<<30, 20<<30)
	if account.MembershipTier != "unknown" || account.MembershipWeight != 0 || account.MaxSingleUploadBytes != 0 {
		t.Fatalf("provider account = %#v", account)
	}
}

func TestProviderAccountInventoryDefaults139PersonalNewToETFSupport(t *testing.T) {
	storage := model.Storage{ID: 10, MountPath: "/139-c", Driver: "139Yun", Status: "work", Addition: `{"type":"personal_new"}`}
	account := providerAccountInventory("node-1", storage, 10<<30, 20<<30)
	mount := providerMountInventory("node-1", storage)

	if !account.SupportsETF || !mount.SupportsETF {
		t.Fatalf("supports ETF account=%v mount=%v, want true by default for personal_new 139", account.SupportsETF, mount.SupportsETF)
	}
}

func TestProviderAccountInventoryAllowsDisabling139ETFSupport(t *testing.T) {
	storage := model.Storage{ID: 11, MountPath: "/139-d", Driver: "139Yun", Status: "work", Addition: `{"type":"personal_new","cluster_dedicated_account":false}`}
	account := providerAccountInventory("node-1", storage, 10<<30, 20<<30)

	if account.SupportsETF {
		t.Fatalf("supports ETF = true, want false when cluster_dedicated_account is explicitly disabled")
	}
}

func TestProviderAccountInventoryMapsConfigured123And115Membership(t *testing.T) {
	for _, storage := range []model.Storage{
		{ID: 1, MountPath: "/123", Driver: "123Pan", Status: "work", Addition: `{"membership_tier":"svip"}`},
		{ID: 2, MountPath: "/115", Driver: "115 Cloud", Status: "work", Addition: `{"membership_tier":"vip"}`},
	} {
		account := providerAccountInventory("node-1", storage, 1<<40, 2<<40)
		if storage.ID == 1 && (account.MembershipTier != "svip" || account.MembershipWeight != 300) {
			t.Fatalf("123 account membership = %#v", account)
		}
		if storage.ID == 2 && (account.MembershipTier != "vip" || account.MembershipWeight != 200) {
			t.Fatalf("115 account membership = %#v", account)
		}
	}
}

func TestProviderAccountInventoryDoesNotAdvertiseUnsupported115OpenShareSave(t *testing.T) {
	storage := model.Storage{ID: 3, MountPath: "/115-open", Driver: "115 Open", Status: "work"}
	account := providerAccountInventory("node-1", storage, 1<<40, 2<<40)
	if account.SupportsShareSave {
		t.Fatalf("115 Open account unexpectedly advertises share-save support: %#v", account)
	}
}
