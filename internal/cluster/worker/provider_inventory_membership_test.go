package worker

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	pan123 "github.com/OpenListTeam/OpenList/v4/drivers/123"
	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
)

func TestApplyRuntimeMembershipUsesRuntimeTierForUnknownAccount(t *testing.T) {
	account := protocol.ProviderAccountInventory{MembershipTier: "unknown"}

	applyRuntimeMembership(&account, model.MembershipDetails{
		Tier:       "svip",
		Status:     "active",
		ExpireDate: "2040-01-31",
	})

	if account.MembershipTier != "svip" || account.MembershipWeight != 300 {
		t.Fatalf("membership tier/weight = %q/%d, want svip/300", account.MembershipTier, account.MembershipWeight)
	}
	if account.MembershipStatus != "active" || account.MembershipExpireDate != "2040-01-31" {
		t.Fatalf("membership status/expiration = %q/%q, want active/2040-01-31", account.MembershipStatus, account.MembershipExpireDate)
	}

	raw, err := json.Marshal(account)
	if err != nil {
		t.Fatalf("marshal provider account: %v", err)
	}
	for _, field := range []string{
		`"membership_status":"active"`,
		`"membership_expire_date":"2040-01-31"`,
	} {
		if !strings.Contains(string(raw), field) {
			t.Fatalf("provider account JSON %s does not contain %s", raw, field)
		}
	}
}

func TestApplyRuntimeMembershipPreservesConfiguredTier(t *testing.T) {
	account := protocol.ProviderAccountInventory{
		MembershipTier:   "vip",
		MembershipWeight: 200,
	}

	applyRuntimeMembership(&account, model.MembershipDetails{
		Tier:       "svip",
		Status:     "active",
		ExpireDate: "2040-01-31",
	})

	if account.MembershipTier != "vip" || account.MembershipWeight != 200 {
		t.Fatalf("configured membership tier/weight = %q/%d, want vip/200", account.MembershipTier, account.MembershipWeight)
	}
	if account.MembershipStatus != "active" || account.MembershipExpireDate != "2040-01-31" {
		t.Fatalf("membership status/expiration = %q/%q, want active/2040-01-31", account.MembershipStatus, account.MembershipExpireDate)
	}
}

func TestDefaultHydrateInventoryStorageUsesMembershipRefreshedByGetDetails(t *testing.T) {
	storage := model.Storage{
		ID:              123,
		MountPath:       "/inventory-membership-refresh",
		Driver:          "123Pan",
		Status:          op.WORK,
		CacheExpiration: 1,
		Addition:        `{"membership_tier":"unknown"}`,
	}
	storageDriver := &refreshingInventoryMembershipDriver{
		Pan123: &pan123.Pan123{
			Storage: storage,
			Addition: pan123.Addition{
				MembershipTier: "unknown",
			},
		},
		membership: model.MembershipDetails{
			Tier:       "ordinary",
			Status:     "inactive",
			ExpireDate: "2020-01-31",
		},
	}

	op.Cache.InvalidateStorageDetails(storageDriver)
	previousLookup := getInventoryStorageByMountPath
	getInventoryStorageByMountPath = func(mountPath string) (driver.Driver, error) {
		if mountPath != storage.MountPath {
			t.Fatalf("storage lookup mount path = %q, want %q", mountPath, storage.MountPath)
		}
		return storageDriver, nil
	}
	t.Cleanup(func() {
		getInventoryStorageByMountPath = previousLookup
		op.Cache.InvalidateStorageDetails(storageDriver)
	})

	snapshot, err := defaultHydrateInventoryStorage(context.Background(), "node-a", storage)
	if err != nil {
		t.Fatalf("hydrate inventory storage: %v", err)
	}
	if snapshot.Mount.TotalBytes != 120 || snapshot.Mount.FreeBytes != 80 {
		t.Fatalf("mount capacity = %d/%d, want total/free 120/80", snapshot.Mount.TotalBytes, snapshot.Mount.FreeBytes)
	}
	if snapshot.Account.TotalBytes != 120 || snapshot.Account.FreeBytes != 80 {
		t.Fatalf("account capacity = %d/%d, want total/free 120/80", snapshot.Account.TotalBytes, snapshot.Account.FreeBytes)
	}
	if snapshot.Account.MembershipTier != "svip" || snapshot.Account.MembershipWeight != 300 {
		t.Fatalf("membership tier/weight = %q/%d, want svip/300", snapshot.Account.MembershipTier, snapshot.Account.MembershipWeight)
	}
	if snapshot.Account.MembershipStatus != "active" || snapshot.Account.MembershipExpireDate != "2040-01-31" {
		t.Fatalf("membership status/expiration = %q/%q, want active/2040-01-31", snapshot.Account.MembershipStatus, snapshot.Account.MembershipExpireDate)
	}
}

type refreshingInventoryMembershipDriver struct {
	*pan123.Pan123
	membership model.MembershipDetails
}

func (d *refreshingInventoryMembershipDriver) ClusterMembershipDetails() model.MembershipDetails {
	return d.membership
}

func (d *refreshingInventoryMembershipDriver) GetDetails(context.Context) (*model.StorageDetails, error) {
	d.membership = model.MembershipDetails{
		Tier:       "svip",
		Status:     "active",
		ExpireDate: "2040-01-31",
	}
	return &model.StorageDetails{
		DiskUsage: model.DiskUsage{
			TotalSpace: 120,
			UsedSpace:  40,
		},
	}, nil
}
