package worker

import (
	"context"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
)

var (
	listInventoryStorages   = db.GetEnabledStorages
	hydrateInventoryStorage = defaultHydrateInventoryStorage
)

type inventoryStorageSnapshot struct {
	Mount   protocol.MountInventory
	Account protocol.ProviderAccountInventory
}

func defaultHydrateInventoryStorage(ctx context.Context, nodeID string, storage model.Storage) (inventoryStorageSnapshot, error) {
	mount := providerMountInventory(nodeID, storage)
	account := providerAccountInventory(nodeID, storage, 0, 0)
	if driver, driverErr := op.GetStorageByMountPath(storage.MountPath); driverErr == nil {
		mount.ReadOnly = driver.Config().NoUpload
		mount.CanUpload = !driver.Config().NoUpload
		account.SupportsUpload = !driver.Config().NoUpload
		account.SupportsDownload = true
		if details, detailsErr := op.GetStorageDetails(ctx, driver); detailsErr == nil && details != nil {
			mount.TotalBytes = details.TotalSpace
			mount.FreeBytes = details.FreeSpace()
			account.TotalBytes = details.TotalSpace
			account.FreeBytes = details.FreeSpace()
		}
	}
	return inventoryStorageSnapshot{Mount: mount, Account: account}, nil
}

func providerMountInventory(nodeID string, storage model.Storage) protocol.MountInventory {
	provider := providerName(storage.Driver)
	return protocol.MountInventory{
		NodeMountID:  stableMountID(nodeID, storage.ID, storage.MountPath),
		Driver:       storage.Driver,
		Provider:     provider,
		MountPath:    storage.MountPath,
		AccountAlias: storage.Remark,
		Status:       storage.Status,
		ReadOnly:     storage.Disabled,
		CanUpload:    !storage.Disabled,
		CanShare:     supportsShare(storage.Driver),
		SupportsETF:  supportsETF(storage.Driver),
	}
}

func providerAccountInventory(nodeID string, storage model.Storage, freeBytes, totalBytes int64) protocol.ProviderAccountInventory {
	provider := providerName(storage.Driver)
	return protocol.ProviderAccountInventory{
		NodeMountID:          stableMountID(nodeID, storage.ID, storage.MountPath),
		Provider:             provider,
		MountPath:            storage.MountPath,
		AccountAlias:         storage.Remark,
		Status:               storage.Status,
		MaxSingleUploadBytes: mobileProviderUploadLimit(provider, ""),
		SupportsUpload:       !storage.Disabled,
		SupportsDownload:     true,
		SupportsShareSave:    supportsShare(storage.Driver),
		SupportsETF:          supportsETF(storage.Driver),
		TotalBytes:           totalBytes,
		FreeBytes:            freeBytes,
	}
}

func mobileProviderUploadLimit(provider, tier string) int64 {
	if provider != "yidong139" {
		return 0
	}
	switch strings.ToLower(strings.TrimSpace(tier)) {
	case "diamond":
		return 500 << 30
	case "gold":
		return 20 << 30
	case "silver":
		return 8 << 30
	default:
		return 0
	}
}
