package subscription

import (
	"context"
	"fmt"
	stdpath "path"
	"sort"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
)

var (
	listProviderTargetStorages   = db.GetEnabledStorages
	storageFreeBytesForMountPath = defaultStorageFreeBytesForMountPath
)

type ResolveProviderTargetRequest struct {
	Provider string
	Folder   string
	FileSize int64
}

type ResolvedProviderTarget struct {
	Provider  string
	StorageID uint
	MountPath string
	Folder    string
	FullPath  string
	FreeBytes int64
}

func ResolveProviderTarget(ctx context.Context, req ResolveProviderTargetRequest) (ResolvedProviderTarget, error) {
	target := NormalizeSubscriptionStorageTarget(model.SubscriptionStorageTarget{
		Provider: req.Provider,
		Folder:   req.Folder,
	})
	if target.Provider == "" {
		return ResolvedProviderTarget{}, fmt.Errorf("provider target provider is required")
	}
	storages, err := listProviderTargetStorages()
	if err != nil {
		return ResolvedProviderTarget{}, err
	}
	type candidate struct {
		storage   model.Storage
		freeBytes int64
		hasFree   bool
	}
	candidates := make([]candidate, 0, len(storages))
	for _, storage := range storages {
		if storageProviderName(storage.Driver) != target.Provider {
			continue
		}
		freeBytes, hasFree := storageFreeBytesForMountPath(ctx, storage.MountPath)
		if req.FileSize > 0 && hasFree && freeBytes < req.FileSize {
			continue
		}
		candidates = append(candidates, candidate{storage: storage, freeBytes: freeBytes, hasFree: hasFree})
	}
	if len(candidates) == 0 {
		return ResolvedProviderTarget{}, fmt.Errorf("no enabled storage for provider %s", target.Provider)
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].hasFree != candidates[j].hasFree {
			return candidates[i].hasFree
		}
		if candidates[i].freeBytes != candidates[j].freeBytes {
			return candidates[i].freeBytes > candidates[j].freeBytes
		}
		left := cleanConfigPath(candidates[i].storage.MountPath)
		right := cleanConfigPath(candidates[j].storage.MountPath)
		if left != right {
			return left < right
		}
		return candidates[i].storage.ID < candidates[j].storage.ID
	})
	selected := candidates[0]
	mountPath := cleanConfigPath(selected.storage.MountPath)
	fullPath := mountPath
	if target.Folder != "" {
		fullPath = cleanConfigPath(stdpath.Join(mountPath, target.Folder))
	}
	return ResolvedProviderTarget{
		Provider:  target.Provider,
		StorageID: selected.storage.ID,
		MountPath: mountPath,
		Folder:    target.Folder,
		FullPath:  fullPath,
		FreeBytes: selected.freeBytes,
	}, nil
}

func defaultStorageFreeBytesForMountPath(ctx context.Context, mountPath string) (int64, bool) {
	driver, err := op.GetStorageByMountPath(mountPath)
	if err != nil {
		return 0, false
	}
	details, err := op.GetStorageDetails(ctx, driver)
	if err != nil || details == nil {
		return 0, false
	}
	return details.FreeSpace(), true
}

func storageProviderName(driverName string) string {
	switch strings.ToLower(strings.TrimSpace(driverName)) {
	case "quark":
		return "quark"
	case "aliyundriveopen", "aliyundrive":
		return "aliyun_drive"
	case "123pan", "123 open":
		return "pan123"
	case "115 cloud", "115 open":
		return "pan115"
	case "139yun", "139 cloud", "139":
		return "yidong139"
	default:
		return strings.ToLower(strings.ReplaceAll(strings.TrimSpace(driverName), " ", "_"))
	}
}

func providerTargetNameForShareProvider(provider ShareProviderName) string {
	switch provider {
	case ShareProviderQuark:
		return "quark"
	case ShareProviderAliyunDrive:
		return "aliyun_drive"
	case ShareProviderPan123:
		return "pan123"
	case ShareProviderPan115:
		return "pan115"
	default:
		return ""
	}
}
