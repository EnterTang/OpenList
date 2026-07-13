package cluster

import (
	"context"
	"encoding/json"
	"path"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func resolveInventoryTargetPath(ctx context.Context, nodeID string, targetPath string) (string, bool) {
	targetPath = strings.TrimSpace(targetPath)
	if targetPath == "" {
		return "", true
	}
	if path.IsAbs(targetPath) {
		return path.Clean(targetPath), true
	}
	var desired model.ClusterNodeDesiredConfig
	if err := db.GetDb().WithContext(ctx).First(&desired, "node_id = ? AND status = ? AND observed_revision >= revision", nodeID, model.ClusterDesiredStatusApplied).Error; err != nil {
		return "", false
	}
	var config protocol.WorkerDesiredConfig
	if json.Unmarshal([]byte(desired.ConfigJSON), &config) != nil {
		return "", false
	}
	binding, ok := config.TargetBindings[targetPath]
	if !ok {
		return "", false
	}
	return path.Clean(binding.MountPath), true
}

func providerAccountsSupportSource(accounts []protocol.ProviderAccountInventory, provider string) bool {
	provider = strings.TrimSpace(provider)
	if provider == "" {
		return true
	}
	for _, account := range accounts {
		if !strings.EqualFold(strings.TrimSpace(account.Provider), provider) {
			continue
		}
		if !account.SupportsShareSave || !account.SupportsDownload {
			continue
		}
		return true
	}
	return false
}

func providerAccountsSupportTarget(accounts []protocol.ProviderAccountInventory, targetPath string, targetProvider string, expectedBytes int64) bool {
	resolvedTargetPath := path.Clean(targetPath)
	targetProvider = strings.TrimSpace(targetProvider)
	for _, account := range accounts {
		if targetProvider != "" && !strings.EqualFold(strings.TrimSpace(account.Provider), targetProvider) {
			continue
		}
		mountPath := strings.TrimRight(path.Clean(account.MountPath), "/")
		if resolvedTargetPath != mountPath && !strings.HasPrefix(resolvedTargetPath, mountPath+"/") {
			continue
		}
		if !account.SupportsUpload || !account.SupportsETF {
			continue
		}
		if expectedBytes > 0 && account.MaxSingleUploadBytes > 0 && expectedBytes > account.MaxSingleUploadBytes {
			continue
		}
		if expectedBytes > 0 && account.FreeBytes > 0 && account.FreeBytes < expectedBytes {
			continue
		}
		return true
	}
	return false
}

func mountsSupportTarget(mounts []protocol.MountInventory, targetPath string, expectedBytes int64) bool {
	resolvedTargetPath := path.Clean(targetPath)
	for _, mount := range mounts {
		mountPath := strings.TrimRight(path.Clean(mount.MountPath), "/")
		if resolvedTargetPath != mountPath && !strings.HasPrefix(resolvedTargetPath, mountPath+"/") {
			continue
		}
		if !mount.CanUpload || !mount.SupportsETF {
			continue
		}
		if expectedBytes > 0 && mount.FreeBytes > 0 && mount.FreeBytes < expectedBytes {
			continue
		}
		return true
	}
	return false
}
