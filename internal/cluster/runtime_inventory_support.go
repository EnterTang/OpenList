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

func providerAccountsSupportTarget(accounts []protocol.ProviderAccountInventory, targetPath string, expectedBytes int64) bool {
	resolvedTargetPath := path.Clean(targetPath)
	for _, account := range accounts {
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
