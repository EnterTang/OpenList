package worker

import (
	"context"
	"fmt"
	"path"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/subscription"
)

func (s *Service) resolveStagingTempRoot(ctx context.Context, task protocol.TaskContext, namespace string) (string, error) {
	namespace = path.Clean(strings.TrimSpace(namespace))
	if namespace == "" || namespace == "." || namespace == "/" {
		return "", fmt.Errorf("cluster staging namespace is required")
	}
	target := subscription.NormalizeSubscriptionStorageTarget(model.SubscriptionStorageTarget{
		Provider: task.StagingTarget.Provider,
		Folder:   task.StagingTarget.Folder,
	})
	if target.Provider != "" {
		resolved, err := subscription.ResolveProviderTarget(ctx, subscription.ResolveProviderTargetRequest{
			Provider: target.Provider,
			Folder:   target.Folder,
			FileSize: stagingRequiredBytes(task),
		})
		if err == nil {
			return path.Join(resolved.FullPath, namespace), nil
		}
		if legacyRoot, ok := legacyProviderMountPath(target.Provider); ok {
			base := legacyRoot
			if target.Folder != "" {
				base = path.Join(base, target.Folder)
			}
			return path.Join(base, namespace), nil
		}
		return "", err
	}
	if configuredRoot := s.providerTempRoot(task.Share.Provider); configuredRoot != "" {
		return path.Join(configuredRoot, namespace), nil
	}
	return namespace, nil
}

func (s *Service) resolveDeliveryTargetRoot(_ context.Context, task protocol.TaskContext) (string, string, error) {
	targetProfileRef := strings.TrimSpace(task.TargetProfile)
	if targetProfileRef == "" || targetProfileRef == "/" {
		return "", "", fmt.Errorf("cluster target profile must be a mounted destination path")
	}
	bindingMount, _, _ := s.resolveTargetBinding(targetProfileRef)
	bindingMount = path.Clean(strings.TrimSpace(bindingMount))
	if bindingMount == "" || bindingMount == "." || bindingMount == "/" {
		return "", "", fmt.Errorf("cluster target binding mount is required")
	}
	target := subscription.NormalizeSubscriptionStorageTarget(model.SubscriptionStorageTarget{
		Provider: task.DeliveryTarget.Provider,
		Folder:   task.DeliveryTarget.Folder,
	})
	root := bindingMount
	if target.Folder != "" {
		root = path.Join(bindingMount, target.Folder)
	}
	return root, bindingMount, nil
}

func stagingRequiredBytes(task protocol.TaskContext) int64 {
	if task.StagingTarget.RequiredBytes > 0 {
		return task.StagingTarget.RequiredBytes
	}
	return primarySourceObject(task.SourceObjects).Size
}

func legacyProviderMountPath(provider string) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(provider)) {
	case "pan123":
		return "/123", true
	case "pan115":
		return "/115", true
	case "yidong139":
		return "/139_60t", true
	default:
		return "", false
	}
}
