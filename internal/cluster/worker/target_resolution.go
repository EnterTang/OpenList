package worker

import (
	"context"
	"fmt"
	"path"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/subscription"
)

var ensureResolvedProviderFolder = subscription.EnsureResolvedProviderFolder

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
		requirement := task.StagingTarget
		requirement.Provider = target.Provider
		requirement.Folder = target.Folder
		requirement.NeedShareSave = true
		requirement.RequiredBytes = stagingRequiredBytes(task)
		resolved, err := s.resolveProviderTargetRequirement(ctx, requirement)
		if err == nil {
			return path.Join(resolved.FullPath, namespace), nil
		}
		return "", err
	}
	if configuredRoot := s.providerTempRoot(task.Share.Provider); configuredRoot != "" {
		return path.Join(configuredRoot, namespace), nil
	}
	return namespace, nil
}

func (s *Service) resolveDeliveryTargetRoot(ctx context.Context, task protocol.TaskContext) (string, string, error) {
	if strings.TrimSpace(task.DeliveryTarget.Provider) != "" {
		requirement := task.DeliveryTarget
		requirement.NeedUpload = true
		if requirement.RequiredBytes <= 0 {
			requirement.RequiredBytes = primarySourceObject(task.SourceObjects).Size
		}
		resolved, err := s.resolveProviderTargetRequirement(ctx, requirement)
		if err != nil {
			return "", "", err
		}
		return resolved.FullPath, resolved.MountPath, nil
	}
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
	storageModel, err := db.GetStorageByMountPath(bindingMount)
	if err != nil {
		return "", "", fmt.Errorf("resolve cluster target binding storage: %w", err)
	}
	if target.Provider != "" && !strings.EqualFold(providerName(storageModel.Driver), target.Provider) {
		return "", "", fmt.Errorf("cluster delivery provider %q does not match bound account provider %q", target.Provider, providerName(storageModel.Driver))
	}
	if task.DeliveryTarget.StorageID != 0 && task.DeliveryTarget.StorageID != storageModel.ID {
		return "", "", fmt.Errorf("cluster delivery storage id does not match the bound account")
	}
	if task.DeliveryTarget.AccountFingerprint != "" && task.DeliveryTarget.AccountFingerprint != accountFingerprint(*storageModel, providerName(storageModel.Driver)) {
		return "", "", fmt.Errorf("cluster delivery account fingerprint does not match the bound account")
	}
	if task.DeliveryTarget.NodeMountID != "" {
		s.mu.Lock()
		nodeID := s.controlNodeID
		s.mu.Unlock()
		if nodeID == "" || task.DeliveryTarget.NodeMountID != stableMountID(nodeID, storageModel.ID, storageModel.MountPath) {
			return "", "", fmt.Errorf("cluster delivery mount identity does not match the bound account")
		}
	}
	root := bindingMount
	if target.Folder != "" {
		root = path.Join(bindingMount, target.Folder)
	}
	return root, bindingMount, nil
}

func (s *Service) resolveProviderTargetRequirement(ctx context.Context, requirement protocol.ProviderTargetRequirement) (subscription.ResolvedProviderTarget, error) {
	storages, err := listInventoryStorages()
	if err != nil {
		return subscription.ResolvedProviderTarget{}, fmt.Errorf("list worker provider accounts: %w", err)
	}
	s.mu.Lock()
	nodeID := s.controlNodeID
	s.mu.Unlock()
	candidates := make([]subscription.ProviderAccountCandidate, 0, len(storages))
	for _, storage := range storages {
		snapshot, hydrateErr := hydrateInventoryStorage(ctx, nodeID, storage)
		if hydrateErr != nil {
			continue
		}
		account := snapshot.Account
		if requirement.StorageID != 0 && account.StorageID != requirement.StorageID {
			continue
		}
		if requirement.NodeMountID != "" && account.NodeMountID != requirement.NodeMountID {
			continue
		}
		if requirement.AccountFingerprint != "" && account.AccountFingerprint != requirement.AccountFingerprint {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(account.Status), "work") {
			continue
		}
		candidates = append(candidates, subscription.ProviderAccountCandidate{
			Provider: account.Provider, StorageID: account.StorageID, MountPath: account.MountPath,
			AccountAlias: account.AccountAlias, Status: account.Status, Disabled: storage.Disabled,
			MembershipTier:   account.MembershipTier,
			MembershipWeight: account.MembershipWeight, MaxSingleUploadBytes: account.MaxSingleUploadBytes,
			SupportsUpload: account.SupportsUpload, SupportsDownload: account.SupportsDownload,
			SupportsShareSave: account.SupportsShareSave, SupportsETF: account.SupportsETF,
			FreeBytes: account.FreeBytes, HasFreeBytes: account.TotalBytes > 0 || account.FreeBytes > 0,
			ActiveJobs: account.ActiveJobs,
		})
	}
	resolved, err := subscription.ResolveProviderTargetFromCandidates(ctx, subscription.ResolveProviderTargetRequest{
		Provider: requirement.Provider, Folder: requirement.Folder,
		NeedUpload: requirement.NeedUpload, NeedShareSave: requirement.NeedShareSave,
		FileSize: requirement.RequiredBytes,
	}, candidates)
	if err != nil {
		return subscription.ResolvedProviderTarget{}, err
	}
	return ensureResolvedProviderFolder(ctx, resolved)
}

func stagingRequiredBytes(task protocol.TaskContext) int64 {
	if task.StagingTarget.RequiredBytes > 0 {
		return task.StagingTarget.RequiredBytes
	}
	return primarySourceObject(task.SourceObjects).Size
}
