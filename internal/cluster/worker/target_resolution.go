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

var (
	ensureResolvedProviderFolder = subscription.EnsureResolvedProviderFolder
	getWorkerSubscriptionConfig  = subscription.GetConfig
)

func (s *Service) resolveStagingTempRoot(ctx context.Context, task protocol.TaskContext) (string, error) {
	target := subscription.NormalizeSubscriptionStorageTarget(model.SubscriptionStorageTarget{
		Provider: task.StagingTarget.Provider,
		Folder:   task.StagingTarget.Folder,
	})
	if localTarget, configured, err := localStagingTarget(task); err != nil {
		return "", err
	} else if configured {
		target.Provider = localTarget.Provider
		target.Folder = localTarget.Folder
	} else if target.Provider != "" && target.Folder == "" {
		return "", fmt.Errorf("worker local %s staging folder is required for a provider-only subscription task", target.Provider)
	}
	if target.Provider != "" {
		requirement := task.StagingTarget
		requirement.Provider = target.Provider
		requirement.Folder = target.Folder
		requirement.NeedShareSave = true
		requirement.RequiredBytes = stagingRequiredBytes(task)
		resolved, err := s.resolveProviderTargetRequirement(ctx, requirement)
		if err == nil {
			return resolved.FullPath, nil
		}
		return "", err
	}
	if configuredRoot := s.providerTempRoot(task.Share.Provider); configuredRoot != "" {
		return configuredRoot, nil
	}
	return "", fmt.Errorf("cluster staging root is required")
}

func (s *Service) resolveDeliveryTargetRoot(ctx context.Context, task protocol.TaskContext) (string, string, error) {
	if strings.TrimSpace(task.DeliveryTarget.Provider) != "" {
		requirement := task.DeliveryTarget
		if localTarget, configured, err := localDeliveryTarget(task); err != nil {
			return "", "", err
		} else if configured {
			requirement.Provider = localTarget.Provider
			requirement.Folder = localTarget.Folder
		} else if strings.TrimSpace(requirement.Folder) == "" {
			return "", "", fmt.Errorf("worker local yidong139 delivery folder is required for a provider-only subscription task")
		}
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

// localStagingTarget returns the local temporary target for a provider-only
// subscription task. Task account bindings remain untouched; the worker owns
// the folder because mounts and directory layouts are local to that worker.
func localStagingTarget(task protocol.TaskContext) (model.SubscriptionStorageTarget, bool, error) {
	provider := normalizeControlKey(task.StagingTarget.Provider)
	if provider != "pan123" && provider != "pan115" {
		return model.SubscriptionStorageTarget{}, false, nil
	}
	cfg, err := getWorkerSubscriptionConfig()
	if err != nil {
		return model.SubscriptionStorageTarget{}, false, fmt.Errorf("load worker subscription config: %w", err)
	}
	var target model.SubscriptionStorageTarget
	switch provider {
	case "pan123":
		target = cfg.Telegram.Pan123.TempTransferTarget
	case "pan115":
		target = cfg.Telegram.Pan115.TempTransferTarget
	}
	target = subscription.NormalizeSubscriptionStorageTarget(target)
	if err := subscription.ValidateSubscriptionStorageTarget(target); err != nil {
		return model.SubscriptionStorageTarget{}, false, fmt.Errorf("invalid worker local %s staging target: %w", provider, err)
	}
	if target.Provider == "" && target.Folder == "" {
		return model.SubscriptionStorageTarget{}, false, nil
	}
	if target.Provider != provider || target.Folder == "" {
		return model.SubscriptionStorageTarget{}, false, fmt.Errorf("worker local %s staging target must use provider %s with a folder", provider, provider)
	}
	return target, true, nil
}

// localDeliveryTarget returns the local ETF delivery target for a provider-only
// subscription task. The coordinator selects the account; it must not select
// the directory underneath that account.
func localDeliveryTarget(task protocol.TaskContext) (model.SubscriptionStorageTarget, bool, error) {
	provider := normalizeControlKey(task.DeliveryTarget.Provider)
	if provider != "yidong139" {
		return model.SubscriptionStorageTarget{}, false, nil
	}
	cfg, err := getWorkerSubscriptionConfig()
	if err != nil {
		return model.SubscriptionStorageTarget{}, false, fmt.Errorf("load worker subscription config: %w", err)
	}
	target := subscription.NormalizeSubscriptionStorageTarget(cfg.DefaultTarget)
	if err := subscription.ValidateSubscriptionStorageTarget(target); err != nil {
		return model.SubscriptionStorageTarget{}, false, fmt.Errorf("invalid worker local delivery target: %w", err)
	}
	if target.Provider == "" && target.Folder == "" {
		return model.SubscriptionStorageTarget{}, false, nil
	}
	if target.Provider != provider || target.Folder == "" {
		return model.SubscriptionStorageTarget{}, false, fmt.Errorf("worker local delivery target must use provider yidong139 with a folder")
	}
	return target, true, nil
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
