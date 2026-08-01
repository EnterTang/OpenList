package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"path"
	"sort"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"gorm.io/gorm"
)

type nodeProviderAccountMatch struct {
	Staging          protocol.ProviderAccountInventory
	Delivery         protocol.ProviderAccountInventory
	MembershipWeight int
	FreeBytes        int64
	ActiveJobs       int
	NodeActiveJobs   int64
}

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

func nodeInventoryProviderMatch(ctx context.Context, nodeID string, taskContext protocol.TaskContext, required []string, expectedBytes int64) (nodeProviderAccountMatch, bool, error) {
	var inventory model.ClusterNodeInventory
	if err := db.GetDb().WithContext(ctx).Where("node_id = ?", nodeID).Order("revision DESC").First(&inventory).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			utils.Log.Warnf("[cluster-dispatch] node=%s inventory not found", nodeID)
			return nodeProviderAccountMatch{}, false, nil
		}
		return nodeProviderAccountMatch{}, false, err
	}
	var capabilities protocol.NodeCapabilities
	if err := json.Unmarshal([]byte(inventory.CapabilitiesJSON), &capabilities); err != nil {
		return nodeProviderAccountMatch{}, false, err
	}
	if !capabilities.RedisDurabilityReady {
		utils.Log.Warnf("[cluster-dispatch] node=%s redis_durability_ready=false", nodeID)
		return nodeProviderAccountMatch{}, false, nil
	}
	for _, operation := range required {
		if !containsFold(capabilities.SupportedOperations, operation) {
			utils.Log.Warnf("[cluster-dispatch] node=%s missing operation=%s supported=%v", nodeID, operation, capabilities.SupportedOperations)
			return nodeProviderAccountMatch{}, false, nil
		}
	}
	stagingRequirement := taskContext.StagingTarget
	if strings.TrimSpace(stagingRequirement.Provider) == "" {
		stagingRequirement.Provider = taskContext.Share.Provider
	}
	if stagingRequirement.RequiredBytes <= 0 {
		stagingRequirement.RequiredBytes = expectedBytes
	}
	stagingRequirement.NeedShareSave = true
	// share.save is a cloud-side copy that does not consume worker-local
	// storage, so skip the free_bytes check for staging (mirrors share.inspect
	// handling above). Providers such as guangyapan routinely report
	// free_bytes=0 because the quota API is unavailable.
	stagingRequirement.RequiredBytes = 0
	if stagingRequirement.Provider != "" && !containsFold(capabilities.SupportedProviders, stagingRequirement.Provider) {
		utils.Log.Warnf("[cluster-dispatch] node=%s staging provider=%s not in supported=%v", nodeID, stagingRequirement.Provider, capabilities.SupportedProviders)
		return nodeProviderAccountMatch{}, false, nil
	}
	var providerAccounts []protocol.ProviderAccountInventory
	if strings.TrimSpace(inventory.ProviderAccountsJSON) != "" {
		if err := json.Unmarshal([]byte(inventory.ProviderAccountsJSON), &providerAccounts); err != nil {
			return nodeProviderAccountMatch{}, false, err
		}
	}
	// share.inspect is metadata-only and has no delivery account. It still
	// requires a healthy provider account with read/share credentials.
	if containsFold(required, model.ClusterJobTypeShareInspect) && strings.TrimSpace(taskContext.TargetProfile) == "" {
		if len(providerAccounts) == 0 {
			return nodeProviderAccountMatch{}, true, nil
		}
		stagingRequirement.RequiredBytes = 0
		staging, ok := selectProviderAccount(providerAccounts, stagingRequirement, "", true)
		return nodeProviderAccountMatch{Staging: staging}, ok, nil
	}
	deliveryRequirement := taskContext.DeliveryTarget
	if deliveryRequirement.RequiredBytes <= 0 {
		deliveryRequirement.RequiredBytes = expectedBytes
	}
	deliveryRequirement.NeedUpload = true
	if deliveryRequirement.Provider != "" && !containsFold(capabilities.SupportedProviders, deliveryRequirement.Provider) {
		return nodeProviderAccountMatch{}, false, nil
	}
	targetPath := ""
	if strings.TrimSpace(taskContext.TargetProfile) != "" {
		var ok bool
		targetPath, ok = resolveInventoryTargetPath(ctx, nodeID, taskContext.TargetProfile)
		if !ok {
			return nodeProviderAccountMatch{}, false, nil
		}
	}
	if len(providerAccounts) == 0 {
		var mounts []protocol.MountInventory
		if err := json.Unmarshal([]byte(inventory.MountsJSON), &mounts); err != nil {
			return nodeProviderAccountMatch{}, false, err
		}
		return nodeProviderAccountMatch{}, mountsSupportTarget(mounts, targetPath, expectedBytes), nil
	}
	staging, ok := selectProviderAccount(providerAccounts, stagingRequirement, "", true)
	if !ok {
		utils.Log.Warnf("[cluster-dispatch] node=%s staging failed: provider=%s needShareSave=%v requiredBytes=%d accounts=%d", nodeID, stagingRequirement.Provider, stagingRequirement.NeedShareSave, stagingRequirement.RequiredBytes, len(providerAccounts))
		return nodeProviderAccountMatch{}, false, nil
	}
	delivery, ok := selectProviderAccount(providerAccounts, deliveryRequirement, targetPath, false)
	if !ok {
		utils.Log.Warnf("[cluster-dispatch] node=%s delivery failed: provider=%s needUpload=%v requiredBytes=%d targetPath=%s accounts=%d", nodeID, deliveryRequirement.Provider, deliveryRequirement.NeedUpload, deliveryRequirement.RequiredBytes, targetPath, len(providerAccounts))
		return nodeProviderAccountMatch{}, false, nil
	}
	var nodeActiveJobs int64
	if err := db.GetDb().WithContext(ctx).Model(&model.ClusterJob{}).
		Where("assigned_node_id = ? AND status IN ?", nodeID, []string{model.ClusterJobStatusLeased, model.ClusterJobStatusRunning, model.ClusterJobStatusCancelRequested}).
		Count(&nodeActiveJobs).Error; err != nil {
		return nodeProviderAccountMatch{}, false, err
	}
	return combineProviderAccountMatch(staging, delivery, nodeActiveJobs), true, nil
}

func providerAccountsSupportSource(accounts []protocol.ProviderAccountInventory, provider string) bool {
	_, ok := selectProviderAccount(accounts, protocol.ProviderTargetRequirement{
		Provider: provider, NeedShareSave: true,
	}, "", true)
	return ok
}

func providerAccountsSupportTarget(accounts []protocol.ProviderAccountInventory, targetPath string, targetProvider string, expectedBytes int64) bool {
	_, ok := selectProviderAccount(accounts, protocol.ProviderTargetRequirement{
		Provider: targetProvider, NeedUpload: true, RequiredBytes: expectedBytes,
	}, targetPath, false)
	return ok
}

func providerAccountHealthy(account protocol.ProviderAccountInventory) bool {
	return strings.EqualFold(strings.TrimSpace(account.Status), "work")
}

func selectProviderAccount(accounts []protocol.ProviderAccountInventory, requirement protocol.ProviderTargetRequirement, targetPath string, staging bool) (protocol.ProviderAccountInventory, bool) {
	candidates := make([]protocol.ProviderAccountInventory, 0, len(accounts))
	for _, account := range accounts {
		if !providerAccountMatches(account, requirement, targetPath, staging) {
			continue
		}
		candidates = append(candidates, account)
	}
	if len(candidates) == 0 {
		return protocol.ProviderAccountInventory{}, false
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].MembershipWeight != candidates[j].MembershipWeight {
			return candidates[i].MembershipWeight > candidates[j].MembershipWeight
		}
		if candidates[i].FreeBytes != candidates[j].FreeBytes {
			return candidates[i].FreeBytes > candidates[j].FreeBytes
		}
		if candidates[i].ActiveJobs != candidates[j].ActiveJobs {
			return candidates[i].ActiveJobs < candidates[j].ActiveJobs
		}
		return providerAccountStableID(candidates[i]) < providerAccountStableID(candidates[j])
	})
	return candidates[0], true
}

func providerAccountMatches(account protocol.ProviderAccountInventory, requirement protocol.ProviderTargetRequirement, targetPath string, staging bool) bool {
	if !providerAccountHealthy(account) {
		return false
	}
	if provider := strings.TrimSpace(requirement.Provider); provider != "" && !strings.EqualFold(strings.TrimSpace(account.Provider), provider) {
		return false
	}
	if requirement.StorageID != 0 && account.StorageID != requirement.StorageID {
		return false
	}
	if requirement.NodeMountID != "" && account.NodeMountID != requirement.NodeMountID {
		return false
	}
	if requirement.AccountFingerprint != "" && account.AccountFingerprint != requirement.AccountFingerprint {
		return false
	}
	if targetPath != "" {
		resolvedTargetPath := path.Clean(targetPath)
		mountPath := strings.TrimRight(path.Clean(account.MountPath), "/")
		if resolvedTargetPath != mountPath && !strings.HasPrefix(resolvedTargetPath, mountPath+"/") {
			return false
		}
	}
	if staging || requirement.NeedShareSave {
		if !account.SupportsShareSave || !account.SupportsDownload {
			return false
		}
	}
	if requirement.NeedUpload {
		if !account.SupportsUpload || !account.SupportsETF {
			return false
		}
		if strings.EqualFold(strings.TrimSpace(account.Provider), "yidong139") && account.MaxSingleUploadBytes <= 0 {
			return false
		}
		if requirement.RequiredBytes > 0 && account.MaxSingleUploadBytes > 0 && requirement.RequiredBytes > account.MaxSingleUploadBytes {
			return false
		}
	}
	if requirement.RequiredBytes > 0 && account.FreeBytes < requirement.RequiredBytes {
		return false
	}
	return true
}

func providerAccountStableID(account protocol.ProviderAccountInventory) string {
	if account.AccountFingerprint != "" {
		return account.AccountFingerprint
	}
	if account.NodeMountID != "" {
		return account.NodeMountID
	}
	return strings.ToLower(strings.TrimSpace(account.Provider)) + ":" + path.Clean(account.MountPath)
}

func combineProviderAccountMatch(staging, delivery protocol.ProviderAccountInventory, nodeActiveJobs int64) nodeProviderAccountMatch {
	freeBytes := staging.FreeBytes
	if freeBytes == 0 || (delivery.FreeBytes > 0 && delivery.FreeBytes < freeBytes) {
		freeBytes = delivery.FreeBytes
	}
	activeJobs := staging.ActiveJobs + delivery.ActiveJobs
	if providerAccountStableID(staging) == providerAccountStableID(delivery) {
		activeJobs = staging.ActiveJobs
	}
	return nodeProviderAccountMatch{
		Staging: staging, Delivery: delivery,
		MembershipWeight: staging.MembershipWeight + delivery.MembershipWeight,
		FreeBytes:        freeBytes, ActiveJobs: activeJobs, NodeActiveJobs: nodeActiveJobs,
	}
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
