package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"gorm.io/gorm"
)

type nodeProviderAccountMatch struct {
	Staging          protocol.ProviderAccountInventory
	Delivery         protocol.ProviderAccountInventory
	MembershipWeight int
	FreeBytes        int64
	ActiveJobs       int
	NodeActiveJobs   int64
	MediaConcurrency int
}

// ResolveTorrentWorker returns the only legal Worker for a torrent transfer.
// A torrent's qB state and content path are local, so the normal provider
// capability fallback must never move this task to another Worker.
func ResolveTorrentWorker(taskContext protocol.TaskContext) (string, error) {
	if taskContext.Torrent == nil {
		return "", nil
	}
	workerNodeID := strings.TrimSpace(taskContext.Torrent.WorkerNodeID)
	if workerNodeID == "" {
		return "", errors.New("torrent task requires a bound worker node")
	}
	return workerNodeID, nil
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

func nodeInventoryProviderMatch(ctx context.Context, nodeID string, taskContext protocol.TaskContext, required []string, expectedBytes int64) (nodeProviderAccountMatch, bool, string, error) {
	const noReason = ""
	var inventory model.ClusterNodeInventory
	if err := db.GetDb().WithContext(ctx).Where("node_id = ?", nodeID).Order("revision DESC").First(&inventory).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nodeProviderAccountMatch{}, false, "inventory not found", nil
		}
		return nodeProviderAccountMatch{}, false, noReason, err
	}
	var capabilities protocol.NodeCapabilities
	if err := json.Unmarshal([]byte(inventory.CapabilitiesJSON), &capabilities); err != nil {
		return nodeProviderAccountMatch{}, false, noReason, err
	}
	if !capabilities.RedisDurabilityReady {
		return nodeProviderAccountMatch{}, false, "redis_durability_ready=false", nil
	}
	if taskContext.Torrent != nil {
		return nodeInventoryTorrentMatch(nodeID, taskContext, capabilities, expectedBytes)
	}
	for _, operation := range required {
		if !containsFold(capabilities.SupportedOperations, operation) {
			return nodeProviderAccountMatch{}, false, fmt.Sprintf("missing operation %q supported=%v", operation, capabilities.SupportedOperations), nil
		}
	}
	stagingRequirement := taskContext.StagingTarget
	if strings.TrimSpace(stagingRequirement.Provider) == "" {
		stagingRequirement.Provider = taskContext.Share.Provider
	}
	if stagingRequirement.RequiredBytes <= 0 {
		stagingRequirement.RequiredBytes = expectedBytes
	}
	directDownload := strings.EqualFold(strings.TrimSpace(taskContext.DeliveryMode), model.SubscriptionDeliveryModeDirectDownload)
	stagingRequirement.NeedShareSave = !directDownload
	stagingRequirement.NeedShareDownload = directDownload
	// share.save is a cloud-side copy that does not consume worker-local
	// storage, so skip the free_bytes check for staging (mirrors share.inspect
	// handling above). Providers such as guangyapan routinely report
	// free_bytes=0 because the quota API is unavailable.
	stagingRequirement.RequiredBytes = 0
	if stagingRequirement.Provider != "" && !containsFold(capabilities.SupportedProviders, stagingRequirement.Provider) {
		return nodeProviderAccountMatch{}, false, fmt.Sprintf("staging provider %q not in supported=%v", stagingRequirement.Provider, capabilities.SupportedProviders), nil
	}
	var providerAccounts []protocol.ProviderAccountInventory
	if strings.TrimSpace(inventory.ProviderAccountsJSON) != "" {
		if err := json.Unmarshal([]byte(inventory.ProviderAccountsJSON), &providerAccounts); err != nil {
			return nodeProviderAccountMatch{}, false, noReason, err
		}
	}
	// share.inspect is metadata-only and has no delivery account. It still
	// requires a healthy provider account with read/share credentials.
	if containsFold(required, model.ClusterJobTypeShareInspect) && strings.TrimSpace(taskContext.TargetProfile) == "" {
		if len(providerAccounts) == 0 {
			return nodeProviderAccountMatch{MediaConcurrency: capabilities.DownloadConcurrency}, true, noReason, nil
		}
		stagingRequirement.RequiredBytes = 0
		staging, ok := selectProviderAccount(providerAccounts, stagingRequirement, "", true)
		reason := ""
		if !ok {
			reason = fmt.Sprintf("share-inspect staging account unavailable: provider=%s accounts=%d", stagingRequirement.Provider, len(providerAccounts))
		}
		return nodeProviderAccountMatch{Staging: staging, MediaConcurrency: capabilities.DownloadConcurrency}, ok, reason, nil
	}
	deliveryRequirement := taskContext.DeliveryTarget
	if deliveryRequirement.RequiredBytes <= 0 {
		deliveryRequirement.RequiredBytes = expectedBytes
	}
	deliveryRequirement.NeedUpload = true
	if deliveryRequirement.Provider != "" && !containsFold(capabilities.SupportedProviders, deliveryRequirement.Provider) {
		return nodeProviderAccountMatch{}, false, fmt.Sprintf("delivery provider %q not in supported=%v", deliveryRequirement.Provider, capabilities.SupportedProviders), nil
	}
	targetPath := ""
	if strings.TrimSpace(taskContext.TargetProfile) != "" {
		var ok bool
		targetPath, ok = resolveInventoryTargetPath(ctx, nodeID, taskContext.TargetProfile)
		if !ok {
			return nodeProviderAccountMatch{}, false, fmt.Sprintf("target profile %q not resolved", taskContext.TargetProfile), nil
		}
	}
	if len(providerAccounts) == 0 {
		var mounts []protocol.MountInventory
		if err := json.Unmarshal([]byte(inventory.MountsJSON), &mounts); err != nil {
			return nodeProviderAccountMatch{}, false, noReason, err
		}
		if ok := mountsSupportTarget(mounts, targetPath, expectedBytes); ok {
			return nodeProviderAccountMatch{MediaConcurrency: capabilities.DownloadConcurrency}, true, noReason, nil
		}
		return nodeProviderAccountMatch{}, false, fmt.Sprintf("no mount supports target=%s expectedBytes=%d", targetPath, expectedBytes), nil
	}
	staging, ok := selectProviderAccount(providerAccounts, stagingRequirement, "", !directDownload)
	if !ok {
		return nodeProviderAccountMatch{}, false, fmt.Sprintf("staging account unavailable: provider=%s needShareSave=%v accounts=%d", stagingRequirement.Provider, stagingRequirement.NeedShareSave, len(providerAccounts)), nil
	}
	delivery, ok := selectProviderAccount(providerAccounts, deliveryRequirement, targetPath, false)
	if !ok {
		return nodeProviderAccountMatch{}, false, fmt.Sprintf("delivery account unavailable: provider=%s needUpload=%v targetPath=%s accounts=%d", deliveryRequirement.Provider, deliveryRequirement.NeedUpload, targetPath, len(providerAccounts)), nil
	}
	var nodeActiveJobs int64
	if err := db.GetDb().WithContext(ctx).Model(&model.ClusterJob{}).
		Where("assigned_node_id = ? AND type = ? AND status IN ?", nodeID, model.ClusterJobTypeMediaTransfer, []string{model.ClusterJobStatusLeased, model.ClusterJobStatusRunning, model.ClusterJobStatusCancelRequested}).
		Count(&nodeActiveJobs).Error; err != nil {
		return nodeProviderAccountMatch{}, false, noReason, err
	}
	match := combineProviderAccountMatch(staging, delivery, nodeActiveJobs)
	match.MediaConcurrency = capabilities.DownloadConcurrency
	return match, true, noReason, nil
}

func nodeInventoryTorrentMatch(nodeID string, taskContext protocol.TaskContext, capabilities protocol.NodeCapabilities, expectedBytes int64) (nodeProviderAccountMatch, bool, string, error) {
	workerNodeID, err := ResolveTorrentWorker(taskContext)
	if err != nil {
		return nodeProviderAccountMatch{}, false, "", err
	}
	if workerNodeID != nodeID {
		return nodeProviderAccountMatch{}, false, fmt.Sprintf("torrent is bound to worker %q", workerNodeID), nil
	}
	torrent := taskContext.Torrent
	operation := "qb.copy"
	if strings.EqualFold(strings.TrimSpace(torrent.Action), "delete") || strings.EqualFold(strings.TrimSpace(torrent.Action), "pause") || strings.EqualFold(strings.TrimSpace(torrent.Action), "resume") {
		operation = "qb.control"
	}
	if !containsFold(capabilities.SupportedOperations, operation) {
		return nodeProviderAccountMatch{}, false, fmt.Sprintf("missing operation %q", operation), nil
	}
	if torrent.Manual {
		// A manually selected qB torrent has no MoviePilot downloader alias to
		// match against inventory. The Worker resolves the configured qB client
		// locally and performs the authoritative completion/path checks before
		// any staging copy. Capacity remains enforced there as well.
		return nodeProviderAccountMatch{}, true, "", nil
	}
	for _, route := range capabilities.MoviePilotRoutes {
		if !strings.EqualFold(strings.TrimSpace(route.BridgeInstanceID), strings.TrimSpace(torrent.BridgeInstanceID)) ||
			!strings.EqualFold(strings.TrimSpace(route.Downloader), strings.TrimSpace(torrent.Downloader)) ||
			!strings.EqualFold(strings.TrimSpace(route.QBClientID), strings.TrimSpace(torrent.QBClientID)) {
			continue
		}
		if health := strings.ToLower(strings.TrimSpace(route.QBHealth)); health != "" && health != "configured" && health != "ready" && health != "healthy" {
			return nodeProviderAccountMatch{}, false, fmt.Sprintf("qB client %q health=%s", route.QBClientID, route.QBHealth), nil
		}
		if operation == "qb.copy" && route.UploadConcurrency > 0 && route.ActiveUploadSlots >= route.UploadConcurrency {
			return nodeProviderAccountMatch{}, false, fmt.Sprintf("qB client %q upload slots are full", route.QBClientID), nil
		}
		if operation == "qb.copy" && expectedBytes > 0 && route.StagingFreeBytes > 0 && route.StagingFreeBytes < expectedBytes {
			return nodeProviderAccountMatch{}, false, fmt.Sprintf("qB staging free space %d is below expected file size %d", route.StagingFreeBytes, expectedBytes), nil
		}
		return nodeProviderAccountMatch{MediaConcurrency: route.UploadConcurrency}, true, "", nil
	}
	return nodeProviderAccountMatch{}, false, fmt.Sprintf("MoviePilot route %q/%q/%q is not advertised by worker", torrent.BridgeInstanceID, torrent.Downloader, torrent.QBClientID), nil
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
	if !strings.EqualFold(strings.TrimSpace(account.Status), "work") {
		return false
	}
	if state := strings.TrimSpace(account.HealthState); state != "" && !strings.EqualFold(state, "ready") {
		return false
	}
	if state := strings.TrimSpace(account.CredentialState); state != "" && !strings.EqualFold(state, "ready") {
		return false
	}
	// Older inventories did not report probe timestamps; keep them usable
	// during the rolling upgrade. New reports must remain inside their probe
	// lease so a stale ready bit cannot route new work forever.
	if !account.CheckedAt.IsZero() {
		now := time.Now().UTC()
		if !account.NextProbeAt.IsZero() && now.After(account.NextProbeAt) {
			return false
		}
		if account.NextProbeAt.IsZero() && now.Sub(account.CheckedAt) > 10*time.Minute {
			return false
		}
	}
	return true
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
	if requirement.NeedShareDownload && !account.SupportsDownload {
		return false
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
