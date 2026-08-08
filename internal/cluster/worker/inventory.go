package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/subscription"
)

var getInventorySubscriptionConfig = subscription.GetConfig

func BuildInventory(ctx context.Context, nodeID string, redisReady bool) (protocol.InventoryReport, error) {
	storages, err := listInventoryStorages()
	if err != nil {
		return protocol.InventoryReport{}, err
	}
	mounts := make([]protocol.MountInventory, 0, len(storages))
	providerAccounts := make([]protocol.ProviderAccountInventory, 0, len(storages))
	providers := make(map[string]struct{})
	var subscriptionConfig model.SubscriptionConfig
	var subscriptionConfigLoaded bool
	var subscriptionConfigErr error
	for _, storage := range storages {
		snapshot, err := hydrateInventoryStorage(ctx, nodeID, storage)
		if err != nil {
			return protocol.InventoryReport{}, err
		}
		// Healthy provider credentials still require worker-local subscription
		// routing. Otherwise the coordinator could assign a provider-only task
		// that the worker must reject because it has no share-provider staging
		// folder or 139 delivery folder.
		if requiresWorkerStagingRouting(snapshot.Account.Provider) || requiresWorkerDeliveryRouting(snapshot.Account.Provider) {
			if !subscriptionConfigLoaded {
				subscriptionConfig, subscriptionConfigErr = getInventorySubscriptionConfig()
				subscriptionConfigLoaded = true
			}
			if requiresWorkerStagingRouting(snapshot.Account.Provider) && (subscriptionConfigErr != nil || !hasWorkerStagingRouting(subscriptionConfig, snapshot.Account.Provider)) {
				snapshot.Account.SupportsShareSave = false
			}
			if requiresWorkerDeliveryRouting(snapshot.Account.Provider) && (subscriptionConfigErr != nil || !hasWorkerDeliveryRouting(subscriptionConfig)) {
				snapshot.Account.SupportsUpload = false
			}
		}
		providers[snapshot.Mount.Provider] = struct{}{}
		mounts = append(mounts, snapshot.Mount)
		providerAccounts = append(providerAccounts, snapshot.Account)
	}
	providerList := make([]string, 0, len(providers))
	for provider := range providers {
		providerList = append(providerList, provider)
	}
	sort.Strings(providerList)
	report := protocol.InventoryReport{
		Revision:    uint64(time.Now().UTC().UnixNano()),
		CollectedAt: time.Now().UTC(),
		Capabilities: protocol.NodeCapabilities{
			SupportedProviders:   providerList,
			SupportedOperations:  []string{"share.inspect", "share.save", "download", "mobile.upload", "result.report", "config.apply", "storage.apply"},
			RedisDurabilityReady: redisReady,
		},
		Mounts:           mounts,
		ProviderAccounts: providerAccounts,
	}
	raw, err := json.Marshal(struct {
		Capabilities     protocol.NodeCapabilities           `json:"capabilities"`
		Mounts           []protocol.MountInventory           `json:"mounts"`
		ProviderAccounts []protocol.ProviderAccountInventory `json:"provider_accounts"`
	}{report.Capabilities, report.Mounts, report.ProviderAccounts})
	if err != nil {
		return protocol.InventoryReport{}, fmt.Errorf("marshal cluster inventory: %w", err)
	}
	sum := sha256.Sum256(raw)
	report.InventoryHash = hex.EncodeToString(sum[:])
	return report, nil
}

func requiresWorkerStagingRouting(provider string) bool {
	switch normalizeControlKey(provider) {
	case "pan123", "pan115", "quark", "aliyun_drive", "guangyapan":
		return true
	default:
		return false
	}
}

func requiresWorkerDeliveryRouting(provider string) bool {
	return normalizeControlKey(provider) == "yidong139"
}

func hasWorkerStagingRouting(cfg model.SubscriptionConfig, provider string) bool {
	provider = normalizeControlKey(provider)
	if !requiresWorkerStagingRouting(provider) {
		return true
	}
	target, ok := workerStagingTargetFromConfig(cfg, provider)
	if !ok {
		return false
	}
	return target.Provider == provider && target.Folder != "" && subscription.ValidateSubscriptionStorageTarget(target) == nil
}

func hasWorkerDeliveryRouting(cfg model.SubscriptionConfig) bool {
	target := subscription.NormalizeSubscriptionStorageTarget(cfg.DefaultTarget)
	return target.Provider == "yidong139" && target.Folder != "" && subscription.ValidateSubscriptionStorageTarget(target) == nil
}

func stableMountID(nodeID string, storageID uint, mountPath string) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s|%d|%s", nodeID, storageID, mountPath)))
	return hex.EncodeToString(sum[:16])
}

func providerName(driver string) string {
	switch strings.ToLower(strings.TrimSpace(driver)) {
	case "aliyundriveopen", "aliyundrive":
		return "aliyun_drive"
	case "115 cloud", "115 open", "115 cd2", "115 sy", "115_sy":
		return "pan115"
	case "123pan", "123 open":
		return "pan123"
	case "quark":
		return "quark"
	case "139yun", "139 cloud", "139":
		return "yidong139"
	default:
		return strings.ToLower(strings.ReplaceAll(strings.TrimSpace(driver), " ", "_"))
	}
}

func supportsETF(driver string) bool {
	lower := strings.ToLower(driver)
	return strings.Contains(lower, "139")
}

func supportsShare(driver string) bool {
	switch strings.ToLower(strings.TrimSpace(driver)) {
	case "123pan", "123 open", "115 cloud", "115 sy", "115_sy", "aliyundriveopen", "quark", "guangyapan":
		return true
	default:
		return false
	}
}
