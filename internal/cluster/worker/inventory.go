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
)

func BuildInventory(ctx context.Context, nodeID string, redisReady bool) (protocol.InventoryReport, error) {
	storages, err := listInventoryStorages()
	if err != nil {
		return protocol.InventoryReport{}, err
	}
	mounts := make([]protocol.MountInventory, 0, len(storages))
	providerAccounts := make([]protocol.ProviderAccountInventory, 0, len(storages))
	providers := make(map[string]struct{})
	for _, storage := range storages {
		snapshot, err := hydrateInventoryStorage(ctx, nodeID, storage)
		if err != nil {
			return protocol.InventoryReport{}, err
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

func stableMountID(nodeID string, storageID uint, mountPath string) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s|%d|%s", nodeID, storageID, mountPath)))
	return hex.EncodeToString(sum[:16])
}

func providerName(driver string) string {
	switch strings.ToLower(strings.TrimSpace(driver)) {
	case "aliyundriveopen", "aliyundrive":
		return "aliyun_drive"
	case "115 cloud", "115 open":
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
	lower := strings.ToLower(driver)
	return strings.Contains(lower, "139") || strings.Contains(lower, "115") || strings.Contains(lower, "aliyun") || strings.Contains(lower, "123") || strings.Contains(lower, "quark")
}
