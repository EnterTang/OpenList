package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
)

var (
	listInventoryStorages          = db.GetEnabledStorages
	hydrateInventoryStorage        = defaultHydrateInventoryStorage
	getInventoryStorageByMountPath = op.GetStorageByMountPath
)

type inventoryStorageSnapshot struct {
	Mount   protocol.MountInventory
	Account protocol.ProviderAccountInventory
}

type clusterMembershipTierReporter interface {
	ClusterMembershipTier() string
}

type clusterMembershipDetailsReporter interface {
	ClusterMembershipDetails() model.MembershipDetails
}

func defaultHydrateInventoryStorage(ctx context.Context, nodeID string, storage model.Storage) (inventoryStorageSnapshot, error) {
	mount := providerMountInventory(nodeID, storage)
	account := providerAccountInventory(nodeID, storage, 0, 0)
	if driver, driverErr := getInventoryStorageByMountPath(storage.MountPath); driverErr == nil {
		healthy := storageHealthy(*driver.GetStorage())
		writable := healthy && !driver.Config().NoUpload
		mount.ReadOnly = !writable
		mount.CanUpload = writable
		mount.CanShare = healthy && supportsShare(storage.Driver)
		mount.SupportsETF = writable && supportsETFAccount(storage)
		account.Status = driver.GetStorage().Status
		account.SupportsUpload = writable
		account.SupportsDownload = healthy
		account.SupportsShareSave = healthy && supportsShare(storage.Driver)
		account.SupportsETF = writable && supportsETFAccount(storage)
		if details, detailsErr := op.GetStorageDetails(ctx, driver); detailsErr == nil && details != nil {
			mount.TotalBytes = details.TotalSpace
			mount.FreeBytes = details.FreeSpace()
			account.TotalBytes = details.TotalSpace
			account.FreeBytes = details.FreeSpace()
		}
		if reporter, ok := driver.(clusterMembershipDetailsReporter); ok {
			applyRuntimeMembership(&account, reporter.ClusterMembershipDetails())
		} else if reporter, ok := driver.(clusterMembershipTierReporter); ok {
			applyRuntimeMembership(&account, model.MembershipDetails{Tier: reporter.ClusterMembershipTier()})
		}
	}
	return inventoryStorageSnapshot{Mount: mount, Account: account}, nil
}

func applyRuntimeMembership(account *protocol.ProviderAccountInventory, membership model.MembershipDetails) {
	account.MembershipStatus = strings.TrimSpace(membership.Status)
	account.MembershipExpireDate = strings.TrimSpace(membership.ExpireDate)
	if account.MembershipTier != "" && account.MembershipTier != "unknown" {
		return
	}
	runtimeTier := normalizeProviderMembershipTier(membership.Tier)
	if runtimeTier == "" || runtimeTier == "unknown" {
		return
	}
	account.MembershipTier = runtimeTier
	account.MembershipWeight = providerMembershipWeight(runtimeTier)
	account.MaxSingleUploadBytes = mobileProviderUploadLimit(account.Provider, runtimeTier)
}

func providerMountInventory(nodeID string, storage model.Storage) protocol.MountInventory {
	provider := providerName(storage.Driver)
	healthy := storageHealthy(storage)
	alias := accountAlias(storage, provider)
	return protocol.MountInventory{
		NodeMountID:        stableMountID(nodeID, storage.ID, storage.MountPath),
		Driver:             storage.Driver,
		Provider:           provider,
		MountPath:          storage.MountPath,
		AccountAlias:       alias,
		AccountFingerprint: accountFingerprint(storage, provider),
		Status:             storage.Status,
		ReadOnly:           storage.Disabled || !healthy,
		CanUpload:          !storage.Disabled && healthy,
		CanShare:           !storage.Disabled && healthy && supportsShare(storage.Driver),
		SupportsETF:        !storage.Disabled && healthy && supportsETFAccount(storage),
	}
}

func providerAccountInventory(nodeID string, storage model.Storage, freeBytes, totalBytes int64) protocol.ProviderAccountInventory {
	provider := providerName(storage.Driver)
	tier := providerMembershipTier(storage, provider)
	healthy := storageHealthy(storage)
	return protocol.ProviderAccountInventory{
		StorageID:            storage.ID,
		NodeMountID:          stableMountID(nodeID, storage.ID, storage.MountPath),
		Provider:             provider,
		MountPath:            storage.MountPath,
		AccountAlias:         accountAlias(storage, provider),
		AccountFingerprint:   accountFingerprint(storage, provider),
		Status:               storage.Status,
		MembershipTier:       tier,
		MembershipWeight:     providerMembershipWeight(tier),
		MaxSingleUploadBytes: mobileProviderUploadLimit(provider, tier),
		SupportsUpload:       !storage.Disabled && healthy,
		SupportsDownload:     !storage.Disabled && healthy,
		SupportsShareSave:    !storage.Disabled && healthy && supportsShare(storage.Driver),
		SupportsETF:          !storage.Disabled && healthy && supportsETFAccount(storage),
		TotalBytes:           totalBytes,
		FreeBytes:            freeBytes,
	}
}

func mobileProviderUploadLimit(provider, tier string) int64 {
	if provider != "yidong139" {
		return 0
	}
	switch strings.ToLower(strings.TrimSpace(tier)) {
	case "diamond":
		return 500 << 30
	case "gold":
		return 20 << 30
	case "silver":
		return 8 << 30
	case "ordinary", "normal":
		return 5 << 30
	default:
		return 0
	}
}

func providerMembershipTier(storage model.Storage, provider string) string {
	values := storageAdditionValues(storage)
	for _, key := range []string{"membership_tier", "member_tier", "vip_tier", "vip_level", "user_level"} {
		if value := strings.TrimSpace(fmt.Sprint(values[key])); value != "" && value != "<nil>" {
			return normalizeProviderMembershipTier(value)
		}
	}
	switch provider {
	case "pan123", "pan115", "yidong139":
		return "unknown"
	default:
		return ""
	}
}

func normalizeProviderMembershipTier(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "diamond", "钻石", "钻石会员":
		return "diamond"
	case "gold", "黄金", "黄金会员":
		return "gold"
	case "silver", "白银", "白银会员":
		return "silver"
	case "ordinary", "normal", "free", "普通", "普通会员", "非会员":
		return "ordinary"
	case "vip", "member", "会员":
		return "vip"
	case "svip", "super_vip", "supervip", "超级会员":
		return "svip"
	case "unknown", "未知", "":
		return "unknown"
	default:
		return strings.ToLower(strings.TrimSpace(value))
	}
}

func providerMembershipWeight(tier string) int {
	switch normalizeProviderMembershipTier(tier) {
	case "diamond":
		return 400
	case "gold":
		return 300
	case "silver":
		return 200
	case "ordinary":
		return 100
	case "vip":
		return 200
	case "svip":
		return 300
	default:
		return 0
	}
}

func storageHealthy(storage model.Storage) bool {
	return !storage.Disabled && strings.EqualFold(strings.TrimSpace(storage.Status), op.WORK)
}

func supportsETFAccount(storage model.Storage) bool {
	if !supportsETF(storage.Driver) {
		return false
	}
	values := storageAdditionValues(storage)
	dedicated, _ := values["cluster_dedicated_account"].(bool)
	typeName := strings.ToLower(strings.TrimSpace(fmt.Sprint(values["type"])))
	return dedicated && (typeName == "" || typeName == "personal_new")
}

func accountAlias(storage model.Storage, provider string) string {
	if alias := strings.TrimSpace(storage.Remark); alias != "" {
		return alias
	}
	return fmt.Sprintf("%s-%d", provider, storage.ID)
}

func accountFingerprint(storage model.Storage, provider string) string {
	values := storageAdditionValues(storage)
	identity := ""
	for _, key := range []string{"account", "username", "user_id", "user_domain_id", "open_id"} {
		if value := strings.TrimSpace(fmt.Sprint(values[key])); value != "" && value != "<nil>" {
			identity = key + ":" + value
			break
		}
	}
	if identity == "" {
		identity = fmt.Sprintf("storage:%d:%s", storage.ID, strings.TrimSpace(storage.MountPath))
	}
	sum := sha256.Sum256([]byte(provider + "\x00" + identity))
	return hex.EncodeToString(sum[:16])
}

func storageAdditionValues(storage model.Storage) map[string]any {
	values := make(map[string]any)
	_ = json.Unmarshal([]byte(storage.Addition), &values)
	return values
}
