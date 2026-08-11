package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
)

var (
	listInventoryStorages          = db.GetEnabledStorages
	hydrateInventoryStorage        = defaultHydrateInventoryStorage
	getInventoryStorageByMountPath = op.GetStorageByMountPath
)

const (
	inventoryStatusStorageUnavailable      = "storage_unavailable"
	inventoryStatusReauthorizationRequired = "reauthorization_required"
)

type inventoryStorageLookupKind uint8

const (
	inventoryStorageLookupOK inventoryStorageLookupKind = iota
	inventoryStorageLookupStaticStorageOnly
	inventoryStorageLookupReauthorizationRequired
	inventoryStorageLookupUnavailable
)

type inventoryStorageSnapshot struct {
	Mount      protocol.MountInventory
	Account    protocol.ProviderAccountInventory
	lookupKind inventoryStorageLookupKind
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
	driver, driverErr := getInventoryStorageByMountPath(storage.MountPath)
	if driverErr != nil {
		lookupKind := inventoryStorageLookupKindFromError(driverErr)
		if lookupKind != inventoryStorageLookupStaticStorageOnly {
			applyInventoryStatus(&mount, &account, inventoryReportedStorageStatusFromError(driverErr))
		}
		return inventoryStorageSnapshot{
			Mount:      mount,
			Account:    account,
			lookupKind: lookupKind,
		}, nil
	}

	liveStorage := *driver.GetStorage()
	reportedStatus := inventoryReportedStorageStatus(liveStorage.Status)
	healthy := !liveStorage.Disabled && strings.EqualFold(strings.TrimSpace(reportedStatus), op.WORK)
	writable := healthy && !driver.Config().NoUpload

	mount.Status = reportedStatus
	mount.ReadOnly = !writable
	mount.CanUpload = writable
	mount.CanShare = healthy && supportsShare(liveStorage.Driver)
	mount.SupportsETF = writable && supportsETFAccount(liveStorage)

	account.Status = reportedStatus
	account.SupportsUpload = writable
	account.SupportsDownload = healthy
	account.SupportsShareSave = healthy && supportsShare(liveStorage.Driver)
	account.SupportsETF = writable && supportsETFAccount(liveStorage)

	if !healthy {
		return inventoryStorageSnapshot{Mount: mount, Account: account}, nil
	}

	if details, detailsErr := op.GetStorageDetails(ctx, driver); detailsErr == nil && details != nil {
		mount.TotalBytes = details.TotalSpace
		mount.FreeBytes = details.FreeSpace()
		account.TotalBytes = details.TotalSpace
		account.FreeBytes = details.FreeSpace()
	} else if detailsErr != nil && !errs.IsNotImplementError(detailsErr) {
		account.HealthState = "degraded"
		account.LastErrorCode = inventoryProbeErrorCode(detailsErr)
		if account.LastErrorCode == inventoryStatusReauthorizationRequired {
			account.CredentialState = inventoryStatusReauthorizationRequired
		}
		account.SupportsShareSave = false
		account.SupportedOperations = nil
	}
	if reporter, ok := driver.(clusterMembershipDetailsReporter); ok {
		applyRuntimeMembership(&account, reporter.ClusterMembershipDetails())
	} else if reporter, ok := driver.(clusterMembershipTierReporter); ok {
		applyRuntimeMembership(&account, model.MembershipDetails{Tier: reporter.ClusterMembershipTier()})
	}
	return inventoryStorageSnapshot{Mount: mount, Account: account}, nil
}

func inventoryProbeErrorCode(err error) string {
	message := strings.ToLower(strings.TrimSpace(err.Error()))
	for _, marker := range []string{"refresh token", "refresh_token", "invalid token", "unauthorized", "forbidden", "credential", "sign invalid", "signature"} {
		if strings.Contains(message, marker) {
			return inventoryStatusReauthorizationRequired
		}
	}
	return "provider_health_probe_failed"
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
	status := inventoryReportedStorageStatus(storage.Status)
	healthy := !storage.Disabled && strings.EqualFold(strings.TrimSpace(status), op.WORK)
	alias := accountAlias(storage, provider)
	return protocol.MountInventory{
		NodeMountID:        stableMountID(nodeID, storage.ID, storage.MountPath),
		Driver:             storage.Driver,
		Provider:           provider,
		MountPath:          storage.MountPath,
		AccountAlias:       alias,
		AccountFingerprint: accountFingerprint(storage, provider),
		Status:             status,
		ReadOnly:           storage.Disabled || !healthy,
		CanUpload:          !storage.Disabled && healthy,
		CanShare:           !storage.Disabled && healthy && supportsShare(storage.Driver),
		SupportsETF:        !storage.Disabled && healthy && supportsETFAccount(storage),
	}
}

func providerAccountInventory(nodeID string, storage model.Storage, freeBytes, totalBytes int64) protocol.ProviderAccountInventory {
	provider := providerName(storage.Driver)
	tier := providerMembershipTier(storage, provider)
	status := inventoryReportedStorageStatus(storage.Status)
	healthy := !storage.Disabled && strings.EqualFold(strings.TrimSpace(status), op.WORK)
	healthState := "unknown"
	credentialState := "unknown"
	if healthy {
		healthState = "ready"
		credentialState = "ready"
	} else if storage.Disabled || strings.EqualFold(strings.TrimSpace(status), op.DISABLED) {
		healthState = "offline"
	}
	checkedAt := time.Now().UTC()
	return protocol.ProviderAccountInventory{
		StorageID:            storage.ID,
		NodeMountID:          stableMountID(nodeID, storage.ID, storage.MountPath),
		Provider:             provider,
		MountPath:            storage.MountPath,
		AccountAlias:         accountAlias(storage, provider),
		AccountFingerprint:   accountFingerprint(storage, provider),
		Status:               status,
		MembershipTier:       tier,
		MembershipWeight:     providerMembershipWeight(tier),
		MaxSingleUploadBytes: mobileProviderUploadLimit(provider, tier),
		SupportsUpload:       !storage.Disabled && healthy,
		SupportsDownload:     !storage.Disabled && healthy,
		SupportsShareSave:    !storage.Disabled && healthy && supportsShare(storage.Driver),
		CredentialState:      credentialState,
		HealthState:          healthState,
		CheckedAt:            checkedAt,
		NextProbeAt:          checkedAt.Add(5 * time.Minute),
		SupportedOperations:  providerSupportedOperations(provider, healthy && !storage.Disabled, healthy && !storage.Disabled && supportsShare(storage.Driver)),
		SupportsETF:          !storage.Disabled && healthy && supportsETFAccount(storage),
		TotalBytes:           totalBytes,
		FreeBytes:            freeBytes,
	}
}

func providerSupportedOperations(provider string, downloadReady, shareSaveReady bool) []string {
	if !downloadReady {
		return nil
	}
	operations := []string{"share.inspect", "result_probe"}
	if shareSaveReady {
		operations = append(operations, "share.save")
	}
	switch provider {
	case "pan123":
		// 123Pan's share-download contract is covered by the provider HTTP
		// tests. The subscription feature gate remains off by default, so this
		// capability is only consumed when direct-download-first is enabled.
		operations = append(operations, "share.download", "instant_upload", "range_download")
	case "guangyapan":
		operations = append(operations, "instant_upload", "range_download")
	}
	operations = append(operations, "download")
	return operations
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
	return !storage.Disabled && strings.EqualFold(strings.TrimSpace(inventoryReportedStorageStatus(storage.Status)), op.WORK)
}

func supportsETFAccount(storage model.Storage) bool {
	if !supportsETF(storage.Driver) {
		return false
	}
	values := storageAdditionValues(storage)
	dedicated := true
	if raw, ok := values["cluster_dedicated_account"]; ok {
		dedicated, _ = raw.(bool)
	}
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

func applyInventoryStatus(mount *protocol.MountInventory, account *protocol.ProviderAccountInventory, status string) {
	mount.Status = status
	mount.ReadOnly = true
	mount.CanUpload = false
	mount.CanShare = false
	mount.SupportsETF = false

	account.Status = status
	account.SupportsUpload = false
	account.SupportsDownload = false
	account.SupportsShareSave = false
	account.CredentialState = "unknown"
	account.HealthState = "offline"
	account.SupportedOperations = nil
	account.SupportsETF = false
}

func inventoryReportedStorageStatusFromError(err error) string {
	if inventoryStorageLookupKindFromError(err) == inventoryStorageLookupReauthorizationRequired {
		return inventoryStatusReauthorizationRequired
	}
	return inventoryStatusStorageUnavailable
}

func inventoryStorageLookupKindFromError(err error) inventoryStorageLookupKind {
	if err == nil {
		return inventoryStorageLookupUnavailable
	}
	message := err.Error()
	if inventoryStatusRequiresReauthorization(message) {
		return inventoryStorageLookupReauthorizationRequired
	}
	if inventoryStorageLookupIsMissingDriver(message) {
		return inventoryStorageLookupStaticStorageOnly
	}
	return inventoryStorageLookupUnavailable
}

func inventoryStorageLookupIsMissingDriver(message string) bool {
	return strings.Contains(strings.ToLower(strings.TrimSpace(message)), "no mount path for an storage is:")
}

func inventoryReportedStorageStatus(status string) string {
	trimmed := strings.TrimSpace(status)
	if trimmed == "" {
		return ""
	}
	if strings.EqualFold(trimmed, op.WORK) {
		return op.WORK
	}
	if strings.EqualFold(trimmed, op.DISABLED) {
		return op.DISABLED
	}
	if inventoryStatusRequiresReauthorization(trimmed) {
		return inventoryStatusReauthorizationRequired
	}
	if inventoryStatusLooksLikeInitializationError(trimmed) {
		return inventoryStatusStorageUnavailable
	}
	return trimmed
}

func inventoryStatusRequiresReauthorization(value string) bool {
	lower := strings.ToLower(strings.TrimSpace(value))
	for _, marker := range []string{
		"refresh",
		"token",
		"auth",
		"unauthor",
		"unauthorization",
		"authorization",
		"unauthorized",
		"reauthor",
	} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}

func inventoryStatusLooksLikeInitializationError(value string) bool {
	lower := strings.ToLower(strings.TrimSpace(value))
	if lower == "" {
		return false
	}
	if !inventoryStatusIsStableToken(lower) {
		return true
	}
	for _, marker := range []string{
		"failed",
		"panic",
		"timeout",
		"unavailable",
		"expired",
		"invalid",
		"cookie",
		"session",
	} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}

func inventoryStatusIsStableToken(value string) bool {
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= '0' && r <= '9':
		case r == '_' || r == '-':
		default:
			return false
		}
	}
	return true
}
