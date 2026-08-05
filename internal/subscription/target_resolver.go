package subscription

import (
	"context"
	"encoding/json"
	"fmt"
	stdpath "path"
	"sort"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/fs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
)

var (
	listProviderTargetStorages     = db.GetEnabledStorages
	storageFreeBytesForMountPath   = defaultStorageFreeBytesForMountPath
	storageActiveJobsForMountPath  = defaultStorageActiveJobsForMountPath
	liveMembershipTierForMountPath = defaultLiveMembershipTierForMountPath
	ensureProviderTargetFolder     = fs.MakeDir
	observeResolvedProviderTarget  func(ResolveProviderTargetRequest, ResolvedProviderTarget)
)

type membershipTierReporter interface {
	ClusterMembershipTier() string
}

type ResolveProviderTargetRequest struct {
	Provider      string
	Folder        string
	NeedUpload    bool
	NeedShareSave bool
	FileSize      int64
}

// ProviderAccountCandidate is the mode-independent account capability input
// used by standalone, hybrid, and worker target resolution.
type ProviderAccountCandidate struct {
	Provider             string
	StorageID            uint
	MountPath            string
	AccountAlias         string
	Status               string
	Disabled             bool
	MembershipTier       string
	MembershipWeight     int
	MaxSingleUploadBytes int64
	SupportsUpload       bool
	SupportsDownload     bool
	SupportsShareSave    bool
	SupportsETF          bool
	FreeBytes            int64
	HasFreeBytes         bool
	ActiveJobs           int
}

type ResolvedProviderTarget struct {
	Provider             string
	StorageID            uint
	MountPath            string
	Folder               string
	FullPath             string
	AccountAlias         string
	MembershipTier       string
	MembershipWeight     int
	MaxSingleUploadBytes int64
	FreeBytes            int64
	ActiveJobs           int
}

func ResolveProviderTarget(ctx context.Context, req ResolveProviderTargetRequest) (ResolvedProviderTarget, error) {
	target := model.SubscriptionStorageTarget{Provider: req.Provider, Folder: req.Folder}
	if err := ValidateSubscriptionStorageTarget(target); err != nil {
		return ResolvedProviderTarget{}, err
	}
	target = NormalizeSubscriptionStorageTarget(target)
	req.Provider = target.Provider
	req.Folder = target.Folder

	storages, err := listProviderTargetStorages()
	if err != nil {
		return ResolvedProviderTarget{}, fmt.Errorf("list provider target storages: %w", err)
	}
	candidates := make([]ProviderAccountCandidate, 0, len(storages))
	for _, storage := range storages {
		candidate := providerAccountCandidateFromStorage(ctx, storage)
		if candidate.Provider == req.Provider {
			candidates = append(candidates, candidate)
		}
	}
	resolved, err := ResolveProviderTargetFromCandidates(ctx, req, candidates)
	if err == nil && observeResolvedProviderTarget != nil {
		observeResolvedProviderTarget(req, resolved)
	}
	return resolved, err
}

func ResolveProviderTargetFromCandidates(_ context.Context, req ResolveProviderTargetRequest, candidates []ProviderAccountCandidate) (ResolvedProviderTarget, error) {
	target := model.SubscriptionStorageTarget{Provider: req.Provider, Folder: req.Folder}
	if err := ValidateSubscriptionStorageTarget(target); err != nil {
		return ResolvedProviderTarget{}, err
	}
	target = NormalizeSubscriptionStorageTarget(target)
	if target.Provider == "" {
		return ResolvedProviderTarget{}, fmt.Errorf("provider target provider is required")
	}

	eligible := make([]ProviderAccountCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		candidate.Provider = strings.ToLower(strings.TrimSpace(candidate.Provider))
		candidate.MountPath = cleanConfigPath(candidate.MountPath)
		if candidate.Provider != target.Provider || candidate.MountPath == "" {
			continue
		}
		if candidate.Disabled || !strings.EqualFold(strings.TrimSpace(candidate.Status), op.WORK) {
			continue
		}
		if req.NeedUpload && !candidate.SupportsUpload {
			continue
		}
		if req.NeedUpload && target.Provider == "yidong139" && candidate.MaxSingleUploadBytes <= 0 {
			continue
		}
		if req.NeedShareSave && (!candidate.SupportsShareSave || !candidate.SupportsDownload) {
			continue
		}
		if req.FileSize > 0 && candidate.HasFreeBytes && candidate.FreeBytes < req.FileSize {
			continue
		}
		if req.NeedUpload && req.FileSize > 0 && candidate.MaxSingleUploadBytes > 0 && req.FileSize > candidate.MaxSingleUploadBytes {
			continue
		}
		eligible = append(eligible, candidate)
	}
	if len(eligible) == 0 {
		return ResolvedProviderTarget{}, fmt.Errorf("no compatible provider account for %s", target.Provider)
	}

	sort.SliceStable(eligible, func(i, j int) bool {
		left, right := eligible[i], eligible[j]
		if left.MembershipWeight != right.MembershipWeight {
			return left.MembershipWeight > right.MembershipWeight
		}
		if left.HasFreeBytes != right.HasFreeBytes {
			return left.HasFreeBytes
		}
		if left.FreeBytes != right.FreeBytes {
			return left.FreeBytes > right.FreeBytes
		}
		if left.ActiveJobs != right.ActiveJobs {
			return left.ActiveJobs < right.ActiveJobs
		}
		if left.MountPath != right.MountPath {
			return left.MountPath < right.MountPath
		}
		return left.StorageID < right.StorageID
	})

	selected := eligible[0]
	fullPath := selected.MountPath
	if target.Folder != "" {
		fullPath = cleanConfigPath(stdpath.Join(selected.MountPath, target.Folder))
	}
	return ResolvedProviderTarget{
		Provider:             target.Provider,
		StorageID:            selected.StorageID,
		MountPath:            selected.MountPath,
		Folder:               target.Folder,
		FullPath:             fullPath,
		AccountAlias:         selected.AccountAlias,
		MembershipTier:       selected.MembershipTier,
		MembershipWeight:     selected.MembershipWeight,
		MaxSingleUploadBytes: selected.MaxSingleUploadBytes,
		FreeBytes:            selected.FreeBytes,
		ActiveJobs:           selected.ActiveJobs,
	}, nil
}

func EnsureResolvedProviderFolder(ctx context.Context, target ResolvedProviderTarget) (ResolvedProviderTarget, error) {
	if strings.TrimSpace(target.FullPath) == "" {
		return ResolvedProviderTarget{}, fmt.Errorf("resolved provider target path is required")
	}
	if err := ensureProviderTargetFolder(ctx, target.FullPath); err != nil {
		return ResolvedProviderTarget{}, fmt.Errorf("ensure provider target folder %s: %w", target.FullPath, err)
	}
	return target, nil
}

func providerAccountCandidateFromStorage(ctx context.Context, storage model.Storage) ProviderAccountCandidate {
	provider := storageProviderName(storage.Driver)
	freeBytes, hasFree := storageFreeBytesForMountPath(ctx, storage.MountPath)
	tier, weight, maxUpload := providerAccountMetadata(storage.Addition)
	if normalizeMembershipTier(tier) == "unknown" {
		if liveTier := normalizeMembershipTier(liveMembershipTierForMountPath(storage.MountPath)); liveTier != "" && liveTier != "unknown" {
			tier = liveTier
		}
	}
	if weight == 0 {
		weight = membershipWeight(tier)
	}
	if provider == "yidong139" && maxUpload == 0 {
		maxUpload = mobile139MaxSingleUploadBytes(tier)
	}
	supportsUpload := !storage.Disabled
	if driver, err := op.GetStorageByMountPath(storage.MountPath); err == nil {
		supportsUpload = supportsUpload && !driver.Config().NoUpload
	}
	return ProviderAccountCandidate{
		Provider:             provider,
		StorageID:            storage.ID,
		MountPath:            storage.MountPath,
		AccountAlias:         strings.TrimSpace(storage.Remark),
		Status:               strings.TrimSpace(storage.Status),
		Disabled:             storage.Disabled,
		MembershipTier:       normalizeMembershipTier(tier),
		MembershipWeight:     weight,
		MaxSingleUploadBytes: maxUpload,
		SupportsUpload:       supportsUpload,
		SupportsDownload:     providerSupportsDownload(provider, storage.Driver),
		SupportsShareSave:    providerSupportsShareSave(provider, storage.Driver),
		SupportsETF:          provider == "yidong139",
		FreeBytes:            freeBytes,
		HasFreeBytes:         hasFree,
		ActiveJobs:           storageActiveJobsForMountPath(storage.MountPath),
	}
}

func defaultLiveMembershipTierForMountPath(mountPath string) string {
	driver, err := op.GetStorageByMountPath(mountPath)
	if err != nil || driver == nil {
		return ""
	}
	reporter, ok := driver.(membershipTierReporter)
	if !ok {
		return ""
	}
	return reporter.ClusterMembershipTier()
}

func providerAccountMetadata(addition string) (tier string, weight int, maxUpload int64) {
	var values map[string]any
	if strings.TrimSpace(addition) == "" || json.Unmarshal([]byte(addition), &values) != nil {
		return "", 0, 0
	}
	tier = firstMetadataString(values, "membership_tier", "membershipTier", "member_tier", "vip_level", "vipLevel")
	weight = int(firstMetadataNumber(values, "membership_weight", "membershipWeight"))
	maxUpload = int64(firstMetadataNumber(values, "max_single_upload_bytes", "maxSingleUploadBytes"))
	return tier, weight, maxUpload
}

func firstMetadataString(values map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := values[key].(string); ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func firstMetadataNumber(values map[string]any, keys ...string) float64 {
	for _, key := range keys {
		if value, ok := values[key].(float64); ok {
			return value
		}
	}
	return 0
}

func normalizeMembershipTier(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "diamond", "钻石":
		return "diamond"
	case "gold", "黄金":
		return "gold"
	case "silver", "白银":
		return "silver"
	case "ordinary", "normal", "普通":
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

func membershipWeight(tier string) int {
	switch normalizeMembershipTier(tier) {
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

func mobile139MaxSingleUploadBytes(tier string) int64 {
	switch normalizeMembershipTier(tier) {
	case "diamond":
		return 500 << 30
	case "gold":
		return 20 << 30
	case "silver":
		return 8 << 30
	case "ordinary":
		return 5 << 30
	default:
		return 0
	}
}

func providerSupportsShareSave(provider, driverName string) bool {
	switch provider {
	case "quark", "aliyun_drive", "pan123":
		return true
	case "pan115":
		return strings.EqualFold(strings.TrimSpace(driverName), "115 cloud")
	default:
		return false
	}
}

func providerSupportsDownload(provider, driverName string) bool {
	driverName = strings.ToLower(strings.TrimSpace(driverName))
	switch provider {
	case "pan123":
		return driverName == "123pan" || driverName == "123 open"
	case "pan115":
		return driverName == "115 cloud" || driverName == "115 open" || driverName == "115 cd2"
	case "aliyun_drive":
		return driverName == "aliyundrive" || driverName == "aliyundriveopen"
	case "quark":
		return driverName == "quark" || driverName == "quarkopen" || driverName == "quarktv"
	case "yidong139":
		return driverName == "139yun" || driverName == "139 cloud" || driverName == "139"
	default:
		return false
	}
}

func defaultStorageFreeBytesForMountPath(ctx context.Context, mountPath string) (int64, bool) {
	driver, err := op.GetStorageByMountPath(mountPath)
	if err != nil {
		return 0, false
	}
	details, err := op.GetStorageDetails(ctx, driver)
	if err != nil || details == nil {
		return 0, false
	}
	return details.FreeSpace(), true
}

func defaultStorageActiveJobsForMountPath(mountPath string) int {
	if db.GetDb() == nil {
		return 0
	}
	var targetDirs []string
	if err := db.GetDb().Model(&model.SubscriptionItem{}).
		Where("status = ?", model.SubscriptionItemStatusTransferring).
		Pluck("target_dir", &targetDirs).Error; err != nil {
		return 0
	}
	mountPath = cleanConfigPath(mountPath)
	count := 0
	for _, targetDir := range targetDirs {
		targetDir = cleanConfigPath(targetDir)
		if targetDir == mountPath || strings.HasPrefix(targetDir, strings.TrimSuffix(mountPath, "/")+"/") {
			count++
		}
	}
	return count
}

func storageProviderName(driverName string) string {
	switch strings.ToLower(strings.TrimSpace(driverName)) {
	case "quark":
		return "quark"
	case "aliyundriveopen", "aliyundrive":
		return "aliyun_drive"
	case "123pan", "123 open":
		return "pan123"
	case "115 cloud", "115 open", "115 cd2":
		return "pan115"
	case "guangyapan":
		return "guangyapan"
	case "139yun", "139 cloud", "139":
		return "yidong139"
	default:
		return strings.ToLower(strings.ReplaceAll(strings.TrimSpace(driverName), " ", "_"))
	}
}

func providerTargetNameForShareProvider(provider ShareProviderName) string {
	switch provider {
	case ShareProviderQuark:
		return "quark"
	case ShareProviderAliyunDrive:
		return "aliyun_drive"
	case ShareProviderPan123:
		return "pan123"
	case ShareProviderPan115:
		return "pan115"
	case ShareProviderGuangYaPan:
		return "guangyapan"
	default:
		return ""
	}
}
