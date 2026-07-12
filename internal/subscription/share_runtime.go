package subscription

import (
	"context"
	"fmt"
	stdpath "path"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

var (
	newShareSaverForProvider = defaultNewShareSaverForProvider
	saveShareToTemp          = SaveShareToTemp
	saveImportedFilesToTemp  = SaveImportedFilesToTemp
)

func trySaveShareLinkToTemp(ctx context.Context, sub *model.Subscription, cfg model.SubscriptionTelegramSourceConfig, rawLink string) (telegramPanSubscriptionSource, bool, error) {
	ref, err := ParseShareURL(rawLink)
	if err != nil {
		if provider, ok := DetectShareProvider(rawLink); ok {
			source, _ := telegramPanSourceForProvider(cfg, provider)
			return source, false, err
		}
		return telegramPanSubscriptionSource{}, false, nil
	}
	source, ok := telegramPanSourceForProvider(cfg, ref.Provider)
	if !ok {
		return telegramPanSubscriptionSource{}, false, nil
	}
	source.Config = telegramPanSourceConfigWithStorageFallback(ref.Provider, source.Config)
	if !telegramPanSourceCanSave(ref.Provider, source.Config) {
		return source, false, nil
	}
	provider, err := newShareSaverForProvider(ref.Provider, source.Config)
	if err != nil {
		return source, false, err
	}
	selected, err := saveShareToTemp(ctx, provider, ref, SaveShareOptions{
		TempRoot:     source.Config.TempTransferRoot,
		Subscription: sub,
		Match: func(entry TreeEntry) bool {
			return boundShareEntryMatches(sub, entry)
		},
	})
	if err != nil {
		return source, false, err
	}
	source.BoundShareNames, source.BoundSharePaths = boundShareMarkers(selected)
	return source, true, nil
}

func boundShareMarkers(entries []TreeEntry) (map[string]struct{}, map[string]struct{}) {
	names := map[string]struct{}{}
	paths := map[string]struct{}{}
	for _, entry := range entries {
		name := strings.TrimSpace(entry.Name)
		if name != "" {
			names[name] = struct{}{}
		}
		path := cleanBoundSharePath(entry.Path)
		if path != "" {
			paths[path] = struct{}{}
		}
	}
	return names, paths
}

func mergeStringSet(dst map[string]struct{}, src map[string]struct{}) map[string]struct{} {
	if len(src) == 0 {
		return dst
	}
	if dst == nil {
		dst = map[string]struct{}{}
	}
	for value := range src {
		dst[value] = struct{}{}
	}
	return dst
}

func mergeBoundShareSource(existing, incoming telegramPanSubscriptionSource) telegramPanSubscriptionSource {
	if existing.Name == "" {
		return incoming
	}
	if incoming.Name == "" {
		return existing
	}
	incoming.BoundShareNames = mergeStringSet(existing.BoundShareNames, incoming.BoundShareNames)
	incoming.BoundSharePaths = mergeStringSet(existing.BoundSharePaths, incoming.BoundSharePaths)
	return incoming
}

func mergeBoundShareMarkers(names, paths map[string]struct{}, entries []TreeEntry) (map[string]struct{}, map[string]struct{}) {
	entryNames, entryPaths := boundShareMarkers(entries)
	return mergeStringSet(names, entryNames), mergeStringSet(paths, entryPaths)
}

func entryMatchesSubscriptionOrBoundShare(sub *model.Subscription, entry TreeEntry, names, paths map[string]struct{}) bool {
	if boundShareMarkerMatches(entry, names, paths) && boundShareEntryMatches(sub, entry) {
		return true
	}
	return subscriptionEntryMatches(sub, entry)
}

func boundShareMarkerMatches(entry TreeEntry, names, paths map[string]struct{}) bool {
	if len(names) == 0 && len(paths) == 0 {
		return false
	}
	if _, ok := paths[cleanBoundSharePath(entry.Path)]; ok {
		return true
	}
	if _, ok := names[strings.TrimSpace(entry.Name)]; ok {
		return true
	}
	return false
}

func cleanBoundSharePath(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	return cleanConfigPath(value)
}

func telegramPanSourceConfigWithStorageFallback(provider ShareProviderName, cfg model.SubscriptionTelegramPanConfig) model.SubscriptionTelegramPanConfig {
	cfg = normalizeTelegramPanConfig(cfg)
	switch provider {
	case ShareProviderAliyunDrive:
		cfg = aliyunDriveConfigWithStorageFallback(cfg)
	case ShareProviderPan123:
		cfg = pan123ConfigWithStorageFallback(cfg)
	}
	cfg = telegramPanTempRootWithStorageFallback(provider, cfg)
	if provider == ShareProviderAliyunDrive {
		cfg = aliyunDriveConfigWithTempRootFallback(cfg)
	}
	return cfg
}

func telegramPanTempRootWithStorageFallback(provider ShareProviderName, cfg model.SubscriptionTelegramPanConfig) model.SubscriptionTelegramPanConfig {
	cfg = normalizeTelegramPanConfig(cfg)
	if cfg.TempTransferRoot == "" || db.GetDb() == nil {
		return cfg
	}
	storages, err := db.GetEnabledStorages()
	if err != nil || tempRootHasEnabledStorage(cfg.TempTransferRoot, storages) {
		return cfg
	}
	mountPath, ok := singleEnabledStorageMountPathForProvider(provider, storages)
	if !ok {
		return cfg
	}
	cfg.TempTransferRoot = cleanConfigPath(stdpath.Join(mountPath, strings.TrimPrefix(cfg.TempTransferRoot, "/")))
	return normalizeTelegramPanConfig(cfg)
}

func tempRootHasEnabledStorage(root string, storages []model.Storage) bool {
	root = cleanConfigPath(root)
	for _, storage := range storages {
		mountPath := cleanConfigPath(storage.MountPath)
		if mountPath == "" {
			continue
		}
		if mountPath == "/" || root == mountPath || strings.HasPrefix(root, strings.TrimSuffix(mountPath, "/")+"/") {
			return true
		}
	}
	return false
}

func singleEnabledStorageMountPathForProvider(provider ShareProviderName, storages []model.Storage) (string, bool) {
	driverName, ok := defaultStorageDriverForShareProvider(provider)
	if !ok {
		return "", false
	}
	var mountPath string
	for _, storage := range storages {
		if storage.Driver != driverName {
			continue
		}
		if mountPath != "" {
			return "", false
		}
		mountPath = cleanConfigPath(storage.MountPath)
	}
	return mountPath, mountPath != ""
}

func defaultStorageDriverForShareProvider(provider ShareProviderName) (string, bool) {
	switch provider {
	case ShareProviderQuark:
		return "Quark", true
	case ShareProviderAliyunDrive:
		return "AliyundriveOpen", true
	case ShareProviderPan123:
		return "123Pan", true
	case ShareProviderPan115:
		return "115 Cloud", true
	default:
		return "", false
	}
}

func telegramPanSourceForProvider(cfg model.SubscriptionTelegramSourceConfig, provider ShareProviderName) (telegramPanSubscriptionSource, bool) {
	var source telegramPanSubscriptionSource
	switch provider {
	case ShareProviderQuark:
		source = telegramPanSubscriptionSource{Name: string(ShareProviderQuark), Config: cfg.Quark}
	case ShareProviderAliyunDrive:
		source = telegramPanSubscriptionSource{Name: string(ShareProviderAliyunDrive), Config: cfg.AliyunDrive}
	case ShareProviderPan123:
		source = telegramPanSubscriptionSource{Name: string(ShareProviderPan123), Config: cfg.Pan123}
	case ShareProviderPan115:
		source = telegramPanSubscriptionSource{Name: string(ShareProviderPan115), Config: cfg.Pan115}
	default:
		return telegramPanSubscriptionSource{}, false
	}
	source.Config = normalizeTelegramPanConfig(source.Config)
	if isZeroTelegramPanConfig(source.Config) {
		return telegramPanSubscriptionSource{}, false
	}
	return source, true
}

func telegramPanSourceCanSave(provider ShareProviderName, cfg model.SubscriptionTelegramPanConfig) bool {
	cfg = normalizeTelegramPanConfig(cfg)
	if cfg.TempTransferRoot == "" {
		return false
	}
	switch provider {
	case ShareProviderQuark, ShareProviderPan115:
		return strings.TrimSpace(cfg.Cookie) != ""
	case ShareProviderAliyunDrive:
		return strings.TrimSpace(cfg.RefreshToken) != "" ||
			(strings.TrimSpace(cfg.AccessToken) != "" && strings.TrimSpace(cfg.DriveID) != "")
	case ShareProviderPan123:
		return strings.TrimSpace(cfg.AccessToken) != ""
	default:
		return false
	}
}

func defaultNewShareSaverForProvider(provider ShareProviderName, cfg model.SubscriptionTelegramPanConfig) (ShareSaver, error) {
	switch provider {
	case ShareProviderQuark:
		return NewQuarkShareProvider(cfg), nil
	case ShareProviderAliyunDrive:
		return NewAliyunDriveShareProvider(cfg), nil
	case ShareProviderPan123:
		return NewPan123ShareProvider(cfg), nil
	case ShareProviderPan115:
		return NewPan115ShareProvider(cfg), nil
	default:
		return nil, fmt.Errorf("unsupported share provider: %s", provider)
	}
}
