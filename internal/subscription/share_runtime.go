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
	source, ref, ok, err := resolveShareLinkSource(sub, cfg, rawLink)
	if err != nil || !ok {
		return source, false, err
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
	if existing.runtimeConfigResolved && !incoming.runtimeConfigResolved {
		incoming.Config = existing.Config
		incoming.runtimeConfigResolved = true
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

func telegramPanSourceConfigWithStorageFallback(provider ShareProviderName, cfg model.SubscriptionTelegramPanConfig) (model.SubscriptionTelegramPanConfig, error) {
	cfg = normalizeTelegramPanConfig(cfg)
	if cfg.TempTransferTarget.Provider == "" && strings.TrimSpace(cfg.TempTransferRoot) != "" {
		return cfg, fmt.Errorf("legacy %s temp_transfer_root %q is not a recognized provider mount and requires manual confirmation", provider, cfg.TempTransferRoot)
	}
	var resolved ResolvedProviderTarget
	var err error
	cfg, resolved, err = telegramPanTempTargetWithResolver(provider, cfg)
	if err != nil {
		return cfg, err
	}
	if resolved.StorageID != 0 || resolved.MountPath != "" {
		cfg, err = bindTelegramPanConfigToStorage(provider, cfg, resolved.StorageID, resolved.MountPath)
		if err != nil {
			return cfg, err
		}
	} else {
		switch provider {
		case ShareProviderAliyunDrive:
			cfg = aliyunDriveConfigWithStorageFallback(cfg)
		case ShareProviderPan123:
			cfg = pan123ConfigWithStorageFallback(cfg)
		case ShareProviderGuangYaPan:
			cfg = guangyapanConfigWithStorageFallback(cfg)
		}
	}
	cfg = telegramPanTempRootWithStorageFallback(provider, cfg)
	if provider == ShareProviderAliyunDrive {
		cfg = aliyunDriveConfigWithTempRootFallback(cfg)
	}
	return cfg, nil
}

func telegramPanTempTargetWithResolver(provider ShareProviderName, cfg model.SubscriptionTelegramPanConfig) (model.SubscriptionTelegramPanConfig, ResolvedProviderTarget, error) {
	cfg = normalizeTelegramPanConfig(cfg)
	target := cfg.TempTransferTarget
	if target.Provider == "" && target.Folder != "" {
		target.Provider = providerTargetNameForShareProvider(provider)
	}
	if target.Provider == "" || target.Folder == "" {
		return cfg, ResolvedProviderTarget{}, nil
	}
	expectedProvider := providerTargetNameForShareProvider(provider)
	if expectedProvider != "" && target.Provider != expectedProvider {
		return cfg, ResolvedProviderTarget{}, fmt.Errorf("%s temp transfer target provider %q does not match share provider %q", provider, target.Provider, expectedProvider)
	}
	resolved, err := ResolveProviderTarget(context.Background(), ResolveProviderTargetRequest{
		Provider:      target.Provider,
		Folder:        target.Folder,
		NeedShareSave: true,
	})
	if err != nil {
		return cfg, ResolvedProviderTarget{}, fmt.Errorf("resolve %s temp transfer target: %w", provider, err)
	}
	cfg.TempTransferRoot = resolved.FullPath
	return normalizeTelegramPanConfig(cfg), resolved, nil
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
	case ShareProviderGuangYaPan:
		return "GuangYaPan", true
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
	case ShareProviderGuangYaPan:
		source = telegramPanSubscriptionSource{Name: string(ShareProviderGuangYaPan), Config: cfg.GuangYaPan}
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
	case ShareProviderGuangYaPan:
		return strings.TrimSpace(cfg.AccessToken) != "" || strings.TrimSpace(cfg.RefreshToken) != ""
	default:
		return false
	}
}

// ResolveShareInspectConfig supplements provider credentials from mounted
// storages when the subscription config lacks tokens. This lets share.inspect
// work on workers that have the provider mounted but didn't receive tokens
// via subscription config.
func ResolveShareInspectConfig(provider ShareProviderName, cfg model.SubscriptionTelegramPanConfig) model.SubscriptionTelegramPanConfig {
	cfg = normalizeTelegramPanConfig(cfg)
	switch provider {
	case ShareProviderGuangYaPan:
		if strings.TrimSpace(cfg.AccessToken) == "" || strings.TrimSpace(cfg.RefreshToken) == "" {
			cfg = guangyapanConfigWithStorageFallback(cfg)
		}
	case ShareProviderPan123:
		if strings.TrimSpace(cfg.AccessToken) == "" {
			cfg = pan123ConfigWithStorageFallback(cfg)
		}
	case ShareProviderPan115:
		cfg = pan115ConfigWithStorageFallback(cfg)
	case ShareProviderAliyunDrive:
		if strings.TrimSpace(cfg.RefreshToken) == "" {
			cfg = aliyunDriveConfigWithStorageFallback(cfg)
		}
	}
	return cfg
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
	case ShareProviderGuangYaPan:
		return NewGuangYaPanShareProvider(cfg), nil
	default:
		return nil, fmt.Errorf("unsupported share provider: %s", provider)
	}
}
