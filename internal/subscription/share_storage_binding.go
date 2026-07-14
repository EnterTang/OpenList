package subscription

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
)

func bindTelegramPanConfigToStorage(provider ShareProviderName, cfg model.SubscriptionTelegramPanConfig, storageID uint, mountPath string) (model.SubscriptionTelegramPanConfig, error) {
	storage, ok := storageForProviderBinding(storageID, mountPath)
	if !ok {
		return cfg, nil
	}
	wantProvider := providerTargetNameForShareProvider(provider)
	gotProvider := storageProviderName(storage.Driver)
	if wantProvider != "" && gotProvider != wantProvider {
		return cfg, fmt.Errorf("%s share provider does not match bound storage provider %q", provider, gotProvider)
	}

	addition := storage.Addition
	if live, err := op.GetStorageByMountPath(storage.MountPath); err == nil && live != nil {
		if body, marshalErr := json.Marshal(live.GetAddition()); marshalErr == nil {
			addition = string(body)
		}
	}
	values := make(map[string]any)
	_ = json.Unmarshal([]byte(addition), &values)
	cfg = normalizeTelegramPanConfig(cfg)
	switch provider {
	case ShareProviderPan123:
		cfg.AccessToken = firstCredential(values, "AccessToken", "access_token")
	case ShareProviderPan115:
		cfg.Cookie = firstCredential(values, "Cookie", "cookie")
	case ShareProviderQuark:
		cfg.Cookie = firstCredential(values, "Cookie", "cookie")
	case ShareProviderAliyunDrive:
		cfg.RefreshToken = firstCredential(values, "WebRefreshToken", "web_refresh_token")
		if driveType := firstCredential(values, "DriveType", "drive_type"); driveType != "" {
			cfg.DriveType = driveType
		}
	}
	return normalizeTelegramPanConfig(cfg), nil
}

func storageForProviderBinding(storageID uint, targetPath string) (model.Storage, bool) {
	if db.GetDb() == nil {
		return model.Storage{}, false
	}
	if storageID != 0 {
		storage, err := db.GetStorageById(storageID)
		if err == nil && storage != nil {
			return *storage, true
		}
	}
	storages, err := db.GetEnabledStorages()
	if err != nil {
		return model.Storage{}, false
	}
	targetPath = cleanConfigPath(targetPath)
	var selected model.Storage
	selectedLength := -1
	for _, storage := range storages {
		mountPath := cleanConfigPath(storage.MountPath)
		if mountPath == "" || (targetPath != mountPath && !strings.HasPrefix(targetPath, strings.TrimSuffix(mountPath, "/")+"/")) {
			continue
		}
		if len(mountPath) > selectedLength {
			selected = storage
			selectedLength = len(mountPath)
		}
	}
	return selected, selectedLength >= 0
}

func firstCredential(values map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := values[key].(string); ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
