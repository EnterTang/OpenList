package subscription

import (
	"encoding/json"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func aliyunDriveConfigWithStorageFallback(cfg model.SubscriptionTelegramPanConfig) model.SubscriptionTelegramPanConfig {
	cfg = normalizeTelegramPanConfig(cfg)
	if cfg.RefreshToken != "" {
		return cfg
	}
	fallback := aliyunDriveOpenConfigFromStorage()
	cfg.RefreshToken = fallback.RefreshToken
	if cfg.DriveType == "" {
		cfg.DriveType = fallback.DriveType
	}
	return normalizeTelegramPanConfig(cfg)
}

func aliyunDriveOpenConfigFromStorage() model.SubscriptionTelegramPanConfig {
	if db.GetDb() == nil {
		return model.SubscriptionTelegramPanConfig{}
	}
	storages, err := db.GetEnabledStorages()
	if err != nil {
		return model.SubscriptionTelegramPanConfig{}
	}
	for _, storage := range storages {
		if storage.Driver != "AliyundriveOpen" {
			continue
		}
		var addition struct {
			WebRefreshToken string `json:"web_refresh_token"`
			DriveType       string `json:"drive_type"`
		}
		if err := json.Unmarshal([]byte(storage.Addition), &addition); err != nil {
			continue
		}
		if token := strings.TrimSpace(addition.WebRefreshToken); token != "" {
			return normalizeTelegramPanConfig(model.SubscriptionTelegramPanConfig{
				RefreshToken: token,
				DriveType:    addition.DriveType,
			})
		}
	}
	return model.SubscriptionTelegramPanConfig{}
}
