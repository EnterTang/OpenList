package subscription

import (
	"encoding/json"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
)

func guangyapanConfigWithStorageFallback(cfg model.SubscriptionTelegramPanConfig) model.SubscriptionTelegramPanConfig {
	cfg = normalizeTelegramPanConfig(cfg)
	fallback := guangyapanConfigFromStorage()
	if token := strings.TrimSpace(fallback.AccessToken); token != "" {
		// Prefer the live GuangYaPan storage token. It is refreshed during
		// normal storage usage, while a manually configured subscription token
		// would go stale.
		cfg.AccessToken = token
	}
	if token := strings.TrimSpace(fallback.RefreshToken); token != "" {
		cfg.RefreshToken = token
	}
	return normalizeTelegramPanConfig(cfg)
}

func guangyapanConfigFromStorage() model.SubscriptionTelegramPanConfig {
	if cfg := guangyapanConfigFromLiveStorage(); !isZeroGuangYaPanCredentials(cfg) {
		return cfg
	}
	if db.GetDb() == nil {
		return model.SubscriptionTelegramPanConfig{}
	}
	storages, err := db.GetEnabledStorages()
	if err != nil {
		return model.SubscriptionTelegramPanConfig{}
	}
	for _, storage := range storages {
		if storage.Driver != "GuangYaPan" {
			continue
		}
		if cfg := guangyapanConfigFromAddition(storage.Addition); !isZeroGuangYaPanCredentials(cfg) {
			return cfg
		}
	}
	return model.SubscriptionTelegramPanConfig{}
}

func guangyapanConfigFromLiveStorage() model.SubscriptionTelegramPanConfig {
	if db.GetDb() == nil {
		return model.SubscriptionTelegramPanConfig{}
	}
	storages, err := db.GetEnabledStorages()
	if err != nil {
		return model.SubscriptionTelegramPanConfig{}
	}
	for _, storage := range storages {
		if storage.Driver != "GuangYaPan" {
			continue
		}
		driverStorage, err := op.GetStorageByMountPath(storage.MountPath)
		if err != nil || driverStorage == nil {
			continue
		}
		body, marshalErr := json.Marshal(driverStorage.GetAddition())
		if marshalErr != nil {
			continue
		}
		if cfg := guangyapanConfigFromAddition(string(body)); !isZeroGuangYaPanCredentials(cfg) {
			return cfg
		}
	}
	return model.SubscriptionTelegramPanConfig{}
}

func guangyapanConfigFromAddition(raw string) model.SubscriptionTelegramPanConfig {
	var addition struct {
		AccessToken       string `json:"access_token"`
		AccessTokenCamel  string `json:"AccessToken"`
		RefreshToken      string `json:"refresh_token"`
		RefreshTokenCamel string `json:"RefreshToken"`
	}
	if err := json.Unmarshal([]byte(raw), &addition); err != nil {
		return model.SubscriptionTelegramPanConfig{}
	}
	return normalizeTelegramPanConfig(model.SubscriptionTelegramPanConfig{
		AccessToken:  firstNonEmpty(strings.TrimSpace(addition.AccessToken), strings.TrimSpace(addition.AccessTokenCamel)),
		RefreshToken: firstNonEmpty(strings.TrimSpace(addition.RefreshToken), strings.TrimSpace(addition.RefreshTokenCamel)),
	})
}

func isZeroGuangYaPanCredentials(cfg model.SubscriptionTelegramPanConfig) bool {
	return strings.TrimSpace(cfg.AccessToken) == "" && strings.TrimSpace(cfg.RefreshToken) == ""
}

// GuangYaPanStorageCredentialsConfigured reports whether an enabled GuangYaPan
// storage already has usable access/refresh tokens for subscription transfer.
func GuangYaPanStorageCredentialsConfigured() (accessConfigured bool, refreshConfigured bool) {
	cfg := guangyapanConfigFromStorage()
	return strings.TrimSpace(cfg.AccessToken) != "", strings.TrimSpace(cfg.RefreshToken) != ""
}
