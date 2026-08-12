package subscription

import (
	"encoding/json"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
)

func pan123ConfigWithStorageFallback(cfg model.SubscriptionTelegramPanConfig) model.SubscriptionTelegramPanConfig {
	cfg = normalizeTelegramPanConfig(cfg)
	fallback := pan123ConfigFromStorage()
	if token := strings.TrimSpace(fallback.AccessToken); token != "" {
		// Prefer the live 123Pan storage token. It is refreshed on 401 during
		// normal storage usage, while a manually configured subscription token
		// would go stale.
		cfg.AccessToken = token
	}
	return normalizeTelegramPanConfig(cfg)
}

func pan123ConfigFromStorage() model.SubscriptionTelegramPanConfig {
	if token := pan123AccessTokenFromLiveStorage(); token != "" {
		return normalizeTelegramPanConfig(model.SubscriptionTelegramPanConfig{
			AccessToken: token,
		})
	}
	if db.GetDb() == nil {
		return model.SubscriptionTelegramPanConfig{}
	}
	storages, err := db.GetEnabledStorages()
	if err != nil {
		return model.SubscriptionTelegramPanConfig{}
	}
	for _, storage := range storages {
		if storage.Driver != "123Pan" {
			continue
		}
		if token := pan123AccessTokenFromAddition(storage.Addition); token != "" {
			return normalizeTelegramPanConfig(model.SubscriptionTelegramPanConfig{
				AccessToken: token,
			})
		}
	}
	return model.SubscriptionTelegramPanConfig{}
}

func pan123AccessTokenFromLiveStorage() string {
	if db.GetDb() == nil {
		return ""
	}
	storages, err := db.GetEnabledStorages()
	if err != nil {
		return ""
	}
	for _, storage := range storages {
		if storage.Driver != "123Pan" {
			continue
		}
		driverStorage, err := op.GetStorageByMountPath(storage.MountPath)
		if err != nil || driverStorage == nil {
			continue
		}
		if token := pan123AccessTokenFromDriverAddition(driverStorage.GetAddition()); token != "" {
			return token
		}
	}
	return ""
}

func pan123AccessTokenFromDriverAddition(addition any) string {
	if addition == nil {
		return ""
	}
	body, err := json.Marshal(addition)
	if err != nil {
		return ""
	}
	return pan123AccessTokenFromAddition(string(body))
}

func pan123AccessTokenFromAddition(raw string) string {
	var addition struct {
		AccessToken      string `json:"AccessToken"`
		AccessTokenSnake string `json:"access_token"`
	}
	if err := json.Unmarshal([]byte(raw), &addition); err != nil {
		return ""
	}
	return firstNonEmpty(strings.TrimSpace(addition.AccessToken), strings.TrimSpace(addition.AccessTokenSnake))
}
