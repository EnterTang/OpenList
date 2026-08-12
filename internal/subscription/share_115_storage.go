package subscription

import (
	"encoding/json"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
)

func pan115ConfigWithStorageFallback(cfg model.SubscriptionTelegramPanConfig) model.SubscriptionTelegramPanConfig {
	cfg = normalizeTelegramPanConfig(cfg)
	if cookie := pan115CookieFromStorage(); cookie != "" {
		// The mounted 115 driver may have a refreshed cookie, so prefer it over
		// a manually copied subscription cookie when resolving worker requests.
		cfg.Cookie = cookie
	}
	return normalizeTelegramPanConfig(cfg)
}

func pan115CookieFromStorage() string {
	if cookie := pan115CookieFromLiveStorage(); cookie != "" {
		return cookie
	}
	if db.GetDb() == nil {
		return ""
	}
	storages, err := db.GetEnabledStorages()
	if err != nil {
		return ""
	}
	for _, storage := range storages {
		if !isPan115CookieStorageDriver(storage.Driver) {
			continue
		}
		if cookie := pan115CookieFromAddition(storage.Addition); cookie != "" {
			return cookie
		}
	}
	return ""
}

func pan115CookieFromLiveStorage() string {
	if db.GetDb() == nil {
		return ""
	}
	storages, err := db.GetEnabledStorages()
	if err != nil {
		return ""
	}
	for _, storage := range storages {
		if !isPan115CookieStorageDriver(storage.Driver) {
			continue
		}
		driverStorage, err := op.GetStorageByMountPath(storage.MountPath)
		if err != nil || driverStorage == nil {
			continue
		}
		if cookie := pan115CookieFromDriverAddition(driverStorage.GetAddition()); cookie != "" {
			return cookie
		}
	}
	return ""
}

// isPan115CookieStorageDriver reports whether a mounted storage can supply the
// Cookie used by pan115 share.inspect / share.save snapshot clients.
// Workers commonly mount "115 SY" rather than legacy "115 Cloud".
func isPan115CookieStorageDriver(driver string) bool {
	switch strings.TrimSpace(driver) {
	case "115 Cloud", "115 SY":
		return true
	default:
		return false
	}
}

func pan115CookieFromDriverAddition(addition any) string {
	if addition == nil {
		return ""
	}
	body, err := json.Marshal(addition)
	if err != nil {
		return ""
	}
	return pan115CookieFromAddition(string(body))
}

func pan115CookieFromAddition(raw string) string {
	var addition struct {
		Cookie      string `json:"Cookie"`
		CookieSnake string `json:"cookie"`
	}
	if err := json.Unmarshal([]byte(raw), &addition); err != nil {
		return ""
	}
	return firstNonEmpty(strings.TrimSpace(addition.Cookie), strings.TrimSpace(addition.CookieSnake))
}
