package subscription

import (
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func NormalizeSubscriptionStorageTarget(target model.SubscriptionStorageTarget) model.SubscriptionStorageTarget {
	target.Provider = strings.ToLower(strings.TrimSpace(target.Provider))
	target.Folder = strings.Trim(strings.TrimSpace(target.Folder), "/")
	if migrated, ok := MigrateLegacyPathTarget(target.Folder); ok {
		if target.Provider == "" {
			target.Provider = migrated.Provider
		}
		target.Folder = migrated.Folder
	}
	return target
}

func MigrateLegacyPathTarget(raw string) (model.SubscriptionStorageTarget, bool) {
	cleaned := cleanConfigPath(raw)
	switch {
	case strings.HasPrefix(cleaned, "/123/"):
		return model.SubscriptionStorageTarget{
			Provider: "pan123",
			Folder:   strings.Trim(strings.TrimPrefix(cleaned, "/123/"), "/"),
		}, true
	case strings.HasPrefix(cleaned, "/115/"):
		return model.SubscriptionStorageTarget{
			Provider: "pan115",
			Folder:   strings.Trim(strings.TrimPrefix(cleaned, "/115/"), "/"),
		}, true
	case strings.HasPrefix(cleaned, "/139_60t/"):
		return model.SubscriptionStorageTarget{
			Provider: "yidong139",
			Folder:   strings.Trim(strings.TrimPrefix(cleaned, "/139_60t/"), "/"),
		}, true
	case strings.HasPrefix(cleaned, "/139/"):
		return model.SubscriptionStorageTarget{
			Provider: "yidong139",
			Folder:   strings.Trim(strings.TrimPrefix(cleaned, "/139/"), "/"),
		}, true
	default:
		return model.SubscriptionStorageTarget{}, false
	}
}
