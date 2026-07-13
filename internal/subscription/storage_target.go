package subscription

import (
	"fmt"
	stdpath "path"
	"path/filepath"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func NormalizeSubscriptionStorageTarget(target model.SubscriptionStorageTarget) model.SubscriptionStorageTarget {
	target.Provider = strings.ToLower(strings.TrimSpace(target.Provider))
	rawFolder := strings.TrimSpace(target.Folder)
	if migrated, ok := MigrateLegacyPathTarget(rawFolder); ok {
		if target.Provider == "" {
			target.Provider = migrated.Provider
		}
		rawFolder = migrated.Folder
	}
	rawFolder = strings.ReplaceAll(rawFolder, `\`, "/")
	if rawFolder == "" {
		target.Folder = ""
		return target
	}
	cleaned := stdpath.Clean(rawFolder)
	if cleaned == "." {
		cleaned = ""
	}
	target.Folder = strings.Trim(cleaned, "/")
	return target
}

func ValidateSubscriptionStorageTarget(target model.SubscriptionStorageTarget) error {
	target.Provider = strings.TrimSpace(target.Provider)
	rawFolder := strings.TrimSpace(target.Folder)
	if target.Provider == "" && rawFolder == "" {
		return nil
	}
	if target.Provider == "" {
		return fmt.Errorf("provider target provider is required")
	}
	if strings.ContainsRune(rawFolder, '\x00') {
		return fmt.Errorf("provider target folder contains an invalid character")
	}
	if rawFolder == "" {
		return nil
	}
	if _, migrated := MigrateLegacyPathTarget(rawFolder); migrated {
		return nil
	}
	normalizedSeparators := strings.ReplaceAll(rawFolder, `\`, "/")
	if stdpath.IsAbs(normalizedSeparators) || filepath.IsAbs(rawFolder) || strings.HasPrefix(rawFolder, `\`) {
		return fmt.Errorf("provider target folder must be relative")
	}
	for _, part := range strings.Split(normalizedSeparators, "/") {
		if part == ".." {
			return fmt.Errorf("provider target folder must not contain parent traversal")
		}
	}
	return nil
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
