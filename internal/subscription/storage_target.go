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
	if migratedTarget, migrated := MigrateLegacyPathTarget(rawFolder); migrated {
		if !strings.EqualFold(target.Provider, migratedTarget.Provider) {
			return fmt.Errorf("provider target provider %q does not match legacy folder provider %q", target.Provider, migratedTarget.Provider)
		}
		return nil
	}
	normalizedSeparators := strings.ReplaceAll(rawFolder, `\`, "/")
	hasWindowsDrivePrefix := len(normalizedSeparators) >= 3 &&
		((normalizedSeparators[0] >= 'a' && normalizedSeparators[0] <= 'z') ||
			(normalizedSeparators[0] >= 'A' && normalizedSeparators[0] <= 'Z')) &&
		normalizedSeparators[1] == ':' && normalizedSeparators[2] == '/'
	if stdpath.IsAbs(normalizedSeparators) || filepath.IsAbs(rawFolder) || strings.HasPrefix(rawFolder, `\`) || hasWindowsDrivePrefix {
		return fmt.Errorf("provider target folder must be relative")
	}
	for _, part := range strings.Split(normalizedSeparators, "/") {
		if part == ".." {
			return fmt.Errorf("provider target folder must not contain parent traversal")
		}
	}
	return nil
}

func validateSubscriptionTempTarget(target model.SubscriptionStorageTarget) error {
	target = NormalizeSubscriptionStorageTarget(target)
	if target.Provider == "" {
		return nil
	}
	if target.Provider != "pan123" && target.Provider != "pan115" {
		return fmt.Errorf("temporary target provider must be pan123 or pan115")
	}
	if target.Folder == "" {
		return fmt.Errorf("temporary target folder is required")
	}
	return nil
}

func validateSubscriptionDeliveryTarget(target model.SubscriptionStorageTarget) error {
	target = NormalizeSubscriptionStorageTarget(target)
	if target.Provider == "" {
		return nil
	}
	if target.Provider != "yidong139" {
		return fmt.Errorf("delivery target provider must be yidong139")
	}
	if target.Folder == "" {
		return fmt.Errorf("delivery target folder is required")
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
