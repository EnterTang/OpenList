package subscription

import (
	"encoding/json"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/pkg/errors"
	"gorm.io/gorm"
)

const ConfigSettingKey = "subscription_config"

func DefaultConfig() model.SubscriptionConfig {
	return model.SubscriptionConfig{
		DefaultCheckIntervalMinutes: 60,
		DefaultMediaType:            "tv",
		Telegram: model.SubscriptionTelegramSourceConfig{
			CommandTimeoutSeconds: 30,
			Limit:                 40,
		},
		PanSou: model.SubscriptionPanSouSourceConfig{
			CommandTimeoutSeconds: 30,
			Limit:                 40,
		},
	}
}

func GetConfig() (model.SubscriptionConfig, error) {
	cfg := DefaultConfig()
	item, err := op.GetSettingItemByKey(ConfigSettingKey)
	if err != nil {
		if errors.Is(errors.Cause(err), gorm.ErrRecordNotFound) || errors.Is(err, gorm.ErrRecordNotFound) {
			return cfg, nil
		}
		return cfg, err
	}
	if strings.TrimSpace(item.Value) == "" {
		return cfg, nil
	}
	if err := json.Unmarshal([]byte(item.Value), &cfg); err != nil {
		return cfg, errors.WithMessage(err, "invalid subscription config")
	}
	return normalizeConfig(cfg), nil
}

func SaveConfig(cfg model.SubscriptionConfig) (model.SubscriptionConfig, error) {
	cfg = normalizeConfig(cfg)
	body, err := json.Marshal(cfg)
	if err != nil {
		return cfg, err
	}
	item := &model.SettingItem{
		Key:   ConfigSettingKey,
		Value: string(body),
		Type:  conf.TypeText,
		Group: model.SINGLE,
		Flag:  model.PRIVATE,
	}
	return cfg, op.SaveSettingItem(item)
}

func ApplyDefaults(sub *model.Subscription) error {
	cfg, err := GetConfig()
	if err != nil {
		return err
	}
	return ApplyConfigDefaults(sub, cfg)
}

func ApplyConfigDefaults(sub *model.Subscription, cfg model.SubscriptionConfig) error {
	if sub == nil {
		return errors.New("subscription is nil")
	}
	cfg = normalizeConfig(cfg)
	if strings.TrimSpace(sub.TargetRoot) == "" && cfg.DefaultTargetRoot != "" {
		sub.TargetRoot = cfg.DefaultTargetRoot
	}
	if sub.CheckIntervalMinutes <= 0 {
		sub.CheckIntervalMinutes = cfg.DefaultCheckIntervalMinutes
	}
	if strings.TrimSpace(sub.MediaType) == "" && cfg.DefaultMediaType != "" {
		sub.MediaType = cfg.DefaultMediaType
	}
	if strings.TrimSpace(sub.Category) == "" && cfg.DefaultCategory != "" {
		sub.Category = cfg.DefaultCategory
	}
	if cfg.DefaultTransferEnabled && !sub.TransferEnabled {
		sub.TransferEnabled = true
	}
	sourceType := strings.ToLower(strings.TrimSpace(sub.SourceType))
	switch sourceType {
	case model.SubscriptionSourceTelegram:
		merged, err := mergeTelegramSourceConfig(sub.SourceConfig, cfg.Telegram)
		if err != nil {
			return err
		}
		sub.SourceConfig = merged
	case model.SubscriptionSourcePanSou:
		merged, err := mergePanSouSourceConfig(sub.SourceConfig, cfg.PanSou)
		if err != nil {
			return err
		}
		sub.SourceConfig = merged
	}
	return nil
}

func normalizeConfig(cfg model.SubscriptionConfig) model.SubscriptionConfig {
	cfg.DefaultTargetRoot = cleanConfigPath(cfg.DefaultTargetRoot)
	cfg.DefaultMediaType = strings.ToLower(strings.TrimSpace(cfg.DefaultMediaType))
	if cfg.DefaultMediaType != "movie" {
		cfg.DefaultMediaType = "tv"
	}
	cfg.DefaultCategory = strings.TrimSpace(cfg.DefaultCategory)
	if cfg.DefaultCheckIntervalMinutes <= 0 {
		cfg.DefaultCheckIntervalMinutes = 60
	}
	cfg.Telegram = normalizeTelegramSourceConfig(cfg.Telegram)
	cfg.PanSou = normalizePanSouSourceConfig(cfg.PanSou)
	return cfg
}

func cleanConfigPath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return ""
	}
	return utils.FixAndCleanPath(path)
}

func mergeTelegramSourceConfig(raw string, defaults model.SubscriptionTelegramSourceConfig) (string, error) {
	defaults = normalizeTelegramSourceConfig(defaults)
	cfg := defaults
	if strings.TrimSpace(raw) != "" {
		if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
			return raw, errors.WithMessage(err, "invalid telegram source config")
		}
		cfg = normalizeTelegramSourceConfig(cfg)
		cfg = fillTelegramSourceConfig(cfg, defaults)
	}
	if isZeroTelegramSourceConfig(cfg) {
		return strings.TrimSpace(raw), nil
	}
	body, err := json.Marshal(cfg)
	if err != nil {
		return raw, err
	}
	return string(body), nil
}

func fillTelegramSourceConfig(cfg, defaults model.SubscriptionTelegramSourceConfig) model.SubscriptionTelegramSourceConfig {
	if cfg.APIID == 0 {
		cfg.APIID = defaults.APIID
	}
	if cfg.APIHash == "" {
		cfg.APIHash = defaults.APIHash
	}
	if cfg.SessionFile == "" {
		cfg.SessionFile = defaults.SessionFile
	}
	if len(cfg.QuarkChannels) == 0 {
		cfg.QuarkChannels = defaults.QuarkChannels
	}
	if len(cfg.AliyunDriveChannels) == 0 {
		cfg.AliyunDriveChannels = defaults.AliyunDriveChannels
	}
	if len(cfg.Pan123Channels) == 0 {
		cfg.Pan123Channels = defaults.Pan123Channels
	}
	if len(cfg.Pan115Channels) == 0 {
		cfg.Pan115Channels = defaults.Pan115Channels
	}
	if len(cfg.Channels) == 0 && !hasTelegramChannelGroups(cfg) {
		cfg.Channels = defaults.Channels
	}
	if len(cfg.SearchCommand) == 0 {
		cfg.SearchCommand = defaults.SearchCommand
	}
	if len(cfg.AuthCommand) == 0 {
		cfg.AuthCommand = defaults.AuthCommand
	}
	if len(cfg.CommandEnv) == 0 {
		cfg.CommandEnv = defaults.CommandEnv
	}
	if cfg.CommandTimeoutSeconds <= 0 {
		cfg.CommandTimeoutSeconds = defaults.CommandTimeoutSeconds
	}
	if cfg.Limit <= 0 {
		cfg.Limit = defaults.Limit
	}
	if cfg.Query == "" {
		cfg.Query = defaults.Query
	}
	return normalizeTelegramSourceConfig(cfg)
}

func normalizeTelegramSourceConfig(cfg model.SubscriptionTelegramSourceConfig) model.SubscriptionTelegramSourceConfig {
	cfg.APIHash = strings.TrimSpace(cfg.APIHash)
	cfg.SessionFile = strings.TrimSpace(cfg.SessionFile)
	cfg.Channels = cleanStringList(cfg.Channels, false)
	cfg.QuarkChannels = cleanStringList(cfg.QuarkChannels, false)
	cfg.AliyunDriveChannels = cleanStringList(cfg.AliyunDriveChannels, false)
	cfg.Pan123Channels = cleanStringList(cfg.Pan123Channels, false)
	cfg.Pan115Channels = cleanStringList(cfg.Pan115Channels, false)
	if hasTelegramChannelGroups(cfg) {
		cfg.Channels = telegramChannelGroups(cfg)
	}
	cfg.SearchCommand = cleanCommandList(cfg.SearchCommand)
	cfg.AuthCommand = cleanCommandList(cfg.AuthCommand)
	cfg.CommandEnv = cleanStringList(cfg.CommandEnv, false)
	cfg.Query = strings.TrimSpace(cfg.Query)
	if cfg.CommandTimeoutSeconds <= 0 {
		cfg.CommandTimeoutSeconds = 30
	}
	if cfg.Limit <= 0 {
		cfg.Limit = 40
	}
	return cfg
}

func isZeroTelegramSourceConfig(cfg model.SubscriptionTelegramSourceConfig) bool {
	cfg = normalizeTelegramSourceConfig(cfg)
	return cfg.APIID == 0 &&
		cfg.APIHash == "" &&
		cfg.SessionFile == "" &&
		len(cfg.Channels) == 0 &&
		len(cfg.QuarkChannels) == 0 &&
		len(cfg.AliyunDriveChannels) == 0 &&
		len(cfg.Pan123Channels) == 0 &&
		len(cfg.Pan115Channels) == 0 &&
		len(cfg.SearchCommand) == 0 &&
		len(cfg.AuthCommand) == 0 &&
		len(cfg.CommandEnv) == 0 &&
		cfg.CommandTimeoutSeconds == 30 &&
		cfg.Limit == 40 &&
		cfg.Query == ""
}

func hasTelegramChannelGroups(cfg model.SubscriptionTelegramSourceConfig) bool {
	return len(cfg.QuarkChannels) > 0 ||
		len(cfg.AliyunDriveChannels) > 0 ||
		len(cfg.Pan123Channels) > 0 ||
		len(cfg.Pan115Channels) > 0
}

func telegramChannelGroups(cfg model.SubscriptionTelegramSourceConfig) []string {
	return cleanStringList(append(append(append(append(
		[]string{},
		cfg.QuarkChannels...),
		cfg.AliyunDriveChannels...),
		cfg.Pan123Channels...),
		cfg.Pan115Channels...), false)
}

func mergePanSouSourceConfig(raw string, defaults model.SubscriptionPanSouSourceConfig) (string, error) {
	defaults = normalizePanSouSourceConfig(defaults)
	cfg := defaults
	if strings.TrimSpace(raw) != "" {
		if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
			return raw, errors.WithMessage(err, "invalid pansou source config")
		}
		cfg = fillPanSouSourceConfig(normalizePanSouSourceConfig(cfg), defaults)
	}
	if isZeroPanSouSourceConfig(cfg) {
		return strings.TrimSpace(raw), nil
	}
	body, err := json.Marshal(cfg)
	if err != nil {
		return raw, err
	}
	return string(body), nil
}

func fillPanSouSourceConfig(cfg, defaults model.SubscriptionPanSouSourceConfig) model.SubscriptionPanSouSourceConfig {
	if cfg.BaseURL == "" {
		cfg.BaseURL = defaults.BaseURL
	}
	if len(cfg.SearchCommand) == 0 {
		cfg.SearchCommand = defaults.SearchCommand
	}
	if len(cfg.CommandEnv) == 0 {
		cfg.CommandEnv = defaults.CommandEnv
	}
	if cfg.CommandTimeoutSeconds <= 0 {
		cfg.CommandTimeoutSeconds = defaults.CommandTimeoutSeconds
	}
	if cfg.Limit <= 0 {
		cfg.Limit = defaults.Limit
	}
	if cfg.Query == "" {
		cfg.Query = defaults.Query
	}
	return normalizePanSouSourceConfig(cfg)
}

func normalizePanSouSourceConfig(cfg model.SubscriptionPanSouSourceConfig) model.SubscriptionPanSouSourceConfig {
	cfg.BaseURL = strings.TrimSpace(cfg.BaseURL)
	cfg.SearchCommand = cleanCommandList(cfg.SearchCommand)
	cfg.CommandEnv = cleanStringList(cfg.CommandEnv, false)
	cfg.Query = strings.TrimSpace(cfg.Query)
	if cfg.CommandTimeoutSeconds <= 0 {
		cfg.CommandTimeoutSeconds = 30
	}
	if cfg.Limit <= 0 {
		cfg.Limit = 40
	}
	return cfg
}

func isZeroPanSouSourceConfig(cfg model.SubscriptionPanSouSourceConfig) bool {
	cfg = normalizePanSouSourceConfig(cfg)
	return cfg.BaseURL == "" &&
		len(cfg.SearchCommand) == 0 &&
		len(cfg.CommandEnv) == 0 &&
		cfg.CommandTimeoutSeconds == 30 &&
		cfg.Limit == 40 &&
		cfg.Query == ""
}

func cleanCommandList(values []string) []string {
	cleaned := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			cleaned = append(cleaned, value)
		}
	}
	return cleaned
}
