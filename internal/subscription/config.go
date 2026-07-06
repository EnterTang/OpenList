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
	if sub.CheckIntervalMinutes <= 0 {
		sub.CheckIntervalMinutes = 60
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
	cfg.DefaultTargetRoot = ""
	cfg.DefaultCheckIntervalMinutes = 0
	cfg.DefaultMediaType = ""
	cfg.DefaultCategory = ""
	cfg.DefaultTransferEnabled = false
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
		cfg = model.SubscriptionTelegramSourceConfig{}
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
	if len(cfg.Quark.Channels) == 0 {
		cfg.Quark.Channels = defaults.Quark.Channels
	}
	if cfg.Quark.TempTransferRoot == "" {
		cfg.Quark.TempTransferRoot = defaults.Quark.TempTransferRoot
	}
	if !cfg.Quark.DeleteSourceAfter {
		cfg.Quark.DeleteSourceAfter = defaults.Quark.DeleteSourceAfter
	}
	if len(cfg.AliyunDrive.Channels) == 0 {
		cfg.AliyunDrive.Channels = defaults.AliyunDrive.Channels
	}
	if cfg.AliyunDrive.TempTransferRoot == "" {
		cfg.AliyunDrive.TempTransferRoot = defaults.AliyunDrive.TempTransferRoot
	}
	if !cfg.AliyunDrive.DeleteSourceAfter {
		cfg.AliyunDrive.DeleteSourceAfter = defaults.AliyunDrive.DeleteSourceAfter
	}
	if len(cfg.Pan123.Channels) == 0 {
		cfg.Pan123.Channels = defaults.Pan123.Channels
	}
	if cfg.Pan123.TempTransferRoot == "" {
		cfg.Pan123.TempTransferRoot = defaults.Pan123.TempTransferRoot
	}
	if !cfg.Pan123.DeleteSourceAfter {
		cfg.Pan123.DeleteSourceAfter = defaults.Pan123.DeleteSourceAfter
	}
	if len(cfg.Pan115.Channels) == 0 {
		cfg.Pan115.Channels = defaults.Pan115.Channels
	}
	if cfg.Pan115.TempTransferRoot == "" {
		cfg.Pan115.TempTransferRoot = defaults.Pan115.TempTransferRoot
	}
	if !cfg.Pan115.DeleteSourceAfter {
		cfg.Pan115.DeleteSourceAfter = defaults.Pan115.DeleteSourceAfter
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
	return normalizeTelegramSourceConfig(cfg)
}

func normalizeTelegramSourceConfig(cfg model.SubscriptionTelegramSourceConfig) model.SubscriptionTelegramSourceConfig {
	cfg.APIHash = strings.TrimSpace(cfg.APIHash)
	cfg.SessionFile = strings.TrimSpace(cfg.SessionFile)
	cfg.Channels = cleanStringList(cfg.Channels, false)
	cfg.Quark.Channels = append(cfg.Quark.Channels, cfg.QuarkChannels...)
	cfg.AliyunDrive.Channels = append(cfg.AliyunDrive.Channels, cfg.AliyunDriveChannels...)
	cfg.Pan123.Channels = append(cfg.Pan123.Channels, cfg.Pan123Channels...)
	cfg.Pan115.Channels = append(cfg.Pan115.Channels, cfg.Pan115Channels...)
	cfg.Quark = normalizeTelegramPanConfig(cfg.Quark)
	cfg.AliyunDrive = normalizeTelegramPanConfig(cfg.AliyunDrive)
	cfg.Pan123 = normalizeTelegramPanConfig(cfg.Pan123)
	cfg.Pan115 = normalizeTelegramPanConfig(cfg.Pan115)
	cfg.QuarkChannels = nil
	cfg.AliyunDriveChannels = nil
	cfg.Pan123Channels = nil
	cfg.Pan115Channels = nil
	if hasTelegramChannelGroups(cfg) {
		cfg.Channels = telegramChannelGroups(cfg)
	}
	cfg.SearchCommand = cleanCommandList(cfg.SearchCommand)
	cfg.AuthCommand = cleanCommandList(cfg.AuthCommand)
	cfg.CommandEnv = cleanStringList(cfg.CommandEnv, false)
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
		isZeroTelegramPanConfig(cfg.Quark) &&
		isZeroTelegramPanConfig(cfg.AliyunDrive) &&
		isZeroTelegramPanConfig(cfg.Pan123) &&
		isZeroTelegramPanConfig(cfg.Pan115) &&
		len(cfg.SearchCommand) == 0 &&
		len(cfg.AuthCommand) == 0 &&
		len(cfg.CommandEnv) == 0 &&
		cfg.CommandTimeoutSeconds == 30 &&
		cfg.Limit == 40
}

func hasTelegramChannelGroups(cfg model.SubscriptionTelegramSourceConfig) bool {
	return len(cfg.Quark.Channels) > 0 ||
		len(cfg.AliyunDrive.Channels) > 0 ||
		len(cfg.Pan123.Channels) > 0 ||
		len(cfg.Pan115.Channels) > 0
}

func telegramChannelGroups(cfg model.SubscriptionTelegramSourceConfig) []string {
	return cleanStringList(append(append(append(append(
		[]string{},
		cfg.Quark.Channels...),
		cfg.AliyunDrive.Channels...),
		cfg.Pan123.Channels...),
		cfg.Pan115.Channels...), false)
}

func normalizeTelegramPanConfig(cfg model.SubscriptionTelegramPanConfig) model.SubscriptionTelegramPanConfig {
	cfg.Channels = cleanStringList(cfg.Channels, false)
	cfg.TempTransferRoot = cleanConfigPath(cfg.TempTransferRoot)
	return cfg
}

func isZeroTelegramPanConfig(cfg model.SubscriptionTelegramPanConfig) bool {
	cfg = normalizeTelegramPanConfig(cfg)
	return len(cfg.Channels) == 0 && cfg.TempTransferRoot == "" && !cfg.DeleteSourceAfter
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
