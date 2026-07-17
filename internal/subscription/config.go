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
	if err := validateSubscriptionConfigTargets(cfg); err != nil {
		return cfg, err
	}
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
	if err := validateSubscriptionConfigTargets(cfg); err != nil {
		return err
	}
	if err := ValidateSubscriptionStorageTarget(sub.TempTarget); err != nil {
		return errors.WithMessage(err, "invalid temp target")
	}
	if err := ValidateSubscriptionStorageTarget(sub.DeliveryTarget); err != nil {
		return errors.WithMessage(err, "invalid delivery target")
	}
	sub.TempTarget = NormalizeSubscriptionStorageTarget(sub.TempTarget)
	sub.DeliveryTarget = NormalizeSubscriptionStorageTarget(sub.DeliveryTarget)
	if sub.DeliveryTarget.Provider == "" && strings.TrimSpace(sub.TargetRoot) != "" {
		if migrated, ok := MigrateLegacyPathTarget(sub.TargetRoot); ok {
			sub.DeliveryTarget = migrated
		} else {
			return errors.Errorf("legacy target_root %q is not a recognized provider mount and requires manual confirmation", sub.TargetRoot)
		}
	}
	if sub.DeliveryTarget.Provider != "" {
		sub.TargetRoot = ""
	}
	if err := validateSubscriptionTempTarget(sub.TempTarget); err != nil {
		return errors.WithMessage(err, "invalid temp target")
	}
	// Delivery targets are intentionally optional on the subscription record.
	// Cluster workers resolve their own staging and delivery folders, while
	// standalone runs resolve an empty target through SubscriptionConfig at
	// runtime. Do not materialize cfg.DefaultTarget here: doing so would leak a
	// coordinator-local path into every newly created subscription.
	if err := validateSubscriptionDeliveryTarget(sub.DeliveryTarget); err != nil {
		return errors.WithMessage(err, "invalid delivery target")
	}
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

func validateSubscriptionConfigTargets(cfg model.SubscriptionConfig) error {
	if err := ValidateSubscriptionStorageTarget(cfg.DefaultTarget); err != nil {
		return errors.WithMessage(err, "invalid default target")
	}
	if err := validateSubscriptionDeliveryTarget(cfg.DefaultTarget); err != nil {
		return errors.WithMessage(err, "invalid default target")
	}
	if cfg.DefaultTarget.Provider == "" && strings.TrimSpace(cfg.DefaultTargetRoot) != "" {
		return errors.Errorf("legacy default_target_root %q is not a recognized provider mount and requires manual confirmation", cfg.DefaultTargetRoot)
	}
	panTargets := []struct {
		name             string
		expectedProvider string
		target           model.SubscriptionStorageTarget
	}{
		{"quark", "quark", cfg.Telegram.Quark.TempTransferTarget},
		{"aliyun_drive", "aliyun_drive", cfg.Telegram.AliyunDrive.TempTransferTarget},
		{"pan123", "pan123", cfg.Telegram.Pan123.TempTransferTarget},
		{"pan115", "pan115", cfg.Telegram.Pan115.TempTransferTarget},
	}
	for _, item := range panTargets {
		if err := ValidateSubscriptionStorageTarget(item.target); err != nil {
			return errors.WithMessagef(err, "invalid %s temp transfer target", item.name)
		}
		target := NormalizeSubscriptionStorageTarget(item.target)
		if target.Provider != "" && (target.Provider != item.expectedProvider || target.Folder == "") {
			return errors.Errorf("invalid %s temp transfer target: provider must be %s and folder is required", item.name, item.expectedProvider)
		}
	}
	panRoots := []struct {
		name   string
		target model.SubscriptionStorageTarget
		root   string
	}{
		{"quark", cfg.Telegram.Quark.TempTransferTarget, cfg.Telegram.Quark.TempTransferRoot},
		{"aliyun_drive", cfg.Telegram.AliyunDrive.TempTransferTarget, cfg.Telegram.AliyunDrive.TempTransferRoot},
		{"pan123", cfg.Telegram.Pan123.TempTransferTarget, cfg.Telegram.Pan123.TempTransferRoot},
		{"pan115", cfg.Telegram.Pan115.TempTransferTarget, cfg.Telegram.Pan115.TempTransferRoot},
	}
	for _, item := range panRoots {
		if item.target.Provider == "" && strings.TrimSpace(item.root) != "" {
			return errors.Errorf("legacy %s temp_transfer_root %q is not a recognized provider mount and requires manual confirmation", item.name, item.root)
		}
	}
	return nil
}

func normalizeConfig(cfg model.SubscriptionConfig) model.SubscriptionConfig {
	cfg.DefaultTargetRoot = cleanConfigPath(cfg.DefaultTargetRoot)
	cfg.DefaultTarget = NormalizeSubscriptionStorageTarget(cfg.DefaultTarget)
	if cfg.DefaultTarget.Provider == "" && cfg.DefaultTargetRoot != "" {
		if migrated, ok := MigrateLegacyPathTarget(cfg.DefaultTargetRoot); ok {
			cfg.DefaultTarget = migrated
			cfg.DefaultTargetRoot = ""
		}
	}
	cfg.DefaultCheckIntervalMinutes = 0
	cfg.DefaultMediaType = ""
	cfg.DefaultCategory = ""
	cfg.DefaultTransferEnabled = false
	cfg.Telegram = normalizeTelegramSourceConfig(cfg.Telegram)
	if len(cfg.Telegram.TransferPriority) == 0 {
		cfg.Telegram.TransferPriority = append([]string(nil), defaultTransferPriority...)
	}
	cfg.PanSou = normalizePanSouSourceConfig(cfg.PanSou)
	cfg.EpisodeEarlyCloseMinBytes = normalizeEarlyCloseMinBytesPtr(cfg.EpisodeEarlyCloseMinBytes, 1<<30)
	cfg.MovieEarlyCloseMinBytes = normalizeEarlyCloseMinBytesPtr(cfg.MovieEarlyCloseMinBytes, 20<<30)
	return cfg
}

func earlyCloseMinBytes(value *int64, defaultBytes int64) int64 {
	if value == nil {
		return defaultBytes
	}
	if *value < 0 {
		return 0
	}
	return *value
}

func normalizeEarlyCloseMinBytesPtr(value *int64, defaultBytes int64) *int64 {
	normalized := earlyCloseMinBytes(value, defaultBytes)
	return &normalized
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
	cfg.Quark = fillTelegramPanConfig(cfg.Quark, defaults.Quark)
	cfg.AliyunDrive = fillTelegramPanConfig(cfg.AliyunDrive, defaults.AliyunDrive)
	cfg.Pan123 = fillTelegramPanConfig(cfg.Pan123, defaults.Pan123)
	cfg.Pan115 = fillTelegramPanConfig(cfg.Pan115, defaults.Pan115)
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
	if len(cfg.TransferPriority) == 0 {
		cfg.TransferPriority = append([]string(nil), defaults.TransferPriority...)
	}
	return normalizeTelegramSourceConfig(cfg)
}

var defaultTransferPriority = []string{"pan123", "pan115", "quark", "aliyun_drive"}

func normalizeTransferPriority(values []string) []string {
	if len(values) == 0 {
		return append([]string(nil), defaultTransferPriority...)
	}
	seen := map[string]struct{}{}
	normalized := make([]string, 0, len(values)+len(defaultTransferPriority))
	for _, value := range values {
		name := normalizeTransferPriorityName(value)
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		normalized = append(normalized, name)
	}
	for _, name := range defaultTransferPriority {
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		normalized = append(normalized, name)
	}
	return normalized
}

func normalizeTransferPriorityName(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "quark":
		return "quark"
	case "aliyun", "aliyun_drive", "ali", "alipan":
		return "aliyun_drive"
	case "123", "pan123":
		return "pan123"
	case "115", "pan115":
		return "pan115"
	default:
		return ""
	}
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
	cfg.TransferPriority = normalizeTransferPriority(cfg.TransferPriority)
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
	cfg.TempTransferTarget = NormalizeSubscriptionStorageTarget(cfg.TempTransferTarget)
	if cfg.TempTransferTarget.Provider == "" && cfg.TempTransferRoot != "" {
		if migrated, ok := MigrateLegacyPathTarget(cfg.TempTransferRoot); ok {
			cfg.TempTransferTarget = migrated
			cfg.TempTransferRoot = ""
		}
	}
	cfg.Cookie = strings.TrimSpace(cfg.Cookie)
	cfg.RefreshToken = strings.TrimSpace(cfg.RefreshToken)
	cfg.AccessToken = strings.TrimSpace(cfg.AccessToken)
	cfg.DriveID = strings.TrimSpace(cfg.DriveID)
	cfg.DriveType = strings.ToLower(strings.TrimSpace(cfg.DriveType))
	return cfg
}

func fillTelegramPanConfig(cfg, defaults model.SubscriptionTelegramPanConfig) model.SubscriptionTelegramPanConfig {
	cfg = normalizeTelegramPanConfig(cfg)
	defaults = normalizeTelegramPanConfig(defaults)
	if len(cfg.Channels) == 0 {
		cfg.Channels = defaults.Channels
	}
	if cfg.TempTransferRoot == "" {
		cfg.TempTransferRoot = defaults.TempTransferRoot
	}
	if cfg.TempTransferTarget.Provider == "" {
		cfg.TempTransferTarget = defaults.TempTransferTarget
	}
	if !cfg.DeleteSourceAfter {
		cfg.DeleteSourceAfter = defaults.DeleteSourceAfter
	}
	if cfg.Cookie == "" {
		cfg.Cookie = defaults.Cookie
	}
	if cfg.RefreshToken == "" {
		cfg.RefreshToken = defaults.RefreshToken
	}
	if cfg.AccessToken == "" {
		cfg.AccessToken = defaults.AccessToken
	}
	if cfg.DriveType == "" {
		cfg.DriveType = defaults.DriveType
	}
	return normalizeTelegramPanConfig(cfg)
}

func isZeroTelegramPanConfig(cfg model.SubscriptionTelegramPanConfig) bool {
	cfg = normalizeTelegramPanConfig(cfg)
	return len(cfg.Channels) == 0 &&
		cfg.TempTransferRoot == "" &&
		cfg.TempTransferTarget.Provider == "" &&
		!cfg.DeleteSourceAfter &&
		cfg.Cookie == "" &&
		cfg.RefreshToken == "" &&
		cfg.AccessToken == "" &&
		cfg.DriveType == ""
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
