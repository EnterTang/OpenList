package subscription

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestApplyConfigDefaultsMergesTelegramConfig(t *testing.T) {
	sub := &model.Subscription{
		SourceType:   model.SubscriptionSourceTelegram,
		SourceConfig: `{"channels":["@custom"],"limit":5}`,
	}
	cfg := model.SubscriptionConfig{
		DefaultTarget:               model.SubscriptionStorageTarget{Provider: "yidong139", Folder: "media"},
		DefaultCheckIntervalMinutes: 120,
		DefaultMediaType:            "tv",
		DefaultCategory:             "欧美剧",
		Telegram: model.SubscriptionTelegramSourceConfig{
			APIID:         123,
			APIHash:       "hash",
			SessionFile:   "data/telegram.session",
			Channels:      []string{"@default"},
			SearchCommand: []string{"node", "telegram_search.mjs"},
		},
	}

	if err := ApplyConfigDefaults(sub, cfg); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}
	if sub.DeliveryTarget.Provider != "" || sub.DeliveryTarget.Folder != "" {
		t.Fatalf("global delivery target was persisted onto subscription: %#v", sub.DeliveryTarget)
	}
	if sub.Category != "" {
		t.Fatalf("removed default category was applied: %#v", sub)
	}
	if sub.CheckIntervalMinutes != 60 {
		t.Fatalf("check interval = %d, want internal fallback 60", sub.CheckIntervalMinutes)
	}
	if sub.MediaType != "" {
		t.Fatalf("media type default was applied: %q", sub.MediaType)
	}

	var source model.SubscriptionTelegramSourceConfig
	if err := json.Unmarshal([]byte(sub.SourceConfig), &source); err != nil {
		t.Fatalf("decode merged source config: %v", err)
	}
	if source.APIID != 123 || source.APIHash != "hash" || source.SessionFile != "data/telegram.session" {
		t.Fatalf("telegram auth defaults were not merged: %#v", source)
	}
	if len(source.Channels) != 1 || source.Channels[0] != "@custom" {
		t.Fatalf("subscription channel override was not preserved: %#v", source.Channels)
	}
	if source.Limit != 5 {
		t.Fatalf("subscription limit override = %d, want 5", source.Limit)
	}
}

func TestApplyConfigDefaultsMergesTelegramChannelGroups(t *testing.T) {
	sub := &model.Subscription{
		SourceType:   model.SubscriptionSourceTelegram,
		SourceConfig: `{"quark_channels":[" @sub-quark ",""],"limit":5,"transfer_priority":["quark","123","aliyun"]}`,
	}
	cfg := model.SubscriptionConfig{
		Telegram: model.SubscriptionTelegramSourceConfig{
			Channels: []string{"@legacy-default"},
			Quark: model.SubscriptionTelegramPanConfig{
				Channels:           []string{"@default-quark"},
				TempTransferTarget: model.SubscriptionStorageTarget{Provider: "quark", Folder: "temp"},
			},
			AliyunDrive: model.SubscriptionTelegramPanConfig{
				Channels:           []string{"@default-aliyun"},
				TempTransferTarget: model.SubscriptionStorageTarget{Provider: "aliyun_drive", Folder: "temp"},
			},
			Pan123: model.SubscriptionTelegramPanConfig{
				Channels:           []string{"@default-123"},
				TempTransferTarget: model.SubscriptionStorageTarget{Provider: "pan123", Folder: "temp"},
				DeleteSourceAfter:  true,
			},
			Pan115: model.SubscriptionTelegramPanConfig{
				Channels: []string{"@default-115"},
			},
		},
	}

	if err := ApplyConfigDefaults(sub, cfg); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}

	var source model.SubscriptionTelegramSourceConfig
	if err := json.Unmarshal([]byte(sub.SourceConfig), &source); err != nil {
		t.Fatalf("decode merged source config: %v", err)
	}
	if got, want := source.Quark.Channels, []string{"@sub-quark"}; !stringSlicesEqual(got, want) {
		t.Fatalf("quark channel override = %#v, want %#v", got, want)
	}
	if source.Quark.TempTransferTarget != cfg.Telegram.Quark.TempTransferTarget {
		t.Fatalf("quark temp target = %#v", source.Quark.TempTransferTarget)
	}
	if got, want := source.AliyunDrive.Channels, []string{"@default-aliyun"}; !stringSlicesEqual(got, want) {
		t.Fatalf("aliyun channels = %#v, want %#v", got, want)
	}
	if source.AliyunDrive.TempTransferTarget != cfg.Telegram.AliyunDrive.TempTransferTarget {
		t.Fatalf("aliyun temp target = %#v", source.AliyunDrive.TempTransferTarget)
	}
	if got, want := source.Pan123.Channels, []string{"@default-123"}; !stringSlicesEqual(got, want) {
		t.Fatalf("123 channels = %#v, want %#v", got, want)
	}
	if source.Pan123.TempTransferTarget != cfg.Telegram.Pan123.TempTransferTarget || !source.Pan123.DeleteSourceAfter {
		t.Fatalf("123 config = %#v, want temp root and cleanup switch", source.Pan123)
	}
	if got, want := source.Pan115.Channels, []string{"@default-115"}; !stringSlicesEqual(got, want) {
		t.Fatalf("115 channels = %#v, want %#v", got, want)
	}
	if len(source.QuarkChannels) != 0 || len(source.AliyunDriveChannels) != 0 || len(source.Pan123Channels) != 0 || len(source.Pan115Channels) != 0 {
		t.Fatalf("legacy channel fields should not be re-emitted: %#v", source)
	}
	if got, want := source.Channels, []string{"@sub-quark", "@default-aliyun", "@default-123", "@default-115"}; !stringSlicesEqual(got, want) {
		t.Fatalf("runtime channels = %#v, want %#v", got, want)
	}
	if got, want := source.TransferPriority, []string{"quark", "pan123", "aliyun_drive", "pan115"}; !stringSlicesEqual(got, want) {
		t.Fatalf("transfer priority = %#v, want %#v", got, want)
	}
}

func TestApplyConfigDefaultsMergesTelegramProviderCredentials(t *testing.T) {
	sub := &model.Subscription{
		SourceType: model.SubscriptionSourceTelegram,
		SourceConfig: `{
			"quark":{"channels":["@sub-quark"],"cookie":" sub-cookie "},
			"aliyun_drive":{"channels":["@sub-aliyun"]},
			"pan123":{"channels":["@sub-123"],"access_token":" sub-access "},
			"pan115":{"channels":["@sub-115"]}
		}`,
	}
	cfg := model.SubscriptionConfig{
		Telegram: model.SubscriptionTelegramSourceConfig{
			Quark: model.SubscriptionTelegramPanConfig{
				Channels:     []string{"@default-quark"},
				Cookie:       " default-quark-cookie ",
				RefreshToken: " default-quark-refresh ",
			},
			AliyunDrive: model.SubscriptionTelegramPanConfig{
				Channels:     []string{"@default-aliyun"},
				RefreshToken: " default-aliyun-refresh ",
				DriveID:      " default-drive ",
			},
			Pan123: model.SubscriptionTelegramPanConfig{
				Channels:    []string{"@default-123"},
				AccessToken: " default-123-access ",
			},
			Pan115: model.SubscriptionTelegramPanConfig{
				Channels: []string{"@default-115"},
				Cookie:   " default-115-cookie ",
			},
		},
	}

	if err := ApplyConfigDefaults(sub, cfg); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}

	var source model.SubscriptionTelegramSourceConfig
	if err := json.Unmarshal([]byte(sub.SourceConfig), &source); err != nil {
		t.Fatalf("decode merged source config: %v", err)
	}
	if source.Quark.Cookie != "sub-cookie" {
		t.Fatalf("quark cookie = %q, want subscription override", source.Quark.Cookie)
	}
	if source.Quark.RefreshToken != "default-quark-refresh" {
		t.Fatalf("quark refresh token = %q, want default", source.Quark.RefreshToken)
	}
	if source.AliyunDrive.RefreshToken != "default-aliyun-refresh" {
		t.Fatalf("aliyun refresh token = %q, want default", source.AliyunDrive.RefreshToken)
	}
	if source.AliyunDrive.DriveID != "" || strings.Contains(sub.SourceConfig, "drive_id") {
		t.Fatalf("aliyun drive id should not be emitted in config: source=%#v raw=%s", source.AliyunDrive, sub.SourceConfig)
	}
	if source.Pan123.AccessToken != "sub-access" {
		t.Fatalf("123 access token = %q, want subscription override", source.Pan123.AccessToken)
	}
	if source.Pan115.Cookie != "default-115-cookie" {
		t.Fatalf("115 cookie = %q, want default", source.Pan115.Cookie)
	}
	if got, want := source.Channels, []string{"@sub-quark", "@sub-aliyun", "@sub-123", "@sub-115"}; !stringSlicesEqual(got, want) {
		t.Fatalf("runtime channels = %#v, want %#v", got, want)
	}
}

func TestApplyConfigDefaultsMergesTelegramProviderTarget(t *testing.T) {
	sub := &model.Subscription{
		SourceType:   model.SubscriptionSourceTelegram,
		SourceConfig: `{"pan123":{"channels":["@sub"]}}`,
	}
	cfg := model.SubscriptionConfig{
		DefaultTarget: model.SubscriptionStorageTarget{Provider: "yidong139", Folder: "剧集"},
		Telegram: model.SubscriptionTelegramSourceConfig{
			Pan123: model.SubscriptionTelegramPanConfig{
				TempTransferTarget: model.SubscriptionStorageTarget{Provider: "pan123", Folder: "转存至移动"},
			},
		},
	}

	if err := ApplyConfigDefaults(sub, cfg); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}
	if sub.DeliveryTarget.Provider != "" || sub.DeliveryTarget.Folder != "" {
		t.Fatalf("global delivery target was persisted onto subscription: %#v", sub.DeliveryTarget)
	}
	var source model.SubscriptionTelegramSourceConfig
	if err := json.Unmarshal([]byte(sub.SourceConfig), &source); err != nil {
		t.Fatalf("decode merged source config: %v", err)
	}
	if source.Pan123.TempTransferTarget != cfg.Telegram.Pan123.TempTransferTarget {
		t.Fatalf("temp target = %#v, want %#v", source.Pan123.TempTransferTarget, cfg.Telegram.Pan123.TempTransferTarget)
	}
}

func TestApplyConfigDefaultsMigratesLegacySubscriptionTarget(t *testing.T) {
	sub := &model.Subscription{TargetRoot: "/139_60t/港台剧"}
	if err := ApplyConfigDefaults(sub, model.SubscriptionConfig{}); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}
	if sub.DeliveryTarget.Provider != "yidong139" || sub.DeliveryTarget.Folder != "港台剧" {
		t.Fatalf("delivery target = %#v", sub.DeliveryTarget)
	}
	if sub.TargetRoot != "" {
		t.Fatalf("legacy target root was not cleared: %q", sub.TargetRoot)
	}
}

func TestApplyConfigDefaultsRejectsUnknownLegacySubscriptionTarget(t *testing.T) {
	sub := &model.Subscription{TargetRoot: "/custom-mount/media"}
	err := ApplyConfigDefaults(sub, model.SubscriptionConfig{})
	if err == nil || !strings.Contains(err.Error(), "manual confirmation") {
		t.Fatalf("error = %v, want manual confirmation error", err)
	}
}

func TestApplyConfigDefaultsAllowsMissingTargetsButRejectsInvalidExplicitTargets(t *testing.T) {
	for _, test := range []struct {
		name string
		sub  model.Subscription
	}{
		{name: "missing targets", sub: model.Subscription{TransferEnabled: true}},
		{name: "invalid temp provider", sub: model.Subscription{TempTarget: model.SubscriptionStorageTarget{Provider: "yidong139", Folder: "temp"}, DeliveryTarget: model.SubscriptionStorageTarget{Provider: "yidong139", Folder: "delivery"}, TransferEnabled: true}},
		{name: "invalid delivery provider", sub: model.Subscription{DeliveryTarget: model.SubscriptionStorageTarget{Provider: "pan123", Folder: "delivery"}, TransferEnabled: true}},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := ApplyConfigDefaults(&test.sub, model.SubscriptionConfig{})
			if test.name == "missing targets" {
				if err != nil {
					t.Fatalf("empty targets should be accepted: %v", err)
				}
				if test.sub.TempTarget.Provider != "" || test.sub.DeliveryTarget.Provider != "" {
					t.Fatalf("empty targets were unexpectedly defaulted: %#v", test.sub)
				}
				return
			}
			if err == nil {
				t.Fatal("expected explicit target validation error")
			}
		})
	}
}

func TestApplyConfigDefaultsRejectsUnknownLegacyTempTransferRoot(t *testing.T) {
	sub := &model.Subscription{SourceType: model.SubscriptionSourceTelegram}
	cfg := model.SubscriptionConfig{Telegram: model.SubscriptionTelegramSourceConfig{
		Pan123: model.SubscriptionTelegramPanConfig{TempTransferRoot: "/custom-mount/temp"},
	}}
	err := ApplyConfigDefaults(sub, cfg)
	if err == nil || !strings.Contains(err.Error(), "manual confirmation") {
		t.Fatalf("error = %v, want manual confirmation error", err)
	}
}

func TestSaveConfigMigratesRecognizedLegacyTargetsBeforeValidation(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	saved, err := SaveConfig(model.SubscriptionConfig{
		DefaultTargetRoot: "/139_60t/剧集",
		Telegram: model.SubscriptionTelegramSourceConfig{
			Pan123: model.SubscriptionTelegramPanConfig{TempTransferRoot: "/123/转存"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if saved.DefaultTarget != (model.SubscriptionStorageTarget{Provider: "yidong139", Folder: "剧集"}) || saved.DefaultTargetRoot != "" {
		t.Fatalf("default target migration = %#v / %q", saved.DefaultTarget, saved.DefaultTargetRoot)
	}
	if saved.Telegram.Pan123.TempTransferTarget != (model.SubscriptionStorageTarget{Provider: "pan123", Folder: "转存"}) || saved.Telegram.Pan123.TempTransferRoot != "" {
		t.Fatalf("temp target migration = %#v / %q", saved.Telegram.Pan123.TempTransferTarget, saved.Telegram.Pan123.TempTransferRoot)
	}
}

func TestSaveConfigRejectsInvalidTargetRoles(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	_, err := SaveConfig(model.SubscriptionConfig{
		DefaultTarget: model.SubscriptionStorageTarget{Provider: "pan123", Folder: "delivery"},
	})
	if err == nil {
		t.Fatal("pan123 default delivery target unexpectedly accepted")
	}
}

func TestSubscriptionConfigEarlyCloseDefaults(t *testing.T) {
	cfg := model.SubscriptionConfig{}
	normalized := normalizeConfig(cfg)
	if normalized.EpisodeEarlyCloseMinBytes == nil || *normalized.EpisodeEarlyCloseMinBytes != 1<<30 {
		t.Fatalf("episode default = %v, want 1GiB", normalized.EpisodeEarlyCloseMinBytes)
	}
	if normalized.MovieEarlyCloseMinBytes == nil || *normalized.MovieEarlyCloseMinBytes != 20<<30 {
		t.Fatalf("movie default = %v, want 20GiB", normalized.MovieEarlyCloseMinBytes)
	}
	zero := int64(0)
	cfg.EpisodeEarlyCloseMinBytes = &zero
	cfg.MovieEarlyCloseMinBytes = &zero
	normalized = normalizeConfig(cfg)
	if *normalized.EpisodeEarlyCloseMinBytes != 0 || *normalized.MovieEarlyCloseMinBytes != 0 {
		t.Fatalf("zero must remain disabled: %#v", normalized)
	}
}

func TestNormalizeConfigDefaultsTelegramTransferPriority(t *testing.T) {
	cfg := normalizeConfig(model.SubscriptionConfig{})
	if got, want := cfg.Telegram.TransferPriority, []string{"pan123", "pan115", "quark", "aliyun_drive"}; !stringSlicesEqual(got, want) {
		t.Fatalf("transfer priority = %#v, want %#v", got, want)
	}
}

func TestNormalizeConfigPreservesDefaultTargetRootOnly(t *testing.T) {
	cfg := normalizeConfig(model.SubscriptionConfig{
		DefaultTargetRoot:           "/media",
		DefaultCheckIntervalMinutes: 120,
		DefaultTransferEnabled:      true,
		DefaultMediaType:            "movie",
		DefaultCategory:             "电影",
	})
	if cfg.DefaultTargetRoot != "/media" {
		t.Fatalf("default target root = %q, want /media", cfg.DefaultTargetRoot)
	}
	if cfg.DefaultCheckIntervalMinutes != 0 || cfg.DefaultTransferEnabled || cfg.DefaultMediaType != "" || cfg.DefaultCategory != "" {
		t.Fatalf("unexpected default behavior fields were preserved: %#v", cfg)
	}
}

func stringSlicesEqual(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
