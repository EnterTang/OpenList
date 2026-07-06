package subscription

import (
	"encoding/json"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestApplyConfigDefaultsMergesTelegramConfig(t *testing.T) {
	sub := &model.Subscription{
		SourceType:   model.SubscriptionSourceTelegram,
		SourceConfig: `{"channels":["@custom"],"limit":5}`,
	}
	cfg := model.SubscriptionConfig{
		DefaultTargetRoot:           "/media",
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
	if sub.TargetRoot != "" || sub.Category != "" {
		t.Fatalf("removed default behavior fields were applied: %#v", sub)
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
		SourceConfig: `{"quark_channels":[" @sub-quark ",""],"limit":5}`,
	}
	cfg := model.SubscriptionConfig{
		Telegram: model.SubscriptionTelegramSourceConfig{
			Channels: []string{"@legacy-default"},
			Quark: model.SubscriptionTelegramPanConfig{
				Channels:         []string{"@default-quark"},
				TempTransferRoot: "/temp/quark",
			},
			AliyunDrive: model.SubscriptionTelegramPanConfig{
				Channels:         []string{"@default-aliyun"},
				TempTransferRoot: "/temp/aliyun",
			},
			Pan123: model.SubscriptionTelegramPanConfig{
				Channels:          []string{"@default-123"},
				TempTransferRoot:  "/temp/123",
				DeleteSourceAfter: true,
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
	if source.Quark.TempTransferRoot != "/temp/quark" {
		t.Fatalf("quark temp root = %q, want /temp/quark", source.Quark.TempTransferRoot)
	}
	if got, want := source.AliyunDrive.Channels, []string{"@default-aliyun"}; !stringSlicesEqual(got, want) {
		t.Fatalf("aliyun channels = %#v, want %#v", got, want)
	}
	if source.AliyunDrive.TempTransferRoot != "/temp/aliyun" {
		t.Fatalf("aliyun temp root = %q, want /temp/aliyun", source.AliyunDrive.TempTransferRoot)
	}
	if got, want := source.Pan123.Channels, []string{"@default-123"}; !stringSlicesEqual(got, want) {
		t.Fatalf("123 channels = %#v, want %#v", got, want)
	}
	if source.Pan123.TempTransferRoot != "/temp/123" || !source.Pan123.DeleteSourceAfter {
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
}

func TestNormalizeConfigRemovesDefaultBehaviorFields(t *testing.T) {
	cfg := normalizeConfig(model.SubscriptionConfig{
		DefaultTargetRoot:           "/media",
		DefaultCheckIntervalMinutes: 120,
		DefaultTransferEnabled:      true,
		DefaultMediaType:            "movie",
		DefaultCategory:             "电影",
	})
	if cfg.DefaultTargetRoot != "" || cfg.DefaultCheckIntervalMinutes != 0 || cfg.DefaultTransferEnabled || cfg.DefaultMediaType != "" || cfg.DefaultCategory != "" {
		t.Fatalf("default behavior fields were not removed: %#v", cfg)
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
