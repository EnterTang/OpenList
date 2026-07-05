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
	if sub.TargetRoot != "/media" || sub.CheckIntervalMinutes != 120 || sub.Category != "欧美剧" {
		t.Fatalf("subscription defaults were not applied: %#v", sub)
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
