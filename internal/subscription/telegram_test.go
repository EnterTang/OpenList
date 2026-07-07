package subscription

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestParseTelegramRowsEnvelopeAndLinks(t *testing.T) {
	body := []byte(`{
		"results": [{
			"msgId": 123,
			"channel": "@movies",
			"text": "新剧 https://pan.example/s/abc 提取码 abcd",
			"links": ["https://pan.example/s/abc"],
			"buttons": [{"text": "open", "url": "https://pan.example/s/def"}]
		}]
	}`)
	rows, err := parseTelegramRows(body)
	if err != nil {
		t.Fatalf("parse rows: %v", err)
	}
	if len(rows) != 1 || rowMessageID(rows[0]) != 123 {
		t.Fatalf("rows = %#v", rows)
	}
	links := rowLinks(rows[0])
	if len(links) != 2 {
		t.Fatalf("links = %#v, want 2", links)
	}
	if got := normalizeTelegramLinkWithAccessCode(links[0], rowAccessCode(rows[0])); got != "https://pan.example/s/abc,abcd" {
		t.Fatalf("normalized link = %q", got)
	}
}

func TestTelegramLinkItemUsesStableMessageSourceKey(t *testing.T) {
	row := telegramCommandRow{MsgID: float64(456), Channel: "@movies"}
	item := telegramLinkItem(&model.Subscription{ID: 7}, row, "https://pan.example/s/abc", time.Now())
	if item.SourceKey == "" || item.SourceURL != "https://pan.example/s/abc" {
		t.Fatalf("item = %#v", item)
	}
	if item.Status != model.SubscriptionItemStatusSkipped {
		t.Fatalf("status = %q", item.Status)
	}

	body, err := json.Marshal(item)
	if err != nil || len(body) == 0 {
		t.Fatalf("marshal item: body=%s err=%v", body, err)
	}
}

func TestRunTelegramAuthCommandStatusWithoutCommandReturnsUnauthorized(t *testing.T) {
	result, err := runTelegramAuthCommand(context.Background(), model.SubscriptionTelegramSourceConfig{}, "status", telegramAuthPayload{})
	if err != nil {
		t.Fatalf("status without auth command: %v", err)
	}
	if result.Authorized {
		t.Fatalf("authorized = true, want false")
	}
}

func TestRunTelegramAuthUsesBuiltinWhenAPIConfigPresentWithoutCommand(t *testing.T) {
	old := builtinTelegramAuth
	builtinTelegramAuth = func(ctx context.Context, cfg model.SubscriptionTelegramSourceConfig, action string, payload telegramAuthPayload) (telegramAuthCommandResp, error) {
		if action != "send-code" {
			t.Fatalf("action = %q, want send-code", action)
		}
		if cfg.APIID != 12345 || cfg.APIHash != "hash" {
			t.Fatalf("cfg = %#v, want api credentials", cfg)
		}
		if payload.Phone != "+8613800000000" {
			t.Fatalf("phone = %q", payload.Phone)
		}
		return telegramAuthCommandResp{OK: true, Authorized: false, PhoneCodeHash: "phone-hash"}, nil
	}
	defer func() { builtinTelegramAuth = old }()

	result, err := runTelegramAuth(context.Background(), model.SubscriptionTelegramSourceConfig{
		APIID:   12345,
		APIHash: "hash",
	}, "send-code", telegramAuthPayload{Phone: "+8613800000000"})
	if err != nil {
		t.Fatalf("run telegram auth: %v", err)
	}
	if result.PhoneCodeHash != "phone-hash" {
		t.Fatalf("phone code hash = %q", result.PhoneCodeHash)
	}
}

func TestRunTelegramSearchUsesBuiltinWhenAPIConfigPresentWithoutCommand(t *testing.T) {
	old := builtinTelegramSearch
	builtinTelegramSearch = func(ctx context.Context, sub *model.Subscription, cfg model.SubscriptionTelegramSourceConfig) ([]telegramCommandRow, error) {
		if sub.TMDBName != "三体" {
			t.Fatalf("tmdb name = %q", sub.TMDBName)
		}
		if got, want := cfg.Channels, []string{"@quark"}; !stringSlicesEqual(got, want) {
			t.Fatalf("channels = %#v, want %#v", got, want)
		}
		return []telegramCommandRow{{MsgID: int64(1), Channel: "@quark", Text: "三体 https://pan.example/s/abc"}}, nil
	}
	defer func() { builtinTelegramSearch = old }()

	rows, err := runTelegramSearch(context.Background(), &model.Subscription{TMDBName: "三体"}, model.SubscriptionTelegramSourceConfig{
		APIID:   12345,
		APIHash: "hash",
		Channels: []string{
			"@quark",
		},
	})
	if err != nil {
		t.Fatalf("run telegram search: %v", err)
	}
	if len(rows) != 1 || rowMessageID(rows[0]) != 1 {
		t.Fatalf("rows = %#v", rows)
	}
}

func TestTelegramSearchQueryUsesSubscriptionNames(t *testing.T) {
	if got := telegramSearchQuery(&model.Subscription{TMDBName: " 三体 ", Name: "fallback"}); got != "三体" {
		t.Fatalf("query = %q, want 三体", got)
	}
	if got := telegramSearchQuery(&model.Subscription{Name: " 流浪地球 "}); got != "流浪地球" {
		t.Fatalf("query = %q, want 流浪地球", got)
	}
	if got := telegramSearchQuery(nil); got != "" {
		t.Fatalf("nil query = %q, want empty", got)
	}
}

func TestTelegramPanSourceForRowUsesNestedPanConfig(t *testing.T) {
	cfg := normalizeTelegramSourceConfig(model.SubscriptionTelegramSourceConfig{
		Quark: model.SubscriptionTelegramPanConfig{
			Channels:         []string{"@quark_new"},
			TempTransferRoot: "/temp/quark",
		},
		Pan115: model.SubscriptionTelegramPanConfig{
			Channels:          []string{"115Channel"},
			TempTransferRoot:  "/temp/115",
			DeleteSourceAfter: true,
		},
	})

	source, ok := telegramPanSourceForRow(telegramCommandRow{Channel: "https://t.me/quark_new"}, cfg)
	if !ok {
		t.Fatal("quark source not matched")
	}
	if source.Name != "quark" || source.Config.TempTransferRoot != "/temp/quark" {
		t.Fatalf("source = %#v, want quark temp config", source)
	}
	source, ok = telegramPanSourceForRow(telegramCommandRow{Channel: "@115Channel"}, cfg)
	if !ok {
		t.Fatal("115 source not matched")
	}
	if source.Name != "pan115" || !source.Config.DeleteSourceAfter {
		t.Fatalf("source = %#v, want pan115 cleanup config", source)
	}
	if _, ok := telegramPanSourceForRow(telegramCommandRow{Channel: "@other"}, cfg); ok {
		t.Fatal("unexpected source match")
	}
}

func TestRowLinksForTelegramPanSourcesFiltersProviderDomains(t *testing.T) {
	cfg := normalizeTelegramSourceConfig(model.SubscriptionTelegramSourceConfig{
		Quark: model.SubscriptionTelegramPanConfig{Channels: []string{"@mixed"}},
	})
	row := telegramCommandRow{
		Channel: "@mixed",
		Text: strings.Join([]string{
			"https://pan.quark.cn/s/bc18e4ea5fb8",
			"https://www.alipan.com/s/odeXVKsEKxr",
			"https://www.123pan.com/s/7Tx1jv-pVu7v?pwd=xoxo#",
			"https://115cdn.com/s/swssal13zrk?password=t58d",
		}, " "),
	}

	links, sources := rowLinksForTelegramPanSources(row, cfg)
	if got, want := providerSourceNames(sources), []string{"quark"}; !stringSlicesEqual(got, want) {
		t.Fatalf("sources = %#v, want %#v", got, want)
	}
	if got, want := links, []string{"https://pan.quark.cn/s/bc18e4ea5fb8"}; !stringSlicesEqual(got, want) {
		t.Fatalf("links = %#v, want %#v", got, want)
	}
}

func TestRowLinksForTelegramPanSourcesAllowsSharedMixedChannel(t *testing.T) {
	cfg := normalizeTelegramSourceConfig(model.SubscriptionTelegramSourceConfig{
		Quark:       model.SubscriptionTelegramPanConfig{Channels: []string{"@mixed"}},
		AliyunDrive: model.SubscriptionTelegramPanConfig{Channels: []string{"@mixed"}},
		Pan123:      model.SubscriptionTelegramPanConfig{Channels: []string{"@mixed"}},
		Pan115:      model.SubscriptionTelegramPanConfig{Channels: []string{"@mixed"}},
	})
	row := telegramCommandRow{
		Channel: "@mixed",
		Text: strings.Join([]string{
			"https://pan.quark.cn/s/bc18e4ea5fb8",
			"https://www.alipan.com/s/odeXVKsEKxr",
			"https://www.123pan.com/s/7Tx1jv-pVu7v?pwd=xoxo#",
			"https://115cdn.com/s/swssal13zrk?password=t58d",
			"https://example.com/not-a-pan-link",
		}, " "),
	}

	links, sources := rowLinksForTelegramPanSources(row, cfg)
	if got, want := providerSourceNames(sources), []string{"quark", "aliyun_drive", "pan123", "pan115"}; !stringSlicesEqual(got, want) {
		t.Fatalf("sources = %#v, want %#v", got, want)
	}
	if got, want := links, []string{
		"https://pan.quark.cn/s/bc18e4ea5fb8",
		"https://www.alipan.com/s/odeXVKsEKxr",
		"https://www.123pan.com/s/7Tx1jv-pVu7v?pwd=xoxo#",
		"https://115cdn.com/s/swssal13zrk?password=t58d",
	}; !stringSlicesEqual(got, want) {
		t.Fatalf("links = %#v, want %#v", got, want)
	}
}

func TestSubscriptionEntryMatchesSubscriptionName(t *testing.T) {
	sub := &model.Subscription{
		Name:     "Fallback Title",
		TMDBName: "三体",
		TMDBYear: 2023,
	}
	if !subscriptionEntryMatches(sub, TreeEntry{
		RootPath: "/quark/temp",
		Path:     "/剧集/三体.2023.S01E01.mkv",
		Name:     "三体.2023.S01E01.mkv",
	}) {
		t.Fatal("expected Chinese title match")
	}
	if !subscriptionEntryMatches(&model.Subscription{Name: "The Long Season"}, TreeEntry{
		RootPath: "/quark/temp",
		Path:     "/The.Long.Season.S01E01.mkv",
		Name:     "The.Long.Season.S01E01.mkv",
	}) {
		t.Fatal("expected punctuation-normalized title match")
	}
	if subscriptionEntryMatches(sub, TreeEntry{
		RootPath: "/quark/temp",
		Path:     "/别的剧/S01E01.mkv",
		Name:     "S01E01.mkv",
	}) {
		t.Fatal("unexpected unrelated title match")
	}
}

func providerSourceNames(sources []telegramPanSubscriptionSource) []string {
	names := make([]string, 0, len(sources))
	for _, source := range sources {
		names = append(names, source.Name)
	}
	return names
}
