package subscription

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestParseTelegramRowsEnvelopeAndLinks(t *testing.T) {
	body := []byte(`{
		"results": [{
			"msgId": 123,
			"channel": "@movies",
			"text": "新剧 https://pan.example/s/abc 提取码? abcd",
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

func TestSubscriptionLinkItemsExposeSourceProvider(t *testing.T) {
	seenAt := time.Now()
	sub := &model.Subscription{ID: 7}
	telegramItem := telegramLinkItem(sub, telegramCommandRow{MsgID: float64(1), Channel: "@quark"}, "https://pan.quark.cn/s/bc18e4ea5fb8", seenAt)
	if telegramItem.SourceProvider != string(ShareProviderQuark) {
		t.Fatalf("telegram provider = %q, want quark", telegramItem.SourceProvider)
	}
	manualItem := manualLinkItem(sub, "https://115cdn.com/s/swssal13zrk?password=t58d", seenAt)
	if manualItem.SourceProvider != string(ShareProviderPan115) {
		t.Fatalf("manual provider = %q, want pan115", manualItem.SourceProvider)
	}
	panSouItem := panSouLinkItem(sub, model.SubscriptionResourceSearchResult{Title: "demo"}, model.SubscriptionResourceSearchLink{
		URL:      "https://www.123pan.com/s/7Tx1jv-pVu7v",
		Provider: "123",
	}, seenAt)
	if panSouItem.SourceProvider != string(ShareProviderPan123) {
		t.Fatalf("pansou provider = %q, want pan123", panSouItem.SourceProvider)
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

func TestTelegramLegacyCursorDoesNotSkipChannelScopedRows(t *testing.T) {
	cursor := parseTelegramCursor("66656")
	pan123Row := telegramCommandRow{MsgID: int64(47100), Channel: "Pan123Movie"}
	if telegramCursorHasSeen(cursor, pan123Row) {
		t.Fatal("legacy global cursor should not skip a lower channel-scoped message id")
	}

	next := cursor
	next.advance(telegramCommandRow{MsgID: int64(66656), Channel: "Aliyun_4K_Movies"})
	next.advance(pan123Row)
	formatted := formatTelegramCursor(next)
	parsed := parseTelegramCursor(formatted)
	if telegramCursorHasSeen(parsed, telegramCommandRow{MsgID: int64(47101), Channel: "Pan123Movie"}) {
		t.Fatalf("formatted cursor %q should not skip newer Pan123Movie message", formatted)
	}
	if !telegramCursorHasSeen(parsed, pan123Row) {
		t.Fatalf("formatted cursor %q should remember processed Pan123Movie message", formatted)
	}
}

func TestBuiltinTelegramLimitAppliesPerChannel(t *testing.T) {
	if got := telegramBuiltinPerChannelLimit(40, 7); got != 40 {
		t.Fatalf("per-channel limit = %d, want 40", got)
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

func TestRowLinksExtractsPan123FastLinkFromMessageText(t *testing.T) {
	fastLink := "123FSLinkV2$a3531a60736740a152e931a6ecee9bfb#500797103#食神·百厨大战.2025.S02E05.mp4"
	row := telegramCommandRow{
		Channel: "@pan123",
		Text:    "🔗分享链接 :\n" + fastLink,
	}
	links := rowLinks(row)
	if got, want := links, []string{fastLink}; !stringSlicesEqual(got, want) {
		t.Fatalf("links = %#v, want %#v", got, want)
	}
	if got := normalizeTelegramLinkWithAccessCode(fastLink, "ABCD"); got != fastLink {
		t.Fatalf("normalized fastlink = %q, want unchanged", got)
	}
}

func TestRowLinksForTelegramPanSourcesKeepsPan123FastLinkOnlyForPan123(t *testing.T) {
	fastLink := "123FSLinkV2$a3531a60736740a152e931a6ecee9bfb#500797103#食神·百厨大战.2025.S02E05.mp4"
	row := telegramCommandRow{
		Channel: "@mixed",
		Text:    fastLink,
	}

	pan123Cfg := normalizeTelegramSourceConfig(model.SubscriptionTelegramSourceConfig{
		Pan123: model.SubscriptionTelegramPanConfig{Channels: []string{"@mixed"}},
	})
	links, sources := rowLinksForTelegramPanSources(row, pan123Cfg)
	if got, want := providerSourceNames(sources), []string{"pan123"}; !stringSlicesEqual(got, want) {
		t.Fatalf("sources = %#v, want %#v", got, want)
	}
	if got, want := links, []string{fastLink}; !stringSlicesEqual(got, want) {
		t.Fatalf("links = %#v, want %#v", got, want)
	}

	quarkCfg := normalizeTelegramSourceConfig(model.SubscriptionTelegramSourceConfig{
		Quark: model.SubscriptionTelegramPanConfig{Channels: []string{"@mixed"}},
	})
	links, sources = rowLinksForTelegramPanSources(row, quarkCfg)
	if len(links) != 0 || len(sources) != 0 {
		t.Fatalf("links/sources = %#v / %#v, want empty links and no triggered source", links, sources)
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

func TestSubscriptionEntryMatchesSelectedSeasons(t *testing.T) {
	sub := &model.Subscription{
		Name:      "Some Show",
		TMDBName:  "Some Show",
		MediaType: "tv",
		Seasons:   []int{2, 4},
	}
	if !subscriptionEntryMatches(sub, TreeEntry{
		RootPath: "/quark/temp",
		Path:     "/Some Show/Season 2/Some.Show.S02E01.mkv",
		Name:     "Some.Show.S02E01.mkv",
	}) {
		t.Fatal("expected selected season 2 to match")
	}
	if subscriptionEntryMatches(sub, TreeEntry{
		RootPath: "/quark/temp",
		Path:     "/Some Show/Season 1/Some.Show.S01E01.mkv",
		Name:     "Some.Show.S01E01.mkv",
	}) {
		t.Fatal("did not expect unselected season 1 to match")
	}
}

func TestSubscriptionEpisodeRangeAppliesOnlyToLatestSelectedSeason(t *testing.T) {
	sub := &model.Subscription{
		MediaType:                "tv",
		Seasons:                  []int{1, 2},
		LatestSeasonEpisodeStart: 5,
		LatestSeasonEpisodeEnd:   8,
	}
	for _, tt := range []struct {
		season  int
		episode int
		want    bool
	}{
		{season: 1, episode: 2, want: true},
		{season: 2, episode: 4, want: false},
		{season: 2, episode: 5, want: true},
		{season: 2, episode: 8, want: true},
		{season: 2, episode: 9, want: false},
		{season: 2, episode: 0, want: false},
	} {
		if got := subscriptionEpisodeMatches(sub, tt.season, tt.episode); got != tt.want {
			t.Fatalf("S%02dE%02d match = %v, want %v", tt.season, tt.episode, got, tt.want)
		}
	}
}

func TestSubscriptionEpisodeRangeSupportsEndOnly(t *testing.T) {
	sub := &model.Subscription{
		MediaType:              "tv",
		Seasons:                []int{3},
		LatestSeasonEpisodeEnd: 6,
	}
	if !subscriptionEpisodeMatches(sub, 3, 6) {
		t.Fatal("episode at configured end should match")
	}
	if subscriptionEpisodeMatches(sub, 3, 7) {
		t.Fatal("episode after configured end unexpectedly matched")
	}
}

func TestSubscriptionEntryMatchesLatestSeasonEpisodeRange(t *testing.T) {
	sub := &model.Subscription{
		Name:                     "Some Show",
		TMDBName:                 "Some Show",
		MediaType:                "tv",
		Seasons:                  []int{2},
		LatestSeasonEpisodeStart: 10,
	}
	if subscriptionEntryMatches(sub, TreeEntry{Name: "Some.Show.S02E09.mkv", Path: "/Some.Show.S02E09.mkv"}) {
		t.Fatal("episode before configured start unexpectedly matched")
	}
	if !subscriptionEntryMatches(sub, TreeEntry{Name: "Some.Show.S02E10.mkv", Path: "/Some.Show.S02E10.mkv"}) {
		t.Fatal("episode at configured start should match")
	}
}

func TestSubscriptionEntryMatchesNoisyMixedLanguagePath(t *testing.T) {
	sub := &model.Subscription{Name: "Rain Man", TMDBName: "雨人"}
	entry := TreeEntry{
		RootPath: "/movie",
		Path:     "/movie/剧情片/雨人 Rain Man 1988 蓝光原盘REMUX",
		Name:     "雨人 Rain Man 1988 蓝光原盘REMUX",
	}
	if !subscriptionEntryMatches(sub, entry) {
		t.Fatal("expected noisy mixed-language path to match subscription")
	}
}

func TestTelegramPanSourcesForTransferMergesConfiguredAndTriggered(t *testing.T) {
	cfg := normalizeTelegramSourceConfig(model.SubscriptionTelegramSourceConfig{
		Pan123: model.SubscriptionTelegramPanConfig{
			Channels:         []string{"@mixed"},
			TempTransferRoot: "/123/temp",
		},
		Quark: model.SubscriptionTelegramPanConfig{
			TempTransferRoot: "/quark/temp",
		},
	})
	triggered := map[string]telegramPanSubscriptionSource{
		"pan115": {Name: "pan115", Config: model.SubscriptionTelegramPanConfig{TempTransferRoot: "/115/temp"}},
	}
	merged := telegramPanSourcesForTransfer(cfg, triggered)
	if got, want := len(merged), 3; got != want {
		t.Fatalf("merged count = %d, want %d (%#v)", got, want, merged)
	}
	if merged["pan123"].Config.TempTransferRoot != "/123/temp" {
		t.Fatalf("pan123 = %#v", merged["pan123"])
	}
	if merged["quark"].Config.TempTransferRoot != "/quark/temp" {
		t.Fatalf("quark = %#v", merged["quark"])
	}
	if merged["pan115"].Config.TempTransferRoot != "/115/temp" {
		t.Fatalf("pan115 = %#v", merged["pan115"])
	}
}

func TestSelectTelegramTempTransferCandidatesPrefersConfiguredProviderPriority(t *testing.T) {
	sub := &model.Subscription{
		ID:         8,
		TMDBName:   "飞常日志",
		TMDBYear:   2024,
		TMDBID:     243236,
		MediaType:  "tv",
		Category:   "港台剧",
		TargetRoot: "/139_60t/上传中转",
		Seasons:    []int{2},
	}
	seenAt := time.Now()
	candidates := []telegramTempCandidate{
		testTelegramTempCandidate(sub, "aliyun_drive", TreeEntry{
			RootPath: "/ali/转存至移动/F飞常日志2（2026）［港剧］",
			Path:     "/10粤语.mp4",
			Name:     "10粤语.mp4",
			ID:       "ali-e10",
			Size:     500,
		}, seenAt),
		testTelegramTempCandidate(sub, "quark", TreeEntry{
			RootPath: "/quark/转存至移动",
			Path:     "/飞常日志.2024.S02E10.第10集.1080p.WEB-DL.mkv",
			Name:     "飞常日志.2024.S02E10.第10集.1080p.WEB-DL.mkv",
			ID:       "quark-e10",
			Size:     800,
		}, seenAt),
		testTelegramTempCandidate(sub, "pan123", TreeEntry{
			RootPath: "/123/转存至移动",
			Path:     "/飞常日志.2024.S02E10.第10集.1080p.MyTVSuper.WEB-DL.mkv",
			Name:     "飞常日志.2024.S02E10.第10集.1080p.MyTVSuper.WEB-DL.mkv",
			ID:       "pan123-e10",
			Size:     700,
		}, seenAt),
	}

	selected := selectTelegramTempTransferCandidates(sub, candidates, []string{"pan123", "pan115", "quark", "aliyun_drive"})
	if got, want := len(selected), 1; got != want {
		t.Fatalf("selected count = %d, want %d: %#v", got, want, selected)
	}
	if selected[0].Source.Name != "pan123" {
		t.Fatalf("selected source = %q, want pan123", selected[0].Source.Name)
	}
	if selected[0].Item.Season != 2 || selected[0].Item.Episode != 10 {
		t.Fatalf("selected season/episode = %d/%d, want 2/10", selected[0].Item.Season, selected[0].Item.Episode)
	}
}

func TestRunTelegramTempTransfersSkipsMissingOptionalRoot(t *testing.T) {
	oldSnapshot := snapshotPaths
	defer func() {
		snapshotPaths = oldSnapshot
	}()

	var roots []string
	snapshotPaths = func(ctx context.Context, requested []string) (*TreeSnapshot, error) {
		root := requested[0]
		roots = append(roots, root)
		if root == "/ali/转存至移动" {
			return nil, errs.ObjectNotFound
		}
		return &TreeSnapshot{Hash: "pan123-hash"}, nil
	}

	_, hash, _, _, _, err := runTelegramTempTransfers(context.Background(), &model.Subscription{}, map[string]telegramPanSubscriptionSource{
		string(ShareProviderAliyunDrive): {
			Name: string(ShareProviderAliyunDrive),
			Config: model.SubscriptionTelegramPanConfig{
				TempTransferRoot: "/ali/转存至移动",
			},
		},
		string(ShareProviderPan123): {
			Name: string(ShareProviderPan123),
			Config: model.SubscriptionTelegramPanConfig{
				TempTransferRoot: "/123/转存至移动",
			},
		},
	}, model.SubscriptionTelegramSourceConfig{
		TransferPriority: []string{string(ShareProviderAliyunDrive), string(ShareProviderPan123)},
	}, false, time.Now())
	if err != nil {
		t.Fatalf("run temp transfers: %v", err)
	}
	if got, want := strings.Join(roots, ","), "/ali/转存至移动,/123/转存至移动"; got != want {
		t.Fatalf("snapshot roots = %q, want %q", got, want)
	}
	if hash == "" {
		t.Fatal("expected available provider snapshot to contribute a hash")
	}
}

func TestSelectTelegramTempTransferCandidatesDedupesSameProviderEpisode(t *testing.T) {
	sub := &model.Subscription{
		ID:         8,
		TMDBName:   "飞常日志",
		TMDBYear:   2024,
		TMDBID:     243236,
		MediaType:  "tv",
		Category:   "港台剧",
		TargetRoot: "/139_60t/上传中转",
		Seasons:    []int{2},
	}
	seenAt := time.Now()
	candidates := []telegramTempCandidate{
		testTelegramTempCandidate(sub, "pan123", TreeEntry{
			RootPath: "/123/转存至移动",
			Path:     "/飞常日志.2024.S02E05.第5集.1080p.MyVideo.WEB-DL.SDR.H.264.30fps.AAC 2.0.mkv",
			Name:     "飞常日志.2024.S02E05.第5集.1080p.MyVideo.WEB-DL.SDR.H.264.30fps.AAC 2.0.mkv",
			ID:       "pan123-e05-myvideo",
			Size:     600,
		}, seenAt),
		testTelegramTempCandidate(sub, "pan123", TreeEntry{
			RootPath: "/123/转存至移动",
			Path:     "/飞常日志.2024.S02E05.第5集.1080p.MyTVSuper.WEB-DL.SDR.H.265.25fps.AAC 2.0.mkv",
			Name:     "飞常日志.2024.S02E05.第5集.1080p.MyTVSuper.WEB-DL.SDR.H.265.25fps.AAC 2.0.mkv",
			ID:       "pan123-e05-mytvsuper",
			Size:     900,
		}, seenAt),
	}

	selected := selectTelegramTempTransferCandidates(sub, candidates, []string{"pan123", "pan115", "quark", "aliyun_drive"})
	if got, want := len(selected), 1; got != want {
		t.Fatalf("selected count = %d, want %d: %#v", got, want, selected)
	}
	if !strings.Contains(selected[0].Entry.Name, "MyTVSuper") {
		t.Fatalf("selected entry = %q, want larger MyTVSuper candidate", selected[0].Entry.Name)
	}
}

func testTelegramTempCandidate(sub *model.Subscription, sourceName string, entry TreeEntry, seenAt time.Time) telegramTempCandidate {
	item := itemFromEntry(sub, entry, seenAt)
	return telegramTempCandidate{
		Source: telegramPanSubscriptionSource{
			Name: sourceName,
		},
		Entry: entry,
		Item:  item,
	}
}

func providerSourceNames(sources []telegramPanSubscriptionSource) []string {
	names := make([]string, 0, len(sources))
	for _, source := range sources {
		names = append(names, source.Name)
	}
	return names
}
