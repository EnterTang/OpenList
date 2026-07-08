package subscription

import (
	"encoding/json"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestNormalizeResourceSearchSources(t *testing.T) {
	got := normalizeResourceSearchSources([]string{" telegram ", "pansou", "telegram"})
	want := []string{model.SubscriptionSourceTelegram, model.SubscriptionSourcePanSou}
	if !stringSlicesEqual(got, want) {
		t.Fatalf("sources = %#v, want %#v", got, want)
	}
	got = normalizeResourceSearchSources(nil)
	if !stringSlicesEqual(got, want) {
		t.Fatalf("default sources = %#v, want %#v", got, want)
	}
}

func TestParseResourceSearchOutputExtractsLinks(t *testing.T) {
	body, err := json.Marshal(map[string]any{
		"data": []map[string]any{
			{
				"title":   "测试剧集 S01",
				"channel": "tg_channel",
				"links": []map[string]any{
					{
						"url":      "https://pan.quark.cn/s/abc123",
						"password": "ABCD",
					},
				},
			},
			{
				"name":    "115 资源",
				"content": "链接 https://115.com/s/example 提取码：wxyz",
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	results, err := parseResourceSearchOutput(model.SubscriptionSourcePanSou, body, 10)
	if err != nil {
		t.Fatalf("parse resource output: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("results len = %d, want 2: %#v", len(results), results)
	}
	if results[0].Provider != string(ShareProviderQuark) || len(results[0].Links) != 1 {
		t.Fatalf("first result links = %#v", results[0])
	}
	if results[0].Links[0].URL != "https://pan.quark.cn/s/abc123,ABCD" {
		t.Fatalf("quark link = %q", results[0].Links[0].URL)
	}
	if results[1].Provider != string(ShareProviderPan115) {
		t.Fatalf("second provider = %q", results[1].Provider)
	}
}

func TestParseResourceSearchOutputFiltersByTitleAndSupportedShareLinks(t *testing.T) {
	body, err := json.Marshal(map[string]any{
		"data": []map[string]any{
			{
				"title":   "完全无关的资源",
				"content": "正文里提到了 雨人，但标题不匹配 https://www.123pan.com/s/abc123",
			},
			{
				"title":   "雨人 Rain Man 1988",
				"content": "只有跳转链接 https://t.me/share_123pan_bot?start=2993 和论坛页 https://123panfx.com/thread-2993.htm",
			},
			{
				"title":   "雨人 Rain Man 1988",
				"content": "可用分享 https://www.123pan.com/s/realshare 提取码：Guce",
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	results, err := parseResourceSearchOutput(model.SubscriptionSourcePanSou, body, 10)
	if err != nil {
		t.Fatalf("parse resource output: %v", err)
	}
	results = filterResourceSearchResults(results, "雨人", 10)
	if len(results) != 1 {
		t.Fatalf("filtered results len = %d, want 1: %#v", len(results), results)
	}
	if results[0].Title != "雨人 Rain Man 1988" {
		t.Fatalf("title = %q, want %q", results[0].Title, "雨人 Rain Man 1988")
	}
	if len(results[0].Links) != 1 || results[0].Links[0].Provider != string(ShareProviderPan123) {
		t.Fatalf("links = %#v, want single 123pan share link", results[0].Links)
	}
}

func TestResourceLinksFromTextIgnoresUnsupportedURLs(t *testing.T) {
	links := resourceLinksFromText("论坛页 https://123panfx.com/thread-2993.htm 机器人 https://t.me/share_123pan_bot?start=2993", "")
	if len(links) != 0 {
		t.Fatalf("links = %#v, want empty", links)
	}
}

func TestPanSouSearchEndpoint(t *testing.T) {
	cases := map[string]string{
		"https://example.com":            "https://example.com/api/search",
		"https://example.com/api":        "https://example.com/api/search",
		"https://example.com/api/search": "https://example.com/api/search",
	}
	for input, want := range cases {
		got, err := panSouSearchEndpoint(input)
		if err != nil {
			t.Fatalf("endpoint(%q): %v", input, err)
		}
		if got != want {
			t.Fatalf("endpoint(%q) = %q, want %q", input, got, want)
		}
	}
}
