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
