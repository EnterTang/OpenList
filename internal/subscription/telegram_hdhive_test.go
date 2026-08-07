package subscription

import (
	"context"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/hdhive"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestResolveTelegramHDHiveLinksFeedsUnlockedShareURL(t *testing.T) {
	client := &telegramHDHiveFakeClient{}
	old := newTelegramHDHiveService
	newTelegramHDHiveService = func(model.SubscriptionTelegramHDHiveConfig) (*hdhive.Service, error) {
		return hdhive.NewService(client), nil
	}
	t.Cleanup(func() { newTelegramHDHiveService = old })

	row := telegramCommandRow{
		Text:    "亡命闺蜜",
		Channel: "oneonefivewpfx",
		Buttons: []struct {
			Text string `json:"text"`
			URL  string `json:"url"`
		}{
			{Text: "直达链接", URL: "https://hdhive.com/resource/115/054da9afa2204d33a11831e58776d1e4"},
		},
	}
	links, err := resolveTelegramHDHiveLinks(context.Background(), row, model.SubscriptionTelegramSourceConfig{
		HDHive: model.SubscriptionTelegramHDHiveConfig{
			Enabled:      true,
			BaseURL:      "https://hdhive.symedia.top",
			UserID:       "test-user",
			ProxyUserKey: "test-key",
			ProxySecret:  "test-secret",
		},
	})
	if err != nil {
		t.Fatalf("resolve HDHive links: %v", err)
	}
	if len(links) != 1 || links[0].URL != client.unlocked.FullURL || links[0].AccessCode != "vvve" {
		t.Fatalf("links = %#v", links)
	}
}

func TestUnlockHDHiveResourceReturnsShareDetails(t *testing.T) {
	client := &telegramHDHiveFakeClient{}
	old := newTelegramHDHiveService
	newTelegramHDHiveService = func(model.SubscriptionTelegramHDHiveConfig) (*hdhive.Service, error) {
		return hdhive.NewService(client), nil
	}
	t.Cleanup(func() { newTelegramHDHiveService = old })

	result, err := UnlockHDHiveResource(context.Background(), "https://hdhive.com/resource/189/054da9afa2204d33a11831e58776d1e4", model.SubscriptionTelegramHDHiveConfig{
		Enabled:         true,
		BaseURL:         "https://hdhive.example",
		UserID:          "unlock-test-user",
		ProxyUserKey:    "test-key",
		ProxySecret:     "test-secret",
		MaxUnlockPoints: 2,
	})
	if err != nil {
		t.Fatalf("unlock HDHive resource: %v", err)
	}
	if result.URL != client.unlocked.FullURL || result.AccessCode != "vvve" || result.FromCache {
		t.Fatalf("result = %#v, want unlocked share details", result)
	}
}

type telegramHDHiveFakeClient struct {
	unlocked hdhive.UnlockResult
}

func (f *telegramHDHiveFakeClient) Status(context.Context) (hdhive.Status, error) {
	return hdhive.Status{Authorized: true}, nil
}

func (f *telegramHDHiveFakeClient) Share(context.Context, string) (hdhive.ResourceDetails, error) {
	return hdhive.ResourceDetails{UnlockPoints: hdhiveIntPtr(1), AccessCode: "vvve"}, nil
}

func hdhiveIntPtr(value int) *int { return &value }

func (f *telegramHDHiveFakeClient) Unlock(context.Context, string) (hdhive.UnlockResult, error) {
	f.unlocked = hdhive.UnlockResult{FullURL: "https://115cdn.com/s/sws46l73np7", AccessCode: "vvve"}
	return f.unlocked, nil
}
