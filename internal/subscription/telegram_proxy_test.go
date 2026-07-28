package subscription

import (
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/gotd/td/telegram"
)

func TestParseTelegramProxyURL(t *testing.T) {
	t.Parallel()

	parsed, err := parseTelegramProxyURL("192.168.1.1:7891")
	if err != nil {
		t.Fatalf("parse bare host: %v", err)
	}
	if parsed.Scheme != "socks5" || parsed.Host != "192.168.1.1:7891" {
		t.Fatalf("bare host = %s://%s, want socks5://192.168.1.1:7891", parsed.Scheme, parsed.Host)
	}

	parsed, err = parseTelegramProxyURL("socks5://user:pass@127.0.0.1:1080")
	if err != nil {
		t.Fatalf("parse socks5: %v", err)
	}
	if parsed.User.Username() != "user" {
		t.Fatalf("username = %q", parsed.User.Username())
	}

	if _, err := parseTelegramProxyURL("ftp://127.0.0.1:21"); err == nil {
		t.Fatal("expected unsupported scheme error")
	}
	if _, err := parseTelegramProxyURL("socks5://"); err == nil {
		t.Fatal("expected missing host error")
	}
}

func TestTelegramProxyDialEmpty(t *testing.T) {
	t.Parallel()
	dial, err := telegramProxyDial(" ")
	if err != nil {
		t.Fatalf("empty proxy: %v", err)
	}
	if dial != nil {
		t.Fatal("expected nil dialer for empty proxy")
	}
}

func TestTelegramClientOptionsWithProxy(t *testing.T) {
	t.Parallel()
	opts, err := telegramClientOptions(model.SubscriptionTelegramSourceConfig{
		ProxyURL: "socks5://127.0.0.1:7891",
	}, telegram.Options{})
	if err != nil {
		t.Fatalf("options: %v", err)
	}
	if opts.Resolver == nil {
		t.Fatal("expected resolver when proxy_url is set")
	}

	opts, err = telegramClientOptions(model.SubscriptionTelegramSourceConfig{}, telegram.Options{})
	if err != nil {
		t.Fatalf("options without proxy: %v", err)
	}
	if opts.Resolver != nil {
		t.Fatal("expected default resolver when proxy_url is empty")
	}
}

func TestFillTelegramSourceConfigProxyURL(t *testing.T) {
	t.Parallel()
	cfg := fillTelegramSourceConfig(model.SubscriptionTelegramSourceConfig{
		APIID: 1, APIHash: "hash",
	}, model.SubscriptionTelegramSourceConfig{
		ProxyURL: "socks5://192.168.1.1:7891",
	})
	if cfg.ProxyURL != "socks5://192.168.1.1:7891" {
		t.Fatalf("proxy_url = %q", cfg.ProxyURL)
	}
}
