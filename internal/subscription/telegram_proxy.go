package subscription

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/dcs"
	"golang.org/x/net/proxy"
)

func telegramClientOptions(cfg model.SubscriptionTelegramSourceConfig, opts telegram.Options) (telegram.Options, error) {
	dial, err := telegramProxyDial(cfg.ProxyURL)
	if err != nil {
		return opts, err
	}
	if dial != nil {
		opts.Resolver = dcs.Plain(dcs.PlainOptions{Dial: dial})
	}
	return opts, nil
}

func telegramProxyDial(raw string) (dcs.DialFunc, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	parsed, err := parseTelegramProxyURL(raw)
	if err != nil {
		return nil, err
	}
	dialer, err := proxy.FromURL(parsed, proxy.Direct)
	if err != nil {
		return nil, fmt.Errorf("telegram proxy_url: %w", err)
	}
	contextDialer, ok := dialer.(proxy.ContextDialer)
	if !ok {
		return nil, fmt.Errorf("telegram proxy_url %q does not support context dialing", parsed.Scheme)
	}
	return contextDialer.DialContext, nil
}

func parseTelegramProxyURL(raw string) (*url.URL, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("telegram proxy_url is empty")
	}
	if !strings.Contains(raw, "://") {
		raw = "socks5://" + raw
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid telegram proxy_url: %w", err)
	}
	switch strings.ToLower(parsed.Scheme) {
	case "socks5", "socks5h", "socks", "http", "https":
	default:
		return nil, fmt.Errorf("unsupported telegram proxy scheme %q (use socks5:// or http://)", parsed.Scheme)
	}
	if parsed.Host == "" {
		return nil, fmt.Errorf("telegram proxy_url is missing host")
	}
	return parsed, nil
}
