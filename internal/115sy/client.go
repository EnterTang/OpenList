package _115sy

import (
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strings"
	"time"
)

type ClientOptions struct {
	HTTPClient     *http.Client
	Cookie         string
	UserAgent      string
	AppVersion     string
	LimitRate      float64
	PageCooldown   time.Duration
	WebBaseURL     string
	AndroidBaseURL string
}

type Client struct {
	httpClient *http.Client
	jar        http.CookieJar

	rawCookie  string
	userAgent  string
	appVersion string

	webBaseURL     string
	androidBaseURL string

	accountLimiter *accountLimiter
	pageLimiter    *pageLimiter
}

func NewClient(opts ClientOptions) (*Client, error) {
	var jar http.CookieJar

	defaultJar, err := cookiejar.New(nil)
	if err != nil {
		return nil, err
	}
	jar = defaultJar

	var httpClient http.Client
	if opts.HTTPClient != nil {
		httpClient = *opts.HTTPClient
		if httpClient.Jar != nil {
			jar = httpClient.Jar
		}
	}
	httpClient.Jar = jar

	pageCooldown := opts.PageCooldown
	if opts.LimitRate <= 0 {
		pageCooldown = 0
	}

	client := &Client{
		httpClient: &httpClient,
		jar:        jar,
		rawCookie:  strings.TrimSpace(opts.Cookie),
		userAgent:  opts.UserAgent,
		appVersion: strings.TrimSpace(opts.AppVersion),
		webBaseURL: defaultBaseURL(strings.TrimSpace(opts.WebBaseURL), DefaultWebBaseURL),
		androidBaseURL: defaultBaseURL(
			strings.TrimSpace(opts.AndroidBaseURL),
			DefaultAndroidBaseURL,
		),
		accountLimiter: newAccountLimiter(opts.LimitRate),
		pageLimiter:    newPageLimiter(pageCooldown),
	}
	if client.appVersion == "" {
		client.appVersion = DefaultAppVersion
	}

	if err := client.seedCookies(client.rawCookie); err != nil {
		return nil, err
	}

	return client, nil
}

func defaultBaseURL(value string, fallback string) string {
	if value == "" {
		return fallback
	}
	return strings.TrimRight(value, "/")
}

func (c *Client) baseURL(profile Profile) string {
	switch profile {
	case ProfileAndroid:
		return c.androidBaseURL
	default:
		return c.webBaseURL
	}
}

func (c *Client) seedCookies(raw string) error {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	cookies := parseCookieHeader(raw)
	if len(cookies) == 0 {
		return nil
	}
	for _, base := range []string{c.webBaseURL, c.androidBaseURL} {
		u, err := url.Parse(base)
		if err != nil {
			return err
		}
		c.jar.SetCookies(u, cookies)
	}
	return nil
}

func parseCookieHeader(raw string) []*http.Cookie {
	parts := strings.Split(raw, ";")
	cookies := make([]*http.Cookie, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		name, value, ok := strings.Cut(part, "=")
		if !ok {
			continue
		}
		name = strings.TrimSpace(name)
		value = strings.TrimSpace(value)
		if name == "" {
			continue
		}
		cookies = append(cookies, &http.Cookie{
			Name:  name,
			Value: value,
			Path:  "/",
		})
	}
	return cookies
}
