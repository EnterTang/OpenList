package _115sy

import (
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strings"
	"sync"
	"time"
)

type ClientOptions struct {
	HTTPClient      *http.Client
	Cookie          string
	UserAgent       string
	AppVersion      string
	LimitRate       float64
	PageCooldown    time.Duration
	WebBaseURL      string
	AndroidBaseURL  string
	UploadBaseURL   string
	QRCodeBaseURL   string
	PassportBaseURL string
}

type Client struct {
	httpClient *http.Client
	jar        http.CookieJar

	rawCookie  string
	userAgent  string
	appVersion string

	webBaseURL      string
	androidBaseURL  string
	uploadBaseURL   string
	qrCodeBaseURL   string
	passportBaseURL string

	authMu   sync.RWMutex
	uploadMu sync.Mutex
	upload   UploadAvailability

	accountLimiter *accountLimiter
	pageLimiter    *pageLimiter
	pathMu         sync.RWMutex
	pathCache      map[string]string
	pathItemCache  map[string]RemoteItem
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
		uploadBaseURL: defaultBaseURL(
			strings.TrimSpace(opts.UploadBaseURL),
			DefaultUploadBaseURL,
		),
		qrCodeBaseURL: defaultBaseURL(
			strings.TrimSpace(opts.QRCodeBaseURL),
			DefaultQRCodeBaseURL,
		),
		passportBaseURL: defaultBaseURL(
			strings.TrimSpace(opts.PassportBaseURL),
			DefaultPassportBaseURL,
		),
		accountLimiter: newAccountLimiter(opts.LimitRate),
		pageLimiter:    newPageLimiter(pageCooldown),
		pathCache:      make(map[string]string),
		pathItemCache:  make(map[string]RemoteItem),
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
	case ProfileChrome:
		return c.androidBaseURL
	case ProfileAndroid:
		return c.androidBaseURL
	case ProfileUpload:
		return c.uploadBaseURL
	case ProfileQRCode:
		return c.qrCodeBaseURL
	case ProfilePassport:
		return c.passportBaseURL
	default:
		return c.webBaseURL
	}
}

func (c *Client) currentRawCookie() string {
	c.authMu.RLock()
	defer c.authMu.RUnlock()
	return c.rawCookie
}

func (c *Client) replaceCookies(raw string) error {
	cookies := parseCookieHeader(raw)
	for _, base := range []string{c.webBaseURL, c.androidBaseURL, c.uploadBaseURL, c.qrCodeBaseURL, c.passportBaseURL} {
		u, err := url.Parse(base)
		if err != nil {
			return err
		}
		for _, old := range c.jar.Cookies(u) {
			c.jar.SetCookies(u, []*http.Cookie{{Name: old.Name, Value: "", Path: "/", MaxAge: -1}})
		}
		c.jar.SetCookies(u, cookies)
	}
	return nil
}

func (c *Client) seedCookies(raw string) error {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	cookies := parseCookieHeader(raw)
	if len(cookies) == 0 {
		return nil
	}
	for _, base := range []string{c.webBaseURL, c.androidBaseURL, c.uploadBaseURL, c.qrCodeBaseURL, c.passportBaseURL} {
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
