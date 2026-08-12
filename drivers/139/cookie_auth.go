package _139

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/drivers/base"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
)

const (
	authModeETF      = "etf"
	authModeOpenList = "openlist"
)

var mobileAuthTokenRefreshEndpoint = "https://aas.caiyun.feixin.10086.cn:443/tellin/authTokenRefresh.do"
var save139DriverStorage = func(d *Yun139) {
	op.MustSaveDriverStorage(d)
}

func (d *Yun139) useCookieAuthMode() bool {
	switch strings.ToLower(strings.TrimSpace(d.AuthMode)) {
	case authModeETF:
		return true
	case authModeOpenList:
		return false
	}
	return strings.TrimSpace(d.CookieHeader) != ""
}

func (d *Yun139) refreshCookieAuth(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	cookieHeader := strings.TrimSpace(d.CookieHeader)
	if cookieHeader == "" {
		return fmt.Errorf("cookie_header is empty")
	}

	authorization := firstNonEmpty139(cookieValue139(cookieHeader, "authorization"), d.Authorization)
	storedAuthorization, account, token, err := normalize139Authorization(authorization)
	if err != nil && authorization != "" {
		return err
	}
	cookieAccount := accountFrom139Cookie(cookieHeader)
	cookieToken := cookieValue139(cookieHeader, "auth_token")
	account = firstNonEmpty139(cookieAccount, account)
	token = firstNonEmpty139(cookieToken, token)
	if account == "" {
		return fmt.Errorf("account is missing in cookie_header")
	}
	if token == "" {
		return fmt.Errorf("auth_token is missing in cookie_header")
	}
	if storedAuthorization == "" {
		storedAuthorization = buildStored139Authorization(account, token)
	}

	if userDomainID := cookieValue139(cookieHeader, "ud_id"); userDomainID != "" && d.UserDomainID == "" {
		d.UserDomainID = userDomainID
	}
	d.Account = account
	d.Authorization = storedAuthorization

	expiresAt, ok := parse139AuthTokenExpiry(token)
	if !ok {
		return fmt.Errorf("auth_token expiry is invalid")
	}
	if !time.Now().Before(expiresAt) {
		return fmt.Errorf("auth_token has expired")
	}
	if time.Until(expiresAt) > 15*24*time.Hour {
		save139DriverStorage(d)
		return nil
	}

	newToken, err := refresh139AuthToken(ctx, account, token)
	if err != nil {
		return err
	}
	d.Authorization = buildStored139Authorization(account, newToken)
	d.CookieHeader = update139CookieHeader(cookieHeader, newToken, "Basic "+d.Authorization)
	save139DriverStorage(d)
	return nil
}

func refresh139AuthToken(ctx context.Context, account, token string) (string, error) {
	var resp RefreshTokenResp
	reqBody := "<root><token>" + token + "</token><account>" + account + "</account><clienttype>656</clienttype></root>"
	_, err := base.RestyClient.R().
		SetContext(ctx).
		ForceContentType("application/xml").
		SetHeader("Accept", "application/xml, text/xml, */*").
		SetBody(reqBody).
		SetResult(&resp).
		Post(mobileAuthTokenRefreshEndpoint)
	if err != nil {
		return "", err
	}
	if resp.Return != "0" || strings.TrimSpace(resp.Token) == "" {
		return "", fmt.Errorf("auth token refresh failed: %s %s", resp.Return, resp.Desc)
	}
	return strings.TrimSpace(resp.Token), nil
}

func normalize139Authorization(authorization string) (storedAuthorization, account, token string, err error) {
	authorization = strings.TrimSpace(authorization)
	if authorization == "" {
		return "", "", "", nil
	}
	if decoded, decodeErr := url.PathUnescape(authorization); decodeErr == nil {
		authorization = strings.TrimSpace(decoded)
	}
	raw := authorization
	if strings.HasPrefix(strings.ToLower(raw), "basic ") {
		raw = strings.TrimSpace(raw[6:])
	}
	decoded := decodeBase64Text139(raw)
	if decoded == "" {
		return "", "", "", fmt.Errorf("authorization decode failed")
	}
	parts := strings.Split(decoded, ":")
	if len(parts) < 3 {
		return "", "", "", fmt.Errorf("authorization is invalid")
	}
	account = strings.TrimSpace(parts[1])
	token = strings.TrimSpace(strings.Join(parts[2:], ":"))
	if !valid139Account(account) || token == "" {
		return "", "", "", fmt.Errorf("authorization is invalid")
	}
	return raw, account, token, nil
}

func buildStored139Authorization(account, token string) string {
	return base64.StdEncoding.EncodeToString([]byte("pc:" + strings.TrimSpace(account) + ":" + strings.TrimSpace(token)))
}

func parse139AuthTokenExpiry(token string) (time.Time, bool) {
	parts := strings.Split(strings.TrimSpace(token), "|")
	if len(parts) < 4 {
		return time.Time{}, false
	}
	expiresMillis, err := strconv.ParseInt(strings.TrimSpace(parts[3]), 10, 64)
	if err != nil || expiresMillis <= 0 {
		return time.Time{}, false
	}
	return time.UnixMilli(expiresMillis), true
}

func update139CookieHeader(cookieHeader, token, authorization string) string {
	parts := split139CookieParts(cookieHeader)
	seenToken := false
	seenAuth := false
	for i, part := range parts {
		key, _, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			continue
		}
		switch strings.ToLower(strings.TrimSpace(key)) {
		case "auth_token":
			parts[i] = "auth_token=" + token
			seenToken = true
		case "authorization":
			parts[i] = "authorization=" + authorization
			seenAuth = true
		}
	}
	if !seenToken {
		parts = append(parts, "auth_token="+token)
	}
	if !seenAuth {
		parts = append(parts, "authorization="+authorization)
	}
	return strings.Join(parts, "; ")
}

func cookieValue139(cookieHeader, name string) string {
	name = strings.ToLower(strings.TrimSpace(name))
	for _, part := range split139CookieParts(cookieHeader) {
		key, value, ok := strings.Cut(part, "=")
		if !ok || strings.ToLower(strings.TrimSpace(key)) != name {
			continue
		}
		value = strings.Trim(strings.TrimSpace(value), `"`)
		if decoded, err := url.PathUnescape(value); err == nil {
			value = decoded
		}
		return strings.TrimSpace(value)
	}
	return ""
}

func split139CookieParts(cookieHeader string) []string {
	rawParts := strings.Split(cookieHeader, ";")
	parts := make([]string, 0, len(rawParts))
	for _, part := range rawParts {
		part = strings.TrimSpace(part)
		if part != "" {
			parts = append(parts, part)
		}
	}
	return parts
}

func accountFrom139Cookie(cookieHeader string) string {
	for _, name := range []string{"ORCHES-I-ACCOUNT-ENCRYPT", "ORCHES-I-ACCOUNT-SIMPLIFY"} {
		value := cookieValue139(cookieHeader, name)
		if value == "" {
			continue
		}
		if decoded := decodeBase64Text139(value); valid139Account(decoded) {
			return decoded
		}
		if valid139Account(value) {
			return value
		}
	}
	return ""
}

func decodeBase64Text139(value string) string {
	value = strings.TrimSpace(strings.Trim(value, `"`))
	if value == "" {
		return ""
	}
	if decoded, err := url.PathUnescape(value); err == nil {
		value = strings.TrimSpace(decoded)
	}
	padded := value + strings.Repeat("=", (4-len(value)%4)%4)
	for _, candidate := range []string{value, padded} {
		for _, encoding := range []*base64.Encoding{
			base64.StdEncoding,
			base64.URLEncoding,
			base64.RawStdEncoding,
			base64.RawURLEncoding,
		} {
			data, err := encoding.DecodeString(candidate)
			if err == nil {
				return strings.TrimSpace(string(data))
			}
		}
	}
	return ""
}

func valid139Account(value string) bool {
	value = strings.TrimSpace(value)
	return len(value) > 5 && !strings.Contains(value, "{") && !strings.Contains(value, "}") && !strings.Contains(value, "*")
}

func firstNonEmpty139(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}
