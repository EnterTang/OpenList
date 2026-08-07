package _115sy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/cookiejar"
	"strconv"
	"strings"
)

const (
	AuthStageCookie   = "cookie"
	AuthStageUserInfo = "user_info"
	AuthStageRoot     = "root"
	AuthStageQRStart  = "qr_start"
	AuthStageQRPoll   = "qr_poll"
	AuthStageQRImport = "qr_import"
)

type AuthError struct {
	Kind    ErrorKind
	Stage   string
	Profile Profile
	Errno   int
	Message string
	Err     error
}

func (e *AuthError) Error() string {
	profile := ""
	if e.Profile != "" {
		profile = fmt.Sprintf(" (%s)", e.Profile)
	}
	message := strings.TrimSpace(e.Message)
	if message != "" {
		return fmt.Sprintf("authentication failed at %s%s: %s", e.Stage, profile, sanitizeMessage(message))
	}
	if e.Errno != 0 {
		return fmt.Sprintf("authentication failed at %s%s with errno %d", e.Stage, profile, e.Errno)
	}
	if e.Err != nil {
		return fmt.Sprintf("authentication failed at %s%s: %s", e.Stage, profile, sanitizeErrorCause(e.Err))
	}
	return fmt.Sprintf("authentication failed at %s%s", e.Stage, profile)
}

func (e *AuthError) Unwrap() error {
	return e.Err
}

func ParseCookie(raw string) (Credential, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return Credential{}, errors.New("invalid cookie header: missing required authentication fields")
	}

	parts := strings.Split(trimmed, ";")
	values := make(map[string]string, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		name, value, ok := strings.Cut(part, "=")
		if !ok {
			return Credential{}, errors.New("invalid cookie header: invalid field format")
		}
		name = strings.TrimSpace(name)
		value = strings.TrimSpace(value)
		if !validCookieFieldName(name) {
			return Credential{}, errors.New("invalid cookie header: invalid field name")
		}
		if !validCookieFieldValue(value) {
			return Credential{}, fmt.Errorf("invalid cookie header: invalid value for %s", sanitizeCookieFieldName(name))
		}
		if value == "" {
			return Credential{}, fmt.Errorf("invalid cookie header: empty value for %s", sanitizeCookieFieldName(name))
		}
		if _, exists := values[name]; exists {
			return Credential{}, fmt.Errorf("invalid cookie header: duplicate %s field", sanitizeCookieFieldName(name))
		}
		values[name] = value
	}

	cred := Credential{
		UID:  values["UID"],
		CID:  values["CID"],
		SEID: values["SEID"],
		KID:  values["KID"],
	}
	if cred.UID == "" || cred.CID == "" || cred.SEID == "" {
		return Credential{}, errors.New("invalid cookie header: missing required authentication fields")
	}
	return cred, nil
}

func validCookieFieldName(name string) bool {
	if name == "" {
		return false
	}
	for _, r := range name {
		if r < 33 || r > 126 || strings.ContainsRune("()<>@,;:\\\"/[]?={}", r) {
			return false
		}
	}
	return true
}

func validCookieFieldValue(value string) bool {
	for _, r := range value {
		if r < 32 || r == 127 {
			return false
		}
	}
	return true
}

func sanitizeCookieFieldName(name string) string {
	switch strings.ToUpper(strings.TrimSpace(name)) {
	case "UID":
		return "UID"
	case "CID":
		return "CID"
	case "SEID":
		return "SEID"
	case "KID":
		return "KID"
	default:
		return "cookie"
	}
}

func (c *Client) Authenticate(ctx context.Context) (*AuthState, error) {
	state, err := c.authenticateWithCookie(ctx, c.currentRawCookie(), true)
	if err != nil {
		return nil, err
	}
	return state, nil
}

func (c *Client) authenticateWithCookie(ctx context.Context, raw string, seedMain bool) (*AuthState, error) {
	cred, err := ParseCookie(raw)
	if err != nil {
		return nil, newAuthError(AuthStageCookie, "", err)
	}

	if seedMain {
		if err := c.seedCookies(raw); err != nil {
			return nil, newAuthError(AuthStageCookie, "", err)
		}
	}

	user, err := c.probeUser(ctx)
	if err != nil {
		return nil, err
	}
	rootCID, capacity, err := c.probeRoot(ctx)
	if err != nil {
		return nil, err
	}

	if seedMain {
		c.authMu.Lock()
		c.rawCookie = strings.TrimSpace(raw)
		c.authMu.Unlock()
	}

	return &AuthState{
		Credential: cred,
		User:       user,
		UserID:     user.ID,
		RootCID:    rootCID,
		Capacity:   capacity,
	}, nil
}

func (c *Client) probeUser(ctx context.Context) (UserInfo, error) {
	var user UserInfo
	if err := c.doJSON(ctx, OperationUserInfo, ProfileWeb, http.MethodGet, EndpointUserInfo, nil, nil, &user); err != nil {
		return UserInfo{}, wrapAuthStage(AuthStageUserInfo, ProfileWeb, err)
	}
	return user, nil
}

func (c *Client) probeRoot(ctx context.Context) (string, Capacity, error) {
	query := map[string][]string{
		"cid":   {"0"},
		"limit": {"1"},
	}

	var payload rootProbePayload
	if err := c.doJSON(ctx, OperationFileList, ProfileAndroid, http.MethodGet, EndpointFileList, query, nil, &payload); err != nil {
		return "", Capacity{}, wrapAuthStage(AuthStageRoot, ProfileAndroid, err)
	}

	rootCID := strings.TrimSpace(firstNonEmpty(payload.CID, "0"))
	capacity := Capacity{
		Total:     int64(payload.SpaceTotal),
		Used:      int64(payload.SpaceUsed),
		Remaining: int64(payload.SpaceRemain),
	}
	if capacity.Remaining == 0 && capacity.Total >= capacity.Used {
		capacity.Remaining = capacity.Total - capacity.Used
	}
	return rootCID, capacity, nil
}

type rootProbePayload struct {
	CID         string        `json:"cid"`
	SpaceTotal  flexibleInt64 `json:"space_total"`
	SpaceUsed   flexibleInt64 `json:"space_used"`
	SpaceRemain flexibleInt64 `json:"space_remain"`
}

func (p *rootProbePayload) UnmarshalJSON(data []byte) error {
	raw := strings.TrimSpace(string(data))
	if strings.HasPrefix(raw, "[") {
		*p = rootProbePayload{CID: "0"}
		return nil
	}
	type rootProbePayloadAlias rootProbePayload
	var value rootProbePayloadAlias
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	*p = rootProbePayload(value)
	return nil
}

type flexibleInt64 int64

func (v *flexibleInt64) UnmarshalJSON(data []byte) error {
	raw := strings.TrimSpace(string(data))
	if raw == "" || raw == "null" {
		*v = 0
		return nil
	}
	raw = strings.Trim(raw, `"`)
	if raw == "" {
		*v = 0
		return nil
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return err
	}
	*v = flexibleInt64(n)
	return nil
}

func newAuthError(stage string, profile Profile, err error) *AuthError {
	authErr := &AuthError{
		Kind:    KindAuth,
		Stage:   stage,
		Profile: profile,
		Err:     err,
	}

	var existing *AuthError
	if errors.As(err, &existing) {
		authErr.Errno = existing.Errno
		authErr.Message = existing.Message
		if authErr.Profile == "" {
			authErr.Profile = existing.Profile
		}
	}

	var businessErr *BusinessError
	if errors.As(err, &businessErr) {
		authErr.Errno = businessErr.Errno
		authErr.Message = businessErr.Message
		if authErr.Profile == "" {
			authErr.Profile = businessErr.Profile
		}
	}

	if authErr.Message == "" && err != nil {
		authErr.Message = sanitizeErrorCause(err)
	}
	return authErr
}

func wrapAuthStage(stage string, profile Profile, err error) error {
	if err == nil {
		return nil
	}
	return newAuthError(stage, profile, err)
}

func (c *Client) cloneWithCookie(raw string) (*Client, error) {
	jar, err := cookiejar.New(nil)
	if err != nil {
		return nil, err
	}

	httpClient := *c.httpClient
	httpClient.Jar = jar

	clone := &Client{
		httpClient:      &httpClient,
		jar:             jar,
		rawCookie:       strings.TrimSpace(raw),
		userAgent:       c.userAgent,
		appVersion:      c.appVersion,
		webBaseURL:      c.webBaseURL,
		androidBaseURL:  c.androidBaseURL,
		qrCodeBaseURL:   c.qrCodeBaseURL,
		passportBaseURL: c.passportBaseURL,
		accountLimiter:  c.accountLimiter,
		pageLimiter:     c.pageLimiter,
	}
	if err := clone.seedCookies(clone.rawCookie); err != nil {
		return nil, err
	}
	return clone, nil
}
