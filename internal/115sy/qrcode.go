package _115sy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	defaultQRMaxPollCount    = 60
	defaultQRMaxPollDuration = 2 * time.Minute
	defaultQRPollInterval    = 2 * time.Second
)

type QRCodeSource string

const (
	QRCodeSourceWeb        QRCodeSource = "web"
	QRCodeSourceAndroid    QRCodeSource = "android"
	QRCodeSourceIOS        QRCodeSource = "ios"
	QRCodeSourceTV         QRCodeSource = "tv"
	QRCodeSourceAlipayMini QRCodeSource = "alipaymini"
	QRCodeSourceWechatMini QRCodeSource = "wechatmini"
	QRCodeSourceQAndroid   QRCodeSource = "qandroid"
)

type QRCodeSession struct {
	Source  QRCodeSource `json:"source"`
	Profile Profile      `json:"profile"`
	UID     string       `json:"uid"`
	Time    int64        `json:"time"`
	Sign    string       `json:"sign"`
	QRCode  string       `json:"qrcode"`
}

// QRCodePollResult mirrors qrcodeapi.115.com's integer status values:
// 0 waiting, 1 scanned, 2 confirmed, -1 expired, -2 canceled.
type QRCodePollResult struct {
	Status    int    `json:"status"`
	Message   string `json:"msg,omitempty"`
	Pending   bool   `json:"-"`
	Confirmed bool   `json:"-"`
	Expired   bool   `json:"-"`
	Canceled  bool   `json:"-"`
}

type QRCodeLoginOptions struct {
	Source          QRCodeSource
	PollInterval    time.Duration
	MaxPollCount    int
	MaxPollDuration time.Duration
}

type qrStartResponse struct {
	UID    string `json:"uid"`
	Time   int64  `json:"time"`
	Sign   string `json:"sign"`
	QRCode string `json:"qrcode"`
}

type qrPollResponse struct {
	Status  int    `json:"status"`
	Message string `json:"msg"`
}

type qrLoginResponse struct {
	Cookie     json.RawMessage `json:"cookie"`
	Credential json.RawMessage `json:"credential"`
}

func (c *Client) StartQRCode(ctx context.Context, source QRCodeSource) (*QRCodeSession, error) {
	source = normalizeQRCodeSource(source)
	var response qrStartResponse
	if err := c.doJSON(ctx, OperationQRCodeToken, ProfileQRCode, http.MethodGet, EndpointQRCodeToken, nil, nil, &response); err != nil {
		return nil, newAuthError(AuthStageQRStart, ProfileQRCode, err)
	}
	if strings.TrimSpace(response.UID) == "" || response.Time == 0 || strings.TrimSpace(response.Sign) == "" {
		return nil, newAuthError(AuthStageQRStart, ProfileQRCode, errors.New("QR code token response is incomplete"))
	}
	return &QRCodeSession{
		Source:  source,
		Profile: ProfileQRCode,
		UID:     strings.TrimSpace(response.UID),
		Time:    response.Time,
		Sign:    strings.TrimSpace(response.Sign),
		QRCode:  strings.TrimSpace(response.QRCode),
	}, nil
}

func (c *Client) PollQRCode(ctx context.Context, session *QRCodeSession) (*QRCodePollResult, error) {
	if session == nil {
		return nil, newAuthError(AuthStageQRPoll, ProfileQRCode, errors.New("missing QR code session"))
	}
	query := url.Values{
		"uid":  {strings.TrimSpace(session.UID)},
		"time": {strconv.FormatInt(session.Time, 10)},
		"sign": {strings.TrimSpace(session.Sign)},
		"_":    {strconv.FormatInt(time.Now().UnixNano(), 10)},
	}
	var response qrPollResponse
	if err := c.doJSON(ctx, OperationQRCodeStatus, ProfileQRCode, http.MethodGet, EndpointQRCodeStatus, query, nil, &response); err != nil {
		return nil, newAuthError(AuthStageQRPoll, ProfileQRCode, err)
	}
	return &QRCodePollResult{
		Status:    response.Status,
		Message:   sanitizeRequestText(response.Message),
		Pending:   response.Status == 0 || response.Status == 1,
		Confirmed: response.Status == 2,
		Expired:   response.Status == -1,
		Canceled:  response.Status == -2,
	}, nil
}

func (c *Client) LoginByQRCode(ctx context.Context, opts QRCodeLoginOptions) (*AuthState, error) {
	source := normalizeQRCodeSource(opts.Source)
	session, err := c.StartQRCode(ctx, source)
	if err != nil {
		return nil, err
	}

	maxPollCount := opts.MaxPollCount
	if maxPollCount <= 0 {
		maxPollCount = defaultQRMaxPollCount
	}
	maxPollDuration := opts.MaxPollDuration
	if maxPollDuration <= 0 {
		maxPollDuration = defaultQRMaxPollDuration
	}
	pollInterval := opts.PollInterval
	if pollInterval <= 0 {
		pollInterval = defaultQRPollInterval
	}

	pollCtx, cancel := context.WithTimeout(ctx, maxPollDuration)
	defer cancel()
	for attempt := 0; attempt < maxPollCount; attempt++ {
		result, err := c.PollQRCode(pollCtx, session)
		if err != nil {
			return nil, err
		}
		switch {
		case result.Confirmed:
			rawCookie, err := c.loginQRCode(pollCtx, source, session.UID)
			if err != nil {
				return nil, err
			}
			return c.importQRCodeCredential(pollCtx, rawCookie, ProfilePassport)
		case result.Expired:
			return nil, newAuthError(AuthStageQRPoll, ProfileQRCode, errors.New("QR code expired"))
		case result.Canceled:
			return nil, newAuthError(AuthStageQRPoll, ProfileQRCode, errors.New("QR code login canceled"))
		case !result.Pending:
			return nil, newAuthError(AuthStageQRPoll, ProfileQRCode, fmt.Errorf("QR code login ended with status %d", result.Status))
		}

		if attempt == maxPollCount-1 {
			break
		}
		timer := time.NewTimer(pollInterval)
		select {
		case <-pollCtx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil, newAuthError(AuthStageQRPoll, ProfileQRCode, pollCtx.Err())
		case <-timer.C:
		}
	}
	if err := pollCtx.Err(); err != nil {
		return nil, newAuthError(AuthStageQRPoll, ProfileQRCode, err)
	}
	return nil, newAuthError(AuthStageQRPoll, ProfileQRCode, errors.New("QR code confirmation timed out"))
}

func (c *Client) loginQRCode(ctx context.Context, source QRCodeSource, uid string) (string, error) {
	var response qrLoginResponse
	endpoint := fmt.Sprintf(EndpointQRCodeLogin, url.PathEscape(string(source)))
	form := url.Values{"account": {strings.TrimSpace(uid)}, "app": {string(source)}}
	if err := c.doForm(ctx, OperationQRCodeLogin, ProfilePassport, http.MethodPost, endpoint, nil, form, &response); err != nil {
		return "", newAuthError(AuthStageQRImport, ProfilePassport, err)
	}
	for _, raw := range []json.RawMessage{response.Cookie, response.Credential} {
		if cookie := decodeQRCodeCredential(raw); cookie != "" {
			return cookie, nil
		}
	}
	return "", newAuthError(AuthStageQRImport, ProfilePassport, errors.New("QR code login returned no credential"))
}

func decodeQRCodeCredential(raw json.RawMessage) string {
	if len(raw) == 0 || string(raw) == "null" {
		return ""
	}
	var cookie string
	if json.Unmarshal(raw, &cookie) == nil {
		return strings.TrimSpace(cookie)
	}
	var credential Credential
	if json.Unmarshal(raw, &credential) != nil || credential.UID == "" || credential.CID == "" || credential.SEID == "" {
		return ""
	}
	return formatCredential(credential)
}

func (c *Client) importQRCodeCredential(ctx context.Context, rawCookie string, profile Profile) (*AuthState, error) {
	clone, err := c.cloneWithCookie(rawCookie)
	if err != nil {
		return nil, newAuthError(AuthStageQRImport, profile, err)
	}
	state, err := clone.authenticateWithCookie(ctx, rawCookie, false)
	if err != nil {
		return nil, newAuthError(AuthStageQRImport, profile, err)
	}

	c.authMu.Lock()
	defer c.authMu.Unlock()
	if err := c.replaceCookies(rawCookie); err != nil {
		return nil, newAuthError(AuthStageQRImport, profile, err)
	}
	c.rawCookie = strings.TrimSpace(rawCookie)
	return state, nil
}

func normalizeQRCodeSource(source QRCodeSource) QRCodeSource {
	if strings.TrimSpace(string(source)) == "" {
		return QRCodeSourceAndroid
	}
	return QRCodeSource(strings.TrimSpace(string(source)))
}

func formatCredential(cred Credential) string {
	parts := []string{
		fmt.Sprintf("UID=%s", strings.TrimSpace(cred.UID)),
		fmt.Sprintf("CID=%s", strings.TrimSpace(cred.CID)),
		fmt.Sprintf("SEID=%s", strings.TrimSpace(cred.SEID)),
	}
	if kid := strings.TrimSpace(cred.KID); kid != "" {
		parts = append(parts, fmt.Sprintf("KID=%s", kid))
	}
	return strings.Join(parts, "; ")
}
