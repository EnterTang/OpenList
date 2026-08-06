package _115sy

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

const (
	endpointQRCodeStart = "/qrcode/start"
	endpointQRCodePoll  = "/qrcode/poll"

	qrFieldSource = "source"

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
	Token   string       `json:"token,omitempty"`
	QRCode  string       `json:"qrcode"`
}

type QRCodePollResult struct {
	Status     string      `json:"status"`
	Pending    bool        `json:"pending"`
	Confirmed  bool        `json:"confirmed"`
	Cookie     string      `json:"cookie,omitempty"`
	Credential *Credential `json:"credential,omitempty"`
}

type QRCodeLoginOptions struct {
	Source          QRCodeSource
	PollInterval    time.Duration
	MaxPollCount    int
	MaxPollDuration time.Duration
}

type qrStartPayload struct {
	Source QRCodeSource `json:"source"`
}

type qrStartResponse struct {
	UID    string `json:"uid"`
	Time   int64  `json:"time"`
	Sign   string `json:"sign"`
	Token  string `json:"token,omitempty"`
	QRCode string `json:"qrcode"`
}

type qrPollPayload struct {
	Source QRCodeSource `json:"source"`
	UID    string       `json:"uid"`
	Time   int64        `json:"time"`
	Sign   string       `json:"sign"`
	Token  string       `json:"token,omitempty"`
}

type qrPollResponse struct {
	Status     string      `json:"status"`
	Cookie     string      `json:"cookie,omitempty"`
	Credential *Credential `json:"credential,omitempty"`
}

func (c *Client) StartQRCode(ctx context.Context, source QRCodeSource) (*QRCodeSession, error) {
	profile := profileForQRCodeSource(source)
	payload := qrStartPayload{Source: normalizeQRCodeSource(source)}

	var response qrStartResponse
	if err := c.doJSON(ctx, OperationUserInfo, profile, http.MethodPost, endpointQRCodeStart, nil, payload, &response); err != nil {
		return nil, newAuthError(AuthStageQRStart, profile, err)
	}

	return &QRCodeSession{
		Source:  payload.Source,
		Profile: profile,
		UID:     strings.TrimSpace(response.UID),
		Time:    response.Time,
		Sign:    strings.TrimSpace(response.Sign),
		Token:   strings.TrimSpace(response.Token),
		QRCode:  strings.TrimSpace(response.QRCode),
	}, nil
}

func (c *Client) PollQRCode(ctx context.Context, session *QRCodeSession) (*QRCodePollResult, error) {
	if session == nil {
		return nil, newAuthError(AuthStageQRPoll, "", errors.New("missing QR code session"))
	}

	profile := session.Profile
	if profile == "" {
		profile = profileForQRCodeSource(session.Source)
	}
	payload := qrPollPayload{
		Source: normalizeQRCodeSource(session.Source),
		UID:    strings.TrimSpace(session.UID),
		Time:   session.Time,
		Sign:   strings.TrimSpace(session.Sign),
		Token:  strings.TrimSpace(session.Token),
	}

	var response qrPollResponse
	if err := c.doJSON(ctx, OperationUserInfo, profile, http.MethodPost, endpointQRCodePoll, nil, payload, &response); err != nil {
		return nil, newAuthError(AuthStageQRPoll, profile, err)
	}

	status := strings.ToLower(strings.TrimSpace(response.Status))
	result := &QRCodePollResult{
		Status:     status,
		Pending:    status == "" || status == "pending" || status == "scanned",
		Confirmed:  status == "confirmed" || status == "success",
		Cookie:     strings.TrimSpace(response.Cookie),
		Credential: response.Credential,
	}
	return result, nil
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
		if result.Confirmed {
			rawCookie := strings.TrimSpace(result.Cookie)
			if rawCookie == "" && result.Credential != nil {
				rawCookie = formatCredential(*result.Credential)
			}
			if rawCookie == "" {
				return nil, newAuthError(AuthStageQRImport, session.Profile, errors.New("QR code login did not return credential cookie"))
			}
			return c.importQRCodeCredential(pollCtx, rawCookie, session.Profile)
		}
		if !result.Pending {
			return nil, newAuthError(AuthStageQRPoll, session.Profile, fmt.Errorf("QR code login ended with status %q", result.Status))
		}

		if attempt == maxPollCount-1 {
			break
		}

		timer := time.NewTimer(pollInterval)
		select {
		case <-pollCtx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return nil, newAuthError(AuthStageQRPoll, session.Profile, pollCtx.Err())
		case <-timer.C:
		}
	}

	if err := pollCtx.Err(); err != nil {
		return nil, newAuthError(AuthStageQRPoll, session.Profile, err)
	}
	return nil, newAuthError(AuthStageQRPoll, session.Profile, errors.New("QR code confirmation timed out"))
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

	c.jar = clone.jar
	c.httpClient.Jar = clone.jar
	c.rawCookie = strings.TrimSpace(rawCookie)
	return state, nil
}

func normalizeQRCodeSource(source QRCodeSource) QRCodeSource {
	if strings.TrimSpace(string(source)) == "" {
		return QRCodeSourceAndroid
	}
	return QRCodeSource(strings.TrimSpace(string(source)))
}

func profileForQRCodeSource(source QRCodeSource) Profile {
	if normalizeQRCodeSource(source) == QRCodeSourceWeb {
		return ProfileWeb
	}
	return ProfileAndroid
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
