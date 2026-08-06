package _115_cd2

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	sdk "github.com/OpenListTeam/115-sdk-go"
	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/server/common"
)

const (
	cloudDrive2QRCodeClientID = "100197353"
	cloudDrive2OAuthClientID  = "100195313"
	cloudDrive2OAuthEndpoint  = "https://passportapi.115.com/open/authorize"
	cloudDrive2OAuthRedirect  = "https://redirect115.zhenyunpan.com"
	cloudDrive2RefreshURL     = sdk.ApiRefreshToken
	cloudDrive2Provider       = "cloud115_open"

	cd2AuthModeOAuth  = "oauth"
	cd2AuthModeQRCode = "qrcode"
)

type cd2AuthConfig struct {
	qrClientID       string
	oauthClientID    string
	oauthEndpoint    string
	oauthRedirectURI string
	refreshEndpoint  string
}

func defaultCD2AuthConfig() cd2AuthConfig {
	return cd2AuthConfig{
		qrClientID:       cloudDrive2QRCodeClientID,
		oauthClientID:    cloudDrive2OAuthClientID,
		oauthEndpoint:    cloudDrive2OAuthEndpoint,
		oauthRedirectURI: cloudDrive2OAuthRedirect,
		refreshEndpoint:  cloudDrive2RefreshURL,
	}
}

func skipUserInfoAtInitForCD2() bool {
	return true
}

func (d *CD2) effectiveClientID() string {
	if clientID := strings.TrimSpace(d.Addition.AppID); clientID != "" {
		return clientID
	}
	if clientID := strings.TrimSpace(os.Getenv("CLOUD115_APP_ID")); clientID != "" {
		return clientID
	}
	return defaultCD2AuthConfig().qrClientID
}

func (d *CD2) authMode() string {
	mode := strings.ToLower(strings.TrimSpace(d.Addition.AuthMode))
	if mode == cd2AuthModeQRCode {
		return cd2AuthModeQRCode
	}
	// Preserve a pending QR login created by an older build whose Addition did
	// not have AuthMode yet. Fresh configurations use the CD2 OAuth flow.
	if mode == "" && d.hasPendingDeviceAuth() {
		return cd2AuthModeQRCode
	}
	return cd2AuthModeOAuth
}

// ensureAuthentication implements CloudDrive2's web OAuth flow by default.
// The QR device-code lifecycle remains available through AuthMode=qrcode for
// compatibility with existing configurations.
func (d *CD2) ensureAuthentication(ctx context.Context) error {
	if strings.TrimSpace(d.Addition.AccessToken) != "" && strings.TrimSpace(d.Addition.RefreshToken) != "" {
		return nil
	}
	if d.authMode() == cd2AuthModeOAuth {
		return d.ensureOAuthAuthentication(ctx)
	}

	client := sdk.New()
	if d.authHTTPClient != nil {
		client.SetHttpClient(d.authHTTPClient)
	}

	if !d.hasPendingDeviceAuth() {
		return d.beginDeviceAuth(ctx, client)
	}

	status, err := client.QrCodeStatus(ctx, d.Addition.QRCodeUID, strconv.FormatInt(d.Addition.QRCodeTime, 10), d.Addition.QRCodeSign)
	if err != nil {
		return fmt.Errorf("115 CD2 query QR authorization status: %w", err)
	}
	if status.Status == 2 {
		tokens, err := client.CodeToToken(ctx, d.Addition.QRCodeUID, d.Addition.CodeVerifier)
		if err != nil {
			return fmt.Errorf("115 CD2 exchange QR authorization token: %w", err)
		}
		if strings.TrimSpace(tokens.AccessToken) == "" || strings.TrimSpace(tokens.RefreshToken) == "" {
			return fmt.Errorf("115 CD2 QR authorization returned an incomplete token pair")
		}
		d.Addition.AccessToken = tokens.AccessToken
		d.Addition.RefreshToken = tokens.RefreshToken
		d.Addition.AccessTokenExpiresAt = accessTokenExpiryUnix(time.Now(), tokens.ExpiresIn)
		d.clearDeviceAuth()
		d.persistAuthenticationState()
		return nil
	}
	if status.Status == -1 || status.Status == -2 {
		d.clearDeviceAuth()
		d.persistAuthenticationState()
		return fmt.Errorf("115 CD2 QR authorization ended with status %d; retry initialization to create a new QR code", status.Status)
	}

	message := strings.TrimSpace(status.Msg)
	if message == "" {
		message = "waiting for QR scan"
	}
	return fmt.Errorf("115 CD2 %s (status=%d, qrcode=%s)", message, status.Status, d.Addition.QRCodeURL)
}

func (d *CD2) ensureOAuthAuthentication(ctx context.Context) error {
	if d.hasPendingOAuth() {
		return fmt.Errorf("115 CD2 OAuth authorization required; open %s", d.Addition.OAuthURL)
	}

	state, err := newCodeVerifier()
	if err != nil {
		return fmt.Errorf("115 CD2 create OAuth state: %w", err)
	}
	callbackURL, err := d.oauthCallbackURL(ctx, state)
	if err != nil {
		return err
	}
	authorizationURL, err := buildCD2OAuthURL(callbackURL)
	if err != nil {
		return fmt.Errorf("115 CD2 build OAuth authorization URL: %w", err)
	}
	d.Addition.OAuthState = state
	d.Addition.OAuthURL = authorizationURL
	d.persistAuthenticationState()
	return fmt.Errorf("115 CD2 OAuth authorization required; open %s", authorizationURL)
}

func (d *CD2) oauthCallbackURL(ctx context.Context, state string) (string, error) {
	baseURL := strings.TrimSuffix(strings.TrimSpace(common.GetApiUrl(ctx)), "/")
	if baseURL == "" && conf.Conf != nil {
		baseURL = strings.TrimSuffix(strings.TrimSpace(common.GetApiUrlFromRequest(nil)), "/")
	}
	if baseURL == "" {
		return "", fmt.Errorf("115 CD2 OAuth requires a public OpenList URL; configure site_url or start authorization from the management API")
	}
	callback, err := url.Parse(baseURL + OAuthCallbackPath)
	if err != nil {
		return "", fmt.Errorf("115 CD2 build OAuth callback URL: %w", err)
	}
	query := callback.Query()
	query.Set("storage_id", strconv.FormatUint(uint64(d.Storage.ID), 10))
	query.Set("state", state)
	callback.RawQuery = query.Encode()
	return callback.String(), nil
}

func (d *CD2) hasPendingOAuth() bool {
	return strings.TrimSpace(d.Addition.OAuthState) != "" && strings.TrimSpace(d.Addition.OAuthURL) != ""
}

func (d *CD2) beginDeviceAuth(ctx context.Context, client *sdk.Client) error {
	verifier, err := newCodeVerifier()
	if err != nil {
		return fmt.Errorf("115 CD2 create QR authorization verifier: %w", err)
	}
	deviceCode, err := client.AuthDeviceCode(ctx, d.effectiveClientID(), verifier)
	if err != nil {
		return fmt.Errorf("115 CD2 create QR authorization: %w", err)
	}
	if deviceCode.UID == "" || deviceCode.Time == 0 || deviceCode.Sign == "" || deviceCode.QrCode == "" {
		return fmt.Errorf("115 CD2 create QR authorization returned incomplete device data")
	}
	d.Addition.QRCodeUID = deviceCode.UID
	d.Addition.QRCodeTime = deviceCode.Time
	d.Addition.QRCodeSign = deviceCode.Sign
	d.Addition.QRCodeURL = deviceCode.QrCode
	d.Addition.CodeVerifier = verifier
	d.persistAuthenticationState()
	return fmt.Errorf("115 CD2 QR authorization required; scan %s and retry initialization", deviceCode.QrCode)
}

func newCodeVerifier() (string, error) {
	value := make([]byte, 32)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return hex.EncodeToString(value), nil
}

func (d *CD2) hasPendingDeviceAuth() bool {
	return d.Addition.QRCodeUID != "" &&
		d.Addition.QRCodeTime != 0 &&
		d.Addition.QRCodeSign != "" &&
		d.Addition.CodeVerifier != ""
}

func (d *CD2) clearDeviceAuth() {
	d.Addition.QRCodeUID = ""
	d.Addition.QRCodeTime = 0
	d.Addition.QRCodeSign = ""
	d.Addition.QRCodeURL = ""
	d.Addition.CodeVerifier = ""
}

func (d *CD2) persistAuthenticationState() {
	// A zero ID is used by unit tests and by a driver before it is attached to
	// a persisted storage. In both cases the in-memory state is sufficient.
	if d.Storage.ID != 0 {
		op.MustSaveDriverStorage(d)
	}
}

func newCD2HTTPClient(endpoint string, base http.RoundTripper) *http.Client {
	if base == nil {
		base = http.DefaultTransport
	}
	return &http.Client{Transport: &cd2RefreshTransport{
		base:     base,
		endpoint: endpoint,
	}}
}

type cd2RefreshTransport struct {
	base     http.RoundTripper
	endpoint string
}

func (t *cd2RefreshTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.String() != sdk.ApiRefreshToken {
		return t.base.RoundTrip(req)
	}
	// The official 115 endpoint expects the SDK's original form-encoded
	// request. Only a non-official endpoint needs the legacy CD2 relay
	// translation below.
	if t.endpoint == sdk.ApiRefreshToken {
		return t.base.RoundTrip(req)
	}

	body, err := readRequestBody(req)
	if err != nil {
		return nil, err
	}
	refreshToken := parseRefreshToken(body)
	if refreshToken == "" {
		return nil, fmt.Errorf("115 CD2 refresh request has no refresh token")
	}
	if strings.TrimSpace(t.endpoint) == "" {
		return nil, fmt.Errorf("115 CD2 refresh relay endpoint is empty")
	}

	relayBody, err := json.Marshal(map[string]string{
		"refresh_token": refreshToken,
		"provider":      cloudDrive2Provider,
	})
	if err != nil {
		return nil, err
	}
	relayReq, err := http.NewRequestWithContext(req.Context(), http.MethodPost, t.endpoint, bytes.NewReader(relayBody))
	if err != nil {
		return nil, err
	}
	relayReq.Header.Set("Content-Type", "application/json")
	relayReq.Header.Set("Accept", "application/json")
	if userAgent := req.Header.Get("User-Agent"); userAgent != "" {
		relayReq.Header.Set("User-Agent", userAgent)
	}

	relayResp, err := t.base.RoundTrip(relayReq)
	if err != nil {
		return nil, fmt.Errorf("115 CD2 refresh relay request: %w", err)
	}
	if relayResp.StatusCode < http.StatusOK || relayResp.StatusCode >= http.StatusMultipleChoices {
		io.Copy(io.Discard, relayResp.Body)
		relayResp.Body.Close()
		return nil, fmt.Errorf("115 CD2 refresh relay returned HTTP %d", relayResp.StatusCode)
	}

	responseBody, err := io.ReadAll(relayResp.Body)
	relayResp.Body.Close()
	if err != nil {
		return nil, err
	}
	if relayResp.Header == nil {
		relayResp.Header = make(http.Header)
	}
	normalized, err := normalizeCD2RefreshResponse(responseBody)
	if err != nil {
		return nil, fmt.Errorf("115 CD2 refresh relay response: %w", err)
	}
	relayResp.Body = io.NopCloser(bytes.NewReader(normalized))
	relayResp.ContentLength = int64(len(normalized))
	relayResp.Header.Set("Content-Length", strconv.Itoa(len(normalized)))
	relayResp.Header.Set("Content-Type", "application/json")
	return relayResp, nil
}

func readRequestBody(req *http.Request) ([]byte, error) {
	if req.Body == nil {
		return nil, nil
	}
	body, err := io.ReadAll(req.Body)
	req.Body.Close()
	return body, err
}

func requestWithBody(req *http.Request, body []byte) *http.Request {
	clone := req.Clone(req.Context())
	clone.Body = io.NopCloser(bytes.NewReader(body))
	clone.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(body)), nil
	}
	clone.ContentLength = int64(len(body))
	return clone
}

func parseRefreshToken(body []byte) string {
	values, err := url.ParseQuery(string(body))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(values.Get("refresh_token"))
}

func normalizeCD2RefreshResponse(body []byte) ([]byte, error) {
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, err
	}
	if _, ok := envelope["access_token"]; ok {
		var tokenResponse struct {
			AccessToken  string `json:"access_token"`
			RefreshToken string `json:"refresh_token"`
			ExpiresIn    int64  `json:"expires_in"`
		}
		if err := json.Unmarshal(body, &tokenResponse); err != nil {
			return nil, err
		}
		if tokenResponse.AccessToken == "" || tokenResponse.RefreshToken == "" {
			return nil, fmt.Errorf("token fields are incomplete")
		}
		return json.Marshal(map[string]any{
			"state": 1,
			"code":  0,
			"data":  tokenResponse,
		})
	}
	if _, ok := envelope["data"]; !ok {
		return nil, fmt.Errorf("missing data")
	}
	if _, ok := envelope["state"]; !ok {
		envelope["state"] = json.RawMessage("1")
	}
	if state, ok := envelope["state"]; ok {
		var boolState bool
		if json.Unmarshal(state, &boolState) == nil {
			if boolState {
				envelope["state"] = json.RawMessage("1")
			} else {
				envelope["state"] = json.RawMessage("0")
			}
		}
	}
	if _, ok := envelope["code"]; !ok {
		envelope["code"] = json.RawMessage("0")
	}
	return json.Marshal(envelope)
}
