package _115_open

import (
	"context"
	"errors"
	"net/http"
)

// RequestClass identifies the 115 API operation being rate limited.
type RequestClass uint8

const (
	RequestFileList RequestClass = iota
	RequestDownloadURL
	RequestRESTAPI
	RequestDownload
)

// RequestWaitFunc is used by wrappers that need operation-specific request
// throttling while reusing the 115 Open implementation.
type RequestWaitFunc func(context.Context, RequestClass) error

// RefreshTokenHandler receives refreshed credentials after the Addition has
// already been updated. A wrapper can use it to persist the owning storage
// instead of the delegate's copied Storage value.
type RefreshTokenHandler func(accessToken, refreshToken string)

func (d *Open115) SetRequestWaitFunc(wait RequestWaitFunc) {
	d.requestWait = wait
}

func (d *Open115) SetRefreshTokenHandler(handler RefreshTokenHandler) {
	d.refreshTokenHandler = handler
}

// SetHTTPClient lets a wrapper preserve the SDK's request and refresh logic
// while routing a provider-specific endpoint through a custom transport.
func (d *Open115) SetHTTPClient(client *http.Client) {
	d.httpClient = client
}

// RefreshToken performs an explicit 115 Open token refresh and returns the
// provider-reported lifetime. Wrappers that implement proactive refresh can
// use the lifetime without reaching into the SDK client.
func (d *Open115) RefreshToken(ctx context.Context) (int64, error) {
	if d.client == nil {
		return 0, errors.New("115 Open driver is not initialized")
	}
	response, err := d.client.RefreshToken(ctx)
	if err != nil {
		return 0, err
	}
	return response.ExpiresIn, nil
}

// SetSkipUserInfoAtInit makes UserInfo optional during initialization. Some
// consumers only need the file APIs and CloudDrive2 does not use UserInfo as
// a mount prerequisite.
func (d *Open115) SetSkipUserInfoAtInit(skip bool) {
	d.skipUserInfoAtInit = skip
}

func (d *Open115) waitRequest(ctx context.Context, class RequestClass) error {
	if d.requestWait != nil {
		return d.requestWait(ctx, class)
	}
	return d.WaitLimit(ctx)
}

func (d *Open115) waitRequestAtOperationStart(ctx context.Context, _ RequestClass) error {
	if d.requestWait != nil {
		return nil
	}
	return d.WaitLimit(ctx)
}

func (d *Open115) waitRequestIfConfigured(ctx context.Context, class RequestClass) error {
	if d.requestWait == nil {
		return nil
	}
	return d.requestWait(ctx, class)
}

func (d *Open115) handleRefreshToken(accessToken, refreshToken string) {
	d.Addition.AccessToken = accessToken
	d.Addition.RefreshToken = refreshToken
	if d.refreshTokenHandler != nil {
		d.refreshTokenHandler(accessToken, refreshToken)
		return
	}
	persistRefreshedToken(d)
}
