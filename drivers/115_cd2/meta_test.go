package _115_cd2

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	sdk "github.com/OpenListTeam/115-sdk-go"
	open115 "github.com/OpenListTeam/OpenList/v4/drivers/115_open"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestCD2AuthenticationDefaultsMatchCloudDrive2(t *testing.T) {
	config := defaultCD2AuthConfig()

	if config.oauthClientID != "100195313" {
		t.Fatalf("CD2 OAuth client ID = %q, want the client ID embedded by CloudDrive2 web OAuth", config.oauthClientID)
	}
	if config.oauthRedirectURI != "https://redirect115.zhenyunpan.com" {
		t.Fatalf("CD2 OAuth redirect URI = %q, want the CloudDrive2 redirect service", config.oauthRedirectURI)
	}
	if config.oauthEndpoint != "https://passportapi.115.com/open/authorize" {
		t.Fatalf("CD2 OAuth endpoint = %q, want the 115 OAuth endpoint", config.oauthEndpoint)
	}
	if config.qrClientID != "100197353" {
		t.Fatalf("CD2 QR client ID = %q, want the CloudDrive2 device-code client ID", config.qrClientID)
	}
	if config.refreshEndpoint != "https://token-server.zhenyunpan.com/refresh_access_token" {
		t.Fatalf("CD2 refresh endpoint = %q, want the CloudDrive2 relay endpoint", config.refreshEndpoint)
	}
}

func TestCD2UsesCloudDrive2AppIDEnvironmentOverride(t *testing.T) {
	t.Setenv("CLOUD115_APP_ID", "env-client")
	driver := &CD2{}
	if got := driver.effectiveClientID(); got != "env-client" {
		t.Fatalf("effectiveClientID() = %q, want env-client", got)
	}
	driver.AppID = "configured-client"
	if got := driver.effectiveClientID(); got != "configured-client" {
		t.Fatalf("configured AppID lost to environment: %q", got)
	}
}

func TestCD2UsesDataAPIsWithoutStartupUserInfo(t *testing.T) {
	if !skipUserInfoAtInitForCD2() {
		t.Fatal("CD2 must not make /open/user/info a prerequisite for mounting")
	}
}

func TestCD2InitAndListDoNotRequireUserInfo(t *testing.T) {
	var userInfoCalled bool
	addition := Addition{
		AccessToken:  "access",
		RefreshToken: "refresh",
		PageSize:     10,
	}
	addition.RootFolderID = "0"
	driver := &CD2{Addition: addition}
	driver.apiHTTPClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/open/user/info":
			userInfoCalled = true
			return jsonHTTPResponse(req, http.StatusOK, `{"state":true,"code":0,"data":{}}`), nil
		case "/open/ufile/files":
			return jsonHTTPResponse(req, http.StatusOK, `{"state":true,"code":0,"data":[{"fid":"1","fc":"0","fn":"folder"}],"count":1}`), nil
		default:
			t.Fatalf("unexpected API URL: %s", req.URL)
			return nil, nil
		}
	})}

	if err := driver.Init(context.Background()); err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	files, err := driver.List(context.Background(), &open115.Obj{Fid: "0", Fc: "0"}, model.ListArgs{})
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(files) != 1 || files[0].GetName() != "folder" {
		t.Fatalf("List() = %#v, want one folder", files)
	}
	if userInfoCalled {
		t.Fatal("CD2 initialization called /open/user/info")
	}
}

func TestCD2ConfigIsDistinctAndUserAgentAware(t *testing.T) {
	if config.Name != "115 CD2" {
		t.Fatalf("config name = %q, want 115 CD2", config.Name)
	}
	if config.DefaultRoot != "0" {
		t.Fatalf("default root = %q, want 0", config.DefaultRoot)
	}
	if config.LinkCacheMode != driver.LinkCacheUA {
		t.Fatalf("link cache mode = %d, want user-agent aware mode", config.LinkCacheMode)
	}
}

func TestCD2AdditionKeeps115OpenAuthenticationFields(t *testing.T) {
	addition := Addition{}
	addition.AccessToken = "access"
	addition.RefreshToken = "refresh"
	addition.RootFolderID = "root"

	if addition.AccessToken != "access" || addition.RefreshToken != "refresh" || addition.RootFolderID != "root" {
		t.Fatalf("115 Open authentication fields were not preserved: %#v", addition)
	}
}

func TestDelegateAdditionDisablesInternalLimiterWithoutDroppingConfiguredRate(t *testing.T) {
	addition := Addition{LimitRate: 2.5}
	delegateAddition, configuredRate := delegateAdditionForInit(addition)

	if delegateAddition.LimitRate != 0 {
		t.Fatalf("delegate limit rate = %v, want 0", delegateAddition.LimitRate)
	}
	if configuredRate != addition.LimitRate {
		t.Fatalf("configured limit rate = %v, want %v", configuredRate, addition.LimitRate)
	}
}

func TestCD2RefreshTokensStayInWrapperAddition(t *testing.T) {
	driver := &CD2{
		Addition: Addition{AccessToken: "old-access", RefreshToken: "old-refresh", LimitRate: 2.5},
		delegate: &open115.Open115{Addition: open115.Addition{LimitRate: 0}},
	}

	driver.syncRefreshedTokens("new-access", "new-refresh")

	if driver.Addition.AccessToken != "new-access" || driver.Addition.RefreshToken != "new-refresh" {
		t.Fatalf("wrapper tokens = (%q, %q), want refreshed values", driver.Addition.AccessToken, driver.Addition.RefreshToken)
	}
	if driver.delegate.Addition.LimitRate != driver.Addition.LimitRate {
		t.Fatalf("delegate limit rate = %v, want %v", driver.delegate.Addition.LimitRate, driver.Addition.LimitRate)
	}
}

func TestCD2DeviceAuthUsesConfiguredAppID(t *testing.T) {
	var gotClientID string
	driver := &CD2{Addition: Addition{AuthMode: cd2AuthModeQRCode, AppID: "custom-client"}}
	driver.authHTTPClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.String() != sdk.ApiAuthDeviceCode {
			t.Fatalf("unexpected auth URL: %s", req.URL)
		}
		body, err := io.ReadAll(req.Body)
		if err != nil {
			t.Fatal(err)
		}
		values, err := url.ParseQuery(string(body))
		if err != nil {
			t.Fatal(err)
		}
		gotClientID = values.Get("client_id")
		return jsonHTTPResponse(req, http.StatusOK, `{"state":1,"code":0,"data":{"uid":"uid","time":123,"qrcode":"https://115.com/scan/test","sign":"sign"}}`), nil
	})}

	err := driver.ensureAuthentication(context.Background())
	if err == nil || !strings.Contains(err.Error(), "https://115.com/scan/test") {
		t.Fatalf("ensureAuthentication() error = %v, want QR prompt", err)
	}
	if gotClientID != "custom-client" {
		t.Fatalf("client_id = %q, want custom-client", gotClientID)
	}
	if !driver.hasPendingDeviceAuth() {
		t.Fatal("device authorization state was not retained")
	}
}

func TestCD2DeviceAuthExchangesAndClearsQRState(t *testing.T) {
	driver := &CD2{Addition: Addition{
		QRCodeUID:    "uid",
		QRCodeTime:   123,
		QRCodeSign:   "sign",
		QRCodeURL:    "https://115.com/scan/test",
		CodeVerifier: "verifier",
	}}
	driver.authHTTPClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch {
		case strings.HasPrefix(req.URL.String(), sdk.ApiQrCodeStatus):
			return jsonHTTPResponse(req, http.StatusOK, `{"state":1,"code":0,"data":{"status":2}}`), nil
		case req.URL.String() == sdk.ApiCodeToToken:
			return jsonHTTPResponse(req, http.StatusOK, `{"state":1,"code":0,"data":{"access_token":"access-new","refresh_token":"refresh-new","expires_in":3600}}`), nil
		default:
			t.Fatalf("unexpected auth URL: %s", req.URL)
			return nil, nil
		}
	})}

	if err := driver.ensureAuthentication(context.Background()); err != nil {
		t.Fatalf("ensureAuthentication() error = %v", err)
	}
	if driver.AccessToken != "access-new" || driver.RefreshToken != "refresh-new" {
		t.Fatalf("tokens = (%q, %q), want refreshed pair", driver.AccessToken, driver.RefreshToken)
	}
	if driver.hasPendingDeviceAuth() || driver.QRCodeURL != "" {
		t.Fatal("device authorization state was not cleared after token exchange")
	}
}

func TestCD2RefreshTransportUsesRelayAndNormalizesToken(t *testing.T) {
	var relayPayload map[string]string
	var gotAccessToken string
	client := sdk.New(sdk.WithRefreshToken("refresh-old"))
	client.SetOnRefreshToken(func(accessToken, _ string) { gotAccessToken = accessToken })
	client.SetHttpClient(newCD2HTTPClient("https://relay.test/refresh", roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.String() != "https://relay.test/refresh" {
			t.Fatalf("unexpected fallback URL: %s", req.URL)
		}
		body, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(body, &relayPayload); err != nil {
			return nil, err
		}
		return jsonHTTPResponse(req, http.StatusOK, `{"access_token":"access-new","refresh_token":"refresh-new","expires_in":3600}`), nil
	})))

	if _, err := client.RefreshToken(context.Background()); err != nil {
		t.Fatalf("RefreshToken() error = %v", err)
	}
	if relayPayload["refresh_token"] != "refresh-old" || relayPayload["provider"] != cloudDrive2Provider {
		t.Fatalf("relay payload = %#v, want CD2 refresh contract", relayPayload)
	}
	if gotAccessToken != "access-new" {
		t.Fatalf("access token callback = %q, want access-new", gotAccessToken)
	}
}

func TestCD2RefreshTransportDoesNotFallBackToOfficialEndpoint(t *testing.T) {
	var officialCalled bool
	client := sdk.New(sdk.WithRefreshToken("refresh-old"))
	client.SetHttpClient(newCD2HTTPClient("https://relay.test/refresh", roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.String() == "https://relay.test/refresh" {
			return jsonHTTPResponse(req, http.StatusNotFound, `{}`), nil
		}
		if req.URL.String() == sdk.ApiRefreshToken {
			officialCalled = true
			return jsonHTTPResponse(req, http.StatusOK, `{"state":1,"code":0,"data":{"access_token":"access-official","refresh_token":"refresh-official","expires_in":3600}}`), nil
		}
		t.Fatalf("unexpected URL: %s", req.URL)
		return nil, nil
	})))

	if _, err := client.RefreshToken(context.Background()); err == nil {
		t.Fatal("RefreshToken() unexpectedly succeeded after relay failure")
	}
	if officialCalled {
		t.Fatal("CD2 refresh unexpectedly fell back to the official endpoint")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func jsonHTTPResponse(req *http.Request, statusCode int, body string) *http.Response {
	return &http.Response{
		StatusCode:    statusCode,
		Status:        http.StatusText(statusCode),
		Header:        http.Header{"Content-Type": []string{"application/json"}},
		Body:          io.NopCloser(bytes.NewBufferString(body)),
		ContentLength: int64(len(body)),
		Request:       req,
	}
}
