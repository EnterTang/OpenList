package _115_cd2

import (
	"context"
	"net/url"
	"strings"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
)

func TestCD2OAuthAuthorizationURLUsesCloudDrive2Contract(t *testing.T) {
	callback := "https://openlist.test/api/115-cd2/oauth/callback?storage_id=42&state=state-token"
	got, err := buildCD2OAuthURL(callback)
	if err != nil {
		t.Fatalf("buildCD2OAuthURL() error = %v", err)
	}
	parsed, err := url.Parse(got)
	if err != nil {
		t.Fatalf("parse authorization URL: %v", err)
	}
	query := parsed.Query()
	if parsed.Scheme+"://"+parsed.Host+parsed.Path != "https://passportapi.115.com/open/authorize" {
		t.Fatalf("authorization endpoint = %q", parsed.Scheme+"://"+parsed.Host+parsed.Path)
	}
	for key, want := range map[string]string{
		"client_id":     "100195313",
		"response_type": "code",
		"redirect_uri":  "https://redirect115.zhenyunpan.com",
		"scope":         "user offline",
		"state":         callback,
	} {
		if query.Get(key) != want {
			t.Fatalf("query[%q] = %q, want %q", key, query.Get(key), want)
		}
	}
}

func TestCD2OAuthInitializationPersistsActionableAuthorizationURL(t *testing.T) {
	previousConf := conf.Conf
	conf.Conf = conf.DefaultConfig("data")
	t.Cleanup(func() { conf.Conf = previousConf })

	driver := &CD2{}
	err := driver.ensureAuthentication(context.WithValue(context.Background(), conf.ApiUrlKey, "https://openlist.test"))
	if err == nil || !strings.Contains(err.Error(), "https://passportapi.115.com/open/authorize") {
		t.Fatalf("ensureAuthentication() error = %v, want actionable OAuth URL", err)
	}
	if driver.AuthMode != "" && driver.AuthMode != cd2AuthModeOAuth {
		t.Fatalf("AuthMode = %q, want OAuth default", driver.AuthMode)
	}
	if driver.OAuthState == "" || driver.OAuthURL == "" {
		t.Fatalf("pending OAuth state not persisted: state=%q url=%q", driver.OAuthState, driver.OAuthURL)
	}
	parsed, err := url.Parse(driver.OAuthURL)
	if err != nil {
		t.Fatalf("parse persisted OAuth URL: %v", err)
	}
	if parsed.Query().Get("state") == "" {
		t.Fatal("persisted OAuth URL has no state")
	}
}

func TestCD2OAuthCallbackRequiresMatchingStateAndCompleteTokens(t *testing.T) {
	addition := &Addition{OAuthState: "expected-state"}
	if err := completeCD2OAuthCallback(addition, url.Values{
		"state":         []string{"wrong-state"},
		"access_token":  []string{"access"},
		"refresh_token": []string{"refresh"},
	}); err == nil || !strings.Contains(err.Error(), "state") {
		t.Fatalf("state mismatch error = %v, want state validation failure", err)
	}
	if addition.AccessToken != "" || addition.RefreshToken != "" {
		t.Fatal("state mismatch modified tokens")
	}

	if err := completeCD2OAuthCallback(addition, url.Values{
		"state":         []string{"expected-state"},
		"access_token":  []string{"access"},
		"refresh_token": []string{"refresh"},
	}); err != nil {
		t.Fatalf("completeCD2OAuthCallback() error = %v", err)
	}
	if addition.AccessToken != "access" || addition.RefreshToken != "refresh" {
		t.Fatalf("tokens = (%q, %q), want callback tokens", addition.AccessToken, addition.RefreshToken)
	}
	if addition.OAuthState != "" || addition.OAuthURL != "" {
		t.Fatal("OAuth state was not cleared after successful callback")
	}
}

func TestCD2OAuthCallbackRejectsProviderErrorAndIncompleteTokens(t *testing.T) {
	for name, query := range map[string]url.Values{
		"provider error": {
			"state":             []string{"expected-state"},
			"error":             []string{"access_denied"},
			"error_description": []string{"denied"},
		},
		"missing refresh token": {
			"state":        []string{"expected-state"},
			"access_token": []string{"access"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			addition := &Addition{OAuthState: "expected-state"}
			if err := completeCD2OAuthCallback(addition, query); err == nil {
				t.Fatal("completeCD2OAuthCallback() unexpectedly succeeded")
			}
			if addition.AccessToken != "" || addition.RefreshToken != "" || addition.OAuthState != "expected-state" {
				t.Fatalf("failed callback changed addition: %#v", addition)
			}
		})
	}
}
