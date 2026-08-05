package _115_cd2

import (
	"crypto/subtle"
	"fmt"
	"net/url"
	"strings"
)

// OAuthCallbackPath is the public OpenList endpoint used as the state URL in
// CloudDrive2's 115 OAuth flow.
const OAuthCallbackPath = "/api/115-cd2/oauth/callback"

func buildCD2OAuthURL(callbackURL string) (string, error) {
	config := defaultCD2AuthConfig()
	endpoint, err := url.Parse(config.oauthEndpoint)
	if err != nil {
		return "", err
	}
	query := endpoint.Query()
	query.Set("client_id", config.oauthClientID)
	query.Set("response_type", "code")
	query.Set("redirect_uri", config.oauthRedirectURI)
	query.Set("scope", "user offline")
	query.Set("state", callbackURL)
	endpoint.RawQuery = query.Encode()
	return endpoint.String(), nil
}

// CompleteOAuthCallback validates and applies the token query returned by the
// CloudDrive2 redirect service. It is intentionally small so the HTTP handler
// can keep database and storage lifecycle concerns outside the driver package.
func CompleteOAuthCallback(addition *Addition, query url.Values) error {
	if addition == nil {
		return fmt.Errorf("115 CD2 OAuth addition is nil")
	}
	expectedState := strings.TrimSpace(addition.OAuthState)
	actualState := strings.TrimSpace(query.Get("state"))
	if expectedState == "" || actualState == "" || subtle.ConstantTimeCompare([]byte(expectedState), []byte(actualState)) != 1 {
		return fmt.Errorf("115 CD2 OAuth state mismatch")
	}
	if providerError := strings.TrimSpace(query.Get("error")); providerError != "" {
		if description := strings.TrimSpace(query.Get("error_description")); description != "" {
			return fmt.Errorf("115 CD2 OAuth provider error %s: %s", providerError, description)
		}
		return fmt.Errorf("115 CD2 OAuth provider error: %s", providerError)
	}
	accessToken := strings.TrimSpace(query.Get("access_token"))
	refreshToken := strings.TrimSpace(query.Get("refresh_token"))
	if accessToken == "" || refreshToken == "" {
		return fmt.Errorf("115 CD2 OAuth callback returned an incomplete token pair")
	}

	addition.AccessToken = accessToken
	addition.RefreshToken = refreshToken
	addition.OAuthState = ""
	addition.OAuthURL = ""
	return nil
}

func completeCD2OAuthCallback(addition *Addition, query url.Values) error {
	return CompleteOAuthCallback(addition, query)
}
