package _139

import (
	"context"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestCookieAuthDerivesAuthorizationFromCookie(t *testing.T) {
	setup139Resty(t)
	setup139CookieAuthSave(t)
	account := "13900000000"
	token := test139Token(time.Now().Add(30 * 24 * time.Hour))
	d := &Yun139{
		Addition: Addition{
			AuthMode:     authModeETF,
			CookieHeader: "auth_token=" + token + "; ORCHES-I-ACCOUNT-ENCRYPT=" + base64.StdEncoding.EncodeToString([]byte(account)) + "; ud_id=domain-id",
		},
	}

	if err := d.refreshToken(); err != nil {
		t.Fatalf("refreshToken returned error: %v", err)
	}
	assert139StoredAuthorization(t, d.Authorization, account, token)
	if d.Account != account {
		t.Fatalf("account = %q, want %q", d.Account, account)
	}
	if d.UserDomainID != "domain-id" {
		t.Fatalf("UserDomainID = %q, want domain-id", d.UserDomainID)
	}
}

func TestCookieAuthRefreshesNearExpiryToken(t *testing.T) {
	setup139Resty(t)
	setup139CookieAuthSave(t)
	account := "13900000000"
	oldToken := test139Token(time.Now().Add(24 * time.Hour))
	newToken := test139Token(time.Now().Add(30 * 24 * time.Hour))
	var sawBody string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		sawBody = string(body)
		w.Header().Set("Content-Type", "application/xml")
		_ = xml.NewEncoder(w).Encode(RefreshTokenResp{
			Return: "0",
			Token:  newToken,
		})
	}))
	defer server.Close()
	oldEndpoint := mobileAuthTokenRefreshEndpoint
	mobileAuthTokenRefreshEndpoint = server.URL
	t.Cleanup(func() {
		mobileAuthTokenRefreshEndpoint = oldEndpoint
	})

	d := &Yun139{
		Addition: Addition{
			AuthMode:     authModeETF,
			CookieHeader: "skey=abc; auth_token=" + oldToken + "; ORCHES-I-ACCOUNT-ENCRYPT=" + base64.StdEncoding.EncodeToString([]byte(account)),
		},
	}
	if err := d.refreshToken(); err != nil {
		t.Fatalf("refreshToken returned error: %v", err)
	}
	if !strings.Contains(sawBody, "<token>"+oldToken+"</token>") || !strings.Contains(sawBody, "<account>"+account+"</account>") {
		t.Fatalf("refresh request body = %q, want old token and account", sawBody)
	}
	assert139StoredAuthorization(t, d.Authorization, account, newToken)
	if !strings.Contains(d.CookieHeader, "auth_token="+newToken) {
		t.Fatalf("CookieHeader = %q, want refreshed auth_token", d.CookieHeader)
	}
	if !strings.Contains(d.CookieHeader, "authorization=Basic "+d.Authorization) {
		t.Fatalf("CookieHeader = %q, want refreshed authorization", d.CookieHeader)
	}
}

func TestCookieAuthUsesAuthorizationCookie(t *testing.T) {
	setup139Resty(t)
	setup139CookieAuthSave(t)
	account := "13900000000"
	token := test139Token(time.Now().Add(30 * 24 * time.Hour))
	storedAuth := buildStored139Authorization(account, token)
	d := &Yun139{
		Addition: Addition{
			AuthMode:     authModeETF,
			CookieHeader: "authorization=" + urlQueryEscape139("Basic "+storedAuth),
		},
	}
	if err := d.refreshCookieAuth(context.Background()); err != nil {
		t.Fatalf("refreshCookieAuth returned error: %v", err)
	}
	assert139StoredAuthorization(t, d.Authorization, account, token)
}

func TestCookieAuthPrefersCookieTokenOverStoredAuthorization(t *testing.T) {
	setup139Resty(t)
	setup139CookieAuthSave(t)
	account := "13900000000"
	oldToken := test139Token(time.Now().Add(30 * 24 * time.Hour))
	newToken := test139Token(time.Now().Add(30 * 24 * time.Hour))
	d := &Yun139{
		Addition: Addition{
			AuthMode: authModeETF,
			CookieHeader: "auth_token=" + newToken +
				"; ORCHES-I-ACCOUNT-ENCRYPT=" + base64.StdEncoding.EncodeToString([]byte(account)),
			Authorization: buildStored139Authorization(account, oldToken),
		},
	}

	if err := d.refreshCookieAuth(context.Background()); err != nil {
		t.Fatalf("refreshCookieAuth returned error: %v", err)
	}
	assert139StoredAuthorization(t, d.Authorization, account, newToken)
}

func test139Token(expiresAt time.Time) string {
	return fmt.Sprintf("token|1|RCS|%d|payload", expiresAt.UnixMilli())
}

func assert139StoredAuthorization(t *testing.T, stored, account, token string) {
	t.Helper()
	decoded, err := base64.StdEncoding.DecodeString(stored)
	if err != nil {
		t.Fatalf("decode stored authorization: %v", err)
	}
	want := "pc:" + account + ":" + token
	if string(decoded) != want {
		t.Fatalf("stored authorization decodes to %q, want %q", string(decoded), want)
	}
}

func setup139CookieAuthSave(t *testing.T) {
	t.Helper()
	old := save139DriverStorage
	save139DriverStorage = func(*Yun139) {}
	t.Cleanup(func() {
		save139DriverStorage = old
	})
}

func urlQueryEscape139(value string) string {
	return strings.ReplaceAll(value, " ", "%20")
}
