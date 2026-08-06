package _115sy

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestCookieParseAcceptsWhitespaceAndOptionalKID(t *testing.T) {
	t.Parallel()

	cred, err := ParseCookie("  UID = user-1 ; CID= root-1; SEID = seid-1 ; KID = kid-1  ")
	if err != nil {
		t.Fatalf("ParseCookie() error = %v", err)
	}
	if cred.UID != "user-1" || cred.CID != "root-1" || cred.SEID != "seid-1" || cred.KID != "kid-1" {
		t.Fatalf("credential = %#v, want populated UID/CID/SEID/KID", cred)
	}
}

func TestCookieParseRejectsInvalidFormsWithoutLeakingSecrets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		raw  string
		want string
	}{
		{name: "missing required", raw: "UID=user-1; SEID=seid-1", want: "missing required"},
		{name: "duplicate uid", raw: "UID=user-1; CID=root-1; SEID=seid-1; UID=user-2", want: "duplicate"},
		{name: "empty value", raw: "UID=user-1; CID= ; SEID=seid-1", want: "empty"},
		{name: "illegal part", raw: "UID=user-1; CID=root-1; broken; SEID=seid-1", want: "invalid"},
		{name: "illegal field name", raw: "UID=user-1; CID=root-1; BAD FIELD=bad; SEID=seid-1", want: "invalid"},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := ParseCookie(tc.raw)
			if err == nil {
				t.Fatal("ParseCookie() error = nil, want failure")
			}
			message := err.Error()
			if !strings.Contains(strings.ToLower(message), tc.want) {
				t.Fatalf("error = %q, want substring %q", message, tc.want)
			}
			for _, forbidden := range []string{"UID=user-1", "CID=root-1", "SEID=seid-1", "user-1", "root-1", "seid-1"} {
				if strings.Contains(message, forbidden) {
					t.Fatalf("error %q leaked cookie value %q", message, forbidden)
				}
			}
		})
	}
}

func TestAuthAuthenticateProbesUserAndRootWithoutLegacyLoginCheck(t *testing.T) {
	t.Parallel()

	var userCalls atomic.Int32
	var rootCalls atomic.Int32
	requests := make(chan requestSnapshot, 2)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- captureRequest(r)
		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case EndpointUserInfo:
			userCalls.Add(1)
			_, _ = w.Write([]byte(`{"state":true,"errno":0,"data":{"id":"user-1","nickname":"tester"}}`))
		case EndpointFileList:
			rootCalls.Add(1)
			if got := r.URL.Query().Get("cid"); got != "0" {
				t.Fatalf("root probe cid = %q, want 0", got)
			}
			if got := r.URL.Query().Get("limit"); got != "1" {
				t.Fatalf("root probe limit = %q, want 1", got)
			}
			_, _ = w.Write([]byte(`{"state":true,"errno":0,"data":{"cid":"0","space_total":"2048","space_used":"512"}}`))
		default:
			t.Fatalf("unexpected endpoint = %s", r.URL.Path)
		}
	}))
	defer server.Close()

	client := newTestClient(t, ClientOptions{
		Cookie:         "UID=user-1; CID=root-cookie; SEID=seid-1; KID=kid-1",
		WebBaseURL:     server.URL,
		AndroidBaseURL: server.URL,
	})

	state, err := client.Authenticate(context.Background())
	if err != nil {
		t.Fatalf("Authenticate() error = %v", err)
	}
	if state.UserID != "user-1" || state.RootCID != "0" {
		t.Fatalf("auth state = %#v, want user/root populated", state)
	}
	if state.Credential.UID != "user-1" || state.Credential.CID != "root-cookie" || state.Credential.SEID != "seid-1" || state.Credential.KID != "kid-1" {
		t.Fatalf("auth credential = %#v, want parsed cookie", state.Credential)
	}
	if state.Capacity.Total != 2048 || state.Capacity.Used != 512 || state.Capacity.Remaining != 1536 {
		t.Fatalf("capacity = %#v, want total=2048 used=512 remaining=1536", state.Capacity)
	}
	if got := userCalls.Load(); got != 1 {
		t.Fatalf("user probe calls = %d, want 1", got)
	}
	if got := rootCalls.Load(); got != 1 {
		t.Fatalf("root probe calls = %d, want 1", got)
	}

	userReq := <-requests
	rootReq := <-requests

	if userReq.path != EndpointUserInfo || !strings.Contains(userReq.cookie, "UID=user-1") || !strings.Contains(userReq.cookie, "SEID=seid-1") {
		t.Fatalf("user request = %#v, want imported cookie on user probe", userReq)
	}
	if rootReq.path != EndpointFileList || !strings.Contains(rootReq.cookie, "CID=root-cookie") {
		t.Fatalf("root request = %#v, want imported cookie on root probe", rootReq)
	}
	if rootReq.app != string(ProfileAndroid) {
		t.Fatalf("root request app = %q, want android profile header", rootReq.app)
	}
}

func TestAuthAuthenticateRejectsInvalidCookieBeforeNetworking(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	client := newTestClient(t, ClientOptions{
		Cookie: "UID=user-1; SEID=seid-1",
		HTTPClient: &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				calls.Add(1)
				return jsonResponse(req, http.StatusOK, `{"state":true,"errno":0}`), nil
			}),
		},
		WebBaseURL:     "https://web.invalid",
		AndroidBaseURL: "https://android.invalid",
	})

	_, err := client.Authenticate(context.Background())
	var authErr *AuthError
	if !errors.As(err, &authErr) {
		t.Fatalf("Authenticate() error = %v, want AuthError", err)
	}
	if authErr.Stage != AuthStageCookie {
		t.Fatalf("auth stage = %q, want %q", authErr.Stage, AuthStageCookie)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("network calls = %d, want 0", got)
	}
}

func TestAuthAuthenticateMaps40101017ToUserInfoStage(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != EndpointUserInfo {
			t.Fatalf("unexpected endpoint = %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"state":false,"errno":40101017,"error":"cookie expired"}`))
	}))
	defer server.Close()

	client := newTestClient(t, ClientOptions{
		Cookie:         "UID=user-1; CID=root-1; SEID=seid-1",
		WebBaseURL:     server.URL,
		AndroidBaseURL: server.URL,
	})

	_, err := client.Authenticate(context.Background())
	var authErr *AuthError
	if !errors.As(err, &authErr) {
		t.Fatalf("Authenticate() error = %v, want AuthError", err)
	}
	if authErr.Stage != AuthStageUserInfo {
		t.Fatalf("auth stage = %q, want %q", authErr.Stage, AuthStageUserInfo)
	}
	if authErr.Errno != 40101017 {
		t.Fatalf("auth errno = %d, want 40101017", authErr.Errno)
	}
	var businessErr *BusinessError
	if !errors.As(err, &businessErr) {
		t.Fatalf("Authenticate() error = %v, want wrapped BusinessError", err)
	}
	if businessErr.Errno != 40101017 || businessErr.Kind != KindAuth {
		t.Fatalf("business error = %#v, want auth kind / errno 40101017", businessErr)
	}
}
