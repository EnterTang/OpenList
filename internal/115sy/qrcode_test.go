package _115sy

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestQRStartUsesSourceProfileHeaders(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		source        QRCodeSource
		wantProfile   Profile
		wantApp       string
		wantHasOrigin bool
	}{
		{name: "web", source: QRCodeSourceWeb, wantProfile: ProfileWeb, wantHasOrigin: true},
		{name: "android", source: QRCodeSourceAndroid, wantProfile: ProfileAndroid, wantApp: string(ProfileAndroid)},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			requests := make(chan requestSnapshot, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests <- captureRequest(r)
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"state":true,"errno":0,"data":{"uid":"qr-uid","time":123,"sign":"qr-sign","qrcode":"https://115.com/scan/test"}}`))
			}))
			defer server.Close()

			client := newTestClient(t, ClientOptions{
				WebBaseURL:     server.URL,
				AndroidBaseURL: server.URL,
			})

			session, err := client.StartQRCode(context.Background(), tc.source)
			if err != nil {
				t.Fatalf("StartQRCode() error = %v", err)
			}
			if session.Source != tc.source || session.Profile != tc.wantProfile || session.UID != "qr-uid" || session.Sign != "qr-sign" {
				t.Fatalf("session = %#v, want source/profile/uid/sign populated", session)
			}

			req := <-requests
			if req.path != endpointQRCodeStart {
				t.Fatalf("path = %q, want %q", req.path, endpointQRCodeStart)
			}
			if !strings.Contains(req.body, `"`+qrFieldSource+`":"`+string(tc.source)+`"`) {
				t.Fatalf("request body = %q, want source %q", req.body, tc.source)
			}
			if req.app != tc.wantApp {
				t.Fatalf("app header = %q, want %q", req.app, tc.wantApp)
			}
			if tc.wantHasOrigin && req.origin == "" {
				t.Fatalf("origin header = %q, want web origin", req.origin)
			}
			if !tc.wantHasOrigin && req.origin != "" {
				t.Fatalf("origin header = %q, want empty", req.origin)
			}
		})
	}
}

func TestQRPollUsesSourceProfileHeaders(t *testing.T) {
	t.Parallel()

	requests := make(chan requestSnapshot, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- captureRequest(r)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"state":true,"errno":0,"data":{"status":"pending"}}`))
	}))
	defer server.Close()

	client := newTestClient(t, ClientOptions{
		WebBaseURL:     server.URL,
		AndroidBaseURL: server.URL,
	})

	result, err := client.PollQRCode(context.Background(), &QRCodeSession{
		Source: QRCodeSourceAndroid,
		UID:    "qr-uid",
		Time:   123,
		Sign:   "qr-sign",
	})
	if err != nil {
		t.Fatalf("PollQRCode() error = %v", err)
	}
	if !result.Pending || result.Confirmed {
		t.Fatalf("poll result = %#v, want pending", result)
	}

	req := <-requests
	if req.path != endpointQRCodePoll {
		t.Fatalf("path = %q, want %q", req.path, endpointQRCodePoll)
	}
	if !strings.Contains(req.body, `"`+qrFieldSource+`":"`+string(QRCodeSourceAndroid)+`"`) {
		t.Fatalf("request body = %q, want android source", req.body)
	}
	if req.app != string(ProfileAndroid) {
		t.Fatalf("app header = %q, want android profile", req.app)
	}
	if req.origin != "" {
		t.Fatalf("origin header = %q, want empty for android profile", req.origin)
	}
}

func TestQRLoginByQRCodeImportsCookieAndRunsProbes(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var steps []requestSnapshot
	var pollCalls atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		steps = append(steps, captureRequest(r))
		mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case endpointQRCodeStart:
			_, _ = w.Write([]byte(`{"state":true,"errno":0,"data":{"uid":"qr-uid","time":123,"sign":"qr-sign","qrcode":"https://115.com/scan/test"}}`))
		case endpointQRCodePoll:
			call := pollCalls.Add(1)
			if call == 1 {
				_, _ = w.Write([]byte(`{"state":true,"errno":0,"data":{"status":"pending"}}`))
				return
			}
			_, _ = w.Write([]byte(`{"state":true,"errno":0,"data":{"status":"confirmed","cookie":"UID=qr-user; CID=qr-root; SEID=qr-seid; KID=qr-kid"}}`))
		case EndpointUserInfo:
			if got := r.Header.Get("Cookie"); !strings.Contains(got, "UID=qr-user") || !strings.Contains(got, "SEID=qr-seid") {
				t.Fatalf("user probe cookie = %q, want imported QR cookie", got)
			}
			_, _ = w.Write([]byte(`{"state":true,"errno":0,"data":{"id":"qr-user","nickname":"qr tester"}}`))
		case EndpointFileList:
			if got := r.Header.Get("Cookie"); !strings.Contains(got, "CID=qr-root") {
				t.Fatalf("root probe cookie = %q, want imported QR cookie", got)
			}
			_, _ = w.Write([]byte(`{"state":true,"errno":0,"data":{"cid":"0","space_total":"4096","space_used":"1024"}}`))
		default:
			t.Fatalf("unexpected endpoint = %s", r.URL.Path)
		}
	}))
	defer server.Close()

	client := newTestClient(t, ClientOptions{
		Cookie:         "UID=old-user; CID=old-root; SEID=old-seid; legacy=sticky",
		WebBaseURL:     server.URL,
		AndroidBaseURL: server.URL,
	})

	state, err := client.LoginByQRCode(context.Background(), QRCodeLoginOptions{
		Source:          QRCodeSourceAndroid,
		PollInterval:    time.Millisecond,
		MaxPollCount:    4,
		MaxPollDuration: time.Second,
	})
	if err != nil {
		t.Fatalf("LoginByQRCode() error = %v", err)
	}
	if state.UserID != "qr-user" || state.RootCID != "0" {
		t.Fatalf("auth state = %#v, want user/root populated", state)
	}
	if client.rawCookie != "UID=qr-user; CID=qr-root; SEID=qr-seid; KID=qr-kid" {
		t.Fatalf("raw cookie = %q, want normalized imported cookie", client.rawCookie)
	}
	baseURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("Parse(%q) error = %v", server.URL, err)
	}
	var jarCookieHeader []string
	for _, cookie := range client.httpClient.Jar.Cookies(baseURL) {
		jarCookieHeader = append(jarCookieHeader, cookie.Name+"="+cookie.Value)
	}
	if got := strings.Join(jarCookieHeader, "; "); strings.Contains(got, "old-user") || strings.Contains(got, "legacy=sticky") {
		t.Fatalf("jar cookies = %q, want old cookies removed after successful QR import", got)
	}
	if got := pollCalls.Load(); got != 2 {
		t.Fatalf("poll calls = %d, want 2", got)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(steps) != 5 {
		t.Fatalf("request count = %d, want start + 2 poll + 2 probes = 5: %#v", len(steps), steps)
	}
	if steps[0].path != endpointQRCodeStart || steps[1].path != endpointQRCodePoll || steps[2].path != endpointQRCodePoll || steps[3].path != EndpointUserInfo || steps[4].path != EndpointFileList {
		t.Fatalf("request order = %#v, want start/poll/poll/user/root", steps)
	}
	if steps[0].app != string(ProfileAndroid) || steps[1].app != string(ProfileAndroid) {
		t.Fatalf("qr headers = %#v, want android profile requests for QR lifecycle", steps[:2])
	}
}

func TestQRLoginByQRCodeTimeoutKeepsExistingCookieAndStopsPolling(t *testing.T) {
	t.Parallel()

	var pollCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case endpointQRCodeStart:
			_, _ = w.Write([]byte(`{"state":true,"errno":0,"data":{"uid":"qr-uid","time":123,"sign":"qr-sign","qrcode":"https://115.com/scan/test"}}`))
		case endpointQRCodePoll:
			pollCalls.Add(1)
			_, _ = w.Write([]byte(`{"state":true,"errno":0,"data":{"status":"pending"}}`))
		default:
			t.Fatalf("unexpected endpoint = %s", r.URL.Path)
		}
	}))
	defer server.Close()

	client := newTestClient(t, ClientOptions{
		Cookie:         "UID=keep-user; CID=keep-root; SEID=keep-seid",
		WebBaseURL:     server.URL,
		AndroidBaseURL: server.URL,
	})

	_, err := client.LoginByQRCode(context.Background(), QRCodeLoginOptions{
		Source:          QRCodeSourceAndroid,
		PollInterval:    time.Millisecond,
		MaxPollCount:    2,
		MaxPollDuration: time.Second,
	})
	var authErr *AuthError
	if !errors.As(err, &authErr) {
		t.Fatalf("LoginByQRCode() error = %v, want AuthError", err)
	}
	if authErr.Stage != AuthStageQRPoll {
		t.Fatalf("auth stage = %q, want %q", authErr.Stage, AuthStageQRPoll)
	}
	if client.rawCookie != "UID=keep-user; CID=keep-root; SEID=keep-seid" {
		t.Fatalf("raw cookie = %q, want original cookie preserved", client.rawCookie)
	}
	if got := pollCalls.Load(); got != 2 {
		t.Fatalf("poll calls = %d, want bounded count 2", got)
	}
}

func TestQRLoginByQRCodeDurationTimeoutKeepsExistingCookie(t *testing.T) {
	t.Parallel()

	var pollCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case endpointQRCodeStart:
			_, _ = w.Write([]byte(`{"state":true,"errno":0,"data":{"uid":"qr-uid","time":123,"sign":"qr-sign","qrcode":"https://115.com/scan/test"}}`))
		case endpointQRCodePoll:
			pollCalls.Add(1)
			_, _ = w.Write([]byte(`{"state":true,"errno":0,"data":{"status":"pending"}}`))
		default:
			t.Fatalf("unexpected endpoint = %s", r.URL.Path)
		}
	}))
	defer server.Close()

	client := newTestClient(t, ClientOptions{
		Cookie:         "UID=keep-user; CID=keep-root; SEID=keep-seid",
		WebBaseURL:     server.URL,
		AndroidBaseURL: server.URL,
	})

	_, err := client.LoginByQRCode(context.Background(), QRCodeLoginOptions{
		Source:          QRCodeSourceAndroid,
		PollInterval:    100 * time.Millisecond,
		MaxPollCount:    10,
		MaxPollDuration: 20 * time.Millisecond,
	})
	var authErr *AuthError
	if !errors.As(err, &authErr) {
		t.Fatalf("LoginByQRCode() error = %v, want AuthError", err)
	}
	if authErr.Stage != AuthStageQRPoll {
		t.Fatalf("auth stage = %q, want %q", authErr.Stage, AuthStageQRPoll)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("LoginByQRCode() error = %v, want context deadline", err)
	}
	if client.rawCookie != "UID=keep-user; CID=keep-root; SEID=keep-seid" {
		t.Fatalf("raw cookie = %q, want original cookie preserved", client.rawCookie)
	}
	if got := pollCalls.Load(); got != 1 {
		t.Fatalf("poll calls = %d, want one poll before duration timeout", got)
	}
}

func TestQRLoginByQRCodeCancelStopsPolling(t *testing.T) {
	t.Parallel()

	var pollCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case endpointQRCodeStart:
			_, _ = w.Write([]byte(`{"state":true,"errno":0,"data":{"uid":"qr-uid","time":123,"sign":"qr-sign","qrcode":"https://115.com/scan/test"}}`))
		case endpointQRCodePoll:
			pollCalls.Add(1)
			_, _ = w.Write([]byte(`{"state":true,"errno":0,"data":{"status":"pending"}}`))
		default:
			t.Fatalf("unexpected endpoint = %s", r.URL.Path)
		}
	}))
	defer server.Close()

	client := newTestClient(t, ClientOptions{
		Cookie:         "UID=keep-user; CID=keep-root; SEID=keep-seid",
		WebBaseURL:     server.URL,
		AndroidBaseURL: server.URL,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, err := client.LoginByQRCode(ctx, QRCodeLoginOptions{
		Source:          QRCodeSourceAndroid,
		PollInterval:    100 * time.Millisecond,
		MaxPollCount:    10,
		MaxPollDuration: time.Second,
	})
	var authErr *AuthError
	if !errors.As(err, &authErr) {
		t.Fatalf("LoginByQRCode() error = %v, want AuthError", err)
	}
	if authErr.Stage != AuthStageQRPoll {
		t.Fatalf("auth stage = %q, want %q", authErr.Stage, AuthStageQRPoll)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("LoginByQRCode() error = %v, want context deadline", err)
	}
	if client.rawCookie != "UID=keep-user; CID=keep-root; SEID=keep-seid" {
		t.Fatalf("raw cookie = %q, want original cookie preserved", client.rawCookie)
	}
	if got := pollCalls.Load(); got != 1 {
		t.Fatalf("poll calls = %d, want one poll before cancellation", got)
	}
}

func TestQRLoginByQRCodeImportFailurePreservesExistingCookie(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case endpointQRCodeStart:
			_, _ = w.Write([]byte(`{"state":true,"errno":0,"data":{"uid":"qr-uid","time":123,"sign":"qr-sign","qrcode":"https://115.com/scan/test"}}`))
		case endpointQRCodePoll:
			_, _ = w.Write([]byte(`{"state":true,"errno":0,"data":{"status":"confirmed","cookie":"UID=broken; CID=broken-root"}}`))
		default:
			t.Fatalf("unexpected endpoint = %s", r.URL.Path)
		}
	}))
	defer server.Close()

	client := newTestClient(t, ClientOptions{
		Cookie:         "UID=keep-user; CID=keep-root; SEID=keep-seid",
		WebBaseURL:     server.URL,
		AndroidBaseURL: server.URL,
	})

	_, err := client.LoginByQRCode(context.Background(), QRCodeLoginOptions{
		Source:          QRCodeSourceAndroid,
		MaxPollCount:    1,
		MaxPollDuration: time.Second,
	})
	var authErr *AuthError
	if !errors.As(err, &authErr) {
		t.Fatalf("LoginByQRCode() error = %v, want AuthError", err)
	}
	if authErr.Stage != AuthStageQRImport {
		t.Fatalf("auth stage = %q, want %q", authErr.Stage, AuthStageQRImport)
	}
	if client.rawCookie != "UID=keep-user; CID=keep-root; SEID=keep-seid" {
		t.Fatalf("raw cookie = %q, want original cookie preserved", client.rawCookie)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("request calls = %d, want only start + confirmed poll", got)
	}
}
