package _115sy

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestQRStartAndPollUseReal115Endpoints(t *testing.T) {
	var pollCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case EndpointQRCodeToken:
			if r.Method != http.MethodGet || r.URL.RawQuery != "" {
				t.Fatalf("token request = %s %s, want GET without query", r.Method, r.URL.RequestURI())
			}
			_, _ = w.Write([]byte(`{"state":1,"errno":0,"data":{"uid":"qr-uid","time":123,"sign":"qr-sign","qrcode":"https://115.com/scan/test"}}`))
		case EndpointQRCodeStatus:
			pollCalls.Add(1)
			if r.Method != http.MethodGet || r.URL.Query().Get("uid") != "qr-uid" || r.URL.Query().Get("time") != "123" || r.URL.Query().Get("sign") != "qr-sign" {
				t.Fatalf("status request = %s, want uid/time/sign query", r.URL.RequestURI())
			}
			_, _ = w.Write([]byte(`{"state":true,"errno":0,"data":{"status":1,"msg":"scanned"}}`))
		default:
			t.Fatalf("unexpected endpoint = %s", r.URL.Path)
		}
	}))
	defer server.Close()

	client := newTestClient(t, ClientOptions{
		QRCodeBaseURL:   server.URL,
		PassportBaseURL: server.URL,
		WebBaseURL:      server.URL,
		AndroidBaseURL:  server.URL,
	})
	session, err := client.StartQRCode(context.Background(), QRCodeSourceAndroid)
	if err != nil {
		t.Fatalf("StartQRCode() error = %v", err)
	}
	if session.UID != "qr-uid" || session.Time != 123 || session.Sign != "qr-sign" || session.Profile != ProfileQRCode {
		t.Fatalf("session = %#v, want real token fields", session)
	}
	result, err := client.PollQRCode(context.Background(), session)
	if err != nil {
		t.Fatalf("PollQRCode() error = %v", err)
	}
	if result.Status != 1 || !result.Pending || result.Confirmed || pollCalls.Load() != 1 {
		t.Fatalf("poll result = %#v, calls=%d, want status 1 pending", result, pollCalls.Load())
	}
}

func TestQRLoginUsesStatusThenPassportLoginAndImportsCookie(t *testing.T) {
	var pollCalls atomic.Int32
	var order []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case EndpointQRCodeToken:
			order = append(order, "token")
			_, _ = w.Write([]byte(`{"state":true,"errno":0,"data":{"uid":"qr-uid","time":123,"sign":"qr-sign","qrcode":"scan"}}`))
		case EndpointQRCodeStatus:
			order = append(order, "status")
			if pollCalls.Add(1) == 1 {
				_, _ = w.Write([]byte(`{"state":true,"errno":0,"data":{"status":0}}`))
				return
			}
			_, _ = w.Write([]byte(`{"state":true,"errno":0,"data":{"status":2}}`))
		case "/app/1.0/android/1.0/login/qrcode":
			order = append(order, "login")
			if r.Method != http.MethodPost {
				t.Fatalf("login method = %s, want POST", r.Method)
			}
			if err := r.ParseForm(); err != nil || r.Form.Get("account") != "qr-uid" || r.Form.Get("app") != "android" {
				t.Fatalf("login form = %#v, want account/app", r.Form)
			}
			_, _ = w.Write([]byte(`{"state":true,"errno":0,"data":{"cookie":{"UID":"qr-user","CID":"qr-root","SEID":"qr-seid","KID":"qr-kid"}}}`))
		case EndpointUserInfo:
			order = append(order, "user")
			if !strings.Contains(r.Header.Get("Cookie"), "UID=qr-user") {
				t.Fatalf("user probe cookie = %q", r.Header.Get("Cookie"))
			}
			_, _ = w.Write([]byte(`{"state":true,"errno":0,"data":{"id":"qr-user","nickname":"tester"}}`))
		case EndpointFileList:
			order = append(order, "root")
			if !strings.Contains(r.Header.Get("Cookie"), "CID=qr-root") {
				t.Fatalf("root probe cookie = %q", r.Header.Get("Cookie"))
			}
			_, _ = w.Write([]byte(`{"state":true,"errno":0,"data":{"cid":"0","space_total":4096,"space_used":1024}}`))
		default:
			t.Fatalf("unexpected endpoint = %s", r.URL.Path)
		}
	}))
	defer server.Close()

	client := newTestClient(t, ClientOptions{
		Cookie:          "UID=old-user; CID=old-root; SEID=old-seid; legacy=sticky",
		QRCodeBaseURL:   server.URL,
		PassportBaseURL: server.URL,
		WebBaseURL:      server.URL,
		AndroidBaseURL:  server.URL,
	})
	state, err := client.LoginByQRCode(context.Background(), QRCodeLoginOptions{
		Source:       QRCodeSourceAndroid,
		PollInterval: time.Millisecond,
		MaxPollCount: 3,
	})
	if err != nil {
		t.Fatalf("LoginByQRCode() error = %v", err)
	}
	if state.UserID != "qr-user" || client.currentRawCookie() != "UID=qr-user; CID=qr-root; SEID=qr-seid; KID=qr-kid" {
		t.Fatalf("state/cookie = %#v/%q", state, client.currentRawCookie())
	}
	if got, want := strings.Join(order, ","), "token,status,status,login,user,root"; got != want {
		t.Fatalf("request order = %q, want %q", got, want)
	}
	base, _ := url.Parse(server.URL)
	jar := client.jar.Cookies(base)
	for _, cookie := range jar {
		if cookie.Name == "legacy" || cookie.Value == "old-user" {
			t.Fatalf("old cookie remained in jar: %#v", jar)
		}
	}
}

func TestQRTimeoutAndCancelAreBoundedAndPreserveCookie(t *testing.T) {
	var pollCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case EndpointQRCodeToken:
			_, _ = w.Write([]byte(`{"state":true,"errno":0,"data":{"uid":"qr-uid","time":123,"sign":"qr-sign"}}`))
		case EndpointQRCodeStatus:
			pollCalls.Add(1)
			_, _ = w.Write([]byte(`{"state":true,"errno":0,"data":{"status":0}}`))
		default:
			t.Fatalf("unexpected endpoint = %s", r.URL.Path)
		}
	}))
	defer server.Close()
	client := newTestClient(t, ClientOptions{
		Cookie:          "UID=keep; CID=root; SEID=seid",
		QRCodeBaseURL:   server.URL,
		PassportBaseURL: server.URL,
		WebBaseURL:      server.URL,
		AndroidBaseURL:  server.URL,
	})

	_, err := client.LoginByQRCode(context.Background(), QRCodeLoginOptions{PollInterval: time.Millisecond, MaxPollCount: 2, MaxPollDuration: time.Second})
	var authErr *AuthError
	if !errors.As(err, &authErr) || authErr.Stage != AuthStageQRPoll || pollCalls.Load() != 2 {
		t.Fatalf("bounded error = %v, auth=%#v, polls=%d", err, authErr, pollCalls.Load())
	}
	if client.currentRawCookie() != "UID=keep; CID=root; SEID=seid" {
		t.Fatalf("cookie after timeout = %q", client.currentRawCookie())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
	defer cancel()
	_, err = client.LoginByQRCode(ctx, QRCodeLoginOptions{PollInterval: time.Second, MaxPollCount: 10, MaxPollDuration: time.Second})
	if !errors.Is(err, context.DeadlineExceeded) || !errors.As(err, &authErr) {
		t.Fatalf("cancel error = %v, want deadline AuthError", err)
	}
	if client.currentRawCookie() != "UID=keep; CID=root; SEID=seid" {
		t.Fatalf("cookie after cancel = %q", client.currentRawCookie())
	}
}

func TestQRImportFailurePreservesExistingCookie(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case EndpointQRCodeToken:
			_, _ = w.Write([]byte(`{"state":true,"errno":0,"data":{"uid":"qr-uid","time":123,"sign":"qr-sign"}}`))
		case EndpointQRCodeStatus:
			_, _ = w.Write([]byte(`{"state":true,"errno":0,"data":{"status":2}}`))
		case "/app/1.0/android/1.0/login/qrcode":
			_, _ = w.Write([]byte(`{"state":true,"errno":0,"data":{}}`))
		default:
			t.Fatalf("unexpected endpoint = %s", r.URL.Path)
		}
	}))
	defer server.Close()
	client := newTestClient(t, ClientOptions{
		Cookie:          "UID=keep; CID=root; SEID=seid",
		QRCodeBaseURL:   server.URL,
		PassportBaseURL: server.URL,
		WebBaseURL:      server.URL,
		AndroidBaseURL:  server.URL,
	})
	_, err := client.LoginByQRCode(context.Background(), QRCodeLoginOptions{MaxPollCount: 1})
	var authErr *AuthError
	if !errors.As(err, &authErr) || authErr.Stage != AuthStageQRImport {
		t.Fatalf("error = %v, auth=%#v, want QR import error", err, authErr)
	}
	if client.currentRawCookie() != "UID=keep; CID=root; SEID=seid" {
		t.Fatalf("cookie after import failure = %q", client.currentRawCookie())
	}
}
