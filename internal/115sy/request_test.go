package _115sy

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func jsonResponse(req *http.Request, status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Status:     http.StatusText(status),
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    req,
	}
}

type requestSnapshot struct {
	method      string
	path        string
	query       string
	body        string
	accept      string
	contentType string
	cookie      string
	origin      string
	referer     string
	userAgent   string
	app         string
	appVersion  string
}

type blockingReadCloser struct {
	started  chan<- struct{}
	release  <-chan struct{}
	payload  []byte
	once     sync.Once
	readDone bool
}

func (b *blockingReadCloser) Read(p []byte) (int, error) {
	b.once.Do(func() {
		close(b.started)
	})
	<-b.release
	if b.readDone {
		return 0, io.EOF
	}
	b.readDone = true
	return copy(p, b.payload), io.EOF
}

func (b *blockingReadCloser) Close() error {
	return nil
}

func newTestClient(t *testing.T, opts ClientOptions) *Client {
	t.Helper()
	client, err := NewClient(opts)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	return client
}

func captureRequest(r *http.Request) requestSnapshot {
	body, _ := io.ReadAll(r.Body)
	_ = r.Body.Close()
	r.Body = io.NopCloser(strings.NewReader(string(body)))
	return requestSnapshot{
		method:      r.Method,
		path:        r.URL.Path,
		query:       r.URL.RawQuery,
		body:        string(body),
		accept:      r.Header.Get("Accept"),
		contentType: r.Header.Get("Content-Type"),
		cookie:      r.Header.Get("Cookie"),
		origin:      r.Header.Get("Origin"),
		referer:     r.Header.Get("Referer"),
		userAgent:   r.Header.Get("User-Agent"),
		app:         r.Header.Get("app"),
		appVersion:  r.Header.Get("appversion"),
	}
}

func TestRequestUsesProfileHeadersCookiesQueryAndBody(t *testing.T) {
	t.Parallel()

	records := make(chan requestSnapshot, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		records <- captureRequest(r)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"state":true,"errno":0,"data":{"id":"u1","nickname":"tester"}}`))
	}))
	defer server.Close()

	client := newTestClient(t, ClientOptions{
		Cookie:         "UID=uid; CID=cid; SEID=seid",
		UserAgent:      "CustomUA/1.0",
		AppVersion:     "99.9.9",
		WebBaseURL:     server.URL,
		AndroidBaseURL: server.URL,
	})

	query := url.Values{"cid": {"123"}, "limit": {"50"}}
	if err := client.doJSON(context.Background(), OperationUserInfo, ProfileWeb, http.MethodGet, EndpointUserInfo, query, nil, &UserInfo{}); err != nil {
		t.Fatalf("web doJSON() error = %v", err)
	}

	form := url.Values{"url": {"https://example.invalid/a"}, "save": {"1"}}
	if err := client.doForm(context.Background(), OperationOffline, ProfileAndroid, http.MethodPost, EndpointOfflineAdd, nil, form, &UserInfo{}); err != nil {
		t.Fatalf("android doForm() error = %v", err)
	}

	webRecord := <-records
	androidRecord := <-records

	if webRecord.method != http.MethodGet || webRecord.path != EndpointUserInfo {
		t.Fatalf("web request = %#v, want GET %s", webRecord, EndpointUserInfo)
	}
	if webRecord.query != query.Encode() {
		t.Fatalf("web query = %q, want %q", webRecord.query, query.Encode())
	}
	if webRecord.body != "" {
		t.Fatalf("web body = %q, want empty", webRecord.body)
	}
	if webRecord.accept != "application/json" {
		t.Fatalf("web accept = %q, want application/json", webRecord.accept)
	}
	if webRecord.contentType != "" {
		t.Fatalf("web content-type = %q, want empty", webRecord.contentType)
	}
	if !strings.Contains(webRecord.cookie, "UID=uid") || !strings.Contains(webRecord.cookie, "SEID=seid") {
		t.Fatalf("web cookie = %q, want configured cookies", webRecord.cookie)
	}
	if webRecord.origin != server.URL {
		t.Fatalf("web origin = %q, want %q", webRecord.origin, server.URL)
	}
	if webRecord.referer != server.URL+"/" {
		t.Fatalf("web referer = %q, want %q", webRecord.referer, server.URL+"/")
	}
	if webRecord.userAgent != "CustomUA/1.0" {
		t.Fatalf("web user-agent = %q, want custom UA", webRecord.userAgent)
	}
	if webRecord.app != "" || webRecord.appVersion != "" {
		t.Fatalf("web app headers = (%q, %q), want empty", webRecord.app, webRecord.appVersion)
	}

	if androidRecord.method != http.MethodPost || androidRecord.path != EndpointOfflineAdd {
		t.Fatalf("android request = %#v, want POST %s", androidRecord, EndpointOfflineAdd)
	}
	if androidRecord.body != form.Encode() {
		t.Fatalf("android body = %q, want %q", androidRecord.body, form.Encode())
	}
	if androidRecord.contentType != "application/x-www-form-urlencoded" {
		t.Fatalf("android content-type = %q, want form encoding", androidRecord.contentType)
	}
	if !strings.Contains(androidRecord.cookie, "CID=cid") {
		t.Fatalf("android cookie = %q, want configured cookies", androidRecord.cookie)
	}
	if androidRecord.origin != "" || androidRecord.referer != "" {
		t.Fatalf("android origin/referer = (%q, %q), want empty", androidRecord.origin, androidRecord.referer)
	}
	if androidRecord.userAgent != "CustomUA/1.0" {
		t.Fatalf("android user-agent = %q, want custom UA", androidRecord.userAgent)
	}
	if androidRecord.app != string(ProfileAndroid) {
		t.Fatalf("android app = %q, want %q", androidRecord.app, ProfileAndroid)
	}
	if androidRecord.appVersion != "99.9.9" {
		t.Fatalf("android appversion = %q, want configured version", androidRecord.appVersion)
	}
}

func TestRequestDecodesJSONData(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"state":true,"errno":0,"data":{"id":"u1","nickname":"tester"}}`))
	}))
	defer server.Close()

	client := newTestClient(t, ClientOptions{
		WebBaseURL:     server.URL,
		AndroidBaseURL: server.URL,
	})

	var got UserInfo
	if err := client.doJSON(context.Background(), OperationUserInfo, ProfileWeb, http.MethodGet, EndpointUserInfo, nil, nil, &got); err != nil {
		t.Fatalf("doJSON() error = %v", err)
	}
	if got.ID != "u1" || got.Nickname != "tester" {
		t.Fatalf("decoded user = %#v, want populated data payload", got)
	}
}

func TestRequestReturnsBusinessErrorFromErrno(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"state":false,"errno":90001,"error":"bad request"}`))
	}))
	defer server.Close()

	client := newTestClient(t, ClientOptions{
		WebBaseURL:     server.URL,
		AndroidBaseURL: server.URL,
	})

	err := client.doJSON(context.Background(), OperationOffline, ProfileAndroid, http.MethodPost, EndpointOfflineAdd, nil, map[string]string{"x": "y"}, nil)
	var businessErr *BusinessError
	if !errors.As(err, &businessErr) {
		t.Fatalf("error = %v, want BusinessError", err)
	}
	if businessErr.Kind != KindBusiness || businessErr.Errno != 90001 {
		t.Fatalf("business error = %#v, want errno 90001 / business kind", businessErr)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("request count = %d, want one request without fallback", got)
	}
}

func TestRequestAcceptsStringErrnoForDelete(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != EndpointFileDelete {
			t.Fatalf("request = %s %s, want POST %s", r.Method, r.URL.Path, EndpointFileDelete)
		}
		if err := r.ParseForm(); err != nil {
			t.Fatal(err)
		}
		if got := r.Form.Get("fid[0]"); got != "file-under-test" {
			t.Fatalf("fid[0] = %q, want file-under-test", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"state":true,"errno":"0","data":{}}`))
	}))
	defer server.Close()

	client := newTestClient(t, ClientOptions{
		LimitRate:      1e6,
		WebBaseURL:     server.URL,
		AndroidBaseURL: server.URL,
	})
	if err := client.Remove(context.Background(), "file-under-test", "0"); err != nil {
		t.Fatalf("Remove() error = %v, want nil for string errno=0", err)
	}
}

func TestRequestFallsBackOnHTTP405(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	profiles := make(chan string, 2)
	client := newTestClient(t, ClientOptions{
		WebBaseURL:     "https://web.invalid",
		AndroidBaseURL: "https://android.invalid",
		HTTPClient: &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				call := calls.Add(1)
				profiles <- req.Header.Get("app")
				if call == 1 {
					return jsonResponse(req, http.StatusMethodNotAllowed, `{"state":false,"errno":0,"error":"unsupported"}`), nil
				}
				return jsonResponse(req, http.StatusOK, `{"state":true,"errno":0,"data":{"id":"u2","nickname":"fallback"}}`), nil
			}),
		},
	})

	var got UserInfo
	if err := client.doJSON(context.Background(), OperationUserInfo, ProfileWeb, http.MethodGet, EndpointUserInfo, nil, nil, &got); err != nil {
		t.Fatalf("doJSON() error = %v", err)
	}
	if got.ID != "u2" {
		t.Fatalf("decoded user = %#v, want fallback response", got)
	}
	if calls.Load() != 2 {
		t.Fatalf("calls = %d, want 2", calls.Load())
	}

	firstProfile := <-profiles
	secondProfile := <-profiles
	if firstProfile != "" || secondProfile != string(ProfileAndroid) {
		t.Fatalf("profiles = (%q, %q), want web then android fallback", firstProfile, secondProfile)
	}
}

func TestRequestDoesNotFallbackOnNetworkError(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	client := newTestClient(t, ClientOptions{
		WebBaseURL:     "https://web.invalid",
		AndroidBaseURL: "https://android.invalid",
		HTTPClient: &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				calls.Add(1)
				return nil, &url.Error{Op: req.Method, URL: req.URL.String(), Err: errors.New("dial tcp: boom")}
			}),
		},
	})

	err := client.doJSON(context.Background(), OperationUserInfo, ProfileWeb, http.MethodGet, EndpointUserInfo, nil, nil, &UserInfo{})
	var networkErr *NetworkError
	if !errors.As(err, &networkErr) {
		t.Fatalf("error = %v, want NetworkError", err)
	}
	if networkErr.Profile != ProfileWeb {
		t.Fatalf("network error profile = %q, want web", networkErr.Profile)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("request count = %d, want one request without fallback", got)
	}
}

func TestRequestDoesNotRetry40101017Forever(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	client := newTestClient(t, ClientOptions{
		WebBaseURL:     "https://web.invalid",
		AndroidBaseURL: "https://android.invalid",
		HTTPClient: &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				calls.Add(1)
				return jsonResponse(req, http.StatusOK, `{"state":false,"errno":40101017,"error":"用户验证失败"}`), nil
			}),
		},
	})

	err := client.doJSON(context.Background(), OperationUserInfo, ProfileWeb, http.MethodGet, EndpointUserInfo, nil, nil, &UserInfo{})
	var authErr *BusinessError
	if !errors.As(err, &authErr) || authErr.Errno != 40101017 || authErr.Kind != KindAuth {
		t.Fatalf("error = %v, want auth BusinessError errno 40101017", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("request count = %d, want one request", got)
	}
}

func TestRequestRedactsSensitiveMaterialFromErrors(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, ClientOptions{
		Cookie:         "UID=uid-secret; CID=cid-secret; SEID=seid-secret; KID=kid-secret",
		WebBaseURL:     "https://web.invalid",
		AndroidBaseURL: "https://android.invalid",
		HTTPClient: &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				cause := strings.Join([]string{
					"receive_code=rc-secret",
					"receive_code=rc-secret",
					"security_code=sc-secret",
					"access_token=access-secret",
					"refresh_token=refresh-secret",
					"token=bearer-secret",
					"UID=uid-secret",
					"CID=cid-secret",
					"SEID=seid-secret",
					"KID=kid-secret",
				}, " ")
				return nil, &url.Error{Op: req.Method, URL: req.URL.String(), Err: errors.New(cause)}
			}),
		},
	})

	err := client.doJSON(
		context.Background(),
		OperationShareReceive,
		ProfileWeb,
		http.MethodPost,
		EndpointShareReceive+"?receive_code=rc-secret&receive_code=rc-secret&security_code=sc-secret&access_token=access-secret&refresh_token=refresh-secret&token=bearer-secret",
		url.Values{"UID": {"uid-secret"}, "CID": {"cid-secret"}, "access_token": {"access-secret"}},
		map[string]string{"code": "value"},
		nil,
	)
	if err == nil {
		t.Fatal("doJSON() error = nil, want network error")
	}

	message := err.Error()
	for _, secret := range []string{"uid-secret", "cid-secret", "seid-secret", "kid-secret", "rc-secret", "sc-secret", "access-secret", "refresh-secret", "bearer-secret"} {
		if strings.Contains(message, secret) {
			t.Fatalf("error %q leaked sensitive value %q", message, secret)
		}
	}
	if !strings.Contains(message, "share/receive") || !strings.Contains(message, "receive_code=[REDACTED]") {
		t.Fatalf("error = %q, want sanitized endpoint and repeated redactions", message)
	}
}

func TestRequestCancelsDuringPageCooldownWithoutConsumingAnotherCooldownWindow(t *testing.T) {
	t.Parallel()

	const (
		cooldown     = 120 * time.Millisecond
		cancelWindow = 20 * time.Millisecond
		buffer       = 20 * time.Millisecond
	)

	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"state":true,"errno":0,"data":{"id":"u1","nickname":"page"}}`))
	}))
	defer server.Close()

	client := newTestClient(t, ClientOptions{
		LimitRate:      1_000_000,
		PageCooldown:   cooldown,
		WebBaseURL:     server.URL,
		AndroidBaseURL: server.URL,
	})

	if err := client.doJSON(context.Background(), OperationFileList, ProfileAndroid, http.MethodGet, EndpointFileList, nil, nil, &UserInfo{}); err != nil {
		t.Fatalf("first doJSON() error = %v", err)
	}
	readyAt := time.Now().Add(cooldown)

	ctx, cancel := context.WithTimeout(context.Background(), cancelWindow)
	defer cancel()

	err := client.doJSON(ctx, OperationFileList, ProfileAndroid, http.MethodGet, EndpointFileList, nil, nil, &UserInfo{})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second doJSON() error = %v, want deadline exceeded", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("calls = %d, want cooldown to prevent second request", calls.Load())
	}

	if sleep := time.Until(readyAt.Add(buffer)); sleep > 0 {
		time.Sleep(sleep)
	}

	ctxThird, cancelThird := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancelThird()

	if err := client.doJSON(ctxThird, OperationFileList, ProfileAndroid, http.MethodGet, EndpointFileList, nil, nil, &UserInfo{}); err != nil {
		t.Fatalf("third doJSON() error = %v, want success once original cooldown expires", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("calls = %d, want third request to execute immediately after original cooldown", calls.Load())
	}
}

func TestRequestWaitsForResponseCompletionBeforeStartingNextPageRequest(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	firstBodyStarted := make(chan struct{})
	releaseFirstBody := make(chan struct{})
	client := newTestClient(t, ClientOptions{
		LimitRate:      1_000_000,
		PageCooldown:   20 * time.Millisecond,
		WebBaseURL:     "https://web.invalid",
		AndroidBaseURL: "https://android.invalid",
		HTTPClient: &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				call := calls.Add(1)
				if call == 1 {
					return &http.Response{
						StatusCode: http.StatusOK,
						Status:     http.StatusText(http.StatusOK),
						Header:     make(http.Header),
						Body: &blockingReadCloser{
							started: firstBodyStarted,
							release: releaseFirstBody,
							payload: []byte(`{"state":true,"errno":0,"data":{"id":"u1","nickname":"first"}}`),
						},
						Request: req,
					}, nil
				}
				return jsonResponse(req, http.StatusOK, `{"state":true,"errno":0,"data":{"id":"u2","nickname":"second"}}`), nil
			}),
		},
	})

	firstErr := make(chan error, 1)
	go func() {
		firstErr <- client.doJSON(context.Background(), OperationFileList, ProfileAndroid, http.MethodGet, EndpointFileList, nil, nil, &UserInfo{})
	}()

	<-firstBodyStarted

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()

	err := client.doJSON(ctx, OperationFileList, ProfileAndroid, http.MethodGet, EndpointFileList, nil, nil, &UserInfo{})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second doJSON() error = %v, want deadline exceeded while first response is still active", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("calls = %d, want second request blocked until first body completes", calls.Load())
	}

	close(releaseFirstBody)
	if err := <-firstErr; err != nil {
		t.Fatalf("first doJSON() error = %v", err)
	}
}

func TestRequestDisablesPageCooldownWhenRateNonPositive(t *testing.T) {
	t.Parallel()

	var callTimes []time.Time
	var mu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		callTimes = append(callTimes, time.Now())
		mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"state":true,"errno":0,"data":{"id":"u1","nickname":"no-cooldown"}}`))
	}))
	defer server.Close()

	client := newTestClient(t, ClientOptions{
		LimitRate:      0,
		PageCooldown:   200 * time.Millisecond,
		WebBaseURL:     server.URL,
		AndroidBaseURL: server.URL,
	})

	start := time.Now()
	if err := client.doJSON(context.Background(), OperationFileList, ProfileAndroid, http.MethodGet, EndpointFileList, nil, nil, &UserInfo{}); err != nil {
		t.Fatalf("first doJSON() error = %v", err)
	}
	if err := client.doJSON(context.Background(), OperationFileList, ProfileAndroid, http.MethodGet, EndpointFileList, nil, nil, &UserInfo{}); err != nil {
		t.Fatalf("second doJSON() error = %v, want no page cooldown when rate limiter disabled", err)
	}
	elapsed := time.Since(start)

	if elapsed >= 100*time.Millisecond {
		t.Fatalf("two requests elapsed = %v, want no meaningful page cooldown delay", elapsed)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(callTimes) != 2 {
		t.Fatalf("calls = %d, want 2 immediate requests", len(callTimes))
	}
	if gap := callTimes[1].Sub(callTimes[0]); gap >= 100*time.Millisecond {
		t.Fatalf("request gap = %v, want page cooldown disabled when LimitRate <= 0", gap)
	}
}
