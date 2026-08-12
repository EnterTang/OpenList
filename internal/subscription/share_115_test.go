package subscription

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	_115sy "github.com/OpenListTeam/OpenList/v4/internal/115sy"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/go-resty/resty/v2"
)

func TestDecodePan115JSON_ReportsHTTPMetadataForHTMLResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte("\n<html><body>login required token=secret-value</body></html>"))
	}))
	defer server.Close()

	resp, err := resty.New().R().Get(server.URL)
	if err != nil {
		t.Fatalf("get response: %v", err)
	}

	var out map[string]any
	err = decodePan115JSON(resp, &out)
	if err == nil {
		t.Fatal("expected decode error")
	}
	msg := err.Error()
	for _, want := range []string{"decode 115 response", "status=403", "content-type=text/html", "kind=html", "body_len=", `first_non_space="<"`} {
		if !strings.Contains(msg, want) {
			t.Fatalf("error = %q, want substring %q", msg, want)
		}
	}
	for _, forbidden := range []string{"login required", "secret-value", "<html>"} {
		if strings.Contains(msg, forbidden) {
			t.Fatalf("error = %q, should not include response body secret %q", msg, forbidden)
		}
	}
}

func TestPan115ShareProviderClassifiesHTMLShareListResponse(t *testing.T) {
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("<html>gateway response</html>"))
	}))
	defer server.Close()

	client, err := _115sy.NewClient(_115sy.ClientOptions{
		Cookie:         "UID=1",
		AndroidBaseURL: server.URL,
		WebBaseURL:     server.URL,
	})
	if err != nil {
		t.Fatalf("new 115sy client: %v", err)
	}
	provider := NewPan115ShareProvider(model.SubscriptionTelegramPanConfig{Cookie: "UID=1"}).(*pan115ShareProvider)
	provider.receiveClient = client

	_, err = provider.ListShareChildren(context.Background(), ShareRef{
		Provider: ShareProviderPan115, ShareID: "share", Passcode: "pass",
	}, "0")
	if err == nil {
		t.Fatal("expected HTML response error")
	}
	if requests != 1 {
		t.Fatalf("request count = %d, want no retry for HTTP 200 HTML", requests)
	}
	if !strings.Contains(err.Error(), "content-type=text/html") || !strings.Contains(err.Error(), "kind=html") {
		t.Fatalf("error = %q, want response metadata", err)
	}
}

func TestPan115ShareProviderListsChildren(t *testing.T) {
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case _115sy.EndpointShareSnapshotApp:
			paths = append(paths, r.URL.Path)
			w.WriteHeader(http.StatusMethodNotAllowed)
			_, _ = w.Write([]byte(`{"state":false,"errno":0,"error":"unsupported"}`))
		case _115sy.EndpointShareSnapshot:
			paths = append(paths, r.URL.Path)
			query := r.URL.Query()
			if query.Get("share_code") != "swssal13zrk" || query.Get("receive_code") != "t58d" || query.Get("cid") != "0" {
				t.Fatalf("share snap query = %s", r.URL.RawQuery)
			}
			_, _ = w.Write([]byte(`{
				"state":true,
				"data":{"count":2,"list":[
					{"fid":"file-1","cid":"0","n":"Movie.mkv","s":1024,"t":"1700000000","ico":"mkv"},
					{"cid":1001,"n":"Season 1","fc":"0","t":"1700000001"}
				]}
			}`))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	client, err := _115sy.NewClient(_115sy.ClientOptions{
		Cookie:         "UID=1;CID=2",
		LimitRate:      1e6,
		AndroidBaseURL: server.URL,
		WebBaseURL:     server.URL,
	})
	if err != nil {
		t.Fatalf("new 115sy client: %v", err)
	}
	provider := NewPan115ShareProvider(model.SubscriptionTelegramPanConfig{Cookie: "UID=1;CID=2"})
	provider.(*pan115ShareProvider).webURL = server.URL
	provider.(*pan115ShareProvider).receiveClient = client
	ref := ShareRef{Provider: ShareProviderPan115, RawURL: "https://115cdn.com/s/swssal13zrk?password=t58d", ShareID: "swssal13zrk", Passcode: "t58d"}

	items, err := provider.ListShareChildren(context.Background(), ref, "")
	if err != nil {
		t.Fatalf("list children: %v", err)
	}
	if got := strings.Join(paths, ","); got != _115sy.EndpointShareSnapshotApp+","+_115sy.EndpointShareSnapshot {
		t.Fatalf("endpoint sequence = %q", got)
	}
	if len(items) != 2 {
		t.Fatalf("items = %#v, want 2", items)
	}
	if items[0].ID != "file-1" || items[0].ParentID != "0" || items[0].Name != "Movie.mkv" || items[0].IsDir {
		t.Fatalf("file item = %#v", items[0])
	}
	if items[0].Size != 1024 || !items[0].Modified.Equal(time.Unix(1700000000, 0)) {
		t.Fatalf("file metadata = %#v", items[0])
	}
	if items[1].ID != "1001" || items[1].ParentID != "0" || !items[1].IsDir {
		t.Fatalf("dir item = %#v", items[1])
	}
}

func TestPan115ShareProviderListsChildrenThrough115syClient(t *testing.T) {
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case _115sy.EndpointShareSnapshotApp:
			if r.Header.Get("app") != string(_115sy.ProfileAndroid) || r.Header.Get("appversion") == "" {
				t.Fatalf("android headers = app=%q appversion=%q", r.Header.Get("app"), r.Header.Get("appversion"))
			}
			if !strings.Contains(r.Header.Get("Cookie"), "UID=1") {
				t.Fatalf("cookie = %q, want UID=1", r.Header.Get("Cookie"))
			}
			w.WriteHeader(http.StatusMethodNotAllowed)
			_, _ = w.Write([]byte(`{"state":false,"errno":0,"error":"unsupported"}`))
		case _115sy.EndpointShareSnapshot:
			if r.URL.Query().Get("share_code") != "share" || r.URL.Query().Get("receive_code") != "pass" || r.URL.Query().Get("cid") != "dir-1" {
				t.Fatalf("share snapshot query = %s", r.URL.RawQuery)
			}
			_, _ = w.Write([]byte(`{
				"state":true,
				"data":{"count":1,"list":[
					{"fid":"nested-file","pid":"dir-1","n":"episode.mkv","fc":"1","s":"20"}
				]}
			}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	client, err := _115sy.NewClient(_115sy.ClientOptions{
		Cookie:         "UID=1;CID=2",
		LimitRate:      1e6,
		AndroidBaseURL: server.URL,
		WebBaseURL:     server.URL,
	})
	if err != nil {
		t.Fatalf("new 115sy client: %v", err)
	}
	provider := NewPan115ShareProvider(model.SubscriptionTelegramPanConfig{Cookie: "UID=1;CID=2"}).(*pan115ShareProvider)
	provider.webURL = server.URL
	provider.receiveClient = client
	ref := ShareRef{Provider: ShareProviderPan115, ShareID: "share", Passcode: "pass"}

	items, err := provider.ListShareChildren(context.Background(), ref, "dir-1")
	if err != nil {
		t.Fatalf("list children: %v", err)
	}
	if got := strings.Join(paths, ","); got != _115sy.EndpointShareSnapshotApp+","+_115sy.EndpointShareSnapshot {
		t.Fatalf("endpoint sequence = %q", got)
	}
	if len(items) != 1 || items[0].ID != "nested-file" || items[0].ParentID != "dir-1" || items[0].Name != "episode.mkv" {
		t.Fatalf("items = %#v, want the child under dir-1", items)
	}
}

func TestPan115ShareProviderRetriesRateLimitedResponse(t *testing.T) {
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		if requestCount == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":"rate limit exceeded"}`))
			return
		}
		_, _ = w.Write([]byte(`{"state":true,"data":{"count":0,"list":[]}}`))
	}))
	defer server.Close()

	client, err := _115sy.NewClient(_115sy.ClientOptions{
		Cookie:         "UID=1;CID=2",
		AndroidBaseURL: server.URL,
		WebBaseURL:     server.URL,
	})
	if err != nil {
		t.Fatalf("new 115sy client: %v", err)
	}
	provider := NewPan115ShareProvider(model.SubscriptionTelegramPanConfig{Cookie: "UID=1;CID=2"}).(*pan115ShareProvider)
	provider.webURL = server.URL
	provider.receiveClient = client
	ref := ShareRef{Provider: ShareProviderPan115, RawURL: server.URL + "/s/share?password=code", ShareID: "share", Passcode: "code"}

	items, err := provider.ListShareChildren(context.Background(), ref, "")
	if err != nil {
		t.Fatalf("list children after rate limit: %v", err)
	}
	if len(items) != 0 || requestCount != 2 {
		t.Fatalf("items=%#v request count=%d, want one retry and empty result", items, requestCount)
	}
}

func TestPan115ShareProviderGetsDirectURLAndFallsBackFromModernEndpoint(t *testing.T) {
	var endpoints []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		endpoints = append(endpoints, r.URL.Path)
		if r.URL.Path == "/modern/share/downurl" {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.WriteHeader(http.StatusMethodNotAllowed)
			_, _ = w.Write([]byte("<html>method not allowed</html>"))
			return
		}
		if r.URL.Path != "/legacy/share/downurl" {
			t.Fatalf("unexpected direct endpoint: %s", r.URL.Path)
		}
		query := r.URL.Query()
		if query.Get("share_code") != "share-1" || query.Get("receive_code") != "pwd-1" || query.Get("file_id") != "file-1" || query.Get("dl") != "1" {
			t.Fatalf("direct query = %s", r.URL.RawQuery)
		}
		if got := r.Header.Get("Cookie"); got != "UID=1;CID=2" {
			t.Fatalf("cookie = %q", got)
		}
		_, _ = w.Write([]byte(`{"state":true,"data":{"fid":"file-1","fn":"Movie.mkv","fs":1024,"url":{"url":"https://download.example/movie.mkv"}}}`))
	}))
	defer server.Close()

	provider := NewPan115ShareProvider(model.SubscriptionTelegramPanConfig{Cookie: "UID=1;CID=2"}).(*pan115ShareProvider)
	provider.limiter = newPan115RateLimiter(0)
	provider.retryBaseDelay = 0
	provider.directAppURL = server.URL + "/modern/share/downurl"
	provider.directWebURL = server.URL + "/legacy/share/downurl"
	ref := ShareRef{Provider: ShareProviderPan115, ShareID: "share-1", Passcode: "pwd-1"}

	link, err := provider.GetShareDownloadURL(context.Background(), ref, ShareItem{ID: "file-1", Name: "Movie.mkv", Size: 1024})
	if err != nil {
		t.Fatalf("get direct URL: %v", err)
	}
	if link.URL != "https://download.example/movie.mkv" || link.FileID != "file-1" || link.Size != 1024 {
		t.Fatalf("direct link = %#v", link)
	}
	if got := strings.Join(endpoints, ","); got != "/modern/share/downurl,/legacy/share/downurl" {
		t.Fatalf("endpoint sequence = %q", got)
	}
}

func TestPan115ShareProviderRejectsDirectoryWithoutDirectFallbackLoop(t *testing.T) {
	provider := NewPan115ShareProvider(model.SubscriptionTelegramPanConfig{Cookie: "UID=1;CID=2"}).(*pan115ShareProvider)
	provider.limiter = newPan115RateLimiter(0)
	ref := ShareRef{Provider: ShareProviderPan115, ShareID: "share-1", Passcode: "pwd-1"}
	_, err := provider.GetShareDownloadURL(context.Background(), ref, ShareItem{ID: "dir-1", Name: "Season", IsDir: true})
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "directory") {
		t.Fatalf("error = %v, want directory validation error", err)
	}
}

func TestPan115ShareProviderSavesItems(t *testing.T) {
	var receiveCalled bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case _115sy.EndpointShareReceiveApp:
			receiveCalled = true
			if r.Header.Get("app") != string(_115sy.ProfileAndroid) || r.Header.Get("appversion") == "" {
				t.Fatalf("android headers = app=%q appversion=%q", r.Header.Get("app"), r.Header.Get("appversion"))
			}
			if err := r.ParseForm(); err != nil {
				t.Fatalf("parse form: %v", err)
			}
			if r.Form.Get("cid") != "dst-dir" || r.Form.Get("share_code") != "swssal13zrk" || r.Form.Get("receive_code") != "t58d" || r.Form.Get("file_id") != "file-1" {
				t.Fatalf("receive form = %#v", r.Form)
			}
			_, _ = w.Write([]byte(`{"state":true}`))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	provider := NewPan115ShareProvider(model.SubscriptionTelegramPanConfig{Cookie: " UID=1;CID=2 "})
	receiveClient, err := _115sy.NewClient(_115sy.ClientOptions{
		Cookie:         "UID=1;CID=2",
		LimitRate:      1e6,
		AndroidBaseURL: server.URL,
		WebBaseURL:     server.URL,
	})
	if err != nil {
		t.Fatalf("new receive client: %v", err)
	}
	provider.(*pan115ShareProvider).receiveClient = receiveClient
	ref := ShareRef{Provider: ShareProviderPan115, RawURL: "https://115cdn.com/s/swssal13zrk?password=t58d", ShareID: "swssal13zrk", Passcode: "t58d"}
	items := []ShareItem{{ID: "file-1", Name: "Movie.mkv", Raw: map[string]any{"share_fid_token": "file-1"}}}

	taskIDs, err := provider.SaveShareItems(context.Background(), ref, "", items, "dst-dir")
	if err != nil {
		t.Fatalf("save items: %v", err)
	}
	if got, want := strings.Join(taskIDs, ","), "pan115_sync_swssal13zrk"; got != want {
		t.Fatalf("task ids = %q, want %q", got, want)
	}
	if err := provider.WaitSaveComplete(context.Background(), taskIDs); err != nil {
		t.Fatalf("wait task: %v", err)
	}
	if !receiveCalled {
		t.Fatal("receive endpoint was not called")
	}
}

func TestPan115ShareProviderDoesNotRetryAmbiguousHTMLReceive(t *testing.T) {
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("<html>gateway response</html>"))
	}))
	defer server.Close()

	receiveClient, err := _115sy.NewClient(_115sy.ClientOptions{
		Cookie:         "UID=1;CID=2",
		AndroidBaseURL: server.URL,
		WebBaseURL:     server.URL,
	})
	if err != nil {
		t.Fatalf("new receive client: %v", err)
	}
	provider := NewPan115ShareProvider(model.SubscriptionTelegramPanConfig{Cookie: "UID=1;CID=2"}).(*pan115ShareProvider)
	provider.receiveClient = receiveClient

	_, err = provider.SaveShareItems(context.Background(), ShareRef{Provider: ShareProviderPan115, ShareID: "share", Passcode: "pass"}, "", []ShareItem{{ID: "file-1", Name: "Movie.mkv"}}, "dst-dir")
	if err == nil {
		t.Fatal("expected ambiguous receive error")
	}
	if requests != 1 {
		t.Fatalf("receive requests = %d, want one request", requests)
	}
	var coded interface{ ClusterErrorCode() string }
	if !errors.As(err, &coded) || coded.ClusterErrorCode() != pan115ClusterErrorCodeShareSaveResultUnknown {
		t.Fatalf("error code = %v, want %q", err, pan115ClusterErrorCodeShareSaveResultUnknown)
	}
	if !strings.Contains(err.Error(), "status=200") || !strings.Contains(err.Error(), "content-type=text/html") {
		t.Fatalf("error = %q, want response metadata", err)
	}
}

func TestPan115ErrorClassifiesPermanentCredentialFailures(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		message   string
		wantCode  string
		wantCoded bool
	}{
		{name: "secret key error", message: "密钥错误", wantCode: "share_save_credentials_invalid", wantCoded: true},
		{name: "invalid signature", message: "签名无效，请重新登录", wantCode: "share_save_credentials_invalid", wantCoded: true},
		{name: "invalid refresh token", message: "refresh_token无效", wantCode: "share_save_credentials_invalid", wantCoded: true},
		{name: "ordinary business error", message: "文件不存在", wantCoded: false},
		{name: "blank message", message: "   ", wantCoded: false},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := pan115Error(tc.message)
			if err == nil {
				t.Fatal("expected error")
			}

			var coded interface{ ClusterErrorCode() string }
			gotCoded := errors.As(err, &coded)
			if gotCoded != tc.wantCoded {
				t.Fatalf("coded=%v, want %v, err=%v", gotCoded, tc.wantCoded, err)
			}
			if tc.wantCoded && coded.ClusterErrorCode() != tc.wantCode {
				t.Fatalf("cluster error code = %q, want %q", coded.ClusterErrorCode(), tc.wantCode)
			}
		})
	}
}

func TestPan115HTML405IsNotRetriedAndIsClassified(t *testing.T) {
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_, _ = w.Write([]byte("<html>method not allowed</html>"))
	}))
	defer server.Close()

	client, err := _115sy.NewClient(_115sy.ClientOptions{
		Cookie:         "UID=1",
		AndroidBaseURL: server.URL,
		WebBaseURL:     server.URL,
	})
	if err != nil {
		t.Fatalf("new 115sy client: %v", err)
	}
	provider := NewPan115ShareProvider(model.SubscriptionTelegramPanConfig{Cookie: "UID=1"}).(*pan115ShareProvider)
	provider.webURL = server.URL
	provider.receiveClient = client
	_, err = provider.ListShareChildren(context.Background(), ShareRef{
		Provider: ShareProviderPan115, ShareID: "share", Passcode: "pass",
	}, "0")
	if err == nil {
		t.Fatal("expected 405 HTML error")
	}
	if requests != 2 {
		t.Fatalf("405 request count = %d, want one profile fallback", requests)
	}
	var coded interface{ ClusterErrorCode() string }
	if !errors.As(err, &coded) || coded.ClusterErrorCode() != pan115ClusterErrorCodeShareSaveMethodNotAllowed {
		t.Fatalf("405 error code = %v, want %q", err, pan115ClusterErrorCodeShareSaveMethodNotAllowed)
	}
}

func TestPan115ErrorClassifiesRetryAndPermanentProviderFailures(t *testing.T) {
	tests := []struct {
		message string
		want    string
	}{
		{message: "rate limit exceeded", want: pan115ClusterErrorCodeShareSaveRateLimited},
		{message: "操作太频繁，请稍候再试", want: pan115ClusterErrorCodeShareSaveRateLimited},
		{message: "请求异常需要重试", want: pan115ClusterErrorCodeShareSaveTransient},
		{message: "分享已失效或取消", want: pan115ClusterErrorCodeShareSaveSourceInvalid},
	}
	for _, tc := range tests {
		t.Run(tc.message, func(t *testing.T) {
			err := pan115Error(tc.message)
			var coded interface{ ClusterErrorCode() string }
			if !errors.As(err, &coded) || coded.ClusterErrorCode() != tc.want {
				t.Fatalf("error code = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestPan115ShareProviderConfirmsSavedItemsWhenEnabled(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != _115sy.EndpointFileList {
			t.Fatalf("confirmation endpoint = %s, want %s", r.URL.Path, _115sy.EndpointFileList)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"state":true,"errno":0,"data":{"items":[{"id":"saved-1","name":"Movie.mkv","size":1024,"is_dir":false}]}}`))
	}))
	defer server.Close()

	client, err := _115sy.NewClient(_115sy.ClientOptions{
		Cookie:         "UID=1;CID=2;SEID=3",
		AndroidBaseURL: server.URL,
		WebBaseURL:     server.URL,
	})
	if err != nil {
		t.Fatalf("new confirmation client: %v", err)
	}
	provider := &pan115ShareProvider{confirmEnabled: true, confirmClient: client}
	taskID := encodePan115SaveOperation(pan115SaveOperation{
		ShareID:       "share-id",
		DestinationID: "dst-dir",
		Items:         []pan115SaveItem{{Name: "Movie.mkv", Size: 1024}},
	})
	if strings.Contains(taskID, "passcode") {
		t.Fatalf("confirmation token leaked passcode: %q", taskID)
	}
	if err := provider.WaitSaveComplete(context.Background(), []string{taskID}); err != nil {
		t.Fatalf("wait save confirmation: %v", err)
	}
}
