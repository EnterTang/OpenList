package subscription

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestPan123ShareProviderGetsShortLivedDirectURLWithoutPersistingCredentials(t *testing.T) {
	var directURL string
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/b/api/share/download/info":
			if got := r.Header.Get("authorization"); got != "Bearer access-123" {
				t.Fatalf("authorization = %q, want bearer token", got)
			}
			_, _ = w.Write([]byte(`{"code":0,"data":{"DownloadURL":"` + server.URL + `/gateway"}}`))
		case "/gateway":
			if got := r.Header.Get("referer"); got == "" {
				t.Fatal("share direct request missing referer")
			}
			directURL = server.URL + "/download/file"
			http.Redirect(w, r, directURL, http.StatusFound)
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	provider := NewPan123ShareProvider(model.SubscriptionTelegramPanConfig{AccessToken: "access-123"}).(*pan123ShareProvider)
	provider.apiURL = server.URL + "/b/api"
	ref := ShareRef{Provider: ShareProviderPan123, ShareID: "share-key", Passcode: "share-pwd"}
	link, err := provider.GetShareDownloadURL(context.Background(), ref, ShareItem{ID: "7", Name: "Movie.mkv", Size: 12, Raw: map[string]any{"etag": "etag-7", "s3key_flag": "flag"}})
	if err != nil {
		t.Fatalf("get share direct url: %v", err)
	}
	if link.URL != directURL || link.FileID != "7" || link.Size != 12 || !link.ExpiresAt.IsZero() {
		t.Fatalf("direct link = %#v, want resolved link without guessed expiry", link)
	}
}

func TestPan123ShareProviderListsChildren(t *testing.T) {
	var listCalled bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/b/api/share/get":
			listCalled = true
			query := r.URL.Query()
			if query.Get("shareKey") != "7Tx1jv-pVu7v" || query.Get("SharePwd") != "xoxo" || query.Get("ParentFileId") != "0" {
				t.Fatalf("share list query = %s", r.URL.RawQuery)
			}
			_, _ = w.Write([]byte(`{
				"code":0,
				"data":{"InfoList":[
					{"FileId":101,"FileName":"Movie.mkv","Type":0,"Size":1024,"Etag":"etag-file","UpdateAt":"2023-11-14T22:13:20Z"},
					{"FileId":102,"FileName":"Season 1","Type":1,"Size":0,"Etag":"","UpdateAt":"2023-11-14T22:13:21Z"}
				],"Next":"-1"}
			}`))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	provider := NewPan123ShareProvider(model.SubscriptionTelegramPanConfig{AccessToken: "access-123"})
	provider.(*pan123ShareProvider).apiURL = server.URL + "/b/api"
	ref := ShareRef{Provider: ShareProviderPan123, RawURL: "https://www.123pan.com/s/7Tx1jv-pVu7v?pwd=xoxo", ShareID: "7Tx1jv-pVu7v", Passcode: "xoxo"}

	items, err := provider.ListShareChildren(context.Background(), ref, "")
	if err != nil {
		t.Fatalf("list children: %v", err)
	}
	if !listCalled {
		t.Fatal("list endpoint was not called")
	}
	if len(items) != 2 {
		t.Fatalf("items = %#v, want 2", items)
	}
	if items[0].ID != "101" || items[0].ParentID != "0" || items[0].Name != "Movie.mkv" || items[0].IsDir {
		t.Fatalf("file item = %#v", items[0])
	}
	if items[0].Size != 1024 || !items[0].Modified.Equal(time.Date(2023, 11, 14, 22, 13, 20, 0, time.UTC)) {
		t.Fatalf("file metadata = %#v", items[0])
	}
	if items[1].ID != "102" || !items[1].IsDir {
		t.Fatalf("dir item = %#v", items[1])
	}
}

func TestPan123ShareProviderListsChildrenAcrossPagesAndDedupes(t *testing.T) {
	var pages []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/b/api/share/get":
			page := r.URL.Query().Get("Page")
			pages = append(pages, page)
			switch page {
			case "1":
				_, _ = w.Write([]byte(`{
					"code":0,
					"data":{"InfoList":[
						{"FileId":101,"FileName":"Movie.mkv","Type":0,"Size":1024,"Etag":"etag-101","UpdateAt":"2023-11-14T22:13:20Z"},
						{"FileId":102,"FileName":"Episode 2.mkv","Type":0,"Size":2048,"Etag":"etag-102","UpdateAt":"2023-11-14T22:13:21Z"}
					],"Next":"cursor-2"}
				}`))
			case "2":
				_, _ = w.Write([]byte(`{
					"code":0,
					"data":{"InfoList":[
						{"FileId":102,"FileName":"Episode 2.mkv","Type":0,"Size":2048,"Etag":"etag-102","UpdateAt":"2023-11-14T22:13:21Z"},
						{"FileId":103,"FileName":"Episode 3.mkv","Type":0,"Size":4096,"Etag":"etag-103","UpdateAt":"2023-11-14T22:13:22Z"}
					],"Next":"-1"}
				}`))
			default:
				t.Fatalf("unexpected page query: %q", page)
			}
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	provider := NewPan123ShareProvider(model.SubscriptionTelegramPanConfig{AccessToken: "access-123"})
	provider.(*pan123ShareProvider).apiURL = server.URL + "/b/api"
	ref := ShareRef{Provider: ShareProviderPan123, RawURL: "https://www.123pan.com/s/7Tx1jv-pVu7v?pwd=xoxo", ShareID: "7Tx1jv-pVu7v", Passcode: "xoxo"}

	items, err := provider.ListShareChildren(context.Background(), ref, "")
	if err != nil {
		t.Fatalf("list children: %v", err)
	}
	if got := strings.Join(pages, ","); got != "1,2" {
		t.Fatalf("requested pages = %q, want %q", got, "1,2")
	}
	if len(items) != 3 {
		t.Fatalf("items = %#v, want 3 deduped entries", items)
	}
	if items[0].ID != "101" || items[1].ID != "102" || items[2].ID != "103" {
		t.Fatalf("deduped order = %#v, want FileId 101,102,103", items)
	}
}

func TestPan123ShareProviderListsFastLinkAsSingleFile(t *testing.T) {
	provider := NewPan123ShareProvider(model.SubscriptionTelegramPanConfig{AccessToken: "access-123"})
	ref := ShareRef{
		Provider: ShareProviderPan123,
		RawURL:   "123FSLinkV2$a3531a60736740a152e931a6ecee9bfb#500797103#食神·百厨大战.2025.S02E05.mp4",
		ShareID:  "a3531a60736740a152e931a6ecee9bfb",
		ParentID: "0",
	}

	items, err := provider.ListShareChildren(context.Background(), ref, "")
	if err != nil {
		t.Fatalf("list fastlink children: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("items = %#v, want 1", items)
	}
	if items[0].ID != "a3531a60736740a152e931a6ecee9bfb" || items[0].Name != "食神·百厨大战.2025.S02E05.mp4" || items[0].Size != 500797103 || items[0].IsDir {
		t.Fatalf("item = %#v, want fastlink single file", items[0])
	}
	if got := rawString(shareItemRawMap(items[0]), "etag"); got != "a3531a60736740a152e931a6ecee9bfb" {
		t.Fatalf("etag = %q, want fastlink etag", got)
	}
}

func TestPan123ShareProviderSavesItems(t *testing.T) {
	var saveCalled bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/b/api/file/upload_request":
			saveCalled = true
			if got := r.Header.Get("authorization"); got != "Bearer access-123" {
				t.Fatalf("authorization = %q, want Bearer access-123", got)
			}
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode save body: %v", err)
			}
			if body["etag"] != "etag-file" || body["fileName"] != "Movie.mkv" || body["parentFileId"] != "dst-dir" {
				t.Fatalf("save body = %#v", body)
			}
			_, _ = w.Write([]byte(`{"code":0,"data":{"Info":{"FileId":201}}}`))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	provider := NewPan123ShareProvider(model.SubscriptionTelegramPanConfig{AccessToken: " access-123 "})
	provider.(*pan123ShareProvider).apiURL = server.URL + "/b/api"
	ref := ShareRef{Provider: ShareProviderPan123, RawURL: "https://www.123pan.com/s/7Tx1jv-pVu7v?pwd=xoxo", ShareID: "7Tx1jv-pVu7v", Passcode: "xoxo"}
	items := []ShareItem{{
		ID:   "101",
		Name: "Movie.mkv",
		Size: 1024,
		Raw:  map[string]any{"etag": "etag-file", "size": float64(1024), "file_name": "Movie.mkv"},
	}}

	taskIDs, err := provider.SaveShareItems(context.Background(), ref, "0", items, "dst-dir")
	if err != nil {
		t.Fatalf("save items: %v", err)
	}
	if len(taskIDs) != 1 {
		t.Fatalf("task ids = %#v, want one per-file result token", taskIDs)
	}
	if err := provider.WaitSaveComplete(context.Background(), taskIDs); err != nil {
		t.Fatalf("wait task: %v", err)
	}
	if !saveCalled {
		t.Fatal("save endpoint was not called")
	}
}

func TestPan123ShareProviderSaveShareItemsConfirmsViaProbeAfterAmbiguousResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/b/api/file/upload_request":
			http.Error(w, "bad gateway", http.StatusBadGateway)
		case "/b/api/file/list/new":
			if got := r.Header.Get("authorization"); got != "Bearer access-123" {
				t.Fatalf("probe authorization = %q, want Bearer access-123", got)
			}
			_, _ = w.Write([]byte(`{
				"code":0,
				"data":{"Next":"-1","Total":1,"InfoList":[
					{"FileId":201,"FileName":"Movie.mkv","Type":0,"Size":1024,"Etag":"etag-file","UpdateAt":"2023-11-14T22:13:20Z"}
				]}
			}`))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	provider := NewPan123ShareProvider(model.SubscriptionTelegramPanConfig{AccessToken: "access-123"})
	provider.(*pan123ShareProvider).apiURL = server.URL + "/b/api"
	ref := ShareRef{Provider: ShareProviderPan123, RawURL: "https://www.123pan.com/s/7Tx1jv-pVu7v?pwd=xoxo", ShareID: "7Tx1jv-pVu7v", Passcode: "xoxo"}
	items := []ShareItem{{
		ID:   "101",
		Name: "Movie.mkv",
		Size: 1024,
		Raw:  map[string]any{"etag": "etag-file", "size": float64(1024), "file_name": "Movie.mkv"},
	}}

	taskIDs, err := provider.SaveShareItems(context.Background(), ref, "0", items, "dst-dir")
	if err != nil {
		t.Fatalf("save items: %v", err)
	}
	if len(taskIDs) != 1 {
		t.Fatalf("task ids = %#v, want one per-file result token", taskIDs)
	}
	if err := provider.WaitSaveComplete(context.Background(), taskIDs); err != nil {
		t.Fatalf("wait task: %v", err)
	}
}

func TestPan123ShareProviderSaveShareItemsReturnsRetryableResultAfterProbeMiss(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/b/api/file/upload_request":
			http.Error(w, "too many requests", http.StatusTooManyRequests)
		case "/b/api/file/list/new":
			_, _ = w.Write([]byte(`{"code":0,"data":{"Next":"-1","Total":0,"InfoList":[]}}`))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	provider := NewPan123ShareProvider(model.SubscriptionTelegramPanConfig{AccessToken: "access-123"})
	provider.(*pan123ShareProvider).apiURL = server.URL + "/b/api"
	ref := ShareRef{Provider: ShareProviderPan123, RawURL: "https://www.123pan.com/s/7Tx1jv-pVu7v?pwd=xoxo", ShareID: "7Tx1jv-pVu7v", Passcode: "xoxo"}
	items := []ShareItem{{ID: "101", Name: "Movie.mkv", Size: 1024, Raw: map[string]any{"etag": "etag-file", "size": float64(1024), "file_name": "Movie.mkv"}}}

	taskIDs, err := provider.SaveShareItems(context.Background(), ref, "0", items, "dst-dir")
	if err != nil {
		t.Fatalf("save items: %v", err)
	}
	err = provider.WaitSaveComplete(context.Background(), taskIDs)
	var coded interface{ ClusterErrorCode() string }
	if !errors.As(err, &coded) || coded.ClusterErrorCode() != "share_save_retryable" {
		t.Fatalf("wait error = %v, want share_save_retryable", err)
	}
}

func TestPan123ShareProviderSaveShareItemsReturnsUnknownResultAfterMalformedSuccessBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/b/api/file/upload_request":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"code":0,"data":`))
		case "/b/api/file/list/new":
			_, _ = w.Write([]byte(`{"code":0,"data":{"Next":"-1","Total":0,"InfoList":[]}}`))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	provider := NewPan123ShareProvider(model.SubscriptionTelegramPanConfig{AccessToken: "access-123"})
	provider.(*pan123ShareProvider).apiURL = server.URL + "/b/api"
	ref := ShareRef{Provider: ShareProviderPan123, RawURL: "https://www.123pan.com/s/7Tx1jv-pVu7v?pwd=xoxo", ShareID: "7Tx1jv-pVu7v", Passcode: "xoxo"}
	items := []ShareItem{{ID: "101", Name: "Movie.mkv", Size: 1024, Raw: map[string]any{"etag": "etag-file", "size": float64(1024), "file_name": "Movie.mkv"}}}

	taskIDs, err := provider.SaveShareItems(context.Background(), ref, "0", items, "dst-dir")
	if err != nil {
		t.Fatalf("save items: %v", err)
	}
	err = provider.WaitSaveComplete(context.Background(), taskIDs)
	var coded interface{ ClusterErrorCode() string }
	if !errors.As(err, &coded) || coded.ClusterErrorCode() != "share_save_result_unknown" {
		t.Fatalf("wait error = %v, want share_save_result_unknown", err)
	}
}

func TestPan123ShareProviderSaveShareItemsReturnsTerminalResultForExplicitAPIRejection(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/b/api/file/upload_request":
			_, _ = w.Write([]byte(`{"code":401,"message":"invalid access token"}`))
		case "/b/api/file/list/new":
			t.Fatal("terminal rejection should not probe destination")
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	provider := NewPan123ShareProvider(model.SubscriptionTelegramPanConfig{AccessToken: "access-123"})
	provider.(*pan123ShareProvider).apiURL = server.URL + "/b/api"
	ref := ShareRef{Provider: ShareProviderPan123, RawURL: "https://www.123pan.com/s/7Tx1jv-pVu7v?pwd=xoxo", ShareID: "7Tx1jv-pVu7v", Passcode: "xoxo"}
	items := []ShareItem{{ID: "101", Name: "Movie.mkv", Size: 1024, Raw: map[string]any{"etag": "etag-file", "size": float64(1024), "file_name": "Movie.mkv"}}}

	taskIDs, err := provider.SaveShareItems(context.Background(), ref, "0", items, "dst-dir")
	if err != nil {
		t.Fatalf("save items: %v", err)
	}
	err = provider.WaitSaveComplete(context.Background(), taskIDs)
	var coded interface{ ClusterErrorCode() string }
	if !errors.As(err, &coded) || coded.ClusterErrorCode() != "share_save_terminal" {
		t.Fatalf("wait error = %v, want share_save_terminal", err)
	}
}
