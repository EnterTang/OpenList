package subscription

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

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
	if got, want := strings.Join(taskIDs, ","), "pan123_sync_7Tx1jv-pVu7v"; got != want {
		t.Fatalf("task ids = %q, want %q", got, want)
	}
	if err := provider.WaitSaveComplete(context.Background(), taskIDs); err != nil {
		t.Fatalf("wait task: %v", err)
	}
	if !saveCalled {
		t.Fatal("save endpoint was not called")
	}
}
