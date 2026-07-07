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

func TestQuarkShareProviderListsChildren(t *testing.T) {
	var tokenCalled bool
	var detailCalled bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/1/clouddrive/share/sharepage/token":
			tokenCalled = true
			if r.Method != http.MethodPost {
				t.Fatalf("token method = %s, want POST", r.Method)
			}
			if got := r.Header.Get("Cookie"); got != "quark-cookie" {
				t.Fatalf("cookie = %q, want quark-cookie", got)
			}
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode token body: %v", err)
			}
			if body["pwd_id"] != "bc18e4ea5fb8" || body["passcode"] != "pass" {
				t.Fatalf("token body = %#v", body)
			}
			_, _ = w.Write([]byte(`{"code":0,"data":{"stoken":"stoken-1"}}`))
		case "/1/clouddrive/share/sharepage/detail":
			detailCalled = true
			if r.Method != http.MethodGet {
				t.Fatalf("detail method = %s, want GET", r.Method)
			}
			query := r.URL.Query()
			if query.Get("pwd_id") != "bc18e4ea5fb8" || query.Get("stoken") != "stoken-1" || query.Get("pdir_fid") != "dir-1" {
				t.Fatalf("detail query = %s", r.URL.RawQuery)
			}
			_, _ = w.Write([]byte(`{
				"code":0,
				"data":{"list":[
					{"fid":"file-1","pdir_fid":"dir-1","file_name":"Movie.mkv","dir":false,"size":1024,"updated_at":1700000000000,"share_fid_token":"token-file-1"},
					{"fid":"dir-2","pdir_fid":"dir-1","file_name":"Season 1","dir":true,"updated_at":1700000001000,"share_fid_token":"token-dir-2"}
				]},
				"metadata":{"_total":2}
			}`))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	provider := NewQuarkShareProvider(model.SubscriptionTelegramPanConfig{Cookie: " quark-cookie "})
	provider.(*quarkShareProvider).baseURL = server.URL + "/1/clouddrive"
	ref := ShareRef{Provider: ShareProviderQuark, RawURL: "https://pan.quark.cn/s/bc18e4ea5fb8", ShareID: "bc18e4ea5fb8", Passcode: "pass"}

	items, err := provider.ListShareChildren(context.Background(), ref, "dir-1")
	if err != nil {
		t.Fatalf("list children: %v", err)
	}
	if !tokenCalled || !detailCalled {
		t.Fatalf("tokenCalled=%v detailCalled=%v, want both true", tokenCalled, detailCalled)
	}
	if len(items) != 2 {
		t.Fatalf("items = %#v, want 2", items)
	}
	if items[0].ID != "file-1" || items[0].ParentID != "dir-1" || items[0].Name != "Movie.mkv" || items[0].IsDir {
		t.Fatalf("file item = %#v", items[0])
	}
	if items[0].Size != 1024 || !items[0].Modified.Equal(time.UnixMilli(1700000000000)) {
		t.Fatalf("file metadata = %#v", items[0])
	}
	if items[1].ID != "dir-2" || !items[1].IsDir {
		t.Fatalf("dir item = %#v", items[1])
	}
}

func TestQuarkShareProviderSavesItemsAndWaitsTask(t *testing.T) {
	var saveCalled bool
	var taskCalled bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/1/clouddrive/share/sharepage/token":
			_, _ = w.Write([]byte(`{"code":0,"data":{"stoken":"stoken-1"}}`))
		case "/1/clouddrive/share/sharepage/save":
			saveCalled = true
			if r.Method != http.MethodPost {
				t.Fatalf("save method = %s, want POST", r.Method)
			}
			var body struct {
				FIDList      []string `json:"fid_list"`
				FIDTokenList []string `json:"fid_token_list"`
				ToPDirFID    string   `json:"to_pdir_fid"`
				PwdID        string   `json:"pwd_id"`
				SToken       string   `json:"stoken"`
				PDirFID      string   `json:"pdir_fid"`
				Scene        string   `json:"scene"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode save body: %v", err)
			}
			if strings.Join(body.FIDList, ",") != "file-1" || strings.Join(body.FIDTokenList, ",") != "share-token-1" {
				t.Fatalf("save body file lists = %#v", body)
			}
			if body.ToPDirFID != "dst-dir" || body.PwdID != "bc18e4ea5fb8" || body.SToken != "stoken-1" || body.PDirFID != "parent-1" || body.Scene != "link" {
				t.Fatalf("save body = %#v", body)
			}
			_, _ = w.Write([]byte(`{"code":0,"data":{"task_id":"task-1"}}`))
		case "/1/clouddrive/task":
			taskCalled = true
			if r.Method != http.MethodGet {
				t.Fatalf("task method = %s, want GET", r.Method)
			}
			if r.URL.Query().Get("task_id") != "task-1" {
				t.Fatalf("task query = %s", r.URL.RawQuery)
			}
			_, _ = w.Write([]byte(`{"status":200,"data":{"status":2,"task_title":"save"}}`))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	provider := NewQuarkShareProvider(model.SubscriptionTelegramPanConfig{Cookie: "quark-cookie"})
	provider.(*quarkShareProvider).baseURL = server.URL + "/1/clouddrive"
	ref := ShareRef{Provider: ShareProviderQuark, RawURL: "https://pan.quark.cn/s/bc18e4ea5fb8", ShareID: "bc18e4ea5fb8"}
	items := []ShareItem{{
		ID:   "file-1",
		Name: "Movie.mkv",
		Raw:  map[string]any{"share_fid_token": "share-token-1"},
	}}

	taskIDs, err := provider.SaveShareItems(context.Background(), ref, "parent-1", items, "dst-dir")
	if err != nil {
		t.Fatalf("save items: %v", err)
	}
	if got, want := strings.Join(taskIDs, ","), "task-1"; got != want {
		t.Fatalf("task ids = %q, want %q", got, want)
	}
	if err := provider.WaitSaveComplete(context.Background(), taskIDs); err != nil {
		t.Fatalf("wait task: %v", err)
	}
	if !saveCalled || !taskCalled {
		t.Fatalf("saveCalled=%v taskCalled=%v, want both true", saveCalled, taskCalled)
	}
}
