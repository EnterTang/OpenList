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

func TestAliyunDriveShareProviderListsChildren(t *testing.T) {
	var tokenCalled bool
	var listCalls []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v2/share_link/get_share_token":
			tokenCalled = true
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode share token body: %v", err)
			}
			if body["share_id"] != "odeXVKsEKxr" || body["share_pwd"] != "pwd1" {
				t.Fatalf("share token body = %#v", body)
			}
			_, _ = w.Write([]byte(`{"share_token":"share-token-1"}`))
		case "/adrive/v2/file/list_by_share":
			if got := r.Header.Get("x-share-token"); got != "share-token-1" {
				t.Fatalf("x-share-token = %q, want share-token-1", got)
			}
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode list body: %v", err)
			}
			if body["share_id"] != "odeXVKsEKxr" || body["parent_file_id"] != "root" {
				t.Fatalf("list body = %#v", body)
			}
			marker, _ := body["marker"].(string)
			listCalls = append(listCalls, marker)
			if marker == "" {
				_, _ = w.Write([]byte(`{
					"items":[
						{"file_id":"file-1","parent_file_id":"root","name":"Movie.mkv","type":"file","size":1024,"updated_at":"2023-11-14T22:13:20Z"}
					],
					"next_marker":"next-page"
				}`))
				return
			}
			if marker != "next-page" {
				t.Fatalf("marker = %q, want next-page", marker)
			}
			_, _ = w.Write([]byte(`{
				"items":[
					{"file_id":"dir-1","parent_file_id":"root","name":"Season 1","type":"folder","updated_at":"2023-11-14T22:13:21Z"}
				],
				"next_marker":""
			}`))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	provider := NewAliyunDriveShareProvider(model.SubscriptionTelegramPanConfig{RefreshToken: " refresh-1 "})
	p := provider.(*aliyunDriveShareProvider)
	p.apiURL = server.URL
	p.authURL = server.URL
	ref := ShareRef{Provider: ShareProviderAliyunDrive, RawURL: "https://www.alipan.com/s/odeXVKsEKxr", ShareID: "odeXVKsEKxr", Passcode: "pwd1"}

	items, err := provider.ListShareChildren(context.Background(), ref, "")
	if err != nil {
		t.Fatalf("list children: %v", err)
	}
	if !tokenCalled || !stringSlicesEqual(listCalls, []string{"", "next-page"}) {
		t.Fatalf("token=%v listCalls=%#v, want token and two paged list calls", tokenCalled, listCalls)
	}
	if len(items) != 2 {
		t.Fatalf("items = %#v, want 2", items)
	}
	if items[0].ID != "file-1" || items[0].ParentID != "root" || items[0].Name != "Movie.mkv" || items[0].IsDir {
		t.Fatalf("file item = %#v", items[0])
	}
	if items[0].Size != 1024 || !items[0].Modified.Equal(time.Date(2023, 11, 14, 22, 13, 20, 0, time.UTC)) {
		t.Fatalf("file metadata = %#v", items[0])
	}
	if items[1].ID != "dir-1" || !items[1].IsDir {
		t.Fatalf("dir item = %#v", items[1])
	}
}

func TestAliyunDriveShareProviderSavesItems(t *testing.T) {
	var batchCalled bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v2/account/token":
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode refresh body: %v", err)
			}
			if body["refresh_token"] != "refresh-1" || body["grant_type"] != "refresh_token" {
				t.Fatalf("refresh body = %#v", body)
			}
			_, _ = w.Write([]byte(`{"access_token":"access-1","refresh_token":"refresh-2","resource_drive_id":"resource-drive-1","default_drive_id":"default-drive-1"}`))
		case "/v2/share_link/get_share_token":
			_, _ = w.Write([]byte(`{"share_token":"share-token-1"}`))
		case "/adrive/v2/batch":
			batchCalled = true
			if got := r.Header.Get("Authorization"); got != "Bearer access-1" {
				t.Fatalf("authorization = %q, want Bearer access-1", got)
			}
			if got := r.Header.Get("x-share-token"); got != "share-token-1" {
				t.Fatalf("x-share-token = %q, want share-token-1", got)
			}
			var body struct {
				Resource string `json:"resource"`
				Requests []struct {
					ID     string         `json:"id"`
					Method string         `json:"method"`
					URL    string         `json:"url"`
					Body   map[string]any `json:"body"`
				} `json:"requests"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode batch body: %v", err)
			}
			if body.Resource != "file" || len(body.Requests) != 1 {
				t.Fatalf("batch body = %#v", body)
			}
			req := body.Requests[0]
			if req.ID != "file-1" || req.Method != http.MethodPost || req.URL != "/file/copy" {
				t.Fatalf("batch request = %#v", req)
			}
			if req.Body["file_id"] != "file-1" || req.Body["share_id"] != "odeXVKsEKxr" || req.Body["to_parent_file_id"] != "dst-dir" || req.Body["to_drive_id"] != "resource-drive-1" {
				t.Fatalf("copy body = %#v", req.Body)
			}
			_, _ = w.Write([]byte(`{"responses":[{"status":201,"body":{"file_id":"saved-1"}}]}`))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	provider := NewAliyunDriveShareProvider(model.SubscriptionTelegramPanConfig{RefreshToken: "refresh-1"})
	p := provider.(*aliyunDriveShareProvider)
	p.apiURL = server.URL
	p.authURL = server.URL
	ref := ShareRef{Provider: ShareProviderAliyunDrive, RawURL: "https://www.alipan.com/s/odeXVKsEKxr", ShareID: "odeXVKsEKxr"}
	items := []ShareItem{{ID: "file-1", Name: "Movie.mkv", Raw: map[string]any{"share_fid_token": "file-1"}}}

	taskIDs, err := provider.SaveShareItems(context.Background(), ref, "root", items, "dst-dir")
	if err != nil {
		t.Fatalf("save items: %v", err)
	}
	if got, want := strings.Join(taskIDs, ","), "saved-1"; got != want {
		t.Fatalf("task ids = %q, want %q", got, want)
	}
	if err := provider.WaitSaveComplete(context.Background(), taskIDs); err != nil {
		t.Fatalf("wait task: %v", err)
	}
	if !batchCalled {
		t.Fatal("batch endpoint was not called")
	}
}

func TestAliyunDriveShareProviderSavesItemsUsesConfiguredDriveID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v2/account/token":
			_, _ = w.Write([]byte(`{"access_token":"access-1","refresh_token":"refresh-2","resource_drive_id":"resource-drive-1"}`))
		case "/v2/share_link/get_share_token":
			_, _ = w.Write([]byte(`{"share_token":"share-token-1"}`))
		case "/adrive/v2/batch":
			var body struct {
				Requests []struct {
					Body map[string]any `json:"body"`
				} `json:"requests"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode batch body: %v", err)
			}
			if got := body.Requests[0].Body["to_drive_id"]; got != "configured-drive" {
				t.Fatalf("to_drive_id = %#v, want configured-drive", got)
			}
			_, _ = w.Write([]byte(`{"responses":[{"status":201,"body":{"file_id":"saved-1"}}]}`))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	provider := NewAliyunDriveShareProvider(model.SubscriptionTelegramPanConfig{RefreshToken: "refresh-1", DriveID: "configured-drive"})
	p := provider.(*aliyunDriveShareProvider)
	p.apiURL = server.URL
	p.authURL = server.URL
	ref := ShareRef{Provider: ShareProviderAliyunDrive, RawURL: "https://www.alipan.com/s/odeXVKsEKxr", ShareID: "odeXVKsEKxr"}
	items := []ShareItem{{ID: "file-1", Name: "Movie.mkv"}}

	if _, err := provider.SaveShareItems(context.Background(), ref, "root", items, "dst-dir"); err != nil {
		t.Fatalf("save items: %v", err)
	}
}

func TestAliyunDriveShareProviderFetchesResourceDriveID(t *testing.T) {
	var userInfoCalled bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v2/account/token":
			_, _ = w.Write([]byte(`{"access_token":"access-1","refresh_token":"refresh-2","default_drive_id":"default-drive-1"}`))
		case "/v2/user/get":
			userInfoCalled = true
			if got := r.Header.Get("Authorization"); got != "Bearer access-1" {
				t.Fatalf("authorization = %q, want Bearer access-1", got)
			}
			_, _ = w.Write([]byte(`{"default_drive_id":"default-drive-1","resource_drive_id":"resource-drive-1","backup_drive_id":"backup-drive-1"}`))
		case "/v2/share_link/get_share_token":
			_, _ = w.Write([]byte(`{"share_token":"share-token-1"}`))
		case "/adrive/v2/batch":
			var body struct {
				Requests []struct {
					Body map[string]any `json:"body"`
				} `json:"requests"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode batch body: %v", err)
			}
			if got := body.Requests[0].Body["to_drive_id"]; got != "resource-drive-1" {
				t.Fatalf("to_drive_id = %#v, want resource-drive-1", got)
			}
			_, _ = w.Write([]byte(`{"responses":[{"status":201,"body":{"file_id":"saved-1"}}]}`))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	provider := NewAliyunDriveShareProvider(model.SubscriptionTelegramPanConfig{
		RefreshToken: "refresh-1",
		DriveType:    "resource",
	})
	p := provider.(*aliyunDriveShareProvider)
	p.apiURL = server.URL
	p.authURL = server.URL
	p.userURL = server.URL
	ref := ShareRef{Provider: ShareProviderAliyunDrive, RawURL: "https://www.alipan.com/s/odeXVKsEKxr", ShareID: "odeXVKsEKxr"}
	items := []ShareItem{{ID: "file-1", Name: "Movie.mkv"}}

	if _, err := provider.SaveShareItems(context.Background(), ref, "root", items, "dst-dir"); err != nil {
		t.Fatalf("save items: %v", err)
	}
	if !userInfoCalled {
		t.Fatal("expected user drive info endpoint to be called")
	}
}

func TestAliyunDriveShareProviderRequiresRefreshTokenForTransferDriveID(t *testing.T) {
	provider := NewAliyunDriveShareProvider(model.SubscriptionTelegramPanConfig{AccessToken: "access-1"})
	ref := ShareRef{Provider: ShareProviderAliyunDrive, RawURL: "https://www.alipan.com/s/odeXVKsEKxr", ShareID: "odeXVKsEKxr"}
	items := []ShareItem{{ID: "file-1", Name: "Movie.mkv"}}

	_, err := provider.SaveShareItems(context.Background(), ref, "root", items, "dst-dir")
	if err == nil || !strings.Contains(err.Error(), "refresh_token or drive_id") {
		t.Fatalf("save error = %v, want refresh_token or drive_id error", err)
	}
}
