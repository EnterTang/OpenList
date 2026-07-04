package _139

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestCreateMobileShareBuildsFolderPayload(t *testing.T) {
	setup139Resty(t)
	oldBaseURL := mobileShareOutLinkBaseURL
	t.Cleanup(func() {
		mobileShareOutLinkBaseURL = oldBaseURL
	})

	var body map[string]any
	var cookieHeader string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != mobileShareOutLinkPath {
			t.Fatalf("path = %q, want %s", r.URL.Path, mobileShareOutLinkPath)
		}
		cookieHeader = r.Header.Get("Cookie")
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		write139JSON(t, w, map[string]any{
			"success": true,
			"data": map[string]any{
				"getOutLinkRes": map[string]any{
					"getOutLinkResSet": []map[string]any{{
						"linkID":  "link-id",
						"linkUrl": "https://share.example/folder",
						"passwd":  "abcd",
						"objID":   "folder-id",
					}},
				},
			},
		})
	}))
	defer server.Close()
	mobileShareOutLinkBaseURL = server.URL

	d := &Yun139{
		Account: "13900000000",
		Addition: Addition{
			Type:         MetaPersonalNew,
			CookieHeader: "auth_token=token; skey=skey",
		},
	}
	link, err := d.CreateMobileShare(context.Background(), &model.Object{
		ID:       "folder-id",
		Name:     "Folder",
		IsFolder: true,
	}, model.MobileShareCreateArgs{PeriodUnit: 1})
	if err != nil {
		t.Fatalf("CreateMobileShare returned error: %v", err)
	}
	if link.LinkID != "link-id" || link.ShareURL == "" || link.ExtractCode != "abcd" {
		t.Fatalf("link = %#v, want created share fields", link)
	}
	if cookieHeader != "auth_token=token; skey=skey" {
		t.Fatalf("Cookie header = %q, want configured cookie", cookieHeader)
	}
	req := body["getOutLinkReq"].(map[string]any)
	if got := req["dedicatedName"]; got != "Folder" {
		t.Fatalf("dedicatedName = %v, want Folder", got)
	}
	if got := req["caIDLst"].([]any); len(got) != 1 || got[0] != "folder-id" {
		t.Fatalf("caIDLst = %#v, want folder-id", got)
	}
	if got := req["coIDLst"].([]any); len(got) != 0 {
		t.Fatalf("coIDLst = %#v, want empty for folder", got)
	}
}

func TestCreateMobileShareBuildsFilePayload(t *testing.T) {
	setup139Resty(t)
	oldBaseURL := mobileShareOutLinkBaseURL
	t.Cleanup(func() {
		mobileShareOutLinkBaseURL = oldBaseURL
	})

	var body map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		write139JSON(t, w, map[string]any{
			"success": true,
			"data": map[string]any{
				"getOutLinkRes": map[string]any{
					"getOutLinkResSet": []map[string]any{{
						"linkID":  "link-id",
						"linkUrl": "https://share.example/file",
						"passwd":  "abcd",
					}},
				},
			},
		})
	}))
	defer server.Close()
	mobileShareOutLinkBaseURL = server.URL

	d := &Yun139{Account: "13900000000", Addition: Addition{Type: MetaPersonalNew}}
	if _, err := d.CreateMobileShare(context.Background(), &model.Object{
		ID:   "file-id",
		Name: "Movie.mkv",
	}, model.MobileShareCreateArgs{}); err != nil {
		t.Fatalf("CreateMobileShare returned error: %v", err)
	}
	req := body["getOutLinkReq"].(map[string]any)
	if got := req["coIDLst"].([]any); len(got) != 1 || got[0] != "file-id" {
		t.Fatalf("coIDLst = %#v, want file-id", got)
	}
	if got := req["caIDLst"].([]any); len(got) != 0 {
		t.Fatalf("caIDLst = %#v, want empty for file", got)
	}
	if got := req["periodUnit"]; got != float64(1) {
		t.Fatalf("periodUnit = %v, want default 1", got)
	}
}
