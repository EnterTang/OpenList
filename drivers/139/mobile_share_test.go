package _139

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/etfmeta"
	"github.com/OpenListTeam/OpenList/v4/internal/media/recognize"
	"github.com/OpenListTeam/OpenList/v4/internal/media/tmdb"
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

func TestDeleteMobileShareBuildsPayload(t *testing.T) {
	setup139Resty(t)
	oldBaseURL := mobileShareOutLinkBaseURL
	t.Cleanup(func() {
		mobileShareOutLinkBaseURL = oldBaseURL
	})

	var body map[string]any
	var cookieHeader string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != mobileShareDeleteOutLinkPath {
			t.Fatalf("path = %q, want %s", r.URL.Path, mobileShareDeleteOutLinkPath)
		}
		cookieHeader = r.Header.Get("Cookie")
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		write139JSON(t, w, map[string]any{"success": true})
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
	err := d.DeleteMobileShare(context.Background(), model.MobileShareDeleteArgs{
		LinkIDs: []string{" link-id ", "link-id", "link-2"},
	})
	if err != nil {
		t.Fatalf("DeleteMobileShare returned error: %v", err)
	}
	if cookieHeader != "auth_token=token; skey=skey" {
		t.Fatalf("Cookie header = %q, want configured cookie", cookieHeader)
	}
	req := body["delOutLinkReq"].(map[string]any)
	got := req["linkIDs"].([]any)
	if len(got) != 2 || got[0] != "link-id" || got[1] != "link-2" {
		t.Fatalf("linkIDs = %#v, want deduplicated link IDs", got)
	}
	accountInfo := req["commonAccountInfo"].(map[string]any)
	if accountInfo["account"] != "13900000000" || accountInfo["accountType"] != float64(1) {
		t.Fatalf("commonAccountInfo = %#v, want account info", accountInfo)
	}
}

func TestMobileShareGetAccountFallsBackToAuthorization(t *testing.T) {
	d := &Yun139{
		Addition: Addition{
			Authorization: base64.StdEncoding.EncodeToString([]byte("pc:13900000000:token")),
		},
	}
	if got := d.getAccount(); got != "13900000000" {
		t.Fatalf("account = %q, want authorization account", got)
	}
}

func TestCreateMobileShareDoesNotRetryOnNonRiskError(t *testing.T) {
	setup139Resty(t)
	oldBaseURL := mobileShareOutLinkBaseURL
	t.Cleanup(func() {
		mobileShareOutLinkBaseURL = oldBaseURL
	})

	shareCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != mobileShareOutLinkPath {
			t.Fatalf("path = %q, want %s", r.URL.Path, mobileShareOutLinkPath)
		}
		shareCalls++
		write139JSON(t, w, map[string]any{"success": false, "message": "普通失败"})
	}))
	defer server.Close()
	mobileShareOutLinkBaseURL = server.URL

	d := &Yun139{
		PersonalCloudHost: server.URL,
		Addition:          Addition{Type: MetaPersonalNew, AutoRenameOnShareRisk: true},
	}
	if _, err := d.CreateMobileShare(context.Background(), &model.Object{ID: "file-id", Name: "非分之罪 S01E01.etf", Path: "/"}, model.MobileShareCreateArgs{}); err == nil {
		t.Fatal("expected error")
	}
	if shareCalls != 1 {
		t.Fatalf("shareCalls = %d, want 1", shareCalls)
	}
}

func TestCreateMobileShareRetriesAfterRiskRelocate(t *testing.T) {
	setup139Resty(t)
	oldBaseURL := mobileShareOutLinkBaseURL
	oldSettingValue := shareRiskSettingValue
	oldTMDBResolve := shareRiskTMDBResolve
	oldPinyin := shareRiskPinyin
	t.Cleanup(func() {
		mobileShareOutLinkBaseURL = oldBaseURL
		shareRiskSettingValue = oldSettingValue
		shareRiskTMDBResolve = oldTMDBResolve
		shareRiskPinyin = oldPinyin
	})
	shareRiskSettingValue = func(key string) string {
		if key == conf.TMDBApiKey {
			return "key"
		}
		return ""
	}
	shareRiskTMDBResolve = func(_ context.Context, _ tmdb.Config, _ recognize.Result) (*tmdb.Metadata, error) {
		return &tmdb.Metadata{Name: "非分之罪", OriginalName: "Guilt", MediaType: "tv"}, nil
	}
	shareRiskPinyin = func(_ string) string {
		return "Fei Fen Zhi Zui"
	}

	shareCalls := 0
	var dedicatedNames []string
	var createdFolders []string
	var uploadedFiles []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case mobileShareOutLinkPath:
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode body: %v", err)
		}
			req := body["getOutLinkReq"].(map[string]any)
			dedicatedNames = append(dedicatedNames, req["dedicatedName"].(string))
			shareCalls++
			if shareCalls == 1 {
				write139JSON(t, w, map[string]any{"success": false, "message": "个人云未知异常"})
				return
			}
			write139JSON(t, w, map[string]any{
				"success": true,
				"data": map[string]any{
					"getOutLinkRes": map[string]any{
						"getOutLinkResSet": []map[string]any{{
							"linkID":  "link-id",
							"linkUrl": "https://share.example/folder",
							"passwd":  "abcd",
						}},
					},
				},
			})
		case "/file/list":
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode body: %v", err)
			}
			switch body["parentFileId"] {
			case "root-id":
				write139JSON(t, w, personalListItems([]map[string]any{{
					"fileId": "season-id", "name": "Season 1", "type": "folder",
				}}))
			case "season-id":
				write139JSON(t, w, personalListItems([]map[string]any{{
					"fileId": "ep1-id", "name": "非分之罪 S01E01.etf", "type": "file", "size": 1,
				}}))
			default:
				write139JSON(t, w, personalListItems(nil))
			}
		case "/file/create":
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode body: %v", err)
			}
			if body["type"] == "folder" {
				createdFolders = append(createdFolders, body["name"].(string))
				write139JSON(t, w, map[string]any{
					"success": true,
					"data": map[string]any{"fileId": "new-" + body["name"].(string)},
				})
			} else {
				uploadedFiles = append(uploadedFiles, body["name"].(string))
				write139JSON(t, w, map[string]any{
					"success": true,
					"data": map[string]any{"fileId": "upload-id"},
				})
			}
		case "/file/getDownloadUrl":
			write139JSON(t, w, map[string]any{
				"success": true,
				"data": map[string]any{"url": "http://" + r.Host + "/download/etf"},
			})
		case "/download/etf":
			etfData, _ := etfmeta.Encode(&etfmeta.Info{
				Name:       "非分之罪 S01E01.etf",
				Size:       1,
				SHA256:     strings.Repeat("A", 64),
				CreateTime: "2024-01-01T00:00:00Z",
			})
			w.WriteHeader(http.StatusOK)
			w.Write(etfData)
		case "/recyclebin/batchTrash":
			write139JSON(t, w, map[string]any{"success": true})
		case "/recyclebin/clean", "/recyclebin/clear", "/recyclebin/empty":
			write139JSON(t, w, map[string]any{"success": true})
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer server.Close()
	mobileShareOutLinkBaseURL = server.URL

	d := &Yun139{
		PersonalCloudHost: server.URL,
		Addition:          Addition{Type: MetaPersonalNew, AutoRenameOnShareRisk: true},
	}
	d.RootFolderID = "root"
	link, err := d.CreateMobileShare(context.Background(), &model.Object{ID: "root-id", Name: "非分之罪", Path: "/", IsFolder: true}, model.MobileShareCreateArgs{SourcePath: "/非分之罪"})
	if err != nil {
		t.Fatalf("CreateMobileShare returned error: %v", err)
	}
	if link.ShareURL != "https://share.example/folder" {
		t.Fatalf("link.ShareURL = %q, want https://share.example/folder", link.ShareURL)
	}
	if shareCalls != 2 {
		t.Fatalf("shareCalls = %d, want 2", shareCalls)
	}
	if len(createdFolders) < 2 {
		t.Fatalf("createdFolders = %#v, want at least 2 (root + Season 1)", createdFolders)
	}
	if createdFolders[0] != "Guilt" {
		t.Fatalf("first created folder = %q, want Guilt", createdFolders[0])
	}
	if len(uploadedFiles) != 1 || uploadedFiles[0] != "Guilt S01E01.etf" {
		t.Fatalf("uploadedFiles = %#v, want [Guilt S01E01.etf]", uploadedFiles)
	}
	if len(dedicatedNames) != 2 || dedicatedNames[0] != "非分之罪" || dedicatedNames[1] != "Guilt" {
		t.Fatalf("dedicatedNames = %#v, want original then relocated name", dedicatedNames)
	}
	if link.SourcePath != "/Guilt" {
		t.Fatalf("link.SourcePath = %q, want /Guilt", link.SourcePath)
	}
	if link.SourceName != "Guilt" {
		t.Fatalf("link.SourceName = %q, want Guilt", link.SourceName)
	}
	if link.CanonicalTitle != "Guilt" {
		t.Fatalf("link.CanonicalTitle = %q, want Guilt", link.CanonicalTitle)
	}
}

func TestCreateMobileShareSkipsRetryWhenRelocatePlanEmpty(t *testing.T) {
	setup139Resty(t)
	oldBaseURL := mobileShareOutLinkBaseURL
	oldSettingValue := shareRiskSettingValue
	oldTMDBResolve := shareRiskTMDBResolve
	oldPinyin := shareRiskPinyin
	t.Cleanup(func() {
		mobileShareOutLinkBaseURL = oldBaseURL
		shareRiskSettingValue = oldSettingValue
		shareRiskTMDBResolve = oldTMDBResolve
		shareRiskPinyin = oldPinyin
	})
	shareRiskSettingValue = func(key string) string {
		if key == conf.TMDBApiKey {
			return "key"
		}
		return ""
	}
	shareRiskTMDBResolve = func(_ context.Context, _ tmdb.Config, _ recognize.Result) (*tmdb.Metadata, error) {
		return &tmdb.Metadata{Name: "Guilt", OriginalName: "Guilt", MediaType: "tv"}, nil
	}
	shareRiskPinyin = func(_ string) string {
		return ""
	}

	shareCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case mobileShareOutLinkPath:
			shareCalls++
			write139JSON(t, w, map[string]any{"success": false, "message": "个人云未知异常"})
		case "/file/create":
			t.Fatal("folder creation should not be attempted when plan is empty")
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer server.Close()
	mobileShareOutLinkBaseURL = server.URL

	d := &Yun139{
		PersonalCloudHost: server.URL,
		Addition:          Addition{Type: MetaPersonalNew, AutoRenameOnShareRisk: true},
	}
	_, err := d.CreateMobileShare(context.Background(), &model.Object{ID: "folder-id", Name: "Guilt", Path: "/", IsFolder: true}, model.MobileShareCreateArgs{})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "个人云未知异常") {
		t.Fatalf("err = %v, want risk error", err)
	}
	if shareCalls != 1 {
		t.Fatalf("shareCalls = %d, want 1", shareCalls)
	}
}
