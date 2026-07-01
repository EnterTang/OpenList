package _139

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/drivers/base"
	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	driverPkg "github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/etfmeta"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	streamPkg "github.com/OpenListTeam/OpenList/v4/internal/stream"
	"github.com/go-resty/resty/v2"
)

func TestPersonalRapidCreateUsesSHA256Payload(t *testing.T) {
	setup139Resty(t)
	var sawAlgorithm string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/file/create" {
			t.Fatalf("path = %q, want /file/create", r.URL.Path)
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		sawAlgorithm, _ = body["contentHashAlgorithm"].(string)
		if body["contentHash"] != strings.Repeat("A", 64) {
			t.Fatalf("contentHash = %v, want uppercase SHA256", body["contentHash"])
		}
		write139JSON(t, w, map[string]any{
			"success": true,
			"data": map[string]any{
				"fileId":      "rapid-file-id",
				"fileName":    "Movie.mkv",
				"partInfos":   []any{},
				"rapidUpload": true,
			},
		})
	}))
	defer server.Close()

	d := &Yun139{PersonalCloudHost: server.URL}
	obj, rapid, err := d.personalRapidCreate(context.Background(), "parent", "Movie.mkv", 2048, strings.Repeat("a", 64))
	if err != nil {
		t.Fatalf("personalRapidCreate returned error: %v", err)
	}
	if !rapid {
		t.Fatal("rapid = false, want true")
	}
	if obj.GetID() != "rapid-file-id" || obj.GetName() != "Movie.mkv" {
		t.Fatalf("obj = %s/%s, want rapid-file-id/Movie.mkv", obj.GetID(), obj.GetName())
	}
	if sawAlgorithm != "SHA256" {
		t.Fatalf("contentHashAlgorithm = %q, want SHA256", sawAlgorithm)
	}
}

func TestPersonalRapidCreateRejectsUploadURLs(t *testing.T) {
	setup139Resty(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		write139JSON(t, w, map[string]any{
			"success": true,
			"data": map[string]any{
				"fileId":   "file-id",
				"fileName": "Movie.mkv",
				"partInfos": []map[string]any{{
					"partNumber": 1,
					"uploadUrl":  "https://upload.example",
				}},
			},
		})
	}))
	defer server.Close()

	d := &Yun139{PersonalCloudHost: server.URL}
	if _, rapid, err := d.personalRapidCreate(context.Background(), "parent", "Movie.mkv", 2048, strings.Repeat("A", 64)); err == nil || rapid {
		t.Fatalf("personalRapidCreate rapid=%v err=%v, want rapid unavailable error", rapid, err)
	}
}

func TestEmptyPersonalRecycleBinFallsBack(t *testing.T) {
	setup139Resty(t)
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		if r.URL.Path == "/recyclebin/clean" {
			write139JSON(t, w, map[string]any{"success": false, "message": "unsupported"})
			return
		}
		write139JSON(t, w, map[string]any{"success": true})
	}))
	defer server.Close()

	d := &Yun139{PersonalCloudHost: server.URL}
	if err := d.emptyPersonalRecycleBin(context.Background()); err != nil {
		t.Fatalf("emptyPersonalRecycleBin returned error: %v", err)
	}
	if len(paths) < 2 || paths[0] != "/recyclebin/clean" {
		t.Fatalf("paths = %#v, want /recyclebin/clean then fallback", paths)
	}
}

func TestEnsurePersonalFolderPathListsBeforeCreating(t *testing.T) {
	setup139Resty(t)
	var calls []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls = append(calls, r.URL.Path)
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		switch r.URL.Path {
		case "/file/list":
			parent := body["parentFileId"]
			items := []map[string]any{}
			if parent == "root" {
				items = append(items, map[string]any{"fileId": "movies-id", "name": "Movies", "type": "folder"})
			}
			write139JSON(t, w, map[string]any{"success": true, "data": map[string]any{"items": items}})
		case "/file/create":
			if body["name"] != "Action" {
				t.Fatalf("created name = %v, want Action", body["name"])
			}
			write139JSON(t, w, map[string]any{"success": true, "data": map[string]any{"fileId": "action-id", "fileName": "Action"}})
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer server.Close()

	d := &Yun139{PersonalCloudHost: server.URL}
	obj, err := d.ensurePersonalFolderPath(context.Background(), "root", "Movies/Action")
	if err != nil {
		t.Fatalf("ensurePersonalFolderPath returned error: %v", err)
	}
	if obj.GetID() != "action-id" {
		t.Fatalf("folder id = %q, want action-id", obj.GetID())
	}
	if strings.Join(calls, ",") != "/file/list,/file/list,/file/create" {
		t.Fatalf("calls = %#v, want list/list/create", calls)
	}
}

func TestETFDownloadRestoreEnabledUsesConfig(t *testing.T) {
	d := &Yun139{Addition: Addition{Type: MetaPersonalNew, ETFDownloadRestore: true}}

	if !d.ETFDownloadRestoreEnabled() {
		t.Fatal("ETFDownloadRestoreEnabled returned false")
	}
}

func TestETFPreviewNameReturnsOriginalName(t *testing.T) {
	setup139Resty(t)
	data, err := etfmeta.Encode(&etfmeta.Info{Name: "Movie.mkv", Size: 2048, SHA256: strings.Repeat("A", 64)})
	if err != nil {
		t.Fatalf("Encode ETF: %v", err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/file/getDownloadUrl":
			write139JSON(t, w, map[string]any{"success": true, "data": map[string]any{"url": serverURL(r) + "/etf"}})
		case "/etf":
			_, _ = w.Write(data)
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer server.Close()

	d := &Yun139{PersonalCloudHost: server.URL, Addition: Addition{Type: MetaPersonalNew, ETFVideoPlayback: true}}
	got, err := d.ETFPreviewName(context.Background(), &model.Object{ID: "etf-id", Name: "Movie.mkv.etf"})
	if err != nil {
		t.Fatalf("ETFPreviewName returned error: %v", err)
	}
	if got != "Movie.mkv" {
		t.Fatalf("preview name = %q, want Movie.mkv", got)
	}
}

func TestLinkETFVideoCreatesTempDownloadsAndCleans(t *testing.T) {
	setup139Resty(t)
	data, err := etfmeta.Encode(&etfmeta.Info{Name: "Movie.mkv", Size: 2048, SHA256: strings.Repeat("A", 64)})
	if err != nil {
		t.Fatalf("Encode ETF: %v", err)
	}
	var calls []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls = append(calls, r.URL.Path)
		switch r.URL.Path {
		case "/file/getDownloadUrl":
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode body: %v", err)
			}
			if body["fileId"] == "etf-id" {
				write139JSON(t, w, map[string]any{"success": true, "data": map[string]any{"url": serverURL(r) + "/etf"}})
			} else {
				write139JSON(t, w, map[string]any{"success": true, "data": map[string]any{"url": "https://download.example/Movie.mkv"}})
			}
		case "/etf":
			_, _ = w.Write(data)
		case "/file/list":
			write139JSON(t, w, map[string]any{"success": true, "data": map[string]any{"items": []any{}}})
		case "/file/create":
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode body: %v", err)
			}
			if body["type"] == "folder" {
				parentID, _ := body["parentFileId"].(string)
				name, _ := body["name"].(string)
				folderIDs := map[string]string{
					"root/Temp":           "temp-folder-id",
					"temp-folder-id/Play": "play-folder-id",
				}
				write139JSON(t, w, map[string]any{"success": true, "data": map[string]any{"fileId": folderIDs[parentID+"/"+name], "fileName": name}})
				return
			}
			if body["parentFileId"] != "play-folder-id" {
				t.Fatalf("temp file parent = %v, want play-folder-id", body["parentFileId"])
			}
			write139JSON(t, w, map[string]any{"success": true, "data": map[string]any{"fileId": "temp-id", "fileName": "Movie.mkv", "partInfos": []any{}}})
		case "/recyclebin/batchTrash", "/recyclebin/clean":
			write139JSON(t, w, map[string]any{"success": true})
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer server.Close()

	d := &Yun139{
		PersonalCloudHost: server.URL,
		Addition:          Addition{Type: MetaPersonalNew, ETFVideoPlayback: true, ETFTempFolder: "/Temp/Play"},
	}
	d.RootFolderID = "root"
	link, err := d.Link(context.Background(), &model.Object{ID: "etf-id", Name: "Movie.mkv.etf"}, model.LinkArgs{Type: "etf_video"})
	if err != nil {
		t.Fatalf("Link returned error: %v", err)
	}
	if link.URL != "https://download.example/Movie.mkv" {
		t.Fatalf("link URL = %q, want download URL", link.URL)
	}
	joined := strings.Join(calls, ",")
	for _, want := range []string{"/file/getDownloadUrl", "/etf", "/file/create", "/recyclebin/batchTrash", "/recyclebin/clean"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("calls = %s, missing %s", joined, want)
		}
	}
}

func TestETFTempFolderRootPathUsesRootFolder(t *testing.T) {
	d := &Yun139{
		Addition: Addition{ETFTempFolder: "/"},
	}
	d.RootFolderID = "root"
	got, err := d.resolveETFTempFolderID(context.Background())
	if err != nil {
		t.Fatalf("resolveETFTempFolderID returned error: %v", err)
	}
	if got != "root" {
		t.Fatalf("temp folder id = %q, want root", got)
	}
}

func TestRestorePersonalFromETFUploadRapidCreatesSource(t *testing.T) {
	setup139Resty(t)
	data, err := etfmeta.Encode(&etfmeta.Info{Name: "Movie.mkv", Size: 2048, SHA256: strings.Repeat("A", 64)})
	if err != nil {
		t.Fatalf("Encode ETF: %v", err)
	}
	var createdName string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/file/create" {
			t.Fatalf("path = %q, want /file/create", r.URL.Path)
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		createdName, _ = body["name"].(string)
		write139JSON(t, w, map[string]any{"success": true, "data": map[string]any{"fileId": "restored-id", "fileName": "Movie.mkv", "partInfos": []any{}}})
	}))
	defer server.Close()

	d := &Yun139{PersonalCloudHost: server.URL, Addition: Addition{Type: MetaPersonalNew, RestoreSourceFromETF: true}}
	restored, err := d.restorePersonalFromETFUpload(context.Background(), &model.Object{ID: "parent"}, &streamPkg.FileStream{
		Obj:    &model.Object{Name: "Movie.mkv.etf", Size: int64(len(data))},
		Reader: strings.NewReader(string(data)),
	})
	if err != nil {
		t.Fatalf("restorePersonalFromETFUpload returned error: %v", err)
	}
	if !restored {
		t.Fatal("restored = false, want true")
	}
	if createdName != "Movie.mkv" {
		t.Fatalf("created name = %q, want Movie.mkv", createdName)
	}
}

func TestAfterPersonalUploadETFUploadsMetadataFile(t *testing.T) {
	setup139Resty(t)
	var createdName string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/file/create" {
			t.Fatalf("path = %q, want /file/create", r.URL.Path)
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		createdName, _ = body["name"].(string)
		write139JSON(t, w, map[string]any{"success": true, "data": map[string]any{"fileId": "etf-id", "fileName": createdName, "partInfos": []any{}}})
	}))
	defer server.Close()

	d := &Yun139{PersonalCloudHost: server.URL, Addition: Addition{Type: MetaPersonalNew, GenerateETF: true}}
	err := d.afterPersonalUploadETF(context.Background(), &model.Object{ID: "parent"}, "Movie.mkv", 2048, strings.Repeat("A", 64), &model.Object{ID: "source", Name: "Movie.mkv"})
	if err != nil {
		t.Fatalf("afterPersonalUploadETF returned error: %v", err)
	}
	if createdName != "Movie.mkv.etf" {
		t.Fatalf("created name = %q, want Movie.mkv.etf", createdName)
	}
}

func TestAfterPersonalUploadETFDeletesSourceAndCleansRecycleBin(t *testing.T) {
	setup139Resty(t)
	var calls []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls = append(calls, r.URL.Path)
		write139JSON(t, w, map[string]any{"success": true, "data": map[string]any{"fileId": "etf-id", "fileName": "Movie.mkv.etf", "partInfos": []any{}}})
	}))
	defer server.Close()

	d := &Yun139{PersonalCloudHost: server.URL, Addition: Addition{Type: MetaPersonalNew, GenerateETF: true, DeleteSourceAfterETF: true}}
	err := d.afterPersonalUploadETF(context.Background(), &model.Object{ID: "parent"}, "Movie.mkv", 2048, strings.Repeat("A", 64), &model.Object{ID: "source", Name: "Movie.mkv"})
	if err != nil {
		t.Fatalf("afterPersonalUploadETF returned error: %v", err)
	}
	joined := strings.Join(calls, ",")
	for _, want := range []string{"/file/create", "/recyclebin/batchTrash", "/recyclebin/clean"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("calls = %s, missing %s", joined, want)
		}
	}
}

func TestAfterPersonalUploadETFUsesTMDBCategoryFolder(t *testing.T) {
	setup139Resty(t)
	tmdbServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/search/multi" {
			t.Fatalf("tmdb path = %q, want /search/multi", r.URL.Path)
		}
		write139JSON(t, w, map[string]any{"results": []map[string]any{{
			"id":                100,
			"media_type":        "movie",
			"title":             "Movie",
			"release_date":      "2024-01-01",
			"genre_ids":         []int{16},
			"original_language": "en",
		}}})
	}))
	defer tmdbServer.Close()
	oldSettingValue := etfSettingValue
	etfSettingValue = func(key string) string {
		switch key {
		case conf.TMDBApiKey:
			return "key"
		case conf.TMDBApiBaseURL:
			return tmdbServer.URL
		case conf.TMDBLanguage:
			return "zh-CN"
		case conf.MediaCategoryRules:
			return "movie:\n  动画片:\n    genre_ids: '16'\n  未分类:\n"
		default:
			return ""
		}
	}
	t.Cleanup(func() {
		etfSettingValue = oldSettingValue
	})

	folderIDs := map[string]string{
		"root/Managed":     "managed-id",
		"managed-id/movie": "movie-id",
		"movie-id/动画片":     "category-id",
	}
	var finalParent string
	var createdFolders []string
	personalServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		switch r.URL.Path {
		case "/file/list":
			write139JSON(t, w, map[string]any{"success": true, "data": map[string]any{"items": []any{}}})
		case "/file/create":
			name, _ := body["name"].(string)
			parentID, _ := body["parentFileId"].(string)
			if body["type"] == "folder" {
				createdFolders = append(createdFolders, name)
				write139JSON(t, w, map[string]any{"success": true, "data": map[string]any{"fileId": folderIDs[parentID+"/"+name], "fileName": name}})
				return
			}
			finalParent = parentID
			if name != "Movie.mkv.etf" {
				t.Fatalf("uploaded ETF name = %q, want Movie.mkv.etf", name)
			}
			write139JSON(t, w, map[string]any{"success": true, "data": map[string]any{"fileId": "etf-id", "fileName": name, "partInfos": []any{}}})
		default:
			t.Fatalf("unexpected personal path: %s", r.URL.Path)
		}
	}))
	defer personalServer.Close()

	d := &Yun139{
		PersonalCloudHost: personalServer.URL,
		Addition: Addition{
			Type:          MetaPersonalNew,
			GenerateETF:   true,
			ETFRootFolder: "Managed",
		},
	}
	d.RootFolderID = "root"
	err := d.afterPersonalUploadETF(context.Background(), &model.Object{ID: "parent", Path: "/Movies"}, "Movie.mkv", 2048, strings.Repeat("A", 64), &model.Object{ID: "source", Name: "Movie.mkv"})
	if err != nil {
		t.Fatalf("afterPersonalUploadETF returned error: %v", err)
	}
	if strings.Join(createdFolders, "/") != "Managed/movie/动画片" {
		t.Fatalf("created folders = %#v, want Managed/movie/动画片", createdFolders)
	}
	if finalParent != "category-id" {
		t.Fatalf("final parent = %q, want category-id", finalParent)
	}
}

func TestRemoveETFFileCleansRecycleBin(t *testing.T) {
	setup139Resty(t)
	var calls []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls = append(calls, r.URL.Path)
		write139JSON(t, w, map[string]any{"success": true})
	}))
	defer server.Close()

	d := &Yun139{PersonalCloudHost: server.URL, Addition: Addition{Type: MetaPersonalNew, GenerateETF: true}}
	if err := d.Remove(context.Background(), &model.Object{ID: "etf-id", Name: "Movie.mkv.etf"}); err != nil {
		t.Fatalf("Remove returned error: %v", err)
	}
	if strings.Join(calls, ",") != "/recyclebin/batchTrash,/recyclebin/clean" {
		t.Fatalf("calls = %#v, want trash then clean", calls)
	}
}

func Test139ETFConfigMetadataIsChineseAndCollapsed(t *testing.T) {
	info := op.GetDriverInfoMap()["139Yun"]
	items := map[string]driverPkg.Item{}
	for _, item := range info.Additional {
		items[item.Name] = item
	}
	for _, name := range []string{"etf_root_folder_id", "etf_temp_folder_id"} {
		if _, ok := items[name]; ok {
			t.Fatalf("%s should be hidden from driver config metadata", name)
		}
	}
	if item, ok := items["auth_mode"]; !ok || item.Label != "授权模式" {
		t.Fatalf("auth_mode metadata = %+v, want Chinese auth mode label", item)
	}
	if item := items["cookie_header"]; item.VisibleWhen != "auth_mode=etf" {
		t.Fatalf("cookie_header visible_when = %q, want auth_mode=etf", item.VisibleWhen)
	}
	if item := items["authorization"]; item.VisibleWhen != "auth_mode=openlist" {
		t.Fatalf("authorization visible_when = %q, want auth_mode=openlist", item.VisibleWhen)
	}
	wantLabels := map[string]string{
		"generate_etf":    "生成 ETF",
		"etf_root_folder": "ETF 管理目录",
		"etf_temp_folder": "ETF 临时播放目录",
	}
	for name, wantLabel := range wantLabels {
		raw, ok := items[name]
		if !ok {
			t.Fatalf("missing ETF config item %s", name)
		}
		item := raw
		if item.Group != "ETF" || !item.Collapsed {
			t.Fatalf("%s group/collapsed = %q/%v, want ETF/true", name, item.Group, item.Collapsed)
		}
		if item.Label != wantLabel || item.Help == "" {
			t.Fatalf("%s label/help should be localized, got label=%q help=%q", name, item.Label, item.Help)
		}
	}
}

func setup139Resty(t *testing.T) {
	t.Helper()
	old := base.RestyClient
	base.RestyClient = resty.New()
	t.Cleanup(func() {
		base.RestyClient = old
	})
}

func serverURL(r *http.Request) string {
	return fmt.Sprintf("http://%s", r.Host)
}

func write139JSON(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		t.Fatalf("encode json: %v", err)
	}
}
