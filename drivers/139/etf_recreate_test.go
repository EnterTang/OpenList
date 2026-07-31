package _139

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/etfmeta"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestRecreateETFFilesReuploadsAndDeletes(t *testing.T) {
	setup139Resty(t)
	etfData, err := etfmeta.Encode(&etfmeta.Info{
		Name:       "Movie.mkv",
		Size:       2048,
		SHA256:     strings.Repeat("A", 64),
		CreateTime: "2024-01-01T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("encode ETF: %v", err)
	}

	var uploadedNames []string
	var trashedIDs []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if r.URL.Path != "/file/getDownloadUrl" && r.URL.Path != "/download/etf" {
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode body: %v", err)
			}
		}
		switch r.URL.Path {
		case "/file/list":
			switch body["parentFileId"] {
			case "root":
				write139JSON(t, w, personalListItems([]map[string]any{
					{"fileId": "folder-id", "name": "Movies", "type": "folder"},
				}))
			case "folder-id":
				write139JSON(t, w, personalListItems([]map[string]any{
					{"fileId": "etf-1", "name": "Movie.mkv.etf", "type": "file", "size": 100},
					{"fileId": "etf-2", "name": "Other.mkv.etf", "type": "file", "size": 200},
					{"fileId": "non-etf", "name": "readme.txt", "type": "file", "size": 10},
				}))
			default:
				write139JSON(t, w, personalListItems(nil))
			}
		case "/file/getDownloadUrl":
			write139JSON(t, w, map[string]any{
				"success": true,
				"data":    map[string]any{"url": "http://" + r.Host + "/download/etf"},
			})
		case "/download/etf":
			w.WriteHeader(http.StatusOK)
			w.Write(etfData)
		case "/file/create":
			uploadedNames = append(uploadedNames, body["name"].(string))
			write139JSON(t, w, map[string]any{
				"success": true,
				"data":    map[string]any{"fileId": "new-file-id", "fileName": body["name"]},
			})
		case "/recyclebin/batchTrash":
			if ids, ok := body["fileIds"].([]any); ok {
				for _, id := range ids {
					trashedIDs = append(trashedIDs, id.(string))
				}
			}
			write139JSON(t, w, map[string]any{"success": true})
		case "/recyclebin/clean", "/recyclebin/clear", "/recyclebin/empty":
			write139JSON(t, w, map[string]any{"success": true})
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer server.Close()

	d := &Yun139{
		PersonalCloudHost: server.URL,
		Addition:          Addition{Type: MetaPersonalNew},
	}
	d.SetStorage(model.Storage{ID: 1, MountPath: "/139"})
	d.RootFolderID = "root"

	result, err := d.RecreateETFFiles(context.Background(), "/Movies", []string{"Movie.mkv.etf", "Other.mkv.etf"})
	if err != nil {
		t.Fatalf("RecreateETFFiles returned error: %v", err)
	}
	if result.Total != 2 {
		t.Fatalf("total = %d, want 2", result.Total)
	}
	if result.Succeeded != 2 {
		t.Fatalf("succeeded = %d, want 2", result.Succeeded)
	}
	if result.Failed != 0 {
		t.Fatalf("failed = %d, want 0, errors: %v", result.Failed, result.Errors)
	}
	if len(uploadedNames) != 2 {
		t.Fatalf("uploadedNames = %#v, want 2 uploads", uploadedNames)
	}
	if uploadedNames[0] != "Movie.mkv.etf" || uploadedNames[1] != "Other.mkv.etf" {
		t.Fatalf("uploadedNames = %#v, want [Movie.mkv.etf Other.mkv.etf]", uploadedNames)
	}
	if len(trashedIDs) != 2 {
		t.Fatalf("trashedIDs = %#v, want 2 trashed", trashedIDs)
	}
}

func TestRecreateETFFilesRejectsNonETFFile(t *testing.T) {
	setup139Resty(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		switch r.URL.Path {
		case "/file/list":
			switch body["parentFileId"] {
			case "root":
				write139JSON(t, w, personalListItems([]map[string]any{
					{"fileId": "folder-id", "name": "Movies", "type": "folder"},
				}))
			case "folder-id":
				write139JSON(t, w, personalListItems([]map[string]any{
					{"fileId": "non-etf", "name": "readme.txt", "type": "file", "size": 10},
				}))
			default:
				write139JSON(t, w, personalListItems(nil))
			}
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer server.Close()

	d := &Yun139{
		PersonalCloudHost: server.URL,
		Addition:          Addition{Type: MetaPersonalNew},
	}
	d.SetStorage(model.Storage{ID: 1, MountPath: "/139"})
	d.RootFolderID = "root"

	result, err := d.RecreateETFFiles(context.Background(), "/Movies", []string{"readme.txt"})
	if err != nil {
		t.Fatalf("RecreateETFFiles returned error: %v", err)
	}
	if result.Succeeded != 0 {
		t.Fatalf("succeeded = %d, want 0", result.Succeeded)
	}
	if result.Failed != 1 {
		t.Fatalf("failed = %d, want 1", result.Failed)
	}
}

func TestRecreateETFFilesReportsFileNotFound(t *testing.T) {
	setup139Resty(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		switch r.URL.Path {
		case "/file/list":
			switch body["parentFileId"] {
			case "root":
				write139JSON(t, w, personalListItems([]map[string]any{
					{"fileId": "folder-id", "name": "Movies", "type": "folder"},
				}))
			case "folder-id":
				write139JSON(t, w, personalListItems(nil))
			default:
				write139JSON(t, w, personalListItems(nil))
			}
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer server.Close()

	d := &Yun139{
		PersonalCloudHost: server.URL,
		Addition:          Addition{Type: MetaPersonalNew},
	}
	d.SetStorage(model.Storage{ID: 1, MountPath: "/139"})
	d.RootFolderID = "root"

	result, err := d.RecreateETFFiles(context.Background(), "/Movies", []string{"missing.etf"})
	if err != nil {
		t.Fatalf("RecreateETFFiles returned error: %v", err)
	}
	if result.Succeeded != 0 {
		t.Fatalf("succeeded = %d, want 0", result.Succeeded)
	}
	if result.Failed != 1 {
		t.Fatalf("failed = %d, want 1", result.Failed)
	}
	if len(result.Errors) == 0 || !strings.Contains(result.Errors[0], "file not found") {
		t.Fatalf("errors = %#v, want file not found", result.Errors)
	}
}
