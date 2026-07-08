package _139

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/media/recognize"
	"github.com/OpenListTeam/OpenList/v4/internal/media/tmdb"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestIsShareRiskStructuralDir(t *testing.T) {
	for _, name := range []string{"Season 1", "Season 01", "Specials", "Extras"} {
		if !isShareRiskStructuralDir(name) {
			t.Fatalf("%q should be treated as structural", name)
		}
	}
	if isShareRiskStructuralDir("非分之罪") {
		t.Fatal("content title directory should not be structural")
	}
}

func TestReplaceShareRiskTitlePreservesSeasonEpisodeAndExtension(t *testing.T) {
	got := replaceShareRiskTitle("非分之罪 S01E01.etf", "非分之罪", "Guilt")
	if got != "Guilt S01E01.etf" {
		t.Fatalf("got %q, want %q", got, "Guilt S01E01.etf")
	}
}

func TestDefaultShareRiskPinyinTransliteratesChineseTitle(t *testing.T) {
	got := defaultShareRiskPinyin("非分之罪")
	if got != "Fei Fen Zhi Zui" {
		t.Fatalf("got %q, want %q", got, "Fei Fen Zhi Zui")
	}
}

func TestBuildShareRiskRenamePlanRenamesRootFolderAndMatchingDescendants(t *testing.T) {
	setup139Resty(t)
	oldSettingValue := shareRiskSettingValue
	oldTMDBResolve := shareRiskTMDBResolve
	oldPinyin := shareRiskPinyin
	t.Cleanup(func() {
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

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/file/list" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
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
	}))
	defer server.Close()

	d := &Yun139{PersonalCloudHost: server.URL, Addition: Addition{Type: MetaPersonalNew}}
	plan, canonicalTitle, err := d.buildShareRiskRenamePlan(context.Background(), &model.Object{ID: "root-id", Name: "非分之罪", Path: "/", IsFolder: true}, "/非分之罪")
	if err != nil {
		t.Fatalf("buildShareRiskRenamePlan returned error: %v", err)
	}
	if canonicalTitle != "Guilt" {
		t.Fatalf("canonicalTitle = %q, want Guilt", canonicalTitle)
	}
	if len(plan) != 2 {
		t.Fatalf("plan len = %d, want 2", len(plan))
	}
	if !containsRenameNode(plan, "root-id", "Guilt") {
		t.Fatalf("plan = %#v, missing root rename", plan)
	}
	if !containsRenameNode(plan, "ep1-id", "Guilt S01E01.etf") {
		t.Fatalf("plan = %#v, missing episode rename", plan)
	}
	if containsRenameNode(plan, "season-id", "Season 1") {
		t.Fatalf("plan = %#v, should not rename Season 1", plan)
	}
}

func TestBuildShareRiskRenamePlanFallsBackToPinyin(t *testing.T) {
	oldSettingValue := shareRiskSettingValue
	oldTMDBResolve := shareRiskTMDBResolve
	oldPinyin := shareRiskPinyin
	t.Cleanup(func() {
		shareRiskSettingValue = oldSettingValue
		shareRiskTMDBResolve = oldTMDBResolve
		shareRiskPinyin = oldPinyin
	})
	shareRiskSettingValue = func(string) string { return "" }
	shareRiskTMDBResolve = func(_ context.Context, _ tmdb.Config, _ recognize.Result) (*tmdb.Metadata, error) {
		return nil, nil
	}
	shareRiskPinyin = func(_ string) string {
		return "Fei Fen Zhi Zui"
	}

	d := &Yun139{Addition: Addition{Type: MetaPersonalNew}}
	plan, canonicalTitle, err := d.buildShareRiskRenamePlan(context.Background(), &model.Object{ID: "file-id", Name: "非分之罪 S01E01.etf", Path: "/"}, "/非分之罪 S01E01.etf")
	if err != nil {
		t.Fatalf("buildShareRiskRenamePlan returned error: %v", err)
	}
	if canonicalTitle != "Fei Fen Zhi Zui" {
		t.Fatalf("canonicalTitle = %q, want Fei Fen Zhi Zui", canonicalTitle)
	}
	if len(plan) != 1 {
		t.Fatalf("plan len = %d, want 1", len(plan))
	}
	if plan[0].NewName != "Fei Fen Zhi Zui S01E01.etf" {
		t.Fatalf("new name = %q, want Fei Fen Zhi Zui S01E01.etf", plan[0].NewName)
	}
}

func TestApplyShareRiskRenamePlanSortsDeepestFirst(t *testing.T) {
	setup139Resty(t)
	var renamed []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/file/update" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		renamed = append(renamed, body["fileId"].(string)+":"+body["name"].(string))
		write139JSON(t, w, map[string]any{"success": true})
	}))
	defer server.Close()

	d := &Yun139{PersonalCloudHost: server.URL, Addition: Addition{Type: MetaPersonalNew}}
	err := d.applyShareRiskRenamePlan(context.Background(), []shareRiskRenameNode{
		{Obj: &model.Object{ID: "root-id", Name: "非分之罪", IsFolder: true}, Depth: 0, OldName: "非分之罪", NewName: "Guilt"},
		{Obj: &model.Object{ID: "ep1-id", Name: "非分之罪 S01E01.etf"}, Depth: 2, OldName: "非分之罪 S01E01.etf", NewName: "Guilt S01E01.etf"},
	})
	if err != nil {
		t.Fatalf("applyShareRiskRenamePlan returned error: %v", err)
	}
	if len(renamed) != 2 {
		t.Fatalf("renamed = %#v, want 2 renames", renamed)
	}
	if renamed[0] != "ep1-id:Guilt S01E01.etf" || renamed[1] != "root-id:Guilt" {
		t.Fatalf("rename order = %#v, want deepest first", renamed)
	}
}

func containsRenameNode(nodes []shareRiskRenameNode, id, newName string) bool {
	for _, node := range nodes {
		if node.Obj.GetID() == id && node.NewName == newName {
			return true
		}
	}
	return false
}
