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

func TestBuildShareRiskRelocatePlanCollectsRootFolderAndMatchingDescendants(t *testing.T) {
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
	plan, err := d.buildShareRiskRelocatePlan(context.Background(), &model.Object{ID: "root-id", Name: "非分之罪", Path: "/", IsFolder: true}, "/非分之罪")
	if err != nil {
		t.Fatalf("buildShareRiskRelocatePlan returned error: %v", err)
	}
	if plan == nil {
		t.Fatal("plan is nil")
	}
	if plan.CanonicalTitle != "Guilt" {
		t.Fatalf("canonicalTitle = %q, want Guilt", plan.CanonicalTitle)
	}
	if plan.NewRootName != "Guilt" {
		t.Fatalf("newRootName = %q, want Guilt", plan.NewRootName)
	}
	if !containsRelocateEntry(plan.Entries, "season-id", "Season 1") {
		t.Fatalf("entries = %#v, missing Season 1 dir", plan.Entries)
	}
	if !containsRelocateEntry(plan.Entries, "ep1-id", "Guilt S01E01.etf") {
		t.Fatalf("entries = %#v, missing episode file with safe name", plan.Entries)
	}
}

func TestBuildShareRiskRelocatePlanFallsBackToPinyin(t *testing.T) {
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
	plan, err := d.buildShareRiskRelocatePlan(context.Background(), &model.Object{ID: "file-id", Name: "非分之罪 S01E01.etf", Path: "/"}, "/非分之罪 S01E01.etf")
	if err != nil {
		t.Fatalf("buildShareRiskRelocatePlan returned error: %v", err)
	}
	if plan == nil {
		t.Fatal("plan is nil")
	}
	if plan.CanonicalTitle != "Fei Fen Zhi Zui" {
		t.Fatalf("canonicalTitle = %q, want Fei Fen Zhi Zui", plan.CanonicalTitle)
	}
	if plan.NewRootName != "Fei Fen Zhi Zui S01E01.etf" {
		t.Fatalf("newRootName = %q, want Fei Fen Zhi Zui S01E01.etf", plan.NewRootName)
	}
}

func TestBuildShareRiskRelocatePlanUsesBilingualRecognizeCandidates(t *testing.T) {
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
	shareRiskTMDBResolve = func(_ context.Context, _ tmdb.Config, result recognize.Result) (*tmdb.Metadata, error) {
		if result.Title != "诊疗中 Shrinking" {
			t.Fatalf("recognized title = %q, want %q", result.Title, "诊疗中 Shrinking")
		}
		if len(result.QueryList) == 0 {
			t.Fatal("recognized query list is empty")
		}
		foundEnglish := false
		for _, candidate := range result.QueryList {
			if candidate == "Shrinking" {
				foundEnglish = true
				break
			}
		}
		if !foundEnglish {
			t.Fatalf("recognized query list = %#v, want English alias Shrinking", result.QueryList)
		}
		return &tmdb.Metadata{Name: "诊疗中", OriginalName: "Shrinking", MediaType: "tv"}, nil
	}
	shareRiskPinyin = func(_ string) string {
		return "Zhen Liao Zhong"
	}

	d := &Yun139{Addition: Addition{Type: MetaPersonalNew}}
	plan, err := d.buildShareRiskRelocatePlan(context.Background(), &model.Object{ID: "file-id", Name: "诊疗中 Shrinking S03E01.mkv", Path: "/"}, "/诊疗中 Shrinking S03E01.mkv")
	if err != nil {
		t.Fatalf("buildShareRiskRelocatePlan returned error: %v", err)
	}
	if plan == nil {
		t.Fatal("plan is nil")
	}
	if plan.CanonicalTitle != "Shrinking" {
		t.Fatalf("canonicalTitle = %q, want %q", plan.CanonicalTitle, "Shrinking")
	}
	if plan.NewRootName != "Shrinking S03E01.mkv" {
		t.Fatalf("newRootName = %q, want %q", plan.NewRootName, "Shrinking S03E01.mkv")
	}
}

func containsRelocateEntry(entries []shareRiskRelocateEntry, id, newName string) bool {
	for _, entry := range entries {
		if entry.Obj != nil && entry.Obj.GetID() == id && entry.NewName == newName {
			return true
		}
	}
	return false
}
