package tmdb

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/media/recognize"
)

func TestResolveExplicitTMDBIDFetchesDetail(t *testing.T) {
	var gotPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		if r.URL.Query().Get("api_key") != "key" {
			t.Fatalf("api_key = %q, want key", r.URL.Query().Get("api_key"))
		}
		writeJSON(t, w, map[string]any{
			"id":                123,
			"title":             "Cars 3",
			"release_date":      "2017-06-15",
			"genre_ids":         []int{16},
			"original_language": "en",
			"origin_country":    []string{"US"},
		})
	}))
	defer server.Close()

	got, err := Resolve(context.Background(), Config{APIKey: "key", BaseURL: server.URL}, recognize.Result{
		TMDBID:        123,
		Title:         "Cars 3",
		MediaTypeHint: "movie",
	})
	if err != nil {
		t.Fatalf("Resolve returned error: %v", err)
	}

	if gotPath != "/movie/123" {
		t.Fatalf("path = %q, want /movie/123", gotPath)
	}
	if got.TMDBID != 123 || got.Name != "Cars 3" || got.Year != 2017 {
		t.Fatalf("metadata = %+v, want Cars 3 detail", got)
	}
}

func TestSearchUsesConfiguredBaseURLAndLanguage(t *testing.T) {
	sawLanguages := map[string]bool{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/search/multi" {
			t.Fatalf("path = %q, want /search/multi", r.URL.Path)
		}
		sawLanguages[r.URL.Query().Get("language")] = true
		if r.URL.Query().Get("api_key") != "key" {
			t.Fatalf("api_key = %q, want key", r.URL.Query().Get("api_key"))
		}
		if r.URL.Query().Get("query") != "Cars 3" {
			t.Fatalf("query = %q, want Cars 3", r.URL.Query().Get("query"))
		}
		writeJSON(t, w, map[string]any{"results": []map[string]any{{
			"id":                123,
			"media_type":        "movie",
			"title":             "Cars 3",
			"release_date":      "2017-06-15",
			"genre_ids":         []int{16},
			"original_language": "en",
			"origin_country":    []string{"US"},
		}}})
	}))
	defer server.Close()

	got, err := Resolve(context.Background(), Config{APIKey: "key", BaseURL: server.URL, Language: "zh-CN"}, recognize.Result{
		Title:     "Cars 3",
		QueryList: []string{"Cars 3"},
		Year:      2017,
	})
	if err != nil {
		t.Fatalf("Resolve returned error: %v", err)
	}
	if !sawLanguages["zh-CN"] || !sawLanguages["en-US"] {
		t.Fatalf("languages = %#v, want zh-CN and en-US", sawLanguages)
	}
	if got == nil || got.TMDBID != 123 {
		t.Fatalf("metadata = %+v, want search result 123", got)
	}
}

func TestLowConfidenceReturnsNoMetadata(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, map[string]any{"results": []map[string]any{{
			"id":                321,
			"media_type":        "movie",
			"title":             "Totally Different",
			"release_date":      "1999-01-01",
			"genre_ids":         []int{28},
			"original_language": "en",
		}}})
	}))
	defer server.Close()

	got, err := Resolve(context.Background(), Config{APIKey: "key", BaseURL: server.URL}, recognize.Result{
		Title:     "Cars 3",
		QueryList: []string{"Cars 3"},
		Year:      2017,
	})
	if err != nil {
		t.Fatalf("Resolve returned error: %v", err)
	}
	if got != nil {
		t.Fatalf("metadata = %+v, want nil for low confidence", got)
	}
}

func TestResolveClassifiesWithCategoryRules(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, map[string]any{"results": []map[string]any{{
			"id":                123,
			"media_type":        "movie",
			"title":             "Cars 3",
			"release_date":      "2017-06-15",
			"genre_ids":         []int{16},
			"original_language": "en",
			"origin_country":    []string{"US"},
		}}})
	}))
	defer server.Close()

	got, err := Resolve(context.Background(), Config{
		APIKey:        "key",
		BaseURL:       server.URL,
		CategoryRules: "movie:\n  动画片:\n    genre_ids: '16'\n  未分类:\n",
	}, recognize.Result{Title: "Cars 3", QueryList: []string{"Cars 3"}, Year: 2017})
	if err != nil {
		t.Fatalf("Resolve returned error: %v", err)
	}
	if got == nil || got.Category != "动画片" {
		t.Fatalf("category = %+v, want 动画片", got)
	}
}

func TestResolveMatchesOriginalNameWhenLocalizedNameDiffers(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, map[string]any{"results": []map[string]any{{
			"id":                287009,
			"media_type":        "tv",
			"name":              "医到孤岛爱上你",
			"original_name":     "Doctor on the Edge",
			"first_air_date":    "2025-01-01",
			"origin_country":    []string{"KR"},
			"original_language": "ko",
		}}})
	}))
	defer server.Close()

	got, err := Resolve(context.Background(), Config{APIKey: "key", BaseURL: server.URL}, recognize.Result{
		Title:     "Doctor on the Edge",
		QueryList: []string{"Doctor on the Edge"},
		Year:      2025,
	})
	if err != nil {
		t.Fatalf("Resolve returned error: %v", err)
	}
	if got == nil || got.TMDBID != 287009 {
		t.Fatalf("metadata = %+v, want original-name match 287009", got)
	}
}

func TestResolveMatchesEnglishOriginalNameWithPunctuation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, map[string]any{"results": []map[string]any{{
			"id":                296206,
			"media_type":        "tv",
			"name":              "金特务：本色回归",
			"original_name":     "Agent Kim: Reactivated",
			"first_air_date":    "2026-01-01",
			"origin_country":    []string{"KR"},
			"original_language": "ko",
		}}})
	}))
	defer server.Close()

	got, err := Resolve(context.Background(), Config{APIKey: "key", BaseURL: server.URL}, recognize.Result{
		Title:         "Agent Kim Reactivated",
		QueryList:     []string{"Agent Kim Reactivated"},
		MediaTypeHint: "tv",
	})
	if err != nil {
		t.Fatalf("Resolve returned error: %v", err)
	}
	if got == nil || got.TMDBID != 296206 || got.Name != "金特务：本色回归" {
		t.Fatalf("metadata = %+v, want Chinese Agent Kim metadata", got)
	}
}

func TestResolveLocalizesEnglishFallbackResult(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/search/multi":
			if r.URL.Query().Get("language") == "zh-CN" {
				writeJSON(t, w, map[string]any{"results": []map[string]any{}})
				return
			}
			writeJSON(t, w, map[string]any{"results": []map[string]any{{
				"id":             296206,
				"media_type":     "tv",
				"name":           "Agent Kim: Reactivated",
				"original_name":  "Agent Kim: Reactivated",
				"first_air_date": "2026-01-01",
			}}})
		case "/tv/296206":
			if r.URL.Query().Get("language") != "zh-CN" {
				t.Fatalf("language = %q, want zh-CN", r.URL.Query().Get("language"))
			}
			writeJSON(t, w, map[string]any{
				"id":                296206,
				"name":              "金特务：本色回归",
				"original_name":     "Agent Kim: Reactivated",
				"first_air_date":    "2026-01-01",
				"origin_country":    []string{"KR"},
				"original_language": "ko",
			})
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer server.Close()

	got, err := Resolve(context.Background(), Config{APIKey: "key", BaseURL: server.URL}, recognize.Result{
		Title:         "Agent Kim Reactivated",
		QueryList:     []string{"Agent Kim Reactivated"},
		MediaTypeHint: "tv",
	})
	if err != nil {
		t.Fatalf("Resolve returned error: %v", err)
	}
	if got == nil || got.Name != "金特务：本色回归" {
		t.Fatalf("metadata = %+v, want localized Chinese title", got)
	}
}

func TestSearchCandidatesReturnsOriginalNameAndCategory(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, map[string]any{"results": []map[string]any{{
			"id":                287009,
			"media_type":        "tv",
			"name":              "医到孤岛爱上你",
			"original_name":     "Doctor on the Edge",
			"first_air_date":    "2025-01-01",
			"genre_ids":         []int{18},
			"origin_country":    []string{"KR"},
			"original_language": "ko",
		}}})
	}))
	defer server.Close()

	got, err := SearchCandidates(context.Background(), Config{
		APIKey:        "key",
		BaseURL:       server.URL,
		CategoryRules: "tv:\n  日韩剧:\n    origin_country: 'KR'\n  未分类:\n",
	}, "Doctor on the Edge")
	if err != nil {
		t.Fatalf("SearchCandidates returned error: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("candidate count = %d, want 1", len(got))
	}
	if got[0].OriginalName != "Doctor on the Edge" || got[0].Category != "日韩剧" {
		t.Fatalf("candidate = %+v, want original name and category", got[0])
	}
}

func TestSearchCandidatesLocalizesEnglishFallbackResult(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/search/multi":
			if r.URL.Query().Get("language") == "zh-CN" {
				writeJSON(t, w, map[string]any{"results": []map[string]any{}})
				return
			}
			writeJSON(t, w, map[string]any{"results": []map[string]any{{
				"id":             296206,
				"media_type":     "tv",
				"name":           "Agent Kim: Reactivated",
				"original_name":  "Agent Kim: Reactivated",
				"first_air_date": "2026-01-01",
			}}})
		case "/tv/296206":
			writeJSON(t, w, map[string]any{
				"id":             296206,
				"name":           "金特务：本色回归",
				"original_name":  "Agent Kim: Reactivated",
				"first_air_date": "2026-01-01",
				"poster_path":    "/poster.jpg",
			})
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer server.Close()

	got, err := SearchCandidates(context.Background(), Config{APIKey: "key", BaseURL: server.URL}, "Agent Kim Reactivated")
	if err != nil {
		t.Fatalf("SearchCandidates returned error: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("candidate count = %d, want 1", len(got))
	}
	if got[0].Name != "金特务：本色回归" || got[0].PosterURL == "" {
		t.Fatalf("candidate = %+v, want localized title and poster", got[0])
	}
}

func TestSearchCandidatesNumericIDFetchesDetailsWithPoster(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/tv/296206":
			writeJSON(t, w, map[string]any{
				"id":                296206,
				"name":              "金特务：本色回归",
				"original_name":     "Agent Kim: Reactivated",
				"first_air_date":    "2026-01-01",
				"poster_path":       "/poster.jpg",
				"genres":            []map[string]any{{"id": 18}},
				"origin_country":    []string{"KR"},
				"original_language": "ko",
			})
		case "/movie/296206":
			http.Error(w, "not found", http.StatusNotFound)
		case "/search/multi":
			writeJSON(t, w, map[string]any{"results": []map[string]any{}})
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer server.Close()

	got, err := SearchCandidates(context.Background(), Config{APIKey: "key", BaseURL: server.URL}, "296206")
	if err != nil {
		t.Fatalf("SearchCandidates returned error: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("candidate count = %d, want 1", len(got))
	}
	if got[0].PosterURL != "https://image.tmdb.org/t/p/w185/poster.jpg" {
		t.Fatalf("PosterURL = %q", got[0].PosterURL)
	}
}

func TestSearchCandidatesReturnsTVSeasonInfo(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/search/multi":
			writeJSON(t, w, map[string]any{"results": []map[string]any{{
				"id":             285205,
				"media_type":     "tv",
				"name":           "百万秘宝攻防战",
				"first_air_date": "2025-01-01",
			}}})
		case "/tv/285205":
			writeJSON(t, w, map[string]any{
				"id":             285205,
				"name":           "百万秘宝攻防战",
				"first_air_date": "2025-01-01",
				"seasons": []map[string]any{
					{"season_number": 0, "episode_count": 1, "name": "Specials"},
					{"season_number": 1, "episode_count": 12, "name": "Season 1"},
					{"season_number": 2, "episode_count": 8, "name": "Season 2"},
				},
			})
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer server.Close()

	got, err := SearchCandidates(context.Background(), Config{APIKey: "key", BaseURL: server.URL}, "百万秘宝攻防战")
	if err != nil {
		t.Fatalf("SearchCandidates returned error: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("candidate count = %d, want 1", len(got))
	}
	if len(got[0].Seasons) != 2 {
		t.Fatalf("seasons = %#v, want regular seasons only", got[0].Seasons)
	}
	if got[0].Seasons[0].SeasonNumber != 1 || got[0].Seasons[0].EpisodeCount != 12 {
		t.Fatalf("first season = %#v, want S1 with 12 episodes", got[0].Seasons[0])
	}
	if got[0].SeasonMap[2] != 8 {
		t.Fatalf("season map = %#v, want season 2 episode count", got[0].SeasonMap)
	}
}

func TestScoreCandidatePrefersExactBilingualEquivalentTitle(t *testing.T) {
	recognized := recognize.Result{Title: "雨人 Rain Man 1988 蓝光原盘", Year: 1988, MediaTypeHint: "movie"}
	exact := tmdbItem{Title: "雨人", OriginalTitle: "Rain Man", ReleaseDate: "1988-12-12", MediaType: "movie"}
	weak := tmdbItem{Title: "雨人诞生秘话", OriginalTitle: "Rain Man Documentary", ReleaseDate: "1988-12-12", MediaType: "movie"}
	if scoreCandidate(exact, recognized) <= scoreCandidate(weak, recognized) {
		t.Fatal("exact bilingual match did not outrank weak documentary-style title for noisy local title")
	}
}

func TestScoreCandidatePenalizesWeakSubstringMatches(t *testing.T) {
	recognized := recognize.Result{Title: "Shrinking", MediaTypeHint: "tv"}
	good := tmdbItem{Name: "Shrinking", OriginalName: "Shrinking", FirstAirDate: "2023-01-01", MediaType: "tv"}
	bad := tmdbItem{Name: "The Making of Shrinking", OriginalName: "The Making of Shrinking", FirstAirDate: "2023-01-01", MediaType: "tv"}
	if scoreCandidate(good, recognized) <= scoreCandidate(bad, recognized) {
		t.Fatal("weak substring candidate outranked exact title")
	}
}

func TestScoreCandidateUsesRecognizeQueryListAliases(t *testing.T) {
	recognized := recognize.Result{
		Title:         "千与千寻",
		QueryList:     []string{"千与千寻", "Spirited Away"},
		MediaTypeHint: "movie",
	}
	good := tmdbItem{Title: "Spirited Away", OriginalTitle: "Spirited Away", ReleaseDate: "2001-07-20", MediaType: "movie"}
	bad := tmdbItem{Title: "千与千寻诞生秘话", OriginalTitle: "Spirited Away Documentary", ReleaseDate: "2001-07-20", MediaType: "movie"}
	if scoreCandidate(good, recognized) <= scoreCandidate(bad, recognized) {
		t.Fatal("query-list alias did not help exact candidate outrank documentary-style title")
	}
}

func writeJSON(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		t.Fatalf("encode json: %v", err)
	}
}
