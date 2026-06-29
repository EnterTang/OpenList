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
	var sawSearch bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/search/multi" {
			t.Fatalf("path = %q, want /search/multi", r.URL.Path)
		}
		sawSearch = true
		if r.URL.Query().Get("api_key") != "key" {
			t.Fatalf("api_key = %q, want key", r.URL.Query().Get("api_key"))
		}
		if r.URL.Query().Get("language") != "zh-CN" {
			t.Fatalf("language = %q, want zh-CN", r.URL.Query().Get("language"))
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
	if !sawSearch {
		t.Fatal("search endpoint was not called")
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

func writeJSON(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		t.Fatalf("encode json: %v", err)
	}
}
