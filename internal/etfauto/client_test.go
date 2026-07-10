package etfauto

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestTargetClientCreatesETFSubscriptionAndParsesTask(t *testing.T) {
	var gotPath string
	var gotBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatalf("decode request body: %v", err)
		}
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"subscription": map[string]any{"id": 12, "tmdb_id": 260868, "media_type": "tv"},
			"task_id":      "task_create",
			"type":         "subscription:check_updates",
			"status":       "pending",
		})
	}))
	defer server.Close()

	client := NewTargetClient(server.URL+"/api/v1", server.Client(), time.Second)
	result, err := client.CreateSubscription(context.Background(), CreateSubscriptionPayload{
		TMDBID:       260868,
		MediaType:    "tv",
		ShareURL:     "https://yun.139.com/w/i/abc",
		AccessCode:   "1234",
		ShareType:    "etf",
		SeasonStart:  1,
		EpisodeStart: 1,
	})
	if err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	if gotPath != "/api/v1/subscriptions" {
		t.Fatalf("path = %q, want /api/v1/subscriptions", gotPath)
	}
	if gotBody["share_type"] != "etf" || gotBody["share_url"] != "https://yun.139.com/w/i/abc" {
		t.Fatalf("request body = %#v, want etf share url", gotBody)
	}
	if result.SubscriptionID != 12 || result.TaskID != "task_create" {
		t.Fatalf("result = %#v, want subscription 12 task_create", result)
	}
}

func TestTargetClientChecksSubscriptionAndParsesTask(t *testing.T) {
	var gotPaths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPaths = append(gotPaths, r.Method+" "+r.URL.Path)
		if r.URL.Path == "/api/v1/subscriptions/77" {
			if r.Method != http.MethodGet {
				t.Fatalf("preflight method = %s, want GET", r.Method)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 77})
			return
		}
		if r.URL.Path != "/api/v1/subscriptions/77/check" {
			t.Fatalf("path = %q, want subscription get or check", r.URL.Path)
		}
		if r.Method != http.MethodPost {
			t.Fatalf("check method = %s, want POST", r.Method)
		}
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"task_id": "task_check",
			"type":    "subscription:check_updates",
			"status":  "pending",
		})
	}))
	defer server.Close()

	client := NewTargetClient(server.URL+"/api/v1/", server.Client(), time.Second)
	result, err := client.CheckSubscription(context.Background(), 77)
	if err != nil {
		t.Fatalf("check subscription: %v", err)
	}
	wantPaths := []string{"GET /api/v1/subscriptions/77", "POST /api/v1/subscriptions/77/check"}
	if fmt.Sprint(gotPaths) != fmt.Sprint(wantPaths) {
		t.Fatalf("paths = %#v, want %#v", gotPaths, wantPaths)
	}
	if result.TaskID != "task_check" {
		t.Fatalf("task id = %q, want task_check", result.TaskID)
	}
}
