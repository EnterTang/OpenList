package etfauto

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestRapidJSONPayloadFromRecord(t *testing.T) {
	season, episode := 2, 7
	payload, err := RapidJSONPayloadFromRecord(&model.ETFArchiveRecord{
		TMDBID: 123, MediaType: "tv", TMDBName: "Example",
		SourceName: "Example.S02E07.mkv", SourceSize: 42,
		SourceSHA256: strings.Repeat("a", 64), Season: season, Episode: episode,
	})
	if err != nil {
		t.Fatal(err)
	}
	if payload.TMDBID != 123 || payload.MediaType != "tv" || len(payload.Items) != 1 {
		t.Fatalf("payload = %#v", payload)
	}
	if payload.Items[0].Season == nil || *payload.Items[0].Season != season || payload.Items[0].Episode == nil || *payload.Items[0].Episode != episode {
		t.Fatalf("item = %#v", payload.Items[0])
	}
}

func TestCreateRapidJSONArchiveSendsExpectedRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/resource-ingest/rapid-json/archive" {
			t.Errorf("path = %s", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer secret" {
			t.Errorf("authorization = %q", r.Header.Get("Authorization"))
		}
		body, _ := io.ReadAll(r.Body)
		var payload RapidJSONArchivePayload
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Errorf("request JSON: %v", err)
		}
		if payload.TMDBID != 123 || len(payload.Items) != 1 {
			t.Errorf("payload = %#v", payload)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"task-1"}`))
	}))
	defer server.Close()
	client := NewTargetClient(server.URL+"/api/v1", "secret", server.Client(), 0)
	response, err := client.CreateRapidJSONArchive(context.Background(), RapidJSONArchivePayload{
		TMDBID: 123, MediaType: "movie", Items: []RapidJSONArchiveItem{{FileName: "movie.mkv", FileSize: 1, SHA256: strings.Repeat("B", 64)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if response != `{"id":"task-1"}` {
		t.Fatalf("response = %s", response)
	}
}
