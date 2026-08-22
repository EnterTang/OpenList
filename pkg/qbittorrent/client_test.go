package qbittorrent

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestGetFilesByHashUsesTheQBTorrentHash(t *testing.T) {
	hash := strings.Repeat("c", 40)
	var requestedHash string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v2/app/version" {
			_, _ = io.WriteString(w, "5.0.0")
			return
		}
		if r.URL.Path != "/api/v2/torrents/files" {
			http.NotFound(w, r)
			return
		}
		if err := r.ParseForm(); err != nil {
			t.Fatal(err)
		}
		requestedHash = r.Form.Get("hash")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]FileInfo{{Name: "Season 01/E01.mkv", Size: 42, Progress: 1}})
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("new qB client: %v", err)
	}
	files, err := client.GetFilesByHash(context.Background(), hash)
	if err != nil {
		t.Fatalf("get files by hash: %v", err)
	}
	if requestedHash != hash {
		t.Fatalf("qB hash = %q, want %q", requestedHash, hash)
	}
	if len(files) != 1 || files[0].Name != "Season 01/E01.mkv" {
		t.Fatalf("unexpected files: %#v", files)
	}
}

func TestGetTorrentByHashUsesHashesParameter(t *testing.T) {
	hash := strings.Repeat("d", 40)
	var requestedHash string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v2/app/version" {
			_, _ = io.WriteString(w, "5.0.0")
			return
		}
		if err := r.ParseForm(); err != nil {
			t.Fatal(err)
		}
		requestedHash = r.Form.Get("hashes")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]TorrentInfo{{Hash: hash, Progress: 1, ContentPath: "/downloads/Show"}})
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("new qB client: %v", err)
	}
	info, err := client.GetTorrentByHash(context.Background(), hash)
	if err != nil {
		t.Fatalf("get torrent by hash: %v", err)
	}
	if requestedHash != hash || info.Hash != hash {
		t.Fatalf("qB hash = %q/%q, want %q", requestedHash, info.Hash, hash)
	}
}

func TestControlByHashSendsHashWithoutTagLookup(t *testing.T) {
	hash := strings.Repeat("e", 40)
	var endpoints []string
	var requestedHashes []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v2/app/version" {
			_, _ = io.WriteString(w, "5.0.0")
			return
		}
		if err := r.ParseForm(); err != nil {
			t.Fatal(err)
		}
		endpoints = append(endpoints, r.URL.Path)
		requestedHashes = append(requestedHashes, r.Form.Get("hashes"))
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("new qB client: %v", err)
	}
	if err := client.StopByHash(context.Background(), hash); err != nil {
		t.Fatalf("stop by hash: %v", err)
	}
	if err := client.StartByHash(context.Background(), hash); err != nil {
		t.Fatalf("start by hash: %v", err)
	}
	if err := client.DeleteByHash(context.Background(), hash, true); err != nil {
		t.Fatalf("delete by hash: %v", err)
	}
	wantEndpoints := []string{"/api/v2/torrents/stop", "/api/v2/torrents/start", "/api/v2/torrents/delete"}
	if strings.Join(endpoints, ",") != strings.Join(wantEndpoints, ",") {
		t.Fatalf("control endpoints = %#v, want %#v", endpoints, wantEndpoints)
	}
	for _, requestedHash := range requestedHashes {
		if requestedHash != hash {
			t.Fatalf("control hash = %q, want %q", requestedHash, hash)
		}
	}
}

func TestQBClientAcceptsContextCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v2/app/version" {
			_, _ = io.WriteString(w, "5.0.0")
			return
		}
		<-r.Context().Done()
	}))
	defer server.Close()
	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("new qB client: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = client.GetFilesByHash(ctx, strings.Repeat("f", 40))
	if err == nil {
		t.Fatal("expected context cancellation error")
	}
}
