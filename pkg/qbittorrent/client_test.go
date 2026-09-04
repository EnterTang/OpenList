package qbittorrent

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

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

func TestGetInfoUsesNativeHashLookupForHashIdentifiers(t *testing.T) {
	hash := strings.Repeat("1", 40)
	var requested url.Values
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v2/app/version" {
			if r.Method != http.MethodGet {
				t.Fatalf("version method = %s, want GET", r.Method)
			}
			_, _ = io.WriteString(w, "5.2.0")
			return
		}
		if r.URL.Path != "/api/v2/torrents/info" {
			http.NotFound(w, r)
			return
		}
		requested = r.URL.Query()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]TorrentInfo{{Hash: hash, ContentPath: "/downloads/Show"}})
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("new qB client: %v", err)
	}
	info, err := client.GetInfo(hash)
	if err != nil {
		t.Fatalf("get info by hash: %v", err)
	}
	if info.Hash != hash || requested.Get("hashes") != hash || requested.Get("tag") != "" {
		t.Fatalf("hash lookup = %#v/%#v, want hashes=%q without tag", info, requested, hash)
	}
}

func TestGetFreeSpaceAtPathUsesQBBvisiblePath(t *testing.T) {
	var requestedPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v2/app/version" {
			_, _ = io.WriteString(w, "5.2.0")
			return
		}
		if r.URL.Path != "/api/v2/app/getFreeSpaceAtPathAction" {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodPost {
			t.Fatalf("free-space method = %s, want POST", r.Method)
		}
		if err := r.ParseForm(); err != nil {
			t.Fatal(err)
		}
		requestedPath = r.Form.Get("path")
		_, _ = io.WriteString(w, "123456789")
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("new qB client: %v", err)
	}
	free, err := client.(FreeSpaceClient).GetFreeSpaceAtPath(context.Background(), `F:\downloads\Show`)
	if err != nil {
		t.Fatalf("get free space at path: %v", err)
	}
	if free != 123456789 || requestedPath != `F:\downloads\Show` {
		t.Fatalf("free space/path = %d/%q, want %d/%q", free, requestedPath, 123456789, `F:\downloads\Show`)
	}
}

func TestGetFreeSpaceUsesSyncMainDataForOlderQB(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v2/app/version" {
			_, _ = io.WriteString(w, "5.2.3")
			return
		}
		if r.URL.Path != "/api/v2/sync/maindata" {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodGet {
			t.Fatalf("global free-space method = %s, want GET", r.Method)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"server_state": map[string]int64{"free_space_on_disk": 386332295168},
		})
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("new qB client: %v", err)
	}
	free, err := client.(GlobalFreeSpaceClient).GetFreeSpace(context.Background())
	if err != nil {
		t.Fatalf("get global free space: %v", err)
	}
	if free != 386332295168 {
		t.Fatalf("global free space = %d, want %d", free, 386332295168)
	}
}

func TestHTTPQBEndpointRetriesHTTPSWhenServerRequiresIt(t *testing.T) {
	webUIURL, err := url.Parse("http://qb.example")
	if err != nil {
		t.Fatal(err)
	}
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	var schemes []string
	qb := &client{url: webUIURL, client: http.Client{Jar: jar, Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		schemes = append(schemes, request.URL.Scheme)
		if request.URL.Scheme == "http" {
			return &http.Response{StatusCode: http.StatusBadRequest, Status: "400 Bad Request", Body: io.NopCloser(strings.NewReader("Client sent an HTTP request to an HTTPS server.")), Header: make(http.Header)}, nil
		}
		return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader("5.2.0")), Header: make(http.Header)}, nil
	})}}

	if !qb.authorized() {
		t.Fatal("HTTPS fallback did not authorize qB request")
	}
	if strings.Join(schemes, ",") != "http,https" {
		t.Fatalf("request schemes = %#v, want HTTP then HTTPS", schemes)
	}
}

func TestGetTorrentsListsAllTorrentsWithoutHashFilter(t *testing.T) {
	hash := strings.Repeat("f", 40)
	var gotForm url.Values
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v2/app/version" {
			_, _ = io.WriteString(w, "5.0.0")
			return
		}
		if r.URL.Path != "/api/v2/torrents/info" {
			http.NotFound(w, r)
			return
		}
		if err := r.ParseForm(); err != nil {
			t.Fatal(err)
		}
		gotForm = r.Form
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]TorrentInfo{{Hash: hash, Name: "Show", Progress: 1}})
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("new qB client: %v", err)
	}
	infos, err := client.GetTorrents(context.Background())
	if err != nil {
		t.Fatalf("get torrents: %v", err)
	}
	if len(infos) != 1 || infos[0].Hash != hash {
		t.Fatalf("unexpected torrents: %#v", infos)
	}
	if len(gotForm) != 0 {
		t.Fatalf("all-torrents request unexpectedly had filters: %#v", gotForm)
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

func TestDeleteUsesNativeHashWithoutDeletingLegacyTag(t *testing.T) {
	hash := strings.Repeat("2", 40)
	deleteTagsCalled := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v2/app/version" {
			_, _ = io.WriteString(w, "5.0.0")
			return
		}
		if r.URL.Path == "/api/v2/torrents/deleteTags" {
			deleteTagsCalled = true
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("new qB client: %v", err)
	}
	if err := client.Delete(hash, false); err != nil {
		t.Fatalf("delete qB torrent by hash: %v", err)
	}
	if deleteTagsCalled {
		t.Fatal("native hash deletion unexpectedly used the legacy tag protocol")
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

func TestNewDoesNotExposeCredentialsWhenLoginIsRejected(t *testing.T) {
	webUIURL, err := url.Parse("https://alice:super-secret@qb.example")
	if err != nil {
		t.Fatalf("parse qB URL: %v", err)
	}
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatalf("create cookie jar: %v", err)
	}
	qb := &client{url: webUIURL, client: http.Client{Jar: jar, Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader("Fails.")), Header: make(http.Header)}, nil
	})}}
	err = qb.login()
	if err == nil {
		t.Fatal("rejected qB login unexpectedly succeeded")
	}
	for _, secret := range []string{"alice", "super-secret"} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("login error leaked %q: %v", secret, err)
		}
	}
}

func TestHashMethodsRejectQBAllSentinelWithoutSendingControl(t *testing.T) {
	webUIURL, err := url.Parse("https://qb.example")
	if err != nil {
		t.Fatal(err)
	}
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	requests := 0
	qb := &client{url: webUIURL, client: http.Client{Jar: jar, Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		requests++
		return &http.Response{StatusCode: http.StatusNoContent, Status: "204 No Content", Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, nil
	})}}

	if err := qb.DeleteByHash(context.Background(), "all", true); err == nil {
		t.Fatal("qB all sentinel was accepted as an exact torrent hash")
	}
	if requests != 0 {
		t.Fatalf("invalid hash sent %d qB request(s)", requests)
	}
}

func TestGetTorrentByHashRejectsMismatchedQBResult(t *testing.T) {
	requestedHash := strings.Repeat("a", 40)
	otherHash := strings.Repeat("b", 40)
	webUIURL, err := url.Parse("https://qb.example")
	if err != nil {
		t.Fatal(err)
	}
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	qb := &client{url: webUIURL, client: http.Client{Jar: jar, Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		body := "5.2.0"
		if request.URL.Path == "/api/v2/torrents/info" {
			body = `[{"hash":"` + otherHash + `"}]`
		}
		return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
	})}}

	if _, err := qb.GetTorrentByHash(context.Background(), requestedHash); err == nil {
		t.Fatal("mismatched qB torrent result was accepted")
	}
}

func TestControlByHashFallsBackToLegacyQBEndpoints(t *testing.T) {
	webUIURL, err := url.Parse("https://qb.example")
	if err != nil {
		t.Fatal(err)
	}
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	var controls []string
	qb := &client{url: webUIURL, client: http.Client{Jar: jar, Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		status := http.StatusOK
		body := "5.0.0"
		if request.URL.Path != "/api/v2/app/version" {
			controls = append(controls, request.URL.Path)
			body = ""
			if request.URL.Path == "/api/v2/torrents/start" || request.URL.Path == "/api/v2/torrents/stop" {
				status = http.StatusNotFound
			}
		}
		return &http.Response{StatusCode: status, Status: http.StatusText(status), Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
	})}}
	hash := strings.Repeat("c", 40)

	if err := qb.StopByHash(context.Background(), hash); err != nil {
		t.Fatalf("legacy pause fallback: %v", err)
	}
	if err := qb.StartByHash(context.Background(), hash); err != nil {
		t.Fatalf("legacy resume fallback: %v", err)
	}
	want := []string{
		"/api/v2/torrents/stop", "/api/v2/torrents/pause",
		"/api/v2/torrents/start", "/api/v2/torrents/resume",
	}
	if strings.Join(controls, ",") != strings.Join(want, ",") {
		t.Fatalf("control endpoints = %#v, want %#v", controls, want)
	}
}

func TestAddFromLinkAcceptsNoContentAndRejectsHTTPFailure(t *testing.T) {
	for _, test := range []struct {
		name      string
		addStatus int
		wantErr   bool
	}{
		{name: "qB 5.2 no-content success", addStatus: http.StatusNoContent},
		{name: "qB rejects torrent", addStatus: http.StatusBadRequest, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			webUIURL, err := url.Parse("https://qb.example")
			if err != nil {
				t.Fatalf("parse qB URL: %v", err)
			}
			jar, err := cookiejar.New(nil)
			if err != nil {
				t.Fatalf("create cookie jar: %v", err)
			}
			qb := &client{url: webUIURL, client: http.Client{Jar: jar, Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
				status := http.StatusOK
				body := "5.2.0"
				if request.URL.Path == "/api/v2/torrents/add" {
					status = test.addStatus
					body = ""
				}
				return &http.Response{StatusCode: status, Status: http.StatusText(status), Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
			})}}

			err = qb.AddFromLink("magnet:?xt=urn:btih:example", "/downloads", "job-1")

			if test.wantErr && err == nil {
				t.Fatal("qB add HTTP failure unexpectedly succeeded")
			}
			if !test.wantErr && err != nil {
				t.Fatalf("qB add no-content response failed: %v", err)
			}
		})
	}
}
