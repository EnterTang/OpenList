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
