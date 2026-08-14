package hdhive

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestSymediaClientPerformsHandshakeAndSignsRequests(t *testing.T) {
	const (
		secret      = "proxy-secret"
		userKey     = "proxy-user-key"
		clientNonce = "client-nonce"
		serverNonce = "server-nonce"
		sessionID   = "session-1"
	)
	var requestCount int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		if r.URL.Path == "/api/v1/auth/session" {
			var request map[string]string
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Fatalf("decode handshake: %v", err)
			}
			if request["client_nonce"] != clientNonce || request["client_proof"] != hmacHex([]byte(secret), []byte("hdhive-openproxy-proof\nclient\n"+clientNonce)) {
				t.Fatalf("handshake request = %#v", request)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]string{
				"server_nonce": serverNonce,
				"session_id":   sessionID,
				"server_proof": hmacHex([]byte(secret), []byte("hdhive-openproxy-proof\nserver\n"+serverNonce)),
			}})
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read request: %v", err)
		}
		sequence := r.Header.Get("X-Proxy-Sequence")
		bodyHash := sha256.Sum256(body)
		expectedHash := hex.EncodeToString(bodyHash[:])
		if r.Header.Get("X-Proxy-Session") != sessionID || r.Header.Get("X-Proxy-User-Key") != userKey || r.Header.Get("X-Proxy-Body-SHA256") != expectedHash {
			t.Fatalf("signed headers = %#v", r.Header)
		}
		canonical := strings.Join([]string{r.Method, r.URL.RequestURI(), sessionID, sequence, expectedHash, userKey}, "\n")
		prk := hmacBytes([]byte("hdhive-openproxy-session:"+clientNonce+":"+serverNonce), []byte(secret))
		sessionKey := hmacBytes(prk, []byte("hdhive-openproxy-session-key\x01"))
		if r.Header.Get("X-Proxy-Signature") != hmacHex(sessionKey, []byte(canonical)) {
			t.Fatalf("signature = %q", r.Header.Get("X-Proxy-Signature"))
		}

		switch r.URL.Path {
		case "/api/v1/users/user-1/status":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"authorized": true}})
		case "/api/v1/open/user-1/shares/22c7835aacad4e3f9fee349d2d803cb1":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"unlock_points": 1}})
		case "/api/v1/open/user-1/resources/unlock":
			if string(body) != `{"slug":"22c7835aacad4e3f9fee349d2d803cb1"}` {
				t.Fatalf("unlock body = %s", body)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"full_url": "https://115cdn.com/s/example", "access_code": "abcd"}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client, err := NewSymediaClient(SymediaConfig{BaseURL: server.URL, UserID: "user-1", ProxyUserKey: userKey, ProxySecret: secret, Timeout: time.Second}, server.Client())
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	client.randomNonce = func() (string, error) { return clientNonce, nil }

	status, err := client.Status(context.Background())
	if err != nil || !status.Authorized {
		t.Fatalf("status = %#v, err = %v", status, err)
	}
	details, err := client.Share(context.Background(), "22c7835aacad4e3f9fee349d2d803cb1")
	if err != nil || details.UnlockPoints == nil || *details.UnlockPoints != 1 {
		t.Fatalf("share = %#v, err = %v", details, err)
	}
	unlocked, err := client.Unlock(context.Background(), "22c7835aacad4e3f9fee349d2d803cb1")
	if err != nil || unlocked.FullURL == "" || unlocked.AccessCode != "abcd" {
		t.Fatalf("unlock = %#v, err = %v", unlocked, err)
	}
	if requestCount != 4 {
		t.Fatalf("request count = %s, want handshake plus three signed requests", strconv.Itoa(requestCount))
	}
}

func TestSymediaClientSearchesResourcesByTMDBID(t *testing.T) {
	const (
		secret      = "proxy-secret"
		userKey     = "proxy-user-key"
		clientNonce = "client-nonce"
		serverNonce = "server-nonce"
		sessionID   = "session-1"
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/auth/session":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]string{
				"server_nonce": serverNonce,
				"session_id":   sessionID,
				"server_proof": hmacHex([]byte(secret), []byte("hdhive-openproxy-proof\nserver\n"+serverNonce)),
			}})
		case "/api/v1/open/user-1/resources/tv/1399":
			if r.Method != http.MethodGet {
				t.Fatalf("search method = %s, want GET", r.Method)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{{
				"slug":             "22c7835aacad4e3f9fee349d2d803cb1",
				"resource_url":     "https://hdhive.com/resource/115/22c7835aacad4e3f9fee349d2d803cb1",
				"title":            "完全不相关的展示标题",
				"pan_type":         "115",
				"unlock_points":    10,
				"is_unlocked":      false,
				"video_resolution": []string{"4K"},
			}}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client, err := NewSymediaClient(SymediaConfig{
		BaseURL:      server.URL,
		UserID:       "user-1",
		ProxyUserKey: userKey,
		ProxySecret:  secret,
		Timeout:      time.Second,
	}, server.Client())
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	client.randomNonce = func() (string, error) { return clientNonce, nil }

	resources, err := client.Search(context.Background(), "tv", 1399)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(resources) != 1 {
		t.Fatalf("resources = %#v, want one resource", resources)
	}
	if resources[0].Slug != "22c7835aacad4e3f9fee349d2d803cb1" || resources[0].Title != "完全不相关的展示标题" || resources[0].PanType != "115" || resources[0].UnlockPoints == nil || *resources[0].UnlockPoints != 10 {
		t.Fatalf("resource = %#v", resources[0])
	}
}

func TestSymediaClientRebuildsSessionAfterProxyAuth403(t *testing.T) {
	const (
		secret      = "proxy-secret"
		userKey     = "proxy-user-key"
		clientNonce = "client-nonce"
	)
	var handshakeCount int
	var searchSessions []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/auth/session":
			handshakeCount++
			sessionID := "session-" + strconv.Itoa(handshakeCount)
			serverNonce := "server-nonce-" + strconv.Itoa(handshakeCount)
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]string{
				"server_nonce": serverNonce,
				"session_id":   sessionID,
				"server_proof": hmacHex([]byte(secret), []byte("hdhive-openproxy-proof\nserver\n"+serverNonce)),
			}})
		case "/api/v1/open/user-1/resources/tv/1399":
			searchSessions = append(searchSessions, r.Header.Get("X-Proxy-Session"))
			if len(searchSessions) == 1 {
				w.WriteHeader(http.StatusForbidden)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"success": false,
					"message": "缺少必要请求头，请更新镜像或客户端版本；密钥错误或签名无效",
				})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{{
				"slug":     "22c7835aacad4e3f9fee349d2d803cb1",
				"title":    "retry-ok",
				"pan_type": "115",
			}}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client, err := NewSymediaClient(SymediaConfig{
		BaseURL:      server.URL,
		UserID:       "user-1",
		ProxyUserKey: userKey,
		ProxySecret:  secret,
		Timeout:      time.Second,
	}, server.Client())
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	client.randomNonce = func() (string, error) { return clientNonce, nil }

	resources, err := client.Search(context.Background(), "tv", 1399)
	if err != nil {
		t.Fatalf("search after proxy auth 403: %v", err)
	}
	if len(resources) != 1 || resources[0].Title != "retry-ok" {
		t.Fatalf("resources = %#v", resources)
	}
	if handshakeCount != 2 {
		t.Fatalf("handshake count = %d, want 2", handshakeCount)
	}
	if len(searchSessions) != 2 || searchSessions[0] != "session-1" || searchSessions[1] != "session-2" {
		t.Fatalf("search sessions = %#v", searchSessions)
	}
}

func TestSymediaClientDoesNotRetryBusiness403(t *testing.T) {
	const (
		secret      = "proxy-secret"
		userKey     = "proxy-user-key"
		clientNonce = "client-nonce"
		serverNonce = "server-nonce"
		sessionID   = "session-1"
	)
	var searchCount int
	var handshakeCount int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/auth/session":
			handshakeCount++
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]string{
				"server_nonce": serverNonce,
				"session_id":   sessionID,
				"server_proof": hmacHex([]byte(secret), []byte("hdhive-openproxy-proof\nserver\n"+serverNonce)),
			}})
		case "/api/v1/open/user-1/resources/tv/1399":
			searchCount++
			w.WriteHeader(http.StatusForbidden)
			_ = json.NewEncoder(w).Encode(map[string]any{"success": false, "message": "no permission"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client, err := NewSymediaClient(SymediaConfig{
		BaseURL:      server.URL,
		UserID:       "user-1",
		ProxyUserKey: userKey,
		ProxySecret:  secret,
		Timeout:      time.Second,
	}, server.Client())
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	client.randomNonce = func() (string, error) { return clientNonce, nil }

	_, err = client.Search(context.Background(), "tv", 1399)
	if err == nil || !strings.Contains(err.Error(), "no permission") {
		t.Fatalf("search error = %v, want no permission", err)
	}
	if handshakeCount != 1 {
		t.Fatalf("handshake count = %d, want 1", handshakeCount)
	}
	if searchCount != 1 {
		t.Fatalf("search count = %d, want 1", searchCount)
	}
}

func TestSymediaClientSignsRequestPathIncludingQuery(t *testing.T) {
	const (
		secret      = "proxy-secret"
		userKey     = "proxy-user-key"
		clientNonce = "client-nonce"
		serverNonce = "server-nonce"
		sessionID   = "session-1"
	)
	var sawQuerySignature bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/auth/session" {
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]string{
				"server_nonce": serverNonce,
				"session_id":   sessionID,
				"server_proof": hmacHex([]byte(secret), []byte("hdhive-openproxy-proof\nserver\n"+serverNonce)),
			}})
			return
		}
		if r.URL.Path != "/api/v1/open/user-1/resources/unlock" || r.URL.RawQuery != "from=test" {
			http.NotFound(w, r)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read request: %v", err)
		}
		bodyHash := sha256.Sum256(body)
		expectedHash := hex.EncodeToString(bodyHash[:])
		canonical := strings.Join([]string{r.Method, r.URL.RequestURI(), sessionID, r.Header.Get("X-Proxy-Sequence"), expectedHash, userKey}, "\n")
		prk := hmacBytes([]byte("hdhive-openproxy-session:"+clientNonce+":"+serverNonce), []byte(secret))
		sessionKey := hmacBytes(prk, []byte("hdhive-openproxy-session-key\x01"))
		if r.Header.Get("X-Proxy-Signature") != hmacHex(sessionKey, []byte(canonical)) {
			t.Fatalf("signature = %q, canonical = %q", r.Header.Get("X-Proxy-Signature"), canonical)
		}
		sawQuerySignature = true
		_ = json.NewEncoder(w).Encode(map[string]any{"success": true})
	}))
	defer server.Close()

	client, err := NewSymediaClient(SymediaConfig{
		BaseURL:      server.URL,
		UserID:       "user-1",
		ProxyUserKey: userKey,
		ProxySecret:  secret,
		Timeout:      time.Second,
	}, server.Client())
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	client.randomNonce = func() (string, error) { return clientNonce, nil }

	_, err = client.signedRequest(context.Background(), http.MethodPost, "/api/v1/open/user-1/resources/unlock?from=test", []byte(`{"slug":"abc"}`))
	if err != nil {
		t.Fatalf("signed request: %v", err)
	}
	if !sawQuerySignature {
		t.Fatal("did not observe a signed unlock request with query")
	}
}
