package _115sy

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestGetIDByPathNormalizesAndCachesComponents(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Query().Get("cid") {
		case "0":
			_, _ = w.Write([]byte(`{"state":true,"errno":0,"data":[{"id":"movies","name":"Movies","is_dir":true,"parent_cid":"0"}]}`))
		case "movies":
			_, _ = w.Write([]byte(`{"state":true,"errno":0,"data":[{"id":"season","name":"Season 1","is_dir":true,"parent_cid":"movies"}]}`))
		default:
			t.Fatalf("unexpected cid = %q", r.URL.Query().Get("cid"))
		}
	}))
	defer server.Close()
	client := newTestClient(t, ClientOptions{LimitRate: 1e6, AndroidBaseURL: server.URL, WebBaseURL: server.URL})
	for _, raw := range []string{"/Movies//Season 1/.", "Movies/Season 1"} {
		id, err := client.GetIDByPath(context.Background(), raw)
		if err != nil || id != "season" {
			t.Fatalf("GetIDByPath(%q) = %q, %v", raw, id, err)
		}
	}
	if calls.Load() != 2 {
		t.Fatalf("list calls = %d, want successful component cache", calls.Load())
	}
	root, err := client.GetIDByPath(context.Background(), ".")
	if err != nil || root != "0" {
		t.Fatalf("root = %q, %v", root, err)
	}
}

func TestGetIDByPathRejectsRootEscape(t *testing.T) {
	client := newTestClient(t, ClientOptions{})
	for _, raw := range []string{"..", "../secret", "../../secret"} {
		_, err := client.GetIDByPath(context.Background(), raw)
		var protocolErr *ProtocolError
		if !errors.As(err, &protocolErr) {
			t.Fatalf("GetIDByPath(%q) error = %v, want ProtocolError", raw, err)
		}
	}
}

func TestPathCacheInvalidatesAfterMutation(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == EndpointDirAdd {
			_, _ = w.Write([]byte(`{"state":true,"errno":0,"data":{"file_id":"new"}}`))
			return
		}
		_, _ = w.Write([]byte(`{"state":true,"errno":0,"data":[{"id":"old","name":"Name","is_dir":true}]}`))
	}))
	defer server.Close()
	client := newTestClient(t, ClientOptions{LimitRate: 1e6, AndroidBaseURL: server.URL, WebBaseURL: server.URL})
	if _, err := client.GetIDByPath(context.Background(), "Name"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.MakeDir(context.Background(), "0", "new"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.GetIDByPath(context.Background(), "Name"); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 3 {
		t.Fatalf("calls = %d, want cache invalidated after mkdir", calls.Load())
	}
}
