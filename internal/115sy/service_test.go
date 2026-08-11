package _115sy

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestParseShareURLAndTargets(t *testing.T) {
	share, err := ParseShareURL("https://115cdn.com/s/swfxusj3wwq?password=m2f2")
	if err != nil || share.ShareCode != "swfxusj3wwq" || share.ReceiveCode != "m2f2" {
		t.Fatalf("share = %#v, error = %v", share, err)
	}
	if _, err := ParseShareURL("https://115.com/s/code"); err == nil {
		t.Fatal("expected missing receive code error")
	}
	targets, err := ParseShareTargets("电影:1001,电视剧:1002,重复:1001")
	if err != nil || len(targets) != 2 || targets[0].CID != "1001" || targets[1].CID != "1002" {
		t.Fatalf("targets = %#v, error = %v", targets, err)
	}
}

func TestShareSnapshotUsesAndroidThenWeb405Fallback(t *testing.T) {
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == EndpointShareSnapshotApp {
			w.WriteHeader(http.StatusMethodNotAllowed)
			_, _ = io.WriteString(w, `{"state":false,"errno":0,"error":"unsupported"}`)
			return
		}
		if r.URL.Path != EndpointShareSnapshot {
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
		_, _ = io.WriteString(w, `{"state":true,"errno":0,"data":{"shareinfo":{"share_title":"Demo"},"count":2,"list":[{"fid":"dir","pid":"0","n":"Folder","fc":"0"},{"fid":"file","pid":"dir","n":"movie.mkv","fc":"1","s":"12"}]}}`)
	}))
	defer server.Close()
	client := newTestClient(t, ClientOptions{LimitRate: 1e6, AndroidBaseURL: server.URL, WebBaseURL: server.URL})
	snapshot, err := client.ShareSnapshot(context.Background(), ShareURL{ShareCode: "code", ReceiveCode: "pass"})
	if err != nil || snapshot.Name != "Demo" || len(snapshot.Items) != 2 || !snapshot.Items[0].IsDir {
		t.Fatalf("snapshot = %#v, error = %v", snapshot, err)
	}
	if strings.Join(paths, ",") != EndpointShareSnapshotApp+","+EndpointShareSnapshot {
		t.Fatalf("paths = %q", paths)
	}
}

func TestShareDownloadURLUsesEncryptedAppThenWeb405Fallback(t *testing.T) {
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case EndpointShareDownloadURLApp:
			if r.Method != http.MethodPost {
				t.Fatalf("app method = %s, want POST", r.Method)
			}
			if err := r.ParseForm(); err != nil || r.Form.Get("data") == "" {
				t.Fatalf("app form = %s", r.Form.Encode())
			}
			w.WriteHeader(http.StatusMethodNotAllowed)
			_, _ = io.WriteString(w, `{"state":false,"errno":0,"error":"unsupported"}`)
		case EndpointShareDownloadURLWeb:
			if r.Method != http.MethodGet || r.URL.Query().Get("share_code") != "code" || r.URL.Query().Get("receive_code") != "pass" || r.URL.Query().Get("file_id") != "file" {
				t.Fatalf("web request = %s %s", r.Method, r.URL.RawQuery)
			}
			_, _ = io.WriteString(w, `{"state":true,"errno":0,"data":{"fid":"file","fn":"Movie.mkv","fs":12,"url":{"url":"https://download.example/movie.mkv"}}}`)
		default:
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
	}))
	defer server.Close()
	client := newTestClient(t, ClientOptions{Cookie: "UID=1;CID=2", LimitRate: 1e6, AndroidBaseURL: server.URL, WebBaseURL: server.URL})
	link, err := client.ShareDownloadURL(context.Background(), ShareURL{ShareCode: "code", ReceiveCode: "pass"}, "file", "test-android-ua")
	if err != nil {
		t.Fatalf("share download URL: %v", err)
	}
	if link.URL != "https://download.example/movie.mkv" || link.Header.Get("User-Agent") != "test-android-ua" {
		t.Fatalf("link = %#v", link)
	}
	if strings.Join(paths, ",") != EndpointShareDownloadURLApp+","+EndpointShareDownloadURLWeb {
		t.Fatalf("paths = %q", paths)
	}
}

func TestReceiveShareUsesAndroidThenWeb405Fallback(t *testing.T) {
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		if err := r.ParseForm(); err != nil {
			t.Fatal(err)
		}
		if r.Form.Get("share_code") != "code" || r.Form.Get("receive_code") != "pass" || r.Form.Get("cid") != "1001" || r.Form.Get("file_id") != "file" {
			t.Fatalf("receive form = %#v", r.Form)
		}
		switch r.URL.Path {
		case EndpointShareReceiveApp:
			if r.Header.Get("app") != string(ProfileAndroid) || r.Header.Get("appversion") == "" {
				t.Fatalf("android headers = app=%q appversion=%q", r.Header.Get("app"), r.Header.Get("appversion"))
			}
			w.WriteHeader(http.StatusMethodNotAllowed)
			_, _ = io.WriteString(w, `{"state":false,"errno":0,"error":"unsupported"}`)
		case EndpointShareReceive:
			_, _ = io.WriteString(w, `{"state":true,"errno":0,"data":{"task_id":"receive-1","cid":"1001"}}`)
		default:
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
	}))
	defer server.Close()
	client := newTestClient(t, ClientOptions{LimitRate: 1e6, AndroidBaseURL: server.URL, WebBaseURL: server.URL})

	received, err := client.ReceiveShare(context.Background(), ReceiveShareRequest{ShareCode: "code", ReceiveCode: "pass", TargetCID: "1001", FileID: "file"})
	if err != nil || received.TaskID != "receive-1" {
		t.Fatalf("received = %#v, error = %v", received, err)
	}
	if strings.Join(paths, ",") != EndpointShareReceiveApp+","+EndpointShareReceive {
		t.Fatalf("paths = %q", paths)
	}
}

func TestReceiveShareAndOfflineTasksUseExpectedForms(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case EndpointShareReceiveApp:
			if err := r.ParseForm(); err != nil {
				t.Fatal(err)
			}
			if r.Form.Get("share_code") != "code" || r.Form.Get("receive_code") != "pass" || r.Form.Get("cid") != "1001" || r.Form.Get("file_id") != "file" {
				t.Fatalf("receive form = %#v", r.Form)
			}
			_, _ = io.WriteString(w, `{"state":true,"errno":0,"data":{"task_id":"receive-1","cid":"1001"}}`)
		case EndpointShareSend:
			if err := r.ParseForm(); err != nil {
				t.Fatal(err)
			}
			if r.Form.Get("file_ids") != "file" || r.Form.Get("ignore_warn") != "1" || r.Form.Get("order") != "file_name" {
				t.Fatalf("share form = %#v", r.Form)
			}
			_, _ = io.WriteString(w, `{"state":true,"errno":0,"data":{"share_code":"share-1","receive_code":"pass-1","share_url":"https://115.com/s/share-1?password=pass-1"}}`)
		case EndpointOfflineAdd:
			if err := r.ParseForm(); err != nil {
				t.Fatal(err)
			}
			if r.Form.Get("wp_path_id") != "1001" || r.Form.Get("url[0]") == "" || r.Form.Get("url[1]") == "" {
				t.Fatalf("offline form = %#v", r.Form)
			}
			_, _ = io.WriteString(w, `{"state":true,"errno":0,"data":{"result":[{"url":"magnet:?xt=urn:btih:abc","info_hash":"hash-a"},{"url":"https://example.com/a","error_msg":"quota"}]}}`)
		case EndpointOfflineList:
			_, _ = io.WriteString(w, `{"state":true,"errno":0,"data":{"tasks":[{"info_hash":"hash-a","name":"demo","status":2,"percentDone":100}]}}`)
		case EndpointOfflineDelete:
			if err := r.ParseForm(); err != nil {
				t.Fatal(err)
			}
			if r.Form.Get("info_hash[0]") != "hash-a" || r.Form.Get("del_source_file") != "1" {
				t.Fatalf("delete form = %#v", r.Form)
			}
			_, _ = io.WriteString(w, `{"state":true,"errno":0}`)
		default:
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
	}))
	defer server.Close()
	client := newTestClient(t, ClientOptions{LimitRate: 1e6, AndroidBaseURL: server.URL, WebBaseURL: server.URL})
	received, err := client.ReceiveShare(context.Background(), ReceiveShareRequest{ShareCode: "code", ReceiveCode: "pass", TargetCID: "1001", FileID: "file"})
	if err != nil || received.TaskID != "receive-1" {
		t.Fatalf("received = %#v, error = %v", received, err)
	}
	created, err := client.CreateShare(context.Background(), CreateShareRequest{FileIDs: []string{"file", "file"}})
	if err != nil || created.ShareCode != "share-1" || created.ReceiveCode != "pass-1" {
		t.Fatalf("created = %#v, error = %v", created, err)
	}
	offline, err := client.AddOfflineTasks(context.Background(), OfflineRequest{TargetCID: "1001", URLs: []string{"magnet:?xt=urn:btih:abc", "magnet:?xt=urn:btih:abc", "https://example.com/a"}})
	if err != nil || len(offline.Items) != 2 || !offline.Items[0].Success || offline.Items[1].Success {
		t.Fatalf("offline = %#v, error = %v", offline, err)
	}
	tasks, err := client.ListOfflineTasks(context.Background())
	if err != nil || len(tasks) != 1 || !tasks[0].Done() {
		t.Fatalf("tasks = %#v, error = %v", tasks, err)
	}
	if err := client.DeleteOfflineTasks(context.Background(), []string{"hash-a"}, true); err != nil {
		t.Fatal(err)
	}
}

func TestOfflineURLValidation(t *testing.T) {
	for _, value := range []string{"magnet:?xt=urn:btih:abc", "ed2k://|file|demo|1|hash|/", "https://example.com/a"} {
		if !validOfflineURL(value) {
			t.Fatalf("validOfflineURL(%q) = false", value)
		}
	}
	if _, err := normalizeOfflineURLs([]string{"ftp://example.com/a"}); err == nil {
		t.Fatal("expected unsupported URL error")
	}
}
