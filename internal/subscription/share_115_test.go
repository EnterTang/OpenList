package subscription

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestPan115ShareProviderListsChildren(t *testing.T) {
	var snapCalled bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/webapi/share/snap":
			snapCalled = true
			query := r.URL.Query()
			if query.Get("share_code") != "swssal13zrk" || query.Get("receive_code") != "t58d" || query.Get("cid") != "" {
				t.Fatalf("share snap query = %s", r.URL.RawQuery)
			}
			if got := r.Header.Get("Referer"); !strings.Contains(got, "swssal13zrk") || !strings.Contains(got, "t58d") {
				t.Fatalf("referer = %q, want share code and password", got)
			}
			_, _ = w.Write([]byte(`{
				"state":true,
				"data":{"count":2,"list":[
					{"fid":"file-1","cid":"0","n":"Movie.mkv","s":1024,"t":"1700000000","ico":"mkv"},
					{"cid":"dir-1","n":"Season 1","t":"1700000001"}
				]}
			}`))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	provider := NewPan115ShareProvider(model.SubscriptionTelegramPanConfig{Cookie: "UID=1;CID=2"})
	provider.(*pan115ShareProvider).webURL = server.URL
	ref := ShareRef{Provider: ShareProviderPan115, RawURL: "https://115cdn.com/s/swssal13zrk?password=t58d", ShareID: "swssal13zrk", Passcode: "t58d"}

	items, err := provider.ListShareChildren(context.Background(), ref, "")
	if err != nil {
		t.Fatalf("list children: %v", err)
	}
	if !snapCalled {
		t.Fatal("share snap endpoint was not called")
	}
	if len(items) != 2 {
		t.Fatalf("items = %#v, want 2", items)
	}
	if items[0].ID != "file-1" || items[0].ParentID != "0" || items[0].Name != "Movie.mkv" || items[0].IsDir {
		t.Fatalf("file item = %#v", items[0])
	}
	if items[0].Size != 1024 || !items[0].Modified.Equal(time.Unix(1700000000, 0)) {
		t.Fatalf("file metadata = %#v", items[0])
	}
	if items[1].ID != "dir-1" || !items[1].IsDir {
		t.Fatalf("dir item = %#v", items[1])
	}
}

func TestPan115ShareProviderSavesItems(t *testing.T) {
	var receiveCalled bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/webapi/share/receive":
			receiveCalled = true
			if got := r.Header.Get("Cookie"); got != "UID=1;CID=2" {
				t.Fatalf("cookie = %q, want UID=1;CID=2", got)
			}
			if err := r.ParseForm(); err != nil {
				t.Fatalf("parse form: %v", err)
			}
			if r.Form.Get("cid") != "dst-dir" || r.Form.Get("share_code") != "swssal13zrk" || r.Form.Get("receive_code") != "t58d" || r.Form.Get("file_id") != "file-1" {
				t.Fatalf("receive form = %#v", r.Form)
			}
			_, _ = w.Write([]byte(`{"state":true}`))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	provider := NewPan115ShareProvider(model.SubscriptionTelegramPanConfig{Cookie: " UID=1;CID=2 "})
	provider.(*pan115ShareProvider).webURL = server.URL
	ref := ShareRef{Provider: ShareProviderPan115, RawURL: "https://115cdn.com/s/swssal13zrk?password=t58d", ShareID: "swssal13zrk", Passcode: "t58d"}
	items := []ShareItem{{ID: "file-1", Name: "Movie.mkv", Raw: map[string]any{"share_fid_token": "file-1"}}}

	taskIDs, err := provider.SaveShareItems(context.Background(), ref, "", items, "dst-dir")
	if err != nil {
		t.Fatalf("save items: %v", err)
	}
	if got, want := strings.Join(taskIDs, ","), "pan115_sync_swssal13zrk"; got != want {
		t.Fatalf("task ids = %q, want %q", got, want)
	}
	if err := provider.WaitSaveComplete(context.Background(), taskIDs); err != nil {
		t.Fatalf("wait task: %v", err)
	}
	if !receiveCalled {
		t.Fatal("receive endpoint was not called")
	}
}
