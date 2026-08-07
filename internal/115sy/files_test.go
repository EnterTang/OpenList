package _115sy

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
)

func TestListFilesPaginatesAndDeduplicatesItems(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		offset := r.URL.Query().Get("offset")
		calls.Add(1)
		switch offset {
		case "0":
			_, _ = w.Write([]byte(`{"state":true,"errno":0,"data":{"items":[{"id":"a","name":"A","parent_cid":"0"},{"id":"b","name":"B","parent_cid":"0"}],"offset":0,"limit":2,"has_more":true}}`))
		case "2":
			_, _ = w.Write([]byte(`{"state":true,"errno":0,"data":{"items":[{"id":"b","name":"B","parent_cid":"0"},{"id":"c","name":"C","parent_cid":"0"}],"offset":2,"limit":2,"has_more":true}}`))
		case "4":
			_, _ = w.Write([]byte(`{"state":true,"errno":0,"data":{"items":[],"offset":4,"limit":2,"has_more":false}}`))
		default:
			t.Fatalf("unexpected offset %q", offset)
		}
	}))
	defer server.Close()
	client := newTestClient(t, ClientOptions{LimitRate: 1e6, AndroidBaseURL: server.URL, WebBaseURL: server.URL})
	items, err := client.ListFiles(context.Background(), "0", ListOptions{PageSize: 2})
	if err != nil {
		t.Fatalf("ListFiles() error = %v", err)
	}
	if got := []string{items[0].ID, items[1].ID, items[2].ID}; strings.Join(got, ",") != "a,b,c" {
		t.Fatalf("items = %v, want a,b,c", got)
	}
	if calls.Load() != 3 {
		t.Fatalf("requests = %d, want 3", calls.Load())
	}
}

func TestListFilesRejectsMalformedPagination(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "empty more", body: `{"state":true,"errno":0,"data":{"items":[],"has_more":true}}`},
		{name: "repeated offset", body: `{"state":true,"errno":0,"data":{"items":[{"id":"a","name":"A"}],"offset":0,"limit":1,"next_offset":0,"has_more":true}}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			client := newTestClient(t, ClientOptions{LimitRate: 1e6, AndroidBaseURL: server.URL, WebBaseURL: server.URL})
			_, err := client.ListFiles(context.Background(), "0", ListOptions{PageSize: 1})
			var protocolErr *ProtocolError
			if !errors.As(err, &protocolErr) {
				t.Fatalf("error = %v, want ProtocolError", err)
			}
		})
	}
}

func TestListFilesFallsBackFromAndroid405ToWeb(t *testing.T) {
	var calls atomic.Int32
	client := newTestClient(t, ClientOptions{
		LimitRate:      1e6,
		AndroidBaseURL: "https://android.invalid",
		WebBaseURL:     "https://web.invalid",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if calls.Add(1) == 1 {
				return jsonResponse(req, http.StatusMethodNotAllowed, `{"state":false,"errno":0,"error":"unsupported"}`), nil
			}
			return jsonResponse(req, http.StatusOK, `{"state":true,"errno":0,"data":[{"id":"web-item","name":"web"}]}`), nil
		})},
	})
	items, err := client.ListFiles(context.Background(), "0", ListOptions{PageSize: 2})
	if err != nil || len(items) != 1 || items[0].ID != "web-item" {
		t.Fatalf("items/error = %#v/%v, want web fallback", items, err)
	}
	if calls.Load() != 2 {
		t.Fatalf("requests = %d, want one fallback", calls.Load())
	}
}

func TestFileCRUDAndCapacityUseExpectedForms(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case EndpointDirAdd:
			_, _ = w.Write([]byte(`{"state":true,"errno":0,"data":{"file_id":"new-dir"}}`))
		case EndpointCategory:
			_, _ = w.Write([]byte(`{"state":true,"errno":0,"data":{"space_total":100,"space_used":25,"space_remain":75}}`))
		default:
			if err := r.ParseForm(); err != nil {
				t.Fatal(err)
			}
			_, _ = w.Write([]byte(`{"state":true,"errno":0,"data":{}}`))
		}
	}))
	defer server.Close()
	client := newTestClient(t, ClientOptions{LimitRate: 1e6, WebBaseURL: server.URL, AndroidBaseURL: server.URL})
	dir, err := client.MakeDir(context.Background(), "0", "new")
	if err != nil || dir.ID != "new-dir" || !dir.IsDir {
		t.Fatalf("MakeDir() = %#v, %v", dir, err)
	}
	if err := client.Move(context.Background(), "f", "0"); err != nil {
		t.Fatal(err)
	}
	if err := client.Rename(context.Background(), "f", "renamed"); err != nil {
		t.Fatal(err)
	}
	if err := client.Copy(context.Background(), "f", "0"); err != nil {
		t.Fatal(err)
	}
	if err := client.Remove(context.Background(), "f", "0"); err != nil {
		t.Fatal(err)
	}
	capacity, err := client.GetCapacity(context.Background())
	if err != nil || capacity.Total != 100 || capacity.Used != 25 || capacity.Remaining != 75 {
		t.Fatalf("capacity = %#v, %v", capacity, err)
	}
	if calls.Load() != 6 {
		t.Fatalf("requests = %d, want 6", calls.Load())
	}
}

func TestDownloadURLUsesOfficialP115ClientProtocol(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != EndpointDownloadURL {
			t.Fatalf("request = %s %s, want POST %s", r.Method, r.URL.Path, EndpointDownloadURL)
		}
		if got := r.URL.Query().Get("pick_code"); got != "" {
			t.Fatalf("query pick_code = %q, want empty", got)
		}
		if err := r.ParseForm(); err != nil {
			t.Fatal(err)
		}
		if r.Form.Get("data") == "" {
			t.Fatal("encrypted data form field is empty")
		}
		if got := r.Header.Get("app"); got != "" {
			t.Fatalf("app header = %q, want empty for p115client chrome endpoint", got)
		}
		if got, want := r.Header.Get("Referer"), "http://"+r.Host; got != want {
			t.Fatalf("referer = %q, want %q", got, want)
		}
		if got := r.Form.Get("data"); got != "cmQSx279oTEKvrYXxJ6Zd4u58ZEfk3Lo+aTJlARoNpE0zEFTbBuCq4lYjUnyufJ3fJYbSkbc7GypyjIKPUyLLoETO74+xvvw3hqACiltOuHpuSR1AT+ORobpWVw/7Vi4oqz689OfDb0dJr+YPOyrfUb6qwLCDnchYEiEPhUnEz4=" {
			t.Fatalf("encrypted data = %q, want official p115cipher fixture", got)
		}
		if strings.Contains(r.Form.Encode(), "pick_code") || strings.Contains(r.Form.Encode(), "pickcode") {
			t.Fatalf("request form contains plaintext pickcode: %q", r.Form.Encode())
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"state":true,"errno":0,"data":""}`))
	}))
	defer server.Close()

	client := newTestClient(t, ClientOptions{
		Cookie:         "UID=8111801_R2_1786064446; CID=root; SEID=seid",
		LimitRate:      1e6,
		AndroidBaseURL: server.URL,
		WebBaseURL:     server.URL,
	})
	if _, err := client.DownloadURL(context.Background(), "pick-code", ""); err == nil || !strings.Contains(err.Error(), "download response data is not encrypted") {
		t.Fatalf("DownloadURL() error = %v, want encrypted response validation", err)
	}
}

func TestP115DecodeResponseUsesOfficialResponseTransform(t *testing.T) {
	randomKey := []byte("0123456789abcdef")
	want := []byte(`{"123":{"file_name":"movie.mkv","url":{"url":"https://download.example/file.bin"}}}`)
	key := p115DeriveKey(randomKey, 12)
	encodedPayload := p115XOR(want, p115RSAKey)
	reverseBytes(encodedPayload)
	encodedPayload = p115XOR(encodedPayload, key)
	raw := append(append([]byte(nil), randomKey...), encodedPayload...)

	got, err := p115DecodeResponse(raw)
	if err != nil {
		t.Fatalf("p115DecodeResponse() error = %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("decoded response = %q, want %q", got, want)
	}
}

func TestListFilesUsesCappedPageSize(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("limit"); got != strconv.FormatInt(maxFilePageSize, 10) {
			t.Fatalf("limit = %q, want %d", got, maxFilePageSize)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"state":true,"errno":0,"data":[]}`))
	}))
	defer server.Close()
	client := newTestClient(t, ClientOptions{LimitRate: 1e6, AndroidBaseURL: server.URL, WebBaseURL: server.URL})
	if _, err := client.ListFiles(context.Background(), "0", ListOptions{PageSize: 99999}); err != nil {
		t.Fatal(err)
	}
}

func TestListFilesUsesTopLevelCountWithArrayData(t *testing.T) {
	var calls atomic.Int32
	client := newTestClient(t, ClientOptions{
		LimitRate:      1e6,
		AndroidBaseURL: "https://android.invalid",
		WebBaseURL:     "https://web.invalid",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			calls.Add(1)
			return jsonResponse(req, http.StatusOK, `{"state":true,"errno":0,"count":1,"offset":0,"limit":1,"data":[{"id":"one","name":"one"}]}`), nil
		})},
	})
	items, err := client.ListFiles(context.Background(), "0", ListOptions{PageSize: 1})
	if err != nil || len(items) != 1 || calls.Load() != 1 {
		t.Fatalf("items/error/calls = %#v/%v/%d, want one page from top-level count", items, err, calls.Load())
	}
}

func TestListFilesStopsWhenHasMoreIsFalseOnFullPage(t *testing.T) {
	var calls atomic.Int32
	client := newTestClient(t, ClientOptions{
		LimitRate:      1e6,
		AndroidBaseURL: "https://android.invalid",
		WebBaseURL:     "https://web.invalid",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			calls.Add(1)
			return jsonResponse(req, http.StatusOK, `{"state":true,"errno":0,"data":{"items":[{"id":"one","name":"one"}],"has_more":false}}`), nil
		})},
	})
	items, err := client.ListFiles(context.Background(), "0", ListOptions{PageSize: 1})
	if err != nil || len(items) != 1 || calls.Load() != 1 {
		t.Fatalf("items/error/calls = %#v/%v/%d, want one page", items, err, calls.Load())
	}
}

func TestListFilesAcceptsNextOffsetWithoutCurrentOffset(t *testing.T) {
	var calls atomic.Int32
	client := newTestClient(t, ClientOptions{
		LimitRate:      1e6,
		AndroidBaseURL: "https://android.invalid",
		WebBaseURL:     "https://web.invalid",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			page := calls.Add(1)
			if page == 1 {
				return jsonResponse(req, http.StatusOK, `{"state":true,"errno":0,"data":{"items":[{"id":"one","name":"one"}],"next_offset":1,"has_more":true}}`), nil
			}
			return jsonResponse(req, http.StatusOK, `{"state":true,"errno":0,"data":{"items":[{"id":"two","name":"two"}],"has_more":false}}`), nil
		})},
	})
	items, err := client.ListFiles(context.Background(), "0", ListOptions{PageSize: 1})
	if err != nil || len(items) != 2 || calls.Load() != 2 {
		t.Fatalf("items/error/calls = %#v/%v/%d, want two pages", items, err, calls.Load())
	}
}

func TestRemoteItemRecognizesLegacyDirectoryMarker(t *testing.T) {
	var item RemoteItem
	if err := json.Unmarshal([]byte(`{"cid":"123","name":"folder","directory":true}`), &item); err != nil {
		t.Fatal(err)
	}
	if !item.IsDir || item.ID != "123" {
		t.Fatalf("item = %#v, want legacy directory", item)
	}
}

func TestRemoteItemRecognizesAndroidShortFields(t *testing.T) {
	var item RemoteItem
	if err := json.Unmarshal([]byte(`{"fid":"123","fn":"movie.mkv","fc":"1","fvs":"42","pc":"pick-code","pid":"0","upt":"1712345678"}`), &item); err != nil {
		t.Fatal(err)
	}
	if item.ID != "123" || item.Name != "movie.mkv" || item.IsDir || item.Size != 42 || item.PickCode != "pick-code" || item.ParentCID != "0" || item.ModifyTime != 1712345678 {
		t.Fatalf("item = %#v, want Android short fields decoded", item)
	}
}
