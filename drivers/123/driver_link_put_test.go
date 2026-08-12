package _123

import (
	"bytes"
	"context"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/drivers/base"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/http_range"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/go-resty/resty/v2"
)

func TestPan123LinkProvidesShortLivedRefreshableRangeReader(t *testing.T) {
	payload := []byte("abcdefghijklmnopqrstuvwxyz")
	var downloadInfoCalls int
	var sourceCalls int
	var directCalls int

	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/b/api/file/download_info":
			downloadInfoCalls++
			encoded := base64.StdEncoding.EncodeToString([]byte(server.URL + "/source?call=" + strconv.Itoa(downloadInfoCalls)))
			_, _ = w.Write([]byte(`{"code":0,"data":{"DownloadUrl":"` + server.URL + `/gateway?params=` + encoded + `"}}`))
		case "/source":
			sourceCalls++
			if got := r.Header.Get("Referer"); got != "https://yun.123pan.com/" {
				t.Fatalf("source referer = %q, want https://yun.123pan.com/", got)
			}
			http.Redirect(w, r, server.URL+"/direct/"+r.URL.Query().Get("call"), http.StatusFound)
		case "/direct/1":
			directCalls++
			w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
			_, _ = w.Write(payload[:5])
		case "/direct/2":
			directCalls++
			start, end := parseTestRange(t, r.Header.Get("Range"), int64(len(payload)))
			if got := r.Header.Get("Referer"); got != server.URL+"/" {
				t.Fatalf("direct referer = %q, want %q", got, server.URL+"/")
			}
			w.Header().Set("Content-Range", http_range.Range{Start: start, Length: end - start + 1}.ContentRange(int64(len(payload))))
			w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(payload[start : end+1])
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	withPan123HTTPClients(t, server.URL, func() {
		driver := &Pan123{Addition: Addition{AccessToken: "access-123", Platform: "web"}}
		link, err := driver.Link(context.Background(), File{
			FileId:      7,
			FileName:    "Movie.mkv",
			Size:        int64(len(payload)),
			Etag:        "etag-123",
			S3KeyFlag:   "flag",
			Type:        0,
			DownloadUrl: server.URL + "/thumb",
		}, model.LinkArgs{})
		if err != nil {
			t.Fatalf("link: %v", err)
		}
		if link.Expiration != nil {
			t.Fatalf("expiration = %v, want provider-controlled refresh without guessed TTL", link.Expiration)
		}
		if link.RangeReader == nil {
			t.Fatal("range reader is nil")
		}

		reader, err := link.RangeReader.RangeRead(context.Background(), http_range.Range{Length: -1})
		if err != nil {
			t.Fatalf("range read open: %v", err)
		}
		defer reader.Close()

		got, err := io.ReadAll(reader)
		if err != nil {
			t.Fatalf("range read payload: %v", err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("payload = %q, want %q", got, payload)
		}
		if downloadInfoCalls < 2 || sourceCalls < 2 || directCalls < 2 {
			t.Fatalf("calls download=%d source=%d direct=%d, want refresh path to execute", downloadInfoCalls, sourceCalls, directCalls)
		}
	})
}

func TestPan123PutRejectsEmptyUploadKeyWithoutConfirmedProbe(t *testing.T) {
	var probeCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/b/api/file/upload_request":
			_, _ = w.Write([]byte(`{"code":0,"data":{"Reuse":false,"FileId":0,"Key":""}}`))
		case "/b/api/file/list/new":
			probeCalls++
			_, _ = w.Write([]byte(`{"code":0,"data":{"Next":"-1","Total":0,"InfoList":[]}}`))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	withPan123HTTPClients(t, server.URL, func() {
		driver := &Pan123{Addition: Addition{AccessToken: "access-123", Platform: "web"}}
		err := driver.Put(context.Background(), File{FileId: 9, Type: 1}, testPan123FileStreamer{name: "Movie.mkv", size: 1024, md5: "0123456789abcdef0123456789abcdef"}, func(float64) {})
		if err == nil {
			t.Fatal("put unexpectedly succeeded with empty upload key and missing probe result")
		}
		if probeCalls == 0 {
			t.Fatal("destination probe was not called")
		}
	})
}

func TestPan123PutAcceptsEmptyUploadKeyAfterConfirmedProbe(t *testing.T) {
	var probeCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/b/api/file/upload_request":
			_, _ = w.Write([]byte(`{"code":0,"data":{"Reuse":false,"FileId":0,"Key":""}}`))
		case "/b/api/file/list/new":
			probeCalls++
			_, _ = w.Write([]byte(`{
				"code":0,
				"data":{"Next":"-1","Total":1,"InfoList":[
					{"FileId":88,"FileName":"Movie.mkv","Type":0,"Size":1024,"Etag":"0123456789abcdef0123456789abcdef","UpdateAt":"2023-11-14T22:13:20Z"}
				]}
			}`))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	withPan123HTTPClients(t, server.URL, func() {
		driver := &Pan123{Addition: Addition{AccessToken: "access-123", Platform: "web"}}
		err := driver.Put(context.Background(), File{FileId: 9, Type: 1}, testPan123FileStreamer{name: "Movie.mkv", size: 1024, md5: "0123456789abcdef0123456789abcdef"}, func(float64) {})
		if err != nil {
			t.Fatalf("put: %v", err)
		}
		if probeCalls != 1 {
			t.Fatalf("probe calls = %d, want 1", probeCalls)
		}
	})
}

func withPan123HTTPClients(t *testing.T, serverURL string, fn func()) {
	t.Helper()
	base.InitClient()
	target, err := url.Parse(serverURL)
	if err != nil {
		t.Fatalf("parse server url: %v", err)
	}
	rewrite := func(rt http.RoundTripper) http.RoundTripper {
		if rt == nil {
			rt = http.DefaultTransport
		}
		return roundTripFunc(func(req *http.Request) (*http.Response, error) {
			clone := req.Clone(req.Context())
			clone.URL.Scheme = target.Scheme
			clone.URL.Host = target.Host
			clone.Host = target.Host
			return rt.RoundTrip(clone)
		})
	}

	oldResty := base.RestyClient
	oldNoRedirect := base.NoRedirectClient
	oldHTTP := base.HttpClient
	t.Cleanup(func() {
		base.RestyClient = oldResty
		base.NoRedirectClient = oldNoRedirect
		base.HttpClient = oldHTTP
	})

	base.RestyClient = resty.New().SetTransport(rewrite(http.DefaultTransport))
	base.NoRedirectClient = resty.New().
		SetRedirectPolicy(resty.RedirectPolicyFunc(func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		})).
		SetTransport(rewrite(http.DefaultTransport))
	base.HttpClient = &http.Client{Transport: rewrite(http.DefaultTransport)}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type testPan123FileStreamer struct {
	name string
	size int64
	md5  string
}

func (f testPan123FileStreamer) Read(p []byte) (int, error) { return 0, io.EOF }
func (f testPan123FileStreamer) Close() error               { return nil }
func (f testPan123FileStreamer) Add(io.Closer)              {}
func (f testPan123FileStreamer) AddIfCloser(any)            {}
func (f testPan123FileStreamer) GetSize() int64             { return f.size }
func (f testPan123FileStreamer) GetName() string            { return f.name }
func (f testPan123FileStreamer) ModTime() time.Time         { return time.Time{} }
func (f testPan123FileStreamer) CreateTime() time.Time      { return time.Time{} }
func (f testPan123FileStreamer) IsDir() bool                { return false }
func (f testPan123FileStreamer) GetID() string              { return "" }
func (f testPan123FileStreamer) GetPath() string            { return "" }
func (f testPan123FileStreamer) GetHash() utils.HashInfo    { return utils.NewHashInfo(utils.MD5, f.md5) }
func (f testPan123FileStreamer) GetMimetype() string        { return "video/mp4" }
func (f testPan123FileStreamer) NeedStore() bool            { return false }
func (f testPan123FileStreamer) IsForceStreamUpload() bool  { return false }
func (f testPan123FileStreamer) GetExist() model.Obj        { return nil }
func (f testPan123FileStreamer) SetExist(model.Obj)         {}
func (f testPan123FileStreamer) RangeRead(http_range.Range) (io.Reader, error) {
	return bytes.NewReader(nil), nil
}
func (f testPan123FileStreamer) CacheFullAndWriter(up *model.UpdateProgress, writer io.Writer) (model.File, error) {
	return nil, nil
}
func (f testPan123FileStreamer) GetFile() model.File { return nil }
