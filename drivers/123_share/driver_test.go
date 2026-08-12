package _123Share

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
	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/http_range"
	"github.com/go-resty/resty/v2"
)

func TestPan123ShareLinkProvidesShortLivedRefreshableRangeReader(t *testing.T) {
	payload := []byte("share-download-payload")
	var infoCalls int
	var sourceCalls int

	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/b/api/share/download/info":
			infoCalls++
			encoded := base64.StdEncoding.EncodeToString([]byte(server.URL + "/source?call=" + strconv.Itoa(infoCalls)))
			_, _ = w.Write([]byte(`{"code":0,"data":{"DownloadURL":"` + server.URL + `/gateway?params=` + encoded + `"}}`))
		case "/source":
			sourceCalls++
			if got := r.Header.Get("Referer"); got != "https://yun.123pan.com/" {
				t.Fatalf("source referer = %q, want https://yun.123pan.com/", got)
			}
			http.Redirect(w, r, server.URL+"/direct/"+r.URL.Query().Get("call"), http.StatusFound)
		case "/direct/1":
			w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
			_, _ = w.Write(payload[:5])
		case "/direct/2":
			start, end := parseShareTestRange(t, r.Header.Get("Range"), int64(len(payload)))
			w.Header().Set("Content-Range", http_range.Range{Start: start, Length: end - start + 1}.ContentRange(int64(len(payload))))
			w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(payload[start : end+1])
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	withPan123ShareHTTPClients(t, server.URL, func() {
		driver := &Pan123Share{Addition: Addition{AccessToken: "access-123", ShareKey: "share-key", SharePwd: "share-pwd"}}
		link, err := driver.Link(context.Background(), File{
			FileId:    8,
			FileName:  "Movie.mkv",
			Size:      int64(len(payload)),
			Etag:      "etag-123",
			S3KeyFlag: "flag",
			Type:      0,
		}, model.LinkArgs{})
		if err != nil {
			t.Fatalf("link: %v", err)
		}
		if link.Expiration == nil || *link.Expiration > 5*time.Minute {
			t.Fatalf("expiration = %v, want short-lived direct link", link.Expiration)
		}
		if link.RangeReader == nil {
			t.Fatal("range reader is nil")
		}

		reader, err := link.RangeReader.RangeRead(context.Background(), http_range.Range{Length: -1})
		if err != nil {
			t.Fatalf("open range reader: %v", err)
		}
		defer reader.Close()

		got, err := io.ReadAll(reader)
		if err != nil {
			t.Fatalf("read payload: %v", err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("payload = %q, want %q", got, payload)
		}
		if infoCalls < 2 || sourceCalls < 2 {
			t.Fatalf("infoCalls=%d sourceCalls=%d, want refresh path to execute", infoCalls, sourceCalls)
		}
	})
}

func TestPan123ShareInitAcceptsPublicShareWithoutAccessToken(t *testing.T) {
	driver := &Pan123Share{Addition: Addition{ShareKey: "share-key"}}
	if err := driver.Init(context.Background()); err != nil {
		t.Fatalf("public share init: %v", err)
	}
}

func TestPan123ShareInitRejectsMissingShareKey(t *testing.T) {
	driver := &Pan123Share{}
	if err := driver.Init(context.Background()); err == nil {
		t.Fatal("missing share key unexpectedly accepted")
	}
}

func withPan123ShareHTTPClients(t *testing.T, serverURL string, fn func()) {
	t.Helper()
	if conf.Conf == nil {
		conf.Conf = conf.DefaultConfig(t.TempDir())
	}
	base.InitClient()
	target, err := url.Parse(serverURL)
	if err != nil {
		t.Fatalf("parse server url: %v", err)
	}
	rewrite := func(rt http.RoundTripper) http.RoundTripper {
		if rt == nil {
			rt = http.DefaultTransport
		}
		return shareRoundTripFunc(func(req *http.Request) (*http.Response, error) {
			clone := req.Clone(req.Context())
			clone.URL.Scheme = target.Scheme
			clone.URL.Host = target.Host
			clone.Host = target.Host
			return rt.RoundTrip(clone)
		})
	}

	oldResty := base.RestyClient
	oldNoRedirect := base.NoRedirectClient
	t.Cleanup(func() {
		base.RestyClient = oldResty
		base.NoRedirectClient = oldNoRedirect
	})

	base.RestyClient = resty.New().SetTransport(rewrite(http.DefaultTransport))
	base.NoRedirectClient = resty.New().
		SetRedirectPolicy(resty.RedirectPolicyFunc(func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		})).
		SetTransport(rewrite(http.DefaultTransport))
}

type shareRoundTripFunc func(*http.Request) (*http.Response, error)

func (f shareRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func parseShareTestRange(t *testing.T, value string, total int64) (int64, int64) {
	t.Helper()
	if len(value) == 0 {
		t.Fatalf("range header is empty")
	}
	parts := bytes.SplitN([]byte(value[len("bytes="):]), []byte("-"), 2)
	start, err := strconv.ParseInt(string(parts[0]), 10, 64)
	if err != nil {
		t.Fatalf("parse range start: %v", err)
	}
	end := total - 1
	if len(parts) > 1 && len(parts[1]) > 0 {
		end, err = strconv.ParseInt(string(parts[1]), 10, 64)
		if err != nil {
			t.Fatalf("parse range end: %v", err)
		}
	}
	return start, end
}
