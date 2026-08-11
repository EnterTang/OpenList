package _123_open

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/drivers/base"
	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/http_range"
)

func TestOpen123LinkProvidesShortLivedRefreshableRangeReader(t *testing.T) {
	payload := []byte("open123-refreshable-payload")
	var directCalls int

	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/direct-link":
			directCalls++
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"code":0,"data":{"url":"` + server.URL + `/direct/` + strconv.Itoa(directCalls) + `"}}`))
		case "/direct/1":
			w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
			_, _ = w.Write(payload[:6])
		case "/direct/2":
			start, end := parseOpen123Range(t, r.Header.Get("Range"), int64(len(payload)))
			w.Header().Set("Content-Range", http_range.Range{Start: start, Length: end - start + 1}.ContentRange(int64(len(payload))))
			w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(payload[start : end+1])
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	oldDirectLink := DirectLink
	DirectLink = InitApiInfo(server.URL+"/direct-link", 5)
	t.Cleanup(func() {
		DirectLink = oldDirectLink
	})
	if conf.Conf == nil {
		conf.Conf = conf.DefaultConfig(t.TempDir())
	}
	base.InitClient()

	driver := &Open123{
		Addition: Addition{
			AccessToken:             "access-123",
			DirectLink:              true,
			DirectLinkValidDuration: 30,
		},
		tm: &tokenManager{expiredAt: time.Now().Add(time.Hour)},
	}

	link, err := driver.Link(context.Background(), File{FileId: 9, FileName: "Movie.mkv", Size: int64(len(payload))}, model.LinkArgs{})
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
	if directCalls < 2 {
		t.Fatalf("directCalls = %d, want refresh path to execute", directCalls)
	}
}

func parseOpen123Range(t *testing.T, value string, total int64) (int64, int64) {
	t.Helper()
	if len(value) <= len("bytes=") {
		t.Fatalf("range header = %q", value)
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
