package _115_sy

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	sy "github.com/OpenListTeam/OpenList/v4/internal/115sy"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

type driverRoundTripFunc func(*http.Request) (*http.Response, error)

func (f driverRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func driverJSONResponse(req *http.Request, body string) *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    req,
	}
}

func Test115SYListAndLinkPreservePickcodeAndUserAgent(t *testing.T) {
	var sawUA string
	client, err := sy.NewClient(sy.ClientOptions{
		LimitRate:      1e6,
		AndroidBaseURL: "https://android.invalid",
		WebBaseURL:     "https://web.invalid",
		HTTPClient: &http.Client{Transport: driverRoundTripFunc(func(req *http.Request) (*http.Response, error) {
			sawUA = req.Header.Get("User-Agent")
			switch req.URL.Path {
			case sy.EndpointFileList:
				return driverJSONResponse(req, `{"state":true,"errno":0,"data":[{"fid":"f1","pid":"0","n":"movie.mkv","s":12,"pc":"pick-1","directory":false}]}`), nil
			case sy.EndpointDownloadURL:
				if req.URL.Query().Get("pick_code") != "pick-1" {
					t.Fatalf("pick_code = %q", req.URL.Query().Get("pick_code"))
				}
				return driverJSONResponse(req, `{"state":true,"errno":0,"data":{"url":"https://cdn.invalid/movie"}}`), nil
			default:
				t.Fatalf("unexpected endpoint %s", req.URL.Path)
				return nil, nil
			}
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	d := &Pan115SY{Addition: Addition{PageSize: 200}, client: client}
	root := &Obj{CID: "0", Directory: true}
	objects, err := d.List(context.Background(), root, model.ListArgs{})
	if err != nil || len(objects) != 1 || objects[0].GetName() != "movie.mkv" {
		t.Fatalf("List() = %#v, %v", objects, err)
	}
	link, err := d.Link(context.Background(), objects[0], model.LinkArgs{Header: http.Header{"User-Agent": []string{"caller-agent"}}})
	if err != nil || link.URL != "https://cdn.invalid/movie" {
		t.Fatalf("Link() = %#v, %v", link, err)
	}
	if sawUA != "caller-agent" {
		t.Fatalf("User-Agent = %q, want caller-agent", sawUA)
	}
}
