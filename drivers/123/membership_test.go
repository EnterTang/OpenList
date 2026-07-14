package _123

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/drivers/base"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/go-resty/resty/v2"
)

func Test123MembershipDetailsFromUserInfo(t *testing.T) {
	tests := []struct {
		name       string
		vip        bool
		vipLevel   int
		vipExpire  string
		wantTier   string
		wantStatus string
	}{
		{name: "ordinary", wantTier: "ordinary", wantStatus: "inactive"},
		{name: "vip", vip: true, vipLevel: 1, vipExpire: "2040-01-31", wantTier: "vip", wantStatus: "active"},
		{name: "svip", vip: true, vipLevel: 2, vipExpire: "2030-06-30", wantTier: "svip", wantStatus: "active"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := &UserInfoResp{}
			response.Data.Vip = test.vip
			response.Data.VipLevel = test.vipLevel
			response.Data.VipExpire = test.vipExpire

			got := membershipDetailsFromUserInfo(response)
			if got.Tier != test.wantTier || got.Status != test.wantStatus || got.ExpireDate != test.vipExpire {
				t.Fatalf("membership = %#v, want tier=%q status=%q expire=%q", got, test.wantTier, test.wantStatus, test.vipExpire)
			}
		})
	}
}

func TestPan123GetDetailsReturnsMembershipFromUserInfo(t *testing.T) {
	setup123Resty(t, func(req *http.Request) (*http.Response, error) {
		if req.Method != http.MethodGet {
			t.Fatalf("method = %s, want GET", req.Method)
		}
		if req.URL.Path != "/b/api/user/info" {
			t.Fatalf("path = %q, want /b/api/user/info", req.URL.Path)
		}
		if req.URL.RawQuery == "" {
			t.Fatal("user-info request is missing the dynamic signature")
		}
		if got := req.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Fatalf("Authorization = %q, want Bearer test-token", got)
		}
		return pan123JSONResponse(req, `{"code":0,"message":"ok","data":{"UID":123,"Nickname":"member","SpaceUsed":40,"SpacePermanent":100,"SpaceTemp":20,"FileCount":7,"Vip":true,"VipLevel":1,"VipExpire":"2040-01-31"}}`), nil
	})

	driver := &Pan123{Addition: Addition{AccessToken: "test-token", Platform: "web"}}
	details, err := driver.GetDetails(context.Background())
	if err != nil {
		t.Fatalf("get details: %v", err)
	}
	if details.TotalSpace != 120 || details.UsedSpace != 40 {
		t.Fatalf("disk usage = %#v, want total=120 used=40", details.DiskUsage)
	}
	if details.Membership == nil {
		t.Fatal("membership is nil")
	}
	want := model.MembershipDetails{Tier: "vip", Status: "active", ExpireDate: "2040-01-31"}
	if *details.Membership != want {
		t.Fatalf("membership = %#v, want %#v", *details.Membership, want)
	}
	if got := driver.ClusterMembershipDetails(); got != want {
		t.Fatalf("runtime membership = %#v, want %#v", got, want)
	}
}

func TestPan123InitCapturesRuntimeMembershipAndHonorsConfiguredTier(t *testing.T) {
	setup123Resty(t, func(req *http.Request) (*http.Response, error) {
		return pan123JSONResponse(req, `{"code":0,"message":"ok","data":{"Vip":true,"VipLevel":2,"VipExpire":"2035-12-31"}}`), nil
	})

	driver := &Pan123{Addition: Addition{
		AccessToken:    "test-token",
		Platform:       "web",
		MembershipTier: "unknown",
	}}
	if err := driver.Init(context.Background()); err != nil {
		t.Fatalf("init driver: %v", err)
	}
	if got := driver.ClusterMembershipTier(); got != "svip" {
		t.Fatalf("runtime membership tier = %q, want svip", got)
	}

	driver.MembershipTier = "vip"
	got := driver.ClusterMembershipDetails()
	want := model.MembershipDetails{Tier: "vip", Status: "active", ExpireDate: "2035-12-31"}
	if got != want {
		t.Fatalf("configured membership = %#v, want %#v", got, want)
	}
}

func setup123Resty(t *testing.T, transport pan123RoundTripFunc) {
	t.Helper()
	old := base.RestyClient
	base.RestyClient = resty.New().SetTransport(transport)
	t.Cleanup(func() {
		base.RestyClient = old
	})
}

type pan123RoundTripFunc func(*http.Request) (*http.Response, error)

func (f pan123RoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func pan123JSONResponse(req *http.Request, body string) *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    req,
	}
}
