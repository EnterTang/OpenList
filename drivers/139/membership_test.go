package _139

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/drivers/base"
)

func TestQueryMembershipTierUsesWebCredentialsAndSelectsHighestTier(t *testing.T) {
	setup139Resty(t)
	oldEndpoint := mobile139MembershipEndpoint
	t.Cleanup(func() {
		mobile139MembershipEndpoint = oldEndpoint
	})

	var requestBody queryUserBenefitsRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/orchestration/group-rebuild/member/v1.0/queryUserBenefits" {
			t.Fatalf("path = %q, want queryUserBenefits", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Basic stored-authorization" {
			t.Fatalf("Authorization = %q, want configured Basic authorization", got)
		}
		if got := r.Header.Get("Cookie"); got != "skey=valid; ud_id=domain-id" {
			t.Fatalf("Cookie = %q, want configured cookie header", got)
		}
		if got := r.Header.Get("Origin"); got != "https://yun.139.com" {
			t.Fatalf("Origin = %q, want yun web origin", got)
		}
		if got := r.Header.Get("X-Svctype"); got != "1" {
			t.Fatalf("x-SvcType = %q, want personal service type", got)
		}
		if got := r.Header.Get("X-Yun-Api-Version"); got != "v1" {
			t.Fatalf("X-Yun-Api-Version = %q, want v1", got)
		}
		if err := json.NewDecoder(r.Body).Decode(&requestBody); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		write139JSON(t, w, map[string]any{
			"success": true,
			"code":    "0",
			"message": "OK",
			"data": map[string]any{
				"result": map[string]any{"resultCode": "0", "resultDesc": "SUCCESS"},
				"userSubMemberList": []map[string]any{
					{"memberLevel": 1, "memberLvName": "白银会员"},
					{"memberLevel": "3", "memberLvName": "钻石会员"},
					{"memberLevel": 2, "memberLvName": "黄金会员"},
				},
			},
		})
	}))
	defer server.Close()
	mobile139MembershipEndpoint = server.URL + "/orchestration/group-rebuild/member/v1.0/queryUserBenefits"

	d := &Yun139{
		Account: "13900000000",
		Addition: Addition{
			Authorization: "stored-authorization",
			CookieHeader:  "skey=valid; ud_id=domain-id",
			UserDomainID:  "domain-id",
		},
	}
	tier, err := d.queryMembershipTier(context.Background())
	if err != nil {
		t.Fatalf("queryMembershipTier returned error: %v", err)
	}
	if tier != "diamond" {
		t.Fatalf("tier = %q, want diamond", tier)
	}
	if requestBody.IsNeedBenefit != 0 {
		t.Fatalf("isNeedBenefit = %d, want 0", requestBody.IsNeedBenefit)
	}
	if requestBody.CommonAccountInfo.UserDomainID != "domain-id" || requestBody.CommonAccountInfo.AccountType != 1 {
		t.Fatalf("commonAccountInfo = %#v, want domain-id/account type 1", requestBody.CommonAccountInfo)
	}
}

func TestNormalize139MembershipTier(t *testing.T) {
	tests := []struct {
		level string
		name  string
		want  string
	}{
		{level: "0", want: "ordinary"},
		{level: "1", want: "silver"},
		{level: "2", want: "gold"},
		{level: "3", want: "diamond"},
		{name: "普通会员", want: "ordinary"},
		{name: "Silver Member", want: "silver"},
		{name: "黄金会员", want: "gold"},
		{name: "Diamond Member", want: "diamond"},
		{level: "unknown", name: "未识别权益", want: ""},
	}
	for _, test := range tests {
		if got := normalize139MembershipTier(test.level, test.name); got != test.want {
			t.Errorf("normalize139MembershipTier(%q, %q) = %q, want %q", test.level, test.name, got, test.want)
		}
	}
}

func TestClusterMembershipTierUsesConfiguredRuntimeAndReferenceTiers(t *testing.T) {
	root := &Yun139{
		Addition:              Addition{MembershipTier: "unknown"},
		runtimeMembershipTier: "diamond",
	}
	child := &Yun139{
		Addition: Addition{MembershipTier: "unknown"},
		ref:      root,
	}
	if got := child.ClusterMembershipTier(); got != "diamond" {
		t.Fatalf("reference runtime tier = %q, want diamond", got)
	}

	child.Addition.MembershipTier = "silver"
	if got := child.ClusterMembershipTier(); got != "silver" {
		t.Fatalf("configured child tier = %q, want silver", got)
	}

	child.Addition.MembershipTier = "unknown"
	root.Addition.MembershipTier = "gold"
	if got := child.ClusterMembershipTier(); got != "gold" {
		t.Fatalf("configured reference tier = %q, want gold", got)
	}
}

func TestUpdateRuntimeMembershipTierKeepsPreviousTierOnQueryFailure(t *testing.T) {
	setup139Resty(t)
	oldEndpoint := mobile139MembershipEndpoint
	t.Cleanup(func() {
		mobile139MembershipEndpoint = oldEndpoint
	})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		write139JSON(t, w, map[string]any{
			"success": false,
			"code":    "04000005",
			"message": "认证失败",
		})
	}))
	defer server.Close()
	mobile139MembershipEndpoint = server.URL

	d := &Yun139{
		Account:               "13900000000",
		runtimeMembershipTier: "silver",
		Addition: Addition{
			Authorization: "stored-authorization",
			CookieHeader:  "skey=invalid",
		},
	}
	d.updateRuntimeMembershipTier(context.Background())
	if d.runtimeMembershipTier != "silver" {
		t.Fatalf("runtime tier = %q, want previous silver tier after query failure", d.runtimeMembershipTier)
	}
}

func TestUpdateRuntimeMembershipTierSkipsConfiguredOverride(t *testing.T) {
	setup139Resty(t)
	oldEndpoint := mobile139MembershipEndpoint
	t.Cleanup(func() {
		mobile139MembershipEndpoint = oldEndpoint
	})

	called := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	mobile139MembershipEndpoint = server.URL

	d := &Yun139{Addition: Addition{MembershipTier: "gold"}}
	d.updateRuntimeMembershipTier(context.Background())
	if called {
		t.Fatal("configured membership tier should skip the automatic query")
	}
}

func TestUpdateRuntimeMembershipTierCapsQueryWithoutExtendingParentDeadline(t *testing.T) {
	tests := []struct {
		name          string
		parentTimeout time.Duration
		wantTimeout   time.Duration
	}{
		{name: "default cap", wantTimeout: mobile139MembershipQueryTimeout},
		{name: "earlier parent deadline", parentTimeout: 2 * time.Second, wantTimeout: 2 * time.Second},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			setup139Resty(t)
			var requestDeadline time.Time
			base.RestyClient.SetTransport(membershipRoundTripFunc(func(req *http.Request) (*http.Response, error) {
				requestDeadline, _ = req.Context().Deadline()
				return nil, context.Canceled
			}))

			ctx := context.Background()
			cancel := func() {}
			if test.parentTimeout > 0 {
				ctx, cancel = context.WithTimeout(ctx, test.parentTimeout)
			}
			defer cancel()
			startedAt := time.Now()
			d := &Yun139{Account: "13900000000"}
			d.updateRuntimeMembershipTier(ctx)
			if requestDeadline.IsZero() {
				t.Fatal("membership request context has no deadline")
			}
			gotTimeout := requestDeadline.Sub(startedAt)
			if delta := gotTimeout - test.wantTimeout; delta < -100*time.Millisecond || delta > 100*time.Millisecond {
				t.Fatalf("request timeout = %s, want approximately %s", gotTimeout, test.wantTimeout)
			}
		})
	}
}

type membershipRoundTripFunc func(*http.Request) (*http.Response, error)

func (f membershipRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
