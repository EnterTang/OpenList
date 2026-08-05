package _115_cd2

import (
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
)

type Addition struct {
	driver.RootID
	OrderBy        string  `json:"order_by" type:"select" options:"file_name,file_size,user_utime,file_type"`
	OrderDirection string  `json:"order_direction" type:"select" options:"asc,desc"`
	LimitRate      float64 `json:"limit_rate" type:"float" default:"1" help:"limit all api request rate ([limit]r/1s)"`
	PageSize       int64   `json:"page_size" type:"number" default:"200" help:"list api per page size of 115open driver"`
	AccessToken    string  `json:"access_token" type:"text" help:"115 Open access token；留空时使用 CD2 OAuth 授权登录"`
	RefreshToken   string  `json:"refresh_token" type:"text" help:"115 Open refresh token；留空时使用 CD2 OAuth 授权登录"`
	MembershipTier string  `json:"membership_tier" type:"select" options:"unknown,ordinary,vip,svip" default:"unknown" label:"会员等级" help:"用于多账号集群调度权重；unknown 时不调用用户信息接口。"`
	AuthMode       string  `json:"auth_mode" type:"select" options:"oauth,qrcode" default:"oauth" label:"授权方式" help:"默认使用 CloudDrive2 的 115 Open OAuth；需要二维码时选择 qrcode。"`
	AppID          string  `json:"app_id" type:"text" help:"二维码授权使用 CloudDrive2 的 CLOUD115_APP_ID；留空使用 CD2 内置值"`

	OAuthState string `json:"oauth_state,omitempty" type:"text" ignore:"true"`
	OAuthURL   string `json:"oauth_url,omitempty" type:"text" ignore:"true"`

	QRCodeUID    string `json:"qrcode_uid,omitempty" type:"text" ignore:"true"`
	QRCodeTime   int64  `json:"qrcode_time,omitempty" ignore:"true"`
	QRCodeSign   string `json:"qrcode_sign,omitempty" type:"text" ignore:"true"`
	QRCodeURL    string `json:"qrcode_url,omitempty" type:"text" ignore:"true"`
	CodeVerifier string `json:"code_verifier,omitempty" type:"text" ignore:"true"`
}

var config = driver.Config{
	Name:          "115 CD2",
	DefaultRoot:   "0",
	OnlyProxy:     true,
	LinkCacheMode: driver.LinkCacheUA,
}

func init() {
	op.RegisterDriver(func() driver.Driver {
		return &CD2{}
	})
}
