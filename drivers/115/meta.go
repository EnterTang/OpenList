package _115

import (
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
)

type Addition struct {
	Cookie         string  `json:"cookie" type:"text" help:"one of QR code token and cookie required"`
	QRCodeToken    string  `json:"qrcode_token" type:"text" help:"one of QR code token and cookie required"`
	QRCodeSource   string  `json:"qrcode_source" type:"select" options:"web,android,ios,tv,alipaymini,wechatmini,qandroid" default:"linux" help:"select the QR code device, default linux"`
	PageSize       int64   `json:"page_size" type:"number" default:"1000" help:"list api per page size of 115 driver"`
	LimitRate      float64 `json:"limit_rate" type:"float" default:"2" help:"limit all api request rate ([limit]r/1s)"`
	MembershipTier string  `json:"membership_tier" type:"select" options:"unknown,ordinary,vip,svip" default:"unknown" label:"会员等级" help:"用于多账号集群调度权重；unknown 时登录后尝试从 115 用户信息自动采集。"`
	driver.RootID
}

var config = driver.Config{
	Name:          "115 Cloud",
	DefaultRoot:   "0",
	LinkCacheMode: driver.LinkCacheUA,
}

func init() {
	op.RegisterDriver(func() driver.Driver {
		return &Pan115{}
	})
}
