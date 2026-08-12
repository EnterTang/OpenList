package _115_open

import (
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
)

type Addition struct {
	// Usually one of two
	driver.RootID
	// define other
	OrderBy        string  `json:"order_by" type:"select" options:"file_name,file_size,user_utime,file_type"`
	OrderDirection string  `json:"order_direction" type:"select" options:"asc,desc"`
	LimitRate      float64 `json:"limit_rate" type:"float" default:"1" help:"limit all api request rate ([limit]r/1s)"`
	PageSize       int64   `json:"page_size" type:"number" default:"200" help:"list api per page size of 115open driver"`
	AccessToken    string  `json:"access_token" required:"true"`
	RefreshToken   string  `json:"refresh_token" required:"true"`
	MembershipTier string  `json:"membership_tier" type:"select" options:"unknown,ordinary,vip,svip" default:"unknown" label:"会员等级" help:"用于多账号集群调度权重；unknown 时登录后尝试从 115 Open 用户信息自动采集。"`
}

var config = driver.Config{
	Name:          "115 Open",
	DefaultRoot:   "0",
	LinkCacheMode: driver.LinkCacheUA,
}

func init() {
	op.RegisterDriver(func() driver.Driver {
		return &Open115{}
	})
}
