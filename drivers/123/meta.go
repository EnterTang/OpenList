package _123

import (
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
)

type Addition struct {
	Username       string `json:"username" required:"true"`
	Password       string `json:"password" required:"true"`
	MembershipTier string `json:"membership_tier" type:"select" options:"unknown,ordinary,vip,svip" default:"unknown" label:"会员等级" help:"用于多账号集群调度权重；无法确认时保留 unknown。"`
	driver.RootID
	//OrderBy        string `json:"order_by" type:"select" options:"file_id,file_name,size,update_at" default:"file_name"`
	//OrderDirection string `json:"order_direction" type:"select" options:"asc,desc" default:"asc"`
	AccessToken  string
	UploadThread int    `json:"UploadThread" type:"number" default:"3" help:"the threads of upload"`
	Platform     string `json:"platform" type:"string" default:"web" help:"the platform header value, sent with API requests"`
}

var config = driver.Config{
	Name:        "123Pan",
	DefaultRoot: "0",
	LocalSort:   true,
	PreferProxy: true,
}

func init() {
	op.RegisterDriver(func() driver.Driver {
		// 新增默认选项 要在RegisterDriver初始化设置 才会对正在使用的用户生效
		return &Pan123{
			Addition: Addition{
				UploadThread: 3,
				Platform:     "web",
			},
		}
	})
}
