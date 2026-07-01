package _139

import (
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
)

type Addition struct {
	//Account       string `json:"account" required:"true"`
	AuthMode      string `json:"auth_mode" type:"radio" options:"etf,openlist" default:"etf" label:"授权模式" help:"ETF 认证模式只需填写 Cookie Header；OpenList 默认模式沿用 Authorization/用户名/密码/邮箱 Cookie。"`
	CookieHeader  string `json:"cookie_header" type:"text" required:"true" visible_when:"auth_mode=etf" label:"Cookie Header" help:"从 yun.139.com 网页端复制完整 Cookie。ETF 认证模式会从其中提取 auth_token、authorization、账号和 ud_id，并自动续期。"`
	Authorization string `json:"authorization" type:"text" required:"true" visible_when:"auth_mode=openlist"`
	Username      string `json:"username" required:"true" visible_when:"auth_mode=openlist"`
	Password      string `json:"password" required:"true" secret:"true" visible_when:"auth_mode=openlist"`
	MailCookies   string `json:"mail_cookies" required:"true" type:"text" visible_when:"auth_mode=openlist" help:"mail.139.com 的 Cookie，用于密码登录换取移动云盘授权。"`
	driver.RootID
	Type                  string `json:"type" type:"select" options:"personal_new,family,group,personal" default:"personal_new"`
	CloudID               string `json:"cloud_id"`
	UserDomainID          string `json:"user_domain_id" help:"ud_id in Cookie, fill in to show disk usage"`
	CustomUploadPartSize  int64  `json:"custom_upload_part_size" type:"number" default:"0" help:"0 for auto"`
	ReportRealSize        bool   `json:"report_real_size" type:"bool" default:"true" help:"Enable to report the real file size during upload"`
	UseLargeThumbnail     bool   `json:"use_large_thumbnail" type:"bool" default:"false" help:"Enable to use large thumbnail for images"`
	GenerateETF           bool   `json:"generate_etf" type:"bool" default:"false" label:"生成 ETF" group:"ETF" collapsed:"true" help:"上传普通文件后自动生成 .etf 秒传元数据文件。"`
	DeleteSourceAfterETF  bool   `json:"delete_source_after_etf" type:"bool" default:"false" label:"生成后删除源文件" group:"ETF" collapsed:"true" help:"生成 .etf 后删除源文件并清空回收站。"`
	RestoreSourceFromETF  bool   `json:"restore_source_from_etf" type:"bool" default:"false" label:"通过 ETF 恢复源文件" group:"ETF" collapsed:"true" help:"上传 .etf 文件时通过秒传恢复原文件。"`
	DeleteETFAfterRestore bool   `json:"delete_etf_after_restore" type:"bool" default:"false" label:"恢复后删除 ETF" group:"ETF" collapsed:"true" help:"恢复源文件后删除 .etf 文件并清空回收站。"`
	ETFDownloadRestore    bool   `json:"etf_download_restore" type:"bool" default:"false" label:"下载 ETF 时恢复" group:"ETF" collapsed:"true" help:"通过 /d 下载 .etf 时，先秒传恢复原文件再返回真实文件下载链接。"`
	ETFVideoPlayback      bool   `json:"etf_video_playback" type:"bool" default:"false" label:"ETF 临时播放" group:"ETF" collapsed:"true" help:"播放 .etf 时临时秒传恢复视频文件，获取播放链接后删除临时文件并清空回收站。"`
	ETFRootFolder         string `json:"etf_root_folder" label:"ETF 管理目录" group:"ETF" collapsed:"true" help:"生成的 .etf 文件保存目录。空表示沿用上传目录；/ 表示网盘根目录；etf管理 或 /etf管理 表示根目录下的 etf管理；/path1/path2 会按层级自动创建。"`
	ETFRootPath           string `json:"etf_root_path" label:"ETF 分类子目录" group:"ETF" collapsed:"true" help:"生成 .etf 文件时追加的固定子目录，可与媒体类型和二级分类目录一起使用。"`
	ETFTempFolder         string `json:"etf_temp_folder" label:"ETF 临时播放目录" group:"ETF" collapsed:"true" help:"通过 .etf 临时恢复播放文件的目录。空表示网盘根目录；/ 表示网盘根目录；temp 或 /temp 表示根目录下的 temp；/path1/path2 会按层级自动创建。"`
	ETFExtAllowlist       string `json:"etf_ext_allowlist" label:"ETF 文件后缀白名单" group:"ETF" collapsed:"true" help:"允许生成 .etf 的源文件后缀，使用逗号分隔；留空表示所有非 .etf 文件都允许。"`
	ETFRootFolderID       string `json:"etf_root_folder_id" ignore:"true"`
	ETFTempFolderID       string `json:"etf_temp_folder_id" ignore:"true"`
}

var config = driver.Config{
	Name:             "139Yun",
	LocalSort:        true,
	ProxyRangeOption: true,
}

func init() {
	op.RegisterDriver(func() driver.Driver {
		d := &Yun139{}
		d.ProxyRange = true
		return d
	})
}
