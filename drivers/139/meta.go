package _139

import (
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
)

type Addition struct {
	//Account       string `json:"account" required:"true"`
	AuthMode      string `json:"auth_mode" type:"select" options:"etf,openlist" default:"etf" label:"授权模式" help:"ETF 认证模式只需填写 Cookie Header；OpenList 默认模式沿用 Authorization/用户名/密码/邮箱 Cookie。"`
	CookieHeader  string `json:"cookie_header" type:"text" required:"true" visible_when:"auth_mode=etf" label:"Cookie Header" help:"从 yun.139.com 网页端复制完整 Cookie。ETF 认证模式会从其中提取 auth_token、authorization、账号和 ud_id，并自动续期。"`
	Authorization string `json:"authorization" type:"text" required:"true" visible_when:"auth_mode=openlist" label:"授权"`
	Username      string `json:"username" required:"true" visible_when:"auth_mode=openlist" label:"用户名"`
	Password      string `json:"password" required:"true" secret:"true" visible_when:"auth_mode=openlist" label:"密码"`
	MailCookies   string `json:"mail_cookies" required:"true" type:"text" visible_when:"auth_mode=openlist" label:"邮箱 Cookie" help:"mail.139.com 的 Cookie，用于密码登录换取移动云盘授权。"`
	driver.RootID
	Type                                         string `json:"type" type:"select" options:"personal_new,family,group,personal" default:"personal_new" label:"类型"`
	CloudID                                      string `json:"cloud_id" label:"Cloud ID"`
	UserDomainID                                 string `json:"user_domain_id" label:"用户域 ID" help:"Cookie 中的 ud_id，填写后可显示容量信息。"`
	CustomUploadPartSize                         int64  `json:"custom_upload_part_size" type:"number" default:"0" label:"自定义分片大小" help:"0 表示自动。"`
	ReportRealSize                               bool   `json:"report_real_size" type:"bool" default:"true" label:"上报真实大小" help:"上传时上报真实文件大小。"`
	UseLargeThumbnail                            bool   `json:"use_large_thumbnail" type:"bool" default:"false" label:"使用大缩略图" help:"为图片使用大缩略图。"`
	GenerateETF                                  bool   `json:"generate_etf" type:"bool" default:"false" label:"生成 ETF" group:"ETF" collapsed:"true" help:"上传普通文件后自动生成 .etf 秒传元数据文件。"`
	ETFArchive                                   bool   `json:"etf_archive" type:"bool" default:"false" label:"ETF 归档" group:"ETF" collapsed:"true" help:"生成同路径 .etf 后，再复制一份到 ETF 管理目录并按 TMDB 与二级分类规则归档。"`
	AutoRenameOnShareRisk                        bool   `json:"auto_rename_on_share_risk" type:"bool" default:"false" label:"分享失败后自动重命名" group:"ETF" collapsed:"true" help:"创建移动分享链接返回“个人云未知异常”时，自动将目标文件或目录内含中文标题的名称改为 TMDB 英文名；若无法匹配则改为拼音，并保留季数集数后重试一次分享。改名为永久生效。"`
	DeleteSourceAfterETF                         bool   `json:"delete_source_after_etf" type:"bool" default:"false" label:"生成后删除源文件" group:"ETF" collapsed:"true" help:"生成 .etf 后删除源文件并清空回收站。"`
	ClusterDedicatedAccount                      bool   `json:"cluster_dedicated_account" type:"bool" default:"false" label:"集群专用账号" group:"ETF" collapsed:"true" help:"允许集群任务在 ETF 结果持久化后删除媒体并清空整个回收站。账号不得存放个人或其他业务文件。"`
	RestoreSourceFromETF                         bool   `json:"restore_source_from_etf" type:"bool" default:"false" label:"通过 ETF 恢复源文件" group:"ETF" collapsed:"true" help:"上传 .etf 文件时通过秒传恢复原文件。"`
	DeleteETFAfterRestore                        bool   `json:"delete_etf_after_restore" type:"bool" default:"false" label:"恢复后删除 ETF" group:"ETF" collapsed:"true" help:"恢复源文件后删除 .etf 文件并清空回收站。"`
	ETFDownloadRestore                           bool   `json:"etf_download_restore" type:"bool" default:"false" label:"下载 ETF 时恢复" group:"ETF" collapsed:"true" help:"通过 /d 下载 .etf 时，先秒传恢复原文件再返回真实文件下载链接。"`
	ETFVideoPlayback                             bool   `json:"etf_video_playback" type:"bool" default:"false" label:"ETF 临时播放" group:"ETF" collapsed:"true" help:"播放 .etf 时临时秒传恢复视频文件，获取播放链接后删除临时文件并清空回收站。"`
	ETFRootFolder                                string `json:"etf_root_folder" label:"ETF 管理目录" group:"ETF" collapsed:"true" help:"生成的 .etf 文件保存目录。空表示沿用上传目录；/ 表示网盘根目录；etf管理 或 /etf管理 表示根目录下的 etf管理；/path1/path2 会按层级自动创建。"`
	ETFRootPath                                  string `json:"etf_root_path" label:"ETF 分类子目录" group:"ETF" collapsed:"true" help:"生成 .etf 文件时追加的固定子目录，可与媒体类型和二级分类目录一起使用。"`
	ETFTempFolder                                string `json:"etf_temp_folder" label:"ETF 临时播放目录" group:"ETF" collapsed:"true" help:"通过 .etf 临时恢复播放文件的目录。空表示网盘根目录；/ 表示网盘根目录；temp 或 /temp 表示根目录下的 temp；/path1/path2 会按层级自动创建。"`
	ETFExtAllowlist                              string `json:"etf_ext_allowlist" label:"ETF 文件后缀白名单" group:"ETF" collapsed:"true" help:"允许生成 .etf 的源文件后缀，使用逗号分隔；留空表示所有非 .etf 文件都允许。"`
	ETFAutoSubscriptionEnabled                   bool   `json:"etf_auto_subscription_enabled" type:"bool" default:"false" label:"ETF 自动订阅" group:"ETF" collapsed:"true" help:"首次创建 ETF 媒体母目录后，自动创建移动云盘分享并通知目标服务创建订阅。"`
	ETFAutoSubscriptionTargetBaseURL             string `json:"etf_auto_subscription_target_base_url" label:"ETF 自动订阅目标地址" group:"ETF" collapsed:"true" help:"目标服务 pan139_fastlink API 地址，例如 http://localhost:8080/api/v1。"`
	ETFAutoSubscriptionTargetAPIToken            string `json:"etf_auto_subscription_target_api_token" secret:"true" label:"ETF 自动订阅 API Token" group:"ETF" collapsed:"true" help:"调用 pan139_fastlink 公网 API 时通过 Authorization: Bearer <token> 携带，并自动使用 /subscriptions/manual 等公网路由。"`
	ETFAutoSubscriptionTargetSupportsIdempotency bool   `json:"etf_auto_subscription_target_supports_idempotency" type:"bool" default:"false" label:"目标服务支持幂等键" group:"ETF" collapsed:"true" help:"仅在目标服务确认支持 Idempotency-Key 或可按该键查询结果时启用；启用后不确定响应可安全自动重试。"`
	ETFAutoSubscriptionQuietSeconds              int    `json:"etf_auto_subscription_quiet_seconds" type:"number" default:"30" label:"ETF 自动订阅批次静默秒数" group:"ETF" collapsed:"true" help:"同一媒体母目录连续归档 .etf 时，等待静默后只通知一次。"`
	ETFAutoSubscriptionTimeoutSeconds            int    `json:"etf_auto_subscription_timeout_seconds" type:"number" default:"30" label:"ETF 自动订阅请求超时秒数" group:"ETF" collapsed:"true"`
	ETFAutoSubscriptionMaxRetries                int    `json:"etf_auto_subscription_max_retries" type:"number" default:"5" label:"ETF 自动订阅最大重试次数" group:"ETF" collapsed:"true"`
	ETFAutoSubscriptionSharePeriodUnit           int    `json:"etf_auto_subscription_share_period_unit" type:"number" default:"1" label:"ETF 自动订阅分享有效期" group:"ETF" collapsed:"true" help:"移动云盘分享接口 periodUnit，默认 1。"`
	ETFAutoSubscriptionShareType                 string `json:"etf_auto_subscription_share_type" type:"select" options:"etf,regular" default:"etf" label:"ETF 自动订阅分享类型" group:"ETF" collapsed:"true"`
	ETFRootFolderID                              string `json:"etf_root_folder_id" ignore:"true"`
	ETFTempFolderID                              string `json:"etf_temp_folder_id" ignore:"true"`
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
