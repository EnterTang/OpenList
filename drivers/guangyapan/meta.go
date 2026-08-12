package guangyapan

import (
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
)

type Addition struct {
	RootPath       string `json:"root_path" help:"光鸭网盘完整路径，作为挂载根目录"`
	PhoneNumber    string `json:"phone_number" type:"text" help:"短信登录手机号，例如 +86 13800000000"`
	CaptchaToken   string `json:"captcha_token" type:"text" help:"调用 /v1/auth/verification 所需的验证码 token"`
	SendCode       bool   `json:"send_code" type:"bool" help:"设为 true 并保存以发送短信验证码；发送后会自动重置为 false"`
	VerifyCode     string `json:"verify_code" type:"text" help:"短信验证码；填写后保存以完成登录"`
	VerificationID string `json:"verification_id" type:"text" help:"发送短信后自动生成，请勿手动修改"`
	AccessToken    string `json:"access_token" type:"text" help:"Bearer access token（若已提供 refresh_token 则可留空）"`
	RefreshToken   string `json:"refresh_token" type:"text" help:"用于自动登录/刷新的 refresh token"`
	ClientID       string `json:"client_id" default:"aMe-8VSlkrbQXpUR"`
	DeviceID       string `json:"device_id" help:"可选自定义设备 ID（32 位十六进制）；为空时自动生成"`
	PageSize       int    `json:"page_size" type:"number" default:"100"`
	OrderBy        int    `json:"order_by" type:"number" options:"0,1,2,3,4" default:"3" help:"文件列表排序字段"`
	SortType       int    `json:"sort_type" type:"number" options:"0,1" default:"1" help:"文件列表排序方向"`
}

var config = driver.Config{
	Name:              "GuangYaPan",
	LocalSort:         false,
	OnlyProxy:         false,
	NoCache:           false,
	NoUpload:          false,
	NeedMs:            false,
	DefaultRoot:       "",
	CheckStatus:       true,
	Alert:             "info|两步短信登录：(1) 填写手机号（必要时填写 captcha_token），将 send_code 设为 true 并保存；(2) 填写 verify_code 并保存以完成登录，系统会自动保存 access_token/refresh_token。",
	NoOverwriteUpload: true,
}

func init() {
	op.RegisterDriver(func() driver.Driver {
		return &GuangYaPan{}
	})
}
