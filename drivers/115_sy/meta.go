package _115_sy

import "github.com/OpenListTeam/OpenList/v4/internal/driver"

type Addition struct {
	driver.RootID
	Cookie             string  `json:"cookie" type:"text" help:"115 Cookie; UID/CID/SEID are required"`
	QRCodeToken        string  `json:"qrcode_token,omitempty" type:"text" help:"optional QR login session token"`
	QRCodeSource       string  `json:"qrcode_source,omitempty" type:"select" options:"web,android,ios,tv,alipaymini,wechatmini,qandroid" default:"android"`
	PageSize           int64   `json:"page_size" type:"number" default:"200"`
	PageCooldown       string  `json:"page_cooldown" type:"text" default:"250ms"`
	LimitRate          float64 `json:"limit_rate" type:"float" default:"1"`
	UserAgent          string  `json:"user_agent,omitempty" type:"text"`
	AppVersion         string  `json:"app_version,omitempty" type:"text" default:"36.2.28"`
	ShareCIDs          string  `json:"share_cids,omitempty" type:"text" help:"name:cid pairs separated by comma"`
	OfflineCID         string  `json:"offline_cid,omitempty" type:"text"`
	AutomationInterval string  `json:"automation_interval,omitempty" type:"text"`
	MembershipTier     string  `json:"membership_tier,omitempty" type:"select" options:"unknown,ordinary,vip,svip" default:"unknown"`
}

var config = driver.Config{
	Name:          "115 SY",
	DefaultRoot:   "0",
	LinkCacheMode: driver.LinkCacheUA,
}
