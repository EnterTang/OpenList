package _139

import (
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
)

type Addition struct {
	//Account       string `json:"account" required:"true"`
	Authorization string `json:"authorization" type:"text" required:"true"`
	Username      string `json:"username" required:"true"`
	Password      string `json:"password" required:"true" secret:"true"`
	MailCookies   string `json:"mail_cookies" required:"true" type:"text" help:"Cookies from mail.139.com used for login authentication."`
	driver.RootID
	Type                  string `json:"type" type:"select" options:"personal_new,family,group,personal" default:"personal_new"`
	CloudID               string `json:"cloud_id"`
	UserDomainID          string `json:"user_domain_id" help:"ud_id in Cookie, fill in to show disk usage"`
	CustomUploadPartSize  int64  `json:"custom_upload_part_size" type:"number" default:"0" help:"0 for auto"`
	ReportRealSize        bool   `json:"report_real_size" type:"bool" default:"true" help:"Enable to report the real file size during upload"`
	UseLargeThumbnail     bool   `json:"use_large_thumbnail" type:"bool" default:"false" help:"Enable to use large thumbnail for images"`
	GenerateETF           bool   `json:"generate_etf" type:"bool" default:"false" help:"After upload, generate a .etf rapid metadata file"`
	DeleteSourceAfterETF  bool   `json:"delete_source_after_etf" type:"bool" default:"false" help:"After generating .etf, delete the source file and clear recycle bin"`
	RestoreSourceFromETF  bool   `json:"restore_source_from_etf" type:"bool" default:"false" help:"Restore source file from .etf metadata"`
	DeleteETFAfterRestore bool   `json:"delete_etf_after_restore" type:"bool" default:"false" help:"After restore, delete the .etf file and clear recycle bin"`
	ETFDownloadRestore    bool   `json:"etf_download_restore" type:"bool" default:"false" help:"Downloading .etf via /d restores and returns the real file"`
	ETFVideoPlayback      bool   `json:"etf_video_playback" type:"bool" default:"false" help:"Allow video playback through temporary rapid-created file from .etf"`
	ETFRootFolderID       string `json:"etf_root_folder_id" help:"Optional folder id for generated .etf files"`
	ETFRootPath           string `json:"etf_root_path" help:"Optional folder path for generated .etf files"`
	ETFTempFolderID       string `json:"etf_temp_folder_id" help:"Optional folder id for temporary .etf playback files"`
	ETFExtAllowlist       string `json:"etf_ext_allowlist" help:"Comma-separated source extensions allowed for .etf generation; empty means all non-.etf files"`
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
