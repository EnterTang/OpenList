package _115sy

type Profile string

const (
	ProfileWeb      Profile = "web"
	ProfileChrome   Profile = "chrome"
	ProfileAndroid  Profile = "android"
	ProfileUpload   Profile = "upload"
	ProfileQRCode   Profile = "qrcode"
	ProfilePassport Profile = "passport"
)

type Operation uint8

const (
	OperationUserInfo Operation = iota
	OperationFileList
	OperationShareSnapshot
	OperationShareReceive
	OperationShareDownloadURL
	OperationDownloadURL
	OperationOffline
	OperationQRCodeToken
	OperationQRCodeStatus
	OperationQRCodeLogin
	OperationUploadInfo
	OperationUploadInit
	OperationUploadOSSToken
)

const (
	DefaultWebBaseURL      = "https://webapi.115.com"
	DefaultAndroidBaseURL  = "https://proapi.115.com"
	DefaultUploadBaseURL   = "https://uplb.115.com"
	DefaultAppVersion      = "36.2.28"
	DefaultWebUserAgent    = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"
	DefaultAndroidUA       = "Mozilla/5.0 (Linux; Android 13; 22101320C Build/TQ1A.230105.001; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/138.0.0.0 Mobile Safari/537.36"
	DefaultQRCodeBaseURL   = "https://qrcodeapi.115.com"
	DefaultPassportBaseURL = "https://passportapi.115.com"
)

const (
	EndpointUserInfo            = "/user/info"
	EndpointFileList            = "/android/2.0/ufile/files"
	EndpointFileListWeb         = "/files"
	EndpointShareSnapshot       = "/share/snap"
	EndpointShareSnapshotApp    = "/android/2.0/share/snap"
	EndpointShareReceive        = "/share/receive"
	EndpointShareReceiveApp     = "/android/2.0/share/receive"
	EndpointShareSend           = "/share/send"
	EndpointShareSendApp        = "/android/2.0/share/send"
	EndpointShareDownloadURLApp = "/app/share/downurl"
	EndpointShareDownloadURLWeb = "/share/downurl"
	EndpointDownloadURL         = "/app/chrome/downurl"
	EndpointOfflineAdd          = "/open/offline/add_task_urls"
	EndpointOfflineList         = "/open/offline/get_task_list"
	EndpointOfflineDelete       = "/open/offline/del_task"
	EndpointQRCodeToken         = "/api/1.0/web/1.0/token"
	EndpointQRCodeStatus        = "/get/status/"
	EndpointQRCodeLogin         = "/app/1.0/%s/1.0/login/qrcode"
	EndpointUploadInfo          = "/app/uploadinfo"
	EndpointUploadInit          = "/4.0/initupload.php"
	EndpointUploadOSSToken      = "/3.0/gettoken.php"
	EndpointFileInfo            = "/files/get_info"
	EndpointDirID               = "/files/getid"
	EndpointDirAdd              = "/files/add"
	EndpointFileMove            = "/files/move"
	EndpointFileRename          = "/files/batch_rename"
	EndpointFileCopy            = "/files/copy"
	EndpointFileDelete          = "/rb/delete"
	EndpointRecycleClean        = "/rb/clean"
	EndpointCategory            = "/android/2.0/category/get"
	EndpointCategoryWeb         = "/category/get"
)

type operationPolicy struct {
	Primary         Profile
	Fallback        Profile
	PageCooldown    bool
	FallbackHTTP405 bool
}

var operationPolicies = map[Operation]operationPolicy{
	OperationUserInfo: {
		Primary:         ProfileWeb,
		Fallback:        ProfileAndroid,
		FallbackHTTP405: true,
	},
	OperationFileList: {
		Primary:         ProfileAndroid,
		Fallback:        ProfileWeb,
		PageCooldown:    true,
		FallbackHTTP405: true,
	},
	OperationShareSnapshot: {
		Primary:         ProfileAndroid,
		Fallback:        ProfileWeb,
		PageCooldown:    true,
		FallbackHTTP405: true,
	},
	OperationShareReceive: {
		Primary:         ProfileAndroid,
		Fallback:        ProfileWeb,
		FallbackHTTP405: true,
	},
	OperationShareDownloadURL: {
		Primary: ProfileAndroid,
	},
	OperationDownloadURL: {
		Primary: ProfileChrome,
	},
	OperationOffline: {
		Primary:         ProfileAndroid,
		Fallback:        ProfileWeb,
		FallbackHTTP405: true,
	},
	OperationQRCodeToken:  {Primary: ProfileQRCode},
	OperationQRCodeStatus: {Primary: ProfileQRCode},
	OperationQRCodeLogin:  {Primary: ProfilePassport},
	OperationUploadInfo:   {Primary: ProfileAndroid},
	OperationUploadInit:   {Primary: ProfileUpload},
	OperationUploadOSSToken: {
		Primary: ProfileUpload,
	},
}

func policyForOperation(operation Operation) operationPolicy {
	if policy, ok := operationPolicies[operation]; ok {
		return policy
	}
	return operationPolicy{Primary: ProfileWeb}
}
