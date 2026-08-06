package _115sy

type Profile string

const (
	ProfileWeb     Profile = "web"
	ProfileAndroid Profile = "android"
)

type Operation uint8

const (
	OperationUserInfo Operation = iota
	OperationFileList
	OperationShareSnapshot
	OperationShareReceive
	OperationDownloadURL
	OperationOffline
)

const (
	DefaultWebBaseURL     = "https://webapi.115.com"
	DefaultAndroidBaseURL = "https://proapi.115.com"
	DefaultAppVersion     = "36.2.28"
	DefaultWebUserAgent   = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"
	DefaultAndroidUA      = "Mozilla/5.0 (Linux; Android 13; 22101320C Build/TQ1A.230105.001; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/138.0.0.0 Mobile Safari/537.36"
)

const (
	EndpointUserInfo      = "/user/info"
	EndpointFileList      = "/files/list"
	EndpointShareSnapshot = "/share/snapshot"
	EndpointShareReceive  = "/share/receive"
	EndpointDownloadURL   = "/download/url"
	EndpointOfflineAdd    = "/offline/add"
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
		Primary:         ProfileWeb,
		Fallback:        ProfileAndroid,
		FallbackHTTP405: true,
	},
	OperationDownloadURL: {
		Primary:         ProfileWeb,
		Fallback:        ProfileAndroid,
		FallbackHTTP405: true,
	},
	OperationOffline: {
		Primary:         ProfileAndroid,
		Fallback:        ProfileWeb,
		FallbackHTTP405: true,
	},
}

func policyForOperation(operation Operation) operationPolicy {
	if policy, ok := operationPolicies[operation]; ok {
		return policy
	}
	return operationPolicy{Primary: ProfileWeb}
}
