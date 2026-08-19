package subscription

import (
	"context"
	"fmt"
	"strings"
	"time"
)

type ShareProviderName string

const (
	ShareProviderQuark       ShareProviderName = "quark"
	ShareProviderAliyunDrive ShareProviderName = "aliyun_drive"
	ShareProviderPan123      ShareProviderName = "pan123"
	ShareProviderPan115      ShareProviderName = "pan115"
	ShareProviderGuangYaPan  ShareProviderName = "guangyapan"
)

type ShareRef struct {
	Provider ShareProviderName
	RawURL   string
	ShareID  string
	Passcode string
	ParentID string

	// quarkSToken is an execution-local token used to keep Quark share
	// metadata and the subsequent save request in the same share session.
	// It is intentionally unexported so it cannot enter persisted task data.
	quarkSToken string
}

type ShareProvider interface {
	Name() ShareProviderName
	ParseURL(raw string) (ShareRef, error)
}

type ShareItem struct {
	ID       string
	ParentID string
	Name     string
	Size     int64
	Modified time.Time
	IsDir    bool
	Raw      any
}

// ShareItemProviderData contains non-secret provider metadata needed by an
// execution-time operation such as direct-link resolution. It must never
// contain cookies, access tokens, passcodes, signatures, or URLs.
type ShareItemProviderData map[string]string

func SanitizeShareItemProviderData(raw any) ShareItemProviderData {
	values, ok := raw.(map[string]any)
	if !ok {
		return nil
	}
	allowed := map[string]struct{}{
		"etag": {}, "s3key_flag": {}, "s3KeyFlag": {},
		"file_name": {}, "size": {}, "file_id": {},
	}
	result := make(ShareItemProviderData, len(allowed))
	for key, value := range values {
		if _, ok := allowed[key]; !ok {
			continue
		}
		text := ""
		switch typed := value.(type) {
		case string:
			text = typed
		case fmt.Stringer:
			text = typed.String()
		default:
			text = fmt.Sprint(typed)
		}
		if strings.TrimSpace(text) != "" {
			result[key] = strings.TrimSpace(text)
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

// ShareDownloadLink is a short-lived provider result. The URL must remain in
// Worker memory and must never be serialized into a coordinator job or a
// durable error record.
type ShareDownloadLink struct {
	URL       string
	Headers   map[string]string
	ExpiresAt time.Time
	FileID    string
	Size      int64
	Hash      string
}

type ShareTreeLister interface {
	ShareProvider
	ListShareChildren(ctx context.Context, ref ShareRef, parentID string) ([]ShareItem, error)
}

type ShareSaver interface {
	ShareTreeLister
	EnsureDir(ctx context.Context, path string) (string, error)
	SaveShareItems(ctx context.Context, ref ShareRef, parentID string, items []ShareItem, dstDirID string) ([]string, error)
	WaitSaveComplete(ctx context.Context, taskIDs []string) error
}

// shareSessionRefProvider prepares an execution-local ShareRef before the
// share tree is listed. Providers use it when metadata and the save request
// must share a short-lived session token.
type shareSessionRefProvider interface {
	prepareShareRef(ctx context.Context, ref ShareRef) (ShareRef, error)
}

// ShareDirectDownloader is optional. Providers only implement it after a
// real endpoint contract and target-storage test have validated the complete
// share -> temporary URL -> download flow. Unsupported providers retain the
// share-save path.
type ShareDirectDownloader interface {
	GetShareDownloadURL(ctx context.Context, ref ShareRef, item ShareItem) (ShareDownloadLink, error)
}
