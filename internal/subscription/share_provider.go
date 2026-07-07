package subscription

import (
	"context"
	"time"
)

type ShareProviderName string

const (
	ShareProviderQuark       ShareProviderName = "quark"
	ShareProviderAliyunDrive ShareProviderName = "aliyun_drive"
	ShareProviderPan123      ShareProviderName = "pan123"
	ShareProviderPan115      ShareProviderName = "pan115"
)

type ShareRef struct {
	Provider ShareProviderName
	RawURL   string
	ShareID  string
	Passcode string
	ParentID string
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
