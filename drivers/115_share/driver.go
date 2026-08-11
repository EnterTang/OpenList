package _115_share

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strings"
	"sync"

	"github.com/OpenListTeam/OpenList/v4/drivers/base"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	driver115 "github.com/SheltonZhu/115driver/pkg/driver"
	"golang.org/x/time/rate"
)

type Pan115Share struct {
	model.Storage
	Addition
	client  *driver115.Pan115Client
	limiter *rate.Limiter
}

var pan115ShareAccountLimiters sync.Map

func pan115ShareLimiter(cookie string, limitRate float64) *rate.Limiter {
	if limitRate <= 0 {
		return nil
	}
	identity := strings.TrimSpace(cookie)
	if identity == "" {
		identity = "anonymous"
	}
	sum := sha256.Sum256([]byte(identity))
	key := hex.EncodeToString(sum[:])
	value, _ := pan115ShareAccountLimiters.LoadOrStore(key, rate.NewLimiter(rate.Limit(limitRate), 1))
	return value.(*rate.Limiter)
}

func (d *Pan115Share) Config() driver.Config {
	return config
}

func (d *Pan115Share) GetAddition() driver.Additional {
	return &d.Addition
}

func (d *Pan115Share) Init(ctx context.Context) error {
	if err := d.login(); err != nil {
		return err
	}
	d.limiter = pan115ShareLimiter(d.Cookie, d.LimitRate)
	return nil
}

func (d *Pan115Share) WaitLimit(ctx context.Context) error {
	if d.limiter != nil {
		return d.limiter.Wait(ctx)
	}
	return nil
}

func (d *Pan115Share) Drop(ctx context.Context) error {
	return nil
}

func (d *Pan115Share) List(ctx context.Context, dir model.Obj, args model.ListArgs) ([]model.Obj, error) {
	var ua string
	// TODO: will use user agent from header
	// if args.Header != nil {
	// 	ua = args.Header.Get("User-Agent")
	// }
	if ua == "" {
		ua = base.UserAgentNT
	}
	files := make([]driver115.ShareFile, 0)
	seen := make(map[string]struct{})
	pageSize := d.PageSize
	if pageSize <= 0 {
		pageSize = 1000
	}
	if pageSize > 1150 {
		pageSize = 1150
	}
	offset := 0
	for page := 0; page < 10000; page++ {
		if err := d.WaitLimit(ctx); err != nil {
			return nil, err
		}
		fileResp, err := d.client.GetShareSnapWithUA(ua, d.ShareCode, d.ReceiveCode, dir.GetID(), driver115.QueryLimit(int(pageSize)), driver115.QueryOffset(offset))
		if err != nil {
			return nil, err
		}
		added := 0
		for _, file := range fileResp.Data.List {
			id := string(file.FileID)
			if id == "" {
				id = string(file.CategoryID) + "\x00" + string(file.FileName)
			}
			if _, exists := seen[id]; exists {
				continue
			}
			seen[id] = struct{}{}
			files = append(files, file)
			added++
		}
		if len(fileResp.Data.List) == 0 || added == 0 || len(fileResp.Data.List) < int(pageSize) || (fileResp.Data.Count > 0 && len(files) >= fileResp.Data.Count) {
			break
		}
		offset += len(fileResp.Data.List)
	}

	return utils.SliceConvert(files, transFunc)
}

func (d *Pan115Share) Link(ctx context.Context, file model.Obj, args model.LinkArgs) (*model.Link, error) {
	if err := d.WaitLimit(ctx); err != nil {
		return nil, err
	}
	var ua string
	if args.Header != nil {
		ua = args.Header.Get("User-Agent")
	}
	if ua == "" {
		ua = base.UserAgent
	}
	downloadInfo, err := d.client.DownloadByShareCodeWithUA(ua, d.ShareCode, d.ReceiveCode, file.GetID())
	if err != nil {
		return nil, err
	}
	header := http.Header{}
	header.Set("User-Agent", ua)
	return &model.Link{
		URL:    downloadInfo.URL.URL,
		Header: header,
	}, nil
}

func (d *Pan115Share) MakeDir(ctx context.Context, parentDir model.Obj, dirName string) error {
	return errs.NotSupport
}

func (d *Pan115Share) Move(ctx context.Context, srcObj, dstDir model.Obj) error {
	return errs.NotSupport
}

func (d *Pan115Share) Rename(ctx context.Context, srcObj model.Obj, newName string) error {
	return errs.NotSupport
}

func (d *Pan115Share) Copy(ctx context.Context, srcObj, dstDir model.Obj) error {
	return errs.NotSupport
}

func (d *Pan115Share) Remove(ctx context.Context, obj model.Obj) error {
	return errs.NotSupport
}

func (d *Pan115Share) Put(ctx context.Context, dstDir model.Obj, stream model.FileStreamer, up driver.UpdateProgress) error {
	return errs.NotSupport
}

var _ driver.Driver = (*Pan115Share)(nil)
