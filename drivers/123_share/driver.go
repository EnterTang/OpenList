package _123Share

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"

	_123 "github.com/OpenListTeam/OpenList/v4/drivers/123"
	"github.com/OpenListTeam/OpenList/v4/drivers/base"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/go-resty/resty/v2"
	"github.com/pkg/errors"
)

type Pan123Share struct {
	model.Storage
	Addition
	ref *_123.Pan123
}

var pan123ShareAccountLimiters sync.Map

func (d *Pan123Share) Config() driver.Config {
	return config
}

func (d *Pan123Share) GetAddition() driver.Additional {
	return &d.Addition
}

func (d *Pan123Share) Init(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(d.ShareKey) == "" {
		return errors.New("123pan share key is required")
	}
	return nil
}

func (d *Pan123Share) InitReference(storage driver.Driver) error {
	refStorage, ok := storage.(*_123.Pan123)
	if ok {
		d.ref = refStorage
		return nil
	}
	return fmt.Errorf("ref: storage is not 123Pan")
}

func (d *Pan123Share) Drop(ctx context.Context) error {
	d.ref = nil
	return nil
}

func (d *Pan123Share) List(ctx context.Context, dir model.Obj, args model.ListArgs) ([]model.Obj, error) {
	// TODO return the files list, required
	files, err := d.getFiles(ctx, dir.GetID())
	if err != nil {
		return nil, err
	}
	return utils.SliceConvert(files, func(src File) (model.Obj, error) {
		return src, nil
	})
}

func (d *Pan123Share) Link(ctx context.Context, file model.Obj, args model.LinkArgs) (*model.Link, error) {
	// TODO return link of file, required
	if f, ok := file.(File); ok {
		initialURL, header, err := d.resolveDownload(ctx, f)
		if err != nil {
			return nil, err
		}
		return _123.NewRefreshableLink(f.Size, initialURL, header, func(refreshCtx context.Context) (string, http.Header, error) {
			return d.resolveDownload(refreshCtx, f)
		}), nil
	}
	return nil, fmt.Errorf("can't convert obj")
}

func (d *Pan123Share) resolveDownload(ctx context.Context, file File) (string, http.Header, error) {
	data := base.Json{
		"shareKey":  d.ShareKey,
		"SharePwd":  d.SharePwd,
		"etag":      file.Etag,
		"fileId":    file.FileId,
		"s3keyFlag": file.S3KeyFlag,
		"size":      file.Size,
	}
	resp, err := d.request(ctx, DownloadInfo, http.MethodPost, func(req *resty.Request) {
		req.SetBody(data).SetContext(ctx)
	}, nil)
	if err != nil {
		return "", nil, err
	}
	downloadURL := utils.Json.Get(resp, "data", "DownloadURL").ToString()
	return _123.ResolveRedirectedDownload(ctx, downloadURL)
}

func (d *Pan123Share) MakeDir(ctx context.Context, parentDir model.Obj, dirName string) error {
	// TODO create folder, optional
	return errs.NotSupport
}

func (d *Pan123Share) Move(ctx context.Context, srcObj, dstDir model.Obj) error {
	// TODO move obj, optional
	return errs.NotSupport
}

func (d *Pan123Share) Rename(ctx context.Context, srcObj model.Obj, newName string) error {
	// TODO rename obj, optional
	return errs.NotSupport
}

func (d *Pan123Share) Copy(ctx context.Context, srcObj, dstDir model.Obj) error {
	// TODO copy obj, optional
	return errs.NotSupport
}

func (d *Pan123Share) Remove(ctx context.Context, obj model.Obj) error {
	// TODO remove obj, optional
	return errs.NotSupport
}

func (d *Pan123Share) Put(ctx context.Context, dstDir model.Obj, stream model.FileStreamer, up driver.UpdateProgress) error {
	// TODO upload file, optional
	return errs.NotSupport
}

//func (d *Pan123Share) Other(ctx context.Context, args model.OtherArgs) (interface{}, error) {
//	return nil, errs.NotSupport
//}

func (d *Pan123Share) APIRateLimit(ctx context.Context, api string) error {
	_ = api // all share operations share one account/public-share gate
	keyMaterial := strings.TrimSpace(d.AccessToken)
	if keyMaterial == "" {
		keyMaterial = "public:" + strings.TrimSpace(d.ShareKey)
	}
	sum := sha256.Sum256([]byte(keyMaterial))
	key := hex.EncodeToString(sum[:])
	value, _ := pan123ShareAccountLimiters.LoadOrStore(key,
		rate.NewLimiter(rate.Every(700*time.Millisecond), 1))
	limiter := value.(*rate.Limiter)

	return limiter.Wait(ctx)
}

var _ driver.Driver = (*Pan123Share)(nil)
