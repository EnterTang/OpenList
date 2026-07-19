package _123

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"github.com/OpenListTeam/OpenList/v4/drivers/base"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/stream"
	"github.com/OpenListTeam/OpenList/v4/pkg/http_range"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/go-resty/resty/v2"
	log "github.com/sirupsen/logrus"
)

type Pan123 struct {
	model.Storage
	Addition
	apiRateLimit      sync.Map
	membershipMu      sync.RWMutex
	runtimeMembership model.MembershipDetails
}

func (d *Pan123) Config() driver.Config {
	return config
}

func (d *Pan123) GetAddition() driver.Additional {
	return &d.Addition
}

func (d *Pan123) Init(ctx context.Context) error {
	var userInfo UserInfoResp
	_, err := d.Request(UserInfo, http.MethodGet, func(req *resty.Request) {
		req.SetHeader("platform", "web")
		req.SetContext(ctx)
	}, &userInfo)
	if err == nil {
		d.setRuntimeMembership(membershipDetailsFromUserInfo(&userInfo))
	}
	return err
}

func membershipDetailsFromUserInfo(userInfo *UserInfoResp) model.MembershipDetails {
	details := model.MembershipDetails{
		Tier:       "ordinary",
		Status:     "inactive",
		ExpireDate: userInfo.Data.VipExpire,
	}
	if userInfo.Data.Vip {
		details.Tier = "vip"
		details.Status = "active"
		if userInfo.Data.VipLevel == 2 {
			details.Tier = "svip"
		}
	}
	return details
}

func (d *Pan123) setRuntimeMembership(details model.MembershipDetails) {
	d.membershipMu.Lock()
	d.runtimeMembership = details
	d.membershipMu.Unlock()
}

func (d *Pan123) ClusterMembershipDetails() model.MembershipDetails {
	d.membershipMu.RLock()
	details := d.runtimeMembership
	d.membershipMu.RUnlock()
	if configured := strings.ToLower(strings.TrimSpace(d.MembershipTier)); configured != "" && configured != "unknown" {
		details.Tier = configured
	}
	return details
}

func (d *Pan123) ClusterMembershipTier() string {
	return d.ClusterMembershipDetails().Tier
}

func (d *Pan123) Drop(ctx context.Context) error {
	_, _ = d.Request(Logout, http.MethodPost, func(req *resty.Request) {
		req.SetBody(base.Json{})
	}, nil)
	return nil
}

func (d *Pan123) List(ctx context.Context, dir model.Obj, args model.ListArgs) ([]model.Obj, error) {
	files, err := d.getFiles(ctx, dir.GetID(), dir.GetName())
	if err != nil {
		return nil, err
	}
	return utils.SliceConvert(files, func(src File) (model.Obj, error) {
		return src, nil
	})
}

func (d *Pan123) Link(ctx context.Context, file model.Obj, args model.LinkArgs) (*model.Link, error) {
	f, ok := file.(File)
	if !ok {
		return nil, fmt.Errorf("can't convert obj")
	}
	initial, err := d.resolveDownload(ctx, f)
	if err != nil {
		return nil, err
	}
	rangeReader := stream.RangeReaderFunc(func(readCtx context.Context, requested http_range.Range) (io.ReadCloser, error) {
		return newPan123DownloadReader(
			readCtx,
			f.Size,
			requested,
			initial,
			func(refreshCtx context.Context) (pan123ResolvedDownload, error) {
				return d.resolveDownload(refreshCtx, f)
			},
			pan123FallbackConfig{},
		)
	})
	return &model.Link{
		URL: initial.URL, Header: initial.Header, RangeReader: rangeReader, ContentLength: f.Size,
	}, nil
}

func (d *Pan123) resolveDownload(ctx context.Context, file File) (pan123ResolvedDownload, error) {
	data := base.Json{
		"driveId":   0,
		"etag":      file.Etag,
		"fileId":    file.FileId,
		"fileName":  file.FileName,
		"s3keyFlag": file.S3KeyFlag,
		"size":      file.Size,
		"type":      file.Type,
	}
	resp, err := d.Request(DownloadInfo, http.MethodPost, func(req *resty.Request) {
		req.SetBody(data).SetContext(ctx)
	}, nil)
	if err != nil {
		return pan123ResolvedDownload{}, err
	}
	downloadURL := utils.Json.Get(resp, "data", "DownloadUrl").ToString()
	originalURL, err := url.Parse(downloadURL)
	if err != nil {
		return pan123ResolvedDownload{}, err
	}
	requestURL := originalURL.String()
	if params := originalURL.Query().Get("params"); params != "" {
		decoded, decodeErr := base64.StdEncoding.DecodeString(params)
		if decodeErr != nil {
			return pan123ResolvedDownload{}, fmt.Errorf("decode 123pan download parameters: %w", decodeErr)
		}
		parsed, parseErr := url.Parse(string(decoded))
		if parseErr != nil {
			return pan123ResolvedDownload{}, parseErr
		}
		requestURL = parsed.String()
	}
	res, err := base.NoRedirectClient.R().
		SetContext(ctx).
		SetHeader("Referer", "https://yun.123pan.com/").
		Get(requestURL)
	if err != nil {
		return pan123ResolvedDownload{}, err
	}
	directURL := requestURL
	if res.StatusCode() == http.StatusFound {
		directURL = res.Header().Get("location")
	} else if res.StatusCode() < http.StatusMultipleChoices {
		directURL = utils.Json.Get(res.Body(), "data", "redirect_url").ToString()
	}
	if directURL == "" {
		return pan123ResolvedDownload{}, errors.New("123pan download URL resolution returned an empty redirect")
	}
	return pan123ResolvedDownload{
		URL: directURL,
		Header: http.Header{
			"Referer": []string{fmt.Sprintf("%s://%s/", originalURL.Scheme, originalURL.Host)},
		},
	}, nil
}

func (d *Pan123) MakeDir(ctx context.Context, parentDir model.Obj, dirName string) error {
	data := base.Json{
		"driveId":      0,
		"etag":         "",
		"fileName":     dirName,
		"parentFileId": parentDir.GetID(),
		"size":         0,
		"type":         1,
	}
	_, err := d.Request(Mkdir, http.MethodPost, func(req *resty.Request) {
		req.SetBody(data)
	}, nil)
	return err
}

func (d *Pan123) Move(ctx context.Context, srcObj, dstDir model.Obj) error {
	data := base.Json{
		"fileIdList":   []base.Json{{"FileId": srcObj.GetID()}},
		"parentFileId": dstDir.GetID(),
	}
	_, err := d.Request(Move, http.MethodPost, func(req *resty.Request) {
		req.SetBody(data)
	}, nil)
	return err
}

func (d *Pan123) Rename(ctx context.Context, srcObj model.Obj, newName string) error {
	data := base.Json{
		"driveId":  0,
		"fileId":   srcObj.GetID(),
		"fileName": newName,
	}
	_, err := d.Request(Rename, http.MethodPost, func(req *resty.Request) {
		req.SetBody(data)
	}, nil)
	return err
}

func (d *Pan123) Copy(ctx context.Context, srcObj, dstDir model.Obj) error {
	return errs.NotSupport
}

func (d *Pan123) Remove(ctx context.Context, obj model.Obj) error {
	if f, ok := obj.(File); ok {
		data := base.Json{
			"driveId":           0,
			"operation":         true,
			"fileTrashInfoList": []File{f},
		}
		_, err := d.Request(Trash, http.MethodPost, func(req *resty.Request) {
			req.SetBody(data)
		}, nil)
		return err
	} else {
		return fmt.Errorf("can't convert obj")
	}
}

func (d *Pan123) Put(ctx context.Context, dstDir model.Obj, file model.FileStreamer, up driver.UpdateProgress) error {
	etag := file.GetHash().GetHash(utils.MD5)
	var err error
	if len(etag) < utils.MD5.Width {
		_, etag, err = stream.CacheFullAndHash(file, &up, utils.MD5)
		if err != nil {
			return err
		}
	}
	data := base.Json{
		"driveId":      0,
		"duplicate":    2, // 2->覆盖 1->重命名 0->默认
		"etag":         strings.ToLower(etag),
		"fileName":     file.GetName(),
		"parentFileId": dstDir.GetID(),
		"size":         file.GetSize(),
		"type":         0,
	}
	var resp UploadResp
	res, err := d.Request(UploadRequest, http.MethodPost, func(req *resty.Request) {
		req.SetBody(data).SetContext(ctx)
	}, &resp)
	if err != nil {
		return err
	}
	log.Debugln("upload request res: ", string(res))
	if resp.Data.Reuse || resp.Data.Key == "" {
		return nil
	}
	if resp.Data.AccessKeyId == "" || resp.Data.SecretAccessKey == "" || resp.Data.SessionToken == "" {
		err = d.newUpload(ctx, &resp, file, up)
		return err
	} else {
		cfg := &aws.Config{
			Credentials:      credentials.NewStaticCredentials(resp.Data.AccessKeyId, resp.Data.SecretAccessKey, resp.Data.SessionToken),
			Region:           aws.String("123pan"),
			Endpoint:         aws.String(resp.Data.EndPoint),
			S3ForcePathStyle: aws.Bool(true),
		}
		s, err := session.NewSession(cfg)
		if err != nil {
			return err
		}
		uploader := s3manager.NewUploader(s)
		if file.GetSize() > s3manager.MaxUploadParts*s3manager.DefaultUploadPartSize {
			uploader.PartSize = file.GetSize() / (s3manager.MaxUploadParts - 1)
		}
		input := &s3manager.UploadInput{
			Bucket: &resp.Data.Bucket,
			Key:    &resp.Data.Key,
			Body: driver.NewLimitedUploadStream(ctx, &driver.ReaderUpdatingProgress{
				Reader:         file,
				UpdateProgress: up,
			}),
		}
		_, err = uploader.UploadWithContext(ctx, input)
		if err != nil {
			return err
		}
	}
	_, err = d.Request(UploadComplete, http.MethodPost, func(req *resty.Request) {
		req.SetBody(base.Json{
			"fileId": resp.Data.FileId,
		}).SetContext(ctx)
	}, nil)
	return err
}

func (d *Pan123) APIRateLimit(ctx context.Context, api string) error {
	value, _ := d.apiRateLimit.LoadOrStore(api,
		rate.NewLimiter(rate.Every(700*time.Millisecond), 1))
	limiter := value.(*rate.Limiter)

	return limiter.Wait(ctx)
}

func (d *Pan123) GetDetails(ctx context.Context) (*model.StorageDetails, error) {
	userInfo, err := d.getUserInfo(ctx)
	if err != nil {
		return nil, err
	}
	membership := membershipDetailsFromUserInfo(userInfo)
	d.setRuntimeMembership(membership)
	return &model.StorageDetails{
		DiskUsage: model.DiskUsage{
			TotalSpace: userInfo.Data.SpacePermanent + userInfo.Data.SpaceTemp,
			UsedSpace:  userInfo.Data.SpaceUsed,
		},
		Membership: &membership,
	}, nil
}

var _ driver.Driver = (*Pan123)(nil)
