package _115_cd2

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	open115 "github.com/OpenListTeam/OpenList/v4/drivers/115_open"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/stream"
)

type CD2 struct {
	model.Storage
	Addition

	delegate       *open115.Open115
	throttler      *requestThrottler
	apiMu          sync.Mutex
	authHTTPClient *http.Client
	apiHTTPClient  *http.Client
	refreshStateMu sync.Mutex
	refreshCancel  context.CancelFunc
	refreshDone    chan struct{}
	refreshRetryAt time.Time
}

func (d *CD2) Config() driver.Config {
	return config
}

func (d *CD2) GetAddition() driver.Additional {
	return &d.Addition
}

func (d *CD2) Init(ctx context.Context) error {
	d.stopTokenRefreshTask()
	if err := d.ensureAuthentication(ctx); err != nil {
		return err
	}
	d.throttler = newRequestThrottler(d.Addition.LimitRate)
	d.delegate = &open115.Open115{}
	d.delegate.SetStorage(d.Storage)
	d.delegate.SetSkipUserInfoAtInit(true)
	apiClient := d.apiHTTPClient
	if apiClient == nil {
		apiClient = newCD2HTTPClient(defaultCD2AuthConfig().refreshEndpoint, nil)
	}
	d.delegate.SetHTTPClient(apiClient)
	d.delegate.SetRequestWaitFunc(func(ctx context.Context, class open115.RequestClass) error {
		return d.throttler.wait(ctx, class)
	})
	d.delegate.SetRefreshTokenHandler(func(accessToken, refreshToken string) {
		d.syncRefreshedTokens(accessToken, refreshToken)
		d.persistAuthenticationState()
	})
	addition, configuredRate := delegateAdditionForInit(d.Addition)
	// The wrapper owns request throttling; disable the delegate's single bucket.
	d.delegate.Addition = addition

	d.apiMu.Lock()
	err := d.delegate.Init(ctx)
	d.apiMu.Unlock()
	if err != nil {
		d.delegate = nil
		return err
	}
	// The delegate did not create its limiter when the rate was zero. Restore
	// the configured value so token refresh persistence keeps the user's setting.
	d.delegate.Addition.LimitRate = configuredRate
	d.startTokenRefreshTask()
	return nil
}

func (d *CD2) syncRefreshedTokens(accessToken, refreshToken string) {
	d.Addition.AccessToken = accessToken
	d.Addition.RefreshToken = refreshToken
	// The SDK callback does not expose expires_in. The explicit proactive
	// refresh path restores it after the SDK call; reactive SDK refreshes keep
	// the expiry unknown instead of reusing a stale timestamp.
	d.Addition.AccessTokenExpiresAt = 0
	d.refreshRetryAt = time.Time{}
	if d.delegate != nil {
		d.delegate.Addition.LimitRate = d.Addition.LimitRate
	}
}

func delegateAdditionForInit(addition Addition) (open115.Addition, float64) {
	delegateAddition := open115.Addition{
		RootID:         addition.RootID,
		OrderBy:        addition.OrderBy,
		OrderDirection: addition.OrderDirection,
		LimitRate:      addition.LimitRate,
		PageSize:       addition.PageSize,
		AccessToken:    addition.AccessToken,
		RefreshToken:   addition.RefreshToken,
		MembershipTier: addition.MembershipTier,
	}
	configuredRate := delegateAddition.LimitRate
	delegateAddition.LimitRate = 0
	return delegateAddition, configuredRate
}

func (d *CD2) Drop(ctx context.Context) error {
	d.stopTokenRefreshTask()
	d.apiMu.Lock()
	defer d.apiMu.Unlock()
	if d.delegate == nil {
		return nil
	}
	err := d.delegate.Drop(ctx)
	d.delegate = nil
	return err
}

func (d *CD2) call(ctx context.Context, fn func(*open115.Open115) error) error {
	d.apiMu.Lock()
	defer d.apiMu.Unlock()
	if d.delegate == nil {
		return errs.StorageNotInit
	}
	if err := d.refreshAccessTokenIfDueLocked(ctx); err != nil {
		return err
	}
	return fn(d.delegate)
}

func (d *CD2) List(ctx context.Context, dir model.Obj, args model.ListArgs) ([]model.Obj, error) {
	var result []model.Obj
	err := d.call(ctx, func(delegate *open115.Open115) error {
		var err error
		result, err = delegate.List(ctx, dir, args)
		return err
	})
	return result, err
}

func (d *CD2) Link(ctx context.Context, file model.Obj, args model.LinkArgs) (*model.Link, error) {
	var link *model.Link
	err := d.call(ctx, func(delegate *open115.Open115) error {
		var err error
		link, err = delegate.Link(ctx, file, args)
		return err
	})
	if err != nil || link == nil || link.URL == "" || file == nil || file.GetSize() <= 0 {
		return link, err
	}
	rangeReader, err := stream.GetRangeReaderFromLink(file.GetSize(), link)
	if err != nil {
		return nil, fmt.Errorf("create CD2 download reader: %w", err)
	}
	link.RangeReader = &throttledRangeReader{
		upstream: rangeReader,
		waiter:   d.throttler.downloadRequest,
	}
	return link, nil
}

func (d *CD2) Get(ctx context.Context, path string) (model.Obj, error) {
	var result model.Obj
	err := d.call(ctx, func(delegate *open115.Open115) error {
		var err error
		result, err = delegate.Get(ctx, path)
		return err
	})
	return result, err
}

func (d *CD2) MakeDir(ctx context.Context, parentDir model.Obj, dirName string) (model.Obj, error) {
	var result model.Obj
	err := d.call(ctx, func(delegate *open115.Open115) error {
		var err error
		result, err = delegate.MakeDir(ctx, parentDir, dirName)
		return err
	})
	return result, err
}

func (d *CD2) Move(ctx context.Context, srcObj, dstDir model.Obj) error {
	return d.call(ctx, func(delegate *open115.Open115) error {
		return delegate.Move(ctx, srcObj, dstDir)
	})
}

func (d *CD2) Rename(ctx context.Context, srcObj model.Obj, newName string) (model.Obj, error) {
	var result model.Obj
	err := d.call(ctx, func(delegate *open115.Open115) error {
		var err error
		result, err = delegate.Rename(ctx, srcObj, newName)
		return err
	})
	return result, err
}

func (d *CD2) Copy(ctx context.Context, srcObj, dstDir model.Obj) error {
	return d.call(ctx, func(delegate *open115.Open115) error {
		return delegate.Copy(ctx, srcObj, dstDir)
	})
}

func (d *CD2) Remove(ctx context.Context, obj model.Obj) error {
	return d.call(ctx, func(delegate *open115.Open115) error {
		return delegate.Remove(ctx, obj)
	})
}

func (d *CD2) Put(ctx context.Context, dstDir model.Obj, file model.FileStreamer, up driver.UpdateProgress) error {
	return d.call(ctx, func(delegate *open115.Open115) error {
		return delegate.Put(ctx, dstDir, file, up)
	})
}

func (d *CD2) GetDetails(ctx context.Context) (*model.StorageDetails, error) {
	var result *model.StorageDetails
	err := d.call(ctx, func(delegate *open115.Open115) error {
		var err error
		result, err = delegate.GetDetails(ctx)
		return err
	})
	return result, err
}

func (d *CD2) ClusterMembershipTier() string {
	d.apiMu.Lock()
	defer d.apiMu.Unlock()
	if d.delegate != nil {
		return d.delegate.ClusterMembershipTier()
	}
	return d.Addition.MembershipTier
}

var _ driver.Driver = (*CD2)(nil)
var _ driver.Getter = (*CD2)(nil)
var _ driver.MkdirResult = (*CD2)(nil)
var _ driver.Move = (*CD2)(nil)
var _ driver.RenameResult = (*CD2)(nil)
var _ driver.Copy = (*CD2)(nil)
var _ driver.Remove = (*CD2)(nil)
var _ driver.Put = (*CD2)(nil)
var _ driver.WithDetails = (*CD2)(nil)
