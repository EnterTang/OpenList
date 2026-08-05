package _115_cd2

import (
	"context"

	open115 "github.com/OpenListTeam/OpenList/v4/drivers/115_open"
	"golang.org/x/time/rate"
)

type requestClass = open115.RequestClass

const (
	requestFileList    = open115.RequestFileList
	requestDownloadURL = open115.RequestDownloadURL
	requestRESTAPI     = open115.RequestRESTAPI
	requestDownload    = open115.RequestDownload
)

type requestWaiter interface {
	Wait(context.Context) error
}

type requestThrottler struct {
	fileList        requestWaiter
	downloadURL     requestWaiter
	restAPI         requestWaiter
	downloadRequest requestWaiter
}

func newRequestThrottler(limit float64) *requestThrottler {
	if limit <= 0 {
		return &requestThrottler{}
	}
	newLimiter := func() requestWaiter {
		return rate.NewLimiter(rate.Limit(limit), 1)
	}
	return &requestThrottler{
		fileList:        newLimiter(),
		downloadURL:     newLimiter(),
		restAPI:         newLimiter(),
		downloadRequest: newLimiter(),
	}
}

func (t *requestThrottler) wait(ctx context.Context, class requestClass) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if t == nil {
		return nil
	}
	var waiter requestWaiter
	switch class {
	case requestFileList:
		waiter = t.fileList
	case requestDownloadURL:
		waiter = t.downloadURL
	case requestRESTAPI:
		waiter = t.restAPI
	case requestDownload:
		waiter = t.downloadRequest
	}
	if waiter == nil {
		return nil
	}
	return waiter.Wait(ctx)
}
