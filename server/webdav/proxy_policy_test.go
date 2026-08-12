package webdav

import (
	"testing"

	"github.com/OpenListTeam/OpenList/v4/drivers/virtual"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestOnlyProxyStorageDoesNotUseWebDAVDirectRedirect(t *testing.T) {
	storage := &virtual.Virtual{Storage: model.Storage{Proxy: model.Proxy{WebdavPolicy: "302_redirect"}}}
	if shouldRedirectToDirectLink(storage) {
		t.Fatal("OnlyProxy storage must not redirect WebDAV requests to direct links")
	}
}

func TestProxyCapableStorageMayUseWebDAVDirectRedirect(t *testing.T) {
	storage := &webDAVRedirectDriver{Virtual: virtual.Virtual{Storage: model.Storage{Proxy: model.Proxy{WebdavPolicy: "302_redirect"}}}}
	if !shouldRedirectToDirectLink(storage) {
		t.Fatal("proxy-capable storage should preserve explicit WebDAV direct redirect")
	}
}

type webDAVRedirectDriver struct {
	virtual.Virtual
}

func (d *webDAVRedirectDriver) Config() driver.Config {
	return driver.Config{Name: "test", OnlyProxy: false}
}
