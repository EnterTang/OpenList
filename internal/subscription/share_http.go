package subscription

import (
	"github.com/OpenListTeam/OpenList/v4/drivers/base"
	"github.com/go-resty/resty/v2"
)

func newShareHTTPClient() *resty.Client {
	return resty.New().
		SetHeader("user-agent", base.UserAgent).
		SetTimeout(base.DefaultTimeout)
}
