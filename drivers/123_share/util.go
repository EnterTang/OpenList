package _123Share

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/drivers/base"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/go-resty/resty/v2"
	jsoniter "github.com/json-iterator/go"
)

const (
	Api          = "https://yun.123pan.com/api"
	AApi         = "https://yun.123pan.com/a/api"
	BApi         = "https://yun.123pan.com/b/api"
	MainApi      = BApi
	FileList     = MainApi + "/share/get"
	DownloadInfo = MainApi + "/share/download/info"
	//AuthKeySalt      = "8-8D$sL8gPjom7bk#cY"
)

func signPath(path string, os string, version string) (k string, v string) {
	table := []byte{'a', 'd', 'e', 'f', 'g', 'h', 'l', 'm', 'y', 'i', 'j', 'n', 'o', 'p', 'k', 'q', 'r', 's', 't', 'u', 'b', 'c', 'v', 'w', 's', 'z'}
	random := fmt.Sprintf("%.f", math.Round(1e7*rand.Float64()))
	now := time.Now().In(time.FixedZone("CST", 8*3600))
	timestamp := fmt.Sprint(now.Unix())
	nowStr := []byte(now.Format("200601021504"))
	for i := 0; i < len(nowStr); i++ {
		nowStr[i] = table[nowStr[i]-48]
	}
	timeSign := fmt.Sprint(crc32.ChecksumIEEE(nowStr))
	data := strings.Join([]string{timestamp, random, path, os, version, timeSign}, "|")
	dataSign := fmt.Sprint(crc32.ChecksumIEEE([]byte(data)))
	return timeSign, strings.Join([]string{timestamp, random, dataSign}, "-")
}

func GetApi(rawUrl string) string {
	u, _ := url.Parse(rawUrl)
	query := u.Query()
	query.Add(signPath(u.Path, "web", "3"))
	u.RawQuery = query.Encode()
	return u.String()
}

func (d *Pan123Share) request(ctx context.Context, url string, method string, callback base.ReqCallback, resp interface{}) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	const maxAttempts = 3
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err := d.APIRateLimit(ctx, url); err != nil {
			return nil, err
		}
		var (
			body   []byte
			status = http.StatusOK
			err    error
		)
		if d.ref != nil {
			body, err = d.ref.Request(url, method, callback, resp)
		} else {
			req := base.RestyClient.R()
			req.SetContext(ctx)
			req.SetHeaders(map[string]string{
				"origin":        "https://yun.123pan.com",
				"referer":       "https://yun.123pan.com/",
				"authorization": "Bearer " + d.AccessToken,
				"user-agent":    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) openlist-client",
				"platform":      "web",
				"app-version":   "3",
			})
			if callback != nil {
				callback(req)
			}
			if resp != nil {
				req.SetResult(resp)
			}
			res, requestErr := req.Execute(method, GetApi(url))
			err = requestErr
			if res != nil {
				body = res.Body()
				status = res.StatusCode()
			}
		}
		if err != nil {
			if attempt+1 < maxAttempts {
				if err := wait123ShareRetry(ctx, attempt); err != nil {
					return nil, err
				}
				continue
			}
			return nil, err
		}
		code := utils.Json.Get(body, "code").ToInt()
		message := jsoniter.Get(body, "message").ToString()
		if !retryable123ShareResponse(status, code, message) || attempt+1 == maxAttempts {
			if status < http.StatusOK || status >= http.StatusMultipleChoices {
				return nil, fmt.Errorf("123pan share request failed with HTTP %d", status)
			}
			if code != 0 {
				return nil, errors.New(message)
			}
			return body, nil
		}
		if err := wait123ShareRetry(ctx, attempt); err != nil {
			return nil, err
		}
	}
	return nil, errors.New("123pan share retry limit exceeded")
}

func (d *Pan123Share) getFiles(ctx context.Context, parentId string) ([]File, error) {
	page := 1
	res := make([]File, 0)
	for {
		var resp Files
		query := map[string]string{
			"limit":          "100",
			"next":           "0",
			"orderBy":        "file_id",
			"orderDirection": "desc",
			"parentFileId":   parentId,
			"Page":           strconv.Itoa(page),
			"shareKey":       d.ShareKey,
			"SharePwd":       d.SharePwd,
		}
		_, err := d.request(ctx, FileList, http.MethodGet, func(req *resty.Request) {
			req.SetQueryParams(query)
		}, &resp)
		if err != nil {
			return nil, err
		}
		page++
		res = append(res, resp.Data.InfoList...)
		if len(resp.Data.InfoList) == 0 || resp.Data.Next == "-1" {
			break
		}
	}
	return res, nil
}

func retryable123ShareResponse(status, code int, message string) bool {
	if status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status >= http.StatusInternalServerError {
		return true
	}
	message = strings.ToLower(strings.TrimSpace(message))
	return code == http.StatusRequestTimeout || code == http.StatusTooManyRequests || strings.Contains(message, "rate") || strings.Contains(message, "频繁") || strings.Contains(message, "限流")
}

func wait123ShareRetry(ctx context.Context, attempt int) error {
	delay := 500 * time.Millisecond * time.Duration(1<<attempt)
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// do others that not defined in Driver interface
