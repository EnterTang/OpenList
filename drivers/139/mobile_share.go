package _139

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/drivers/base"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/go-resty/resty/v2"
)

const (
	mobileShareOutLinkPath       = "/orchestration/personalCloud-rebuild/outlink/v1.0/getOutLink"
	mobileShareDeleteOutLinkPath = "/orchestration/personalCloud-rebuild/outlink/v1.0/delOutLink"
)

var mobileShareOutLinkBaseURL = "https://yun.139.com"

type mobileShareOutLinkResp struct {
	BaseResp
	Data struct {
		GetOutLinkRes struct {
			GetOutLinkResSet []struct {
				LinkID  string `json:"linkID"`
				LinkURL string `json:"linkUrl"`
				Passwd  string `json:"passwd"`
				ObjID   string `json:"objID"`
			} `json:"getOutLinkResSet"`
		} `json:"getOutLinkRes"`
	} `json:"data"`
}

func (d *Yun139) shouldAutoRenameAfterShareRisk(err error) bool {
	if err == nil || !d.AutoRenameOnShareRisk || d.Addition.Type != MetaPersonalNew {
		return false
	}
	return strings.Contains(err.Error(), "个人云未知异常")
}

func (d *Yun139) CreateMobileShare(ctx context.Context, obj model.Obj, args model.MobileShareCreateArgs) (*model.MobileShareLink, error) {
	link, err := d.createMobileShareOnce(ctx, obj, args)
	if err == nil || !d.shouldAutoRenameAfterShareRisk(err) {
		return link, err
	}
	plan, _, planErr := d.buildShareRiskRenamePlan(ctx, obj, shareRiskActualPath(obj))
	if planErr != nil {
		return nil, fmt.Errorf("%w (auto rename planning failed: %v)", err, planErr)
	}
	if len(plan) == 0 {
		return nil, err
	}
	if applyErr := d.applyShareRiskRenamePlan(ctx, plan); applyErr != nil {
		return nil, fmt.Errorf("%w (auto rename apply failed: %v)", err, applyErr)
	}
	retried, retryErr := d.createMobileShareOnce(ctx, obj, args)
	if retryErr != nil {
		return nil, fmt.Errorf("个人云未知异常，已尝试自动重命名后重新创建分享，但仍失败: %w", retryErr)
	}
	return retried, nil
}

func (d *Yun139) createMobileShareOnce(ctx context.Context, obj model.Obj, args model.MobileShareCreateArgs) (*model.MobileShareLink, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if d.Addition.Type != MetaPersonalNew {
		return nil, errs.NotSupport
	}
	if obj == nil {
		return nil, fmt.Errorf("mobile share target is nil")
	}
	targetID := strings.TrimSpace(obj.GetID())
	if targetID == "" {
		return nil, fmt.Errorf("mobile share target id is empty")
	}
	periodUnit := args.PeriodUnit
	if periodUnit <= 0 {
		periodUnit = 1
	}
	coIDList := []string{targetID}
	caIDList := []string{}
	if obj.IsDir() {
		coIDList = []string{}
		caIDList = []string{targetID}
	}

	payload := base.Json{
		"getOutLinkReq": base.Json{
			"subLinkType":   0,
			"encrypt":       1,
			"coIDLst":       coIDList,
			"caIDLst":       caIDList,
			"pubType":       1,
			"dedicatedName": obj.GetName(),
			"periodUnit":    periodUnit,
			"viewerLst":     []string{},
			"extInfo": base.Json{
				"isWatermark":  0,
				"shareChannel": "3001",
			},
			"commonAccountInfo": base.Json{
				"account":     d.getAccount(),
				"accountType": 1,
			},
		},
	}
	var resp mobileShareOutLinkResp
	if _, err := d.mobileSharePost(mobileShareOutLinkPath, payload, &resp); err != nil {
		return nil, err
	}
	if len(resp.Data.GetOutLinkRes.GetOutLinkResSet) == 0 {
		return nil, fmt.Errorf("mobile share response missing outlink result")
	}
	item := resp.Data.GetOutLinkRes.GetOutLinkResSet[0]
	link := &model.MobileShareLink{
		LinkID:      strings.TrimSpace(item.LinkID),
		ShareURL:    strings.TrimSpace(item.LinkURL),
		ExtractCode: strings.TrimSpace(item.Passwd),
		ObjID:       strings.TrimSpace(item.ObjID),
	}
	if link.ShareURL == "" {
		return nil, fmt.Errorf("mobile share response missing share url")
	}
	return link, nil
}

func (d *Yun139) DeleteMobileShare(ctx context.Context, args model.MobileShareDeleteArgs) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.Addition.Type != MetaPersonalNew {
		return errs.NotSupport
	}
	linkIDs := normalizeMobileShareLinkIDs(args.LinkIDs)
	if len(linkIDs) == 0 {
		return fmt.Errorf("mobile share link ids are empty")
	}
	for start := 0; start < len(linkIDs); start += 50 {
		end := start + 50
		if end > len(linkIDs) {
			end = len(linkIDs)
		}
		payload := base.Json{
			"delOutLinkReq": base.Json{
				"linkIDs": linkIDs[start:end],
				"commonAccountInfo": base.Json{
					"account":     d.getAccount(),
					"accountType": 1,
				},
			},
		}
		var resp BaseResp
		if _, err := d.mobileSharePost(mobileShareDeleteOutLinkPath, payload, &resp); err != nil {
			return err
		}
	}
	return nil
}

func normalizeMobileShareLinkIDs(linkIDs []string) []string {
	seen := make(map[string]struct{}, len(linkIDs))
	normalized := make([]string, 0, len(linkIDs))
	for _, linkID := range linkIDs {
		linkID = strings.TrimSpace(linkID)
		if linkID == "" {
			continue
		}
		if _, ok := seen[linkID]; ok {
			continue
		}
		seen[linkID] = struct{}{}
		normalized = append(normalized, linkID)
	}
	return normalized
}

func (d *Yun139) mobileSharePost(pathname string, data interface{}, resp interface{}) ([]byte, error) {
	headers := map[string]string{
		"Mcloud-Route": "001",
	}
	if cookieHeader := d.getCookieHeader(); cookieHeader != "" {
		headers["Cookie"] = cookieHeader
	}
	return d.request(mobileShareOutLinkBaseURL+pathname, http.MethodPost, func(req *resty.Request) {
		req.SetBody(data)
		req.SetHeaders(headers)
	}, resp)
}

func (d *Yun139) getCookieHeader() string {
	if d.ref != nil {
		return d.ref.getCookieHeader()
	}
	return strings.TrimSpace(d.CookieHeader)
}
