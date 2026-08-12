package _139

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/drivers/base"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	log "github.com/sirupsen/logrus"
)

var mobile139MembershipEndpoint = "https://yun.139.com/orchestration/group-rebuild/member/v1.0/queryUserBenefits"

const mobile139MembershipQueryTimeout = 30 * time.Second

type queryUserBenefitsRequest struct {
	IsNeedBenefit     int                                `json:"isNeedBenefit"`
	CommonAccountInfo queryUserBenefitsCommonAccountInfo `json:"commonAccountInfo"`
}

type queryUserBenefitsCommonAccountInfo struct {
	UserDomainID string `json:"userDomainId"`
	AccountType  int    `json:"accountType"`
}

type queryUserBenefitsResponse struct {
	Success bool                  `json:"success"`
	Code    string                `json:"code"`
	Message string                `json:"message"`
	Data    queryUserBenefitsData `json:"data"`
}

type queryUserBenefitsData struct {
	Result            queryUserBenefitsResult   `json:"result"`
	UserSubMemberList []queryUserBenefitsMember `json:"userSubMemberList"`
}

type queryUserBenefitsResult struct {
	ResultCode string `json:"resultCode"`
	ResultDesc string `json:"resultDesc"`
}

type queryUserBenefitsMember struct {
	MemberLevel     mobile139MembershipLevel `json:"memberLevel"`
	MemberLvName    string                   `json:"memberLvName"`
	MemberLevelName string                   `json:"memberLevelName"`
}

type mobile139MembershipLevel string

func (level *mobile139MembershipLevel) UnmarshalJSON(data []byte) error {
	raw := strings.TrimSpace(string(data))
	if raw == "" || raw == "null" {
		*level = ""
		return nil
	}
	if strings.HasPrefix(raw, `"`) {
		var value string
		if err := json.Unmarshal(data, &value); err != nil {
			return err
		}
		*level = mobile139MembershipLevel(strings.TrimSpace(value))
		return nil
	}
	var number json.Number
	if err := json.Unmarshal(data, &number); err != nil {
		return fmt.Errorf("decode member level: %w", err)
	}
	if _, err := number.Int64(); err != nil {
		return fmt.Errorf("decode member level: %w", err)
	}
	*level = mobile139MembershipLevel(number.String())
	return nil
}

func (d *Yun139) ClusterMembershipTier() string {
	configured := strings.ToLower(strings.TrimSpace(d.Addition.MembershipTier))
	if configured != "" && configured != "unknown" {
		if normalized := normalize139MembershipTier(configured, ""); normalized != "" {
			return normalized
		}
		return configured
	}
	if d.ref != nil {
		return d.ref.ClusterMembershipTier()
	}
	return d.runtimeMembershipTier
}

func (d *Yun139) updateRuntimeMembershipTier(ctx context.Context) {
	configured := strings.ToLower(strings.TrimSpace(d.Addition.MembershipTier))
	if configured != "" && configured != "unknown" {
		return
	}
	queryCtx, cancel := context.WithTimeout(ctx, mobile139MembershipQueryTimeout)
	defer cancel()
	tier, err := d.queryMembershipTier(queryCtx)
	if err != nil {
		log.Warnf("139yun: failed to query membership tier: %v", err)
		return
	}
	d.runtimeMembershipTier = tier
}

func (d *Yun139) queryMembershipTier(ctx context.Context) (string, error) {
	userDomainID := strings.TrimSpace(d.UserDomainID)
	if userDomainID == "" {
		userDomainID = d.getAccount()
	}
	if userDomainID == "" {
		return "", fmt.Errorf("user domain id and account are empty")
	}

	deviceID := d.personalUploadDeviceID()
	req := base.RestyClient.R().
		SetContext(ctx).
		SetHeaders(map[string]string{
			"Accept":               "application/json, text/plain, */*",
			"Accept-Language":      "zh-CN,zh;q=0.9",
			"Content-Type":         "application/json;charset=UTF-8",
			"Origin":               "https://yun.139.com",
			"Referer":              "https://yun.139.com/w/",
			"User-Agent":           "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
			"x-DeviceInfo":         "||9|7.17.4|chrome|145.0.0.0|" + deviceID + "||macos 10.15.7||zh-CN|||",
			"x-inner-ntwk":         "2",
			"x-m4c-caller":         "PC",
			"x-m4c-src":            "10002",
			"x-SvcType":            "1",
			"X-Yun-Api-Version":    "v1",
			"X-Yun-App-Channel":    "10000034",
			"X-Yun-Channel-Source": "10000034",
			"X-Yun-Client-Info":    "||9|7.17.4|chrome|145.0.0.0|" + deviceID + "||macos 10.15.7||zh-CN|||dW5kZWZpbmVk||",
			"X-Yun-Module-Type":    "100",
			"X-Yun-Svc-Type":       "1",
		}).
		SetBody(queryUserBenefitsRequest{
			IsNeedBenefit: 0,
			CommonAccountInfo: queryUserBenefitsCommonAccountInfo{
				UserDomainID: userDomainID,
				AccountType:  1,
			},
		})
	if authorization := strings.TrimSpace(d.getAuthorization()); authorization != "" {
		req.SetHeader("Authorization", "Basic "+authorization)
	}
	if cookieHeader := d.getCookieHeader(); cookieHeader != "" {
		req.SetHeader("Cookie", cookieHeader)
	}

	res, err := req.Post(mobile139MembershipEndpoint)
	if err != nil {
		return "", err
	}
	if res.IsError() {
		return "", fmt.Errorf("query membership returned HTTP %d", res.StatusCode())
	}
	var response queryUserBenefitsResponse
	if err := utils.Json.Unmarshal(res.Body(), &response); err != nil {
		return "", fmt.Errorf("decode membership response: %w", err)
	}
	if !response.Success {
		return "", fmt.Errorf("query membership failed: %s %s", response.Code, response.Message)
	}
	if code := strings.TrimSpace(response.Data.Result.ResultCode); code != "" && strings.Trim(code, "0") != "" {
		return "", fmt.Errorf("query membership failed: %s %s", code, response.Data.Result.ResultDesc)
	}

	bestTier := ""
	bestRank := 0
	for _, member := range response.Data.UserSubMemberList {
		name := firstNonEmpty139(member.MemberLvName, member.MemberLevelName)
		tier := normalize139MembershipTier(string(member.MemberLevel), name)
		if rank := mobile139MembershipRank(tier); rank > bestRank {
			bestTier = tier
			bestRank = rank
		}
	}
	if bestTier == "" {
		return "", fmt.Errorf("membership response does not contain a recognized tier")
	}
	return bestTier, nil
}

func normalize139MembershipTier(level, name string) string {
	level = strings.ToLower(strings.TrimSpace(level))
	name = strings.ToLower(strings.TrimSpace(name))
	text := level + " " + name
	switch {
	case level == "3", strings.Contains(text, "钻石"), strings.Contains(text, "diamond"):
		return "diamond"
	case level == "2", strings.Contains(text, "黄金"), strings.Contains(text, "gold"):
		return "gold"
	case level == "1", strings.Contains(text, "白银"), strings.Contains(text, "silver"):
		return "silver"
	case level == "0", strings.Contains(text, "普通"), strings.Contains(text, "ordinary"), strings.Contains(text, "normal"), strings.Contains(text, "standard"), strings.Contains(text, "free"), strings.Contains(text, "非会员"):
		return "ordinary"
	default:
		return ""
	}
}

func mobile139MembershipRank(tier string) int {
	switch tier {
	case "diamond":
		return 4
	case "gold":
		return 3
	case "silver":
		return 2
	case "ordinary":
		return 1
	default:
		return 0
	}
}
