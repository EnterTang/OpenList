package handles

import (
	"errors"
	"sort"
	"strconv"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/subscription"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/OpenListTeam/OpenList/v4/server/common"
	"github.com/gin-gonic/gin"
)

type listSubscriptionsReq struct {
	model.PageReq
	Keyword    string `form:"keyword" json:"keyword"`
	SourceType string `form:"source_type" json:"source_type"`
	Active     string `form:"active" json:"active"`
}

type listSubscriptionRunsReq struct {
	model.PageReq
	SubscriptionID uint   `form:"subscription_id" json:"subscription_id"`
	Status         string `form:"status" json:"status"`
}

func ListSubscriptions(c *gin.Context) {
	var req listSubscriptionsReq
	if err := c.ShouldBind(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	req.Validate()
	var active *bool
	if req.Active != "" {
		value, err := strconv.ParseBool(req.Active)
		if err != nil {
			common.ErrorResp(c, err, 400)
			return
		}
		active = &value
	}
	items, total, err := db.ListSubscriptions(db.SubscriptionFilter{
		Keyword:    req.Keyword,
		SourceType: strings.TrimSpace(req.SourceType),
		Active:     active,
		Page:       req.Page,
		PerPage:    req.PerPage,
	})
	if err != nil {
		common.ErrorResp(c, err, 500)
		return
	}
	common.SuccessResp(c, common.PageResp{Content: items, Total: total})
}

func GetSubscription(c *gin.Context) {
	id, err := strconv.ParseUint(c.Query("id"), 10, 64)
	if err != nil || id == 0 {
		common.ErrorStrResp(c, "id is required", 400)
		return
	}
	item, err := db.GetSubscriptionByID(uint(id))
	if err != nil {
		common.ErrorResp(c, err, 404)
		return
	}
	items, err := db.ListSubscriptionItems(item.ID)
	if err != nil {
		common.ErrorResp(c, err, 500)
		return
	}
	common.SuccessResp(c, gin.H{"subscription": item, "items": filterDisplayedSubscriptionItems(items)})
}

func CreateSubscription(c *gin.Context) {
	var req model.Subscription
	if err := c.ShouldBindJSON(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	if err := subscription.ApplyDefaults(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	normalizeSubscription(&req)
	if err := validateSubscriptionEpisodeRange(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	if req.Name == "" {
		common.ErrorStrResp(c, "name is required", 400)
		return
	}
	if req.TMDBName == "" {
		common.ErrorStrResp(c, "tmdb_name is required", 400)
		return
	}
	if err := db.CreateSubscription(&req); err != nil {
		common.ErrorResp(c, err, 500)
		return
	}
	common.SuccessResp(c, req)
}

func UpdateSubscription(c *gin.Context) {
	var req model.Subscription
	if err := c.ShouldBindJSON(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	if req.ID == 0 {
		common.ErrorStrResp(c, "id is required", 400)
		return
	}
	existing, err := db.GetSubscriptionByID(req.ID)
	if err != nil {
		common.ErrorResp(c, err, 404)
		return
	}
	if err := subscription.ApplyDefaults(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	normalizeSubscription(&req)
	if err := validateSubscriptionEpisodeRange(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	req.CreatedAt = existing.CreatedAt
	req.LastCheckedAt = existing.LastCheckedAt
	req.LastCursor = existing.LastCursor
	req.LastTreeHash = existing.LastTreeHash
	req.LastStatus = existing.LastStatus
	req.LastError = existing.LastError
	if err := db.UpdateSubscription(&req); err != nil {
		common.ErrorResp(c, err, 500)
		return
	}
	common.SuccessResp(c, req)
}

func DeleteSubscription(c *gin.Context) {
	var req struct {
		ID uint `json:"id" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	if err := db.DeleteSubscription(req.ID); err != nil {
		common.ErrorResp(c, err, 500)
		return
	}
	common.SuccessResp(c)
}

func PreviewSubscription(c *gin.Context) {
	var req model.SubscriptionPreviewReq
	if err := c.ShouldBindJSON(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	items, err := subscription.Preview(c.Request.Context(), req.ID)
	if err != nil {
		common.ErrorResp(c, err, 500)
		return
	}
	common.SuccessResp(c, items)
}

func CheckSubscription(c *gin.Context) {
	var req model.SubscriptionCheckReq
	if err := c.ShouldBindJSON(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	result, err := subscription.Run(c.Request.Context(), req.ID, req.Transfer)
	if err != nil {
		common.ErrorResp(c, err, 500)
		return
	}
	common.SuccessResp(c, result)
}

func ListSubscriptionRuns(c *gin.Context) {
	var req listSubscriptionRunsReq
	if err := c.ShouldBind(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	req.Validate()
	items, total, err := db.ListSubscriptionRuns(db.SubscriptionRunFilter{
		SubscriptionID: req.SubscriptionID,
		Status:         strings.TrimSpace(req.Status),
		Page:           req.Page,
		PerPage:        req.PerPage,
	})
	if err != nil {
		common.ErrorResp(c, err, 500)
		return
	}
	common.SuccessResp(c, common.PageResp{Content: items, Total: total})
}

func DeleteSubscriptionRun(c *gin.Context) {
	var req struct {
		ID uint `json:"id" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	if err := db.DeleteSubscriptionRun(req.ID); err != nil {
		common.ErrorResp(c, err, 500)
		return
	}
	common.SuccessResp(c)
}

func ClearFailedSubscriptionRuns(c *gin.Context) {
	deleted, err := db.ClearFailedSubscriptionRuns()
	if err != nil {
		common.ErrorResp(c, err, 500)
		return
	}
	common.SuccessResp(c, gin.H{"deleted": deleted})
}

func SearchSubscriptionResources(c *gin.Context) {
	var req model.SubscriptionResourceSearchReq
	if err := c.ShouldBindJSON(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	result, err := subscription.SearchResources(c.Request.Context(), req)
	if err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	common.SuccessResp(c, result)
}

func GetSubscriptionConfig(c *gin.Context) {
	cfg, err := subscription.GetConfig()
	if err != nil {
		common.ErrorResp(c, err, 500)
		return
	}
	common.SuccessResp(c, cfg)
}

func SaveSubscriptionConfig(c *gin.Context) {
	var req model.SubscriptionConfig
	if err := c.ShouldBindJSON(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	cfg, err := subscription.SaveConfig(req)
	if err != nil {
		common.ErrorResp(c, err, 500)
		return
	}
	common.SuccessResp(c, cfg)
}

func TelegramSubscriptionStatus(c *gin.Context) {
	runTelegramSubscriptionAuth(c, "status")
}

func TelegramSubscriptionSendCode(c *gin.Context) {
	runTelegramSubscriptionAuth(c, "send-code")
}

func TelegramSubscriptionSignIn(c *gin.Context) {
	runTelegramSubscriptionAuth(c, "signin")
}

func TelegramSubscriptionLogout(c *gin.Context) {
	runTelegramSubscriptionAuth(c, "logout")
}

func runTelegramSubscriptionAuth(c *gin.Context, action string) {
	var req model.SubscriptionTelegramAuthReq
	if err := c.ShouldBindJSON(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	switch action {
	case "send-code":
		if strings.TrimSpace(req.Phone) == "" {
			common.ErrorStrResp(c, "phone is required", 400)
			return
		}
	case "signin":
		if strings.TrimSpace(req.Phone) == "" || strings.TrimSpace(req.Code) == "" || strings.TrimSpace(req.PhoneCodeHash) == "" {
			common.ErrorStrResp(c, "phone, code and phone_code_hash are required", 400)
			return
		}
	}
	result, err := subscription.TelegramAuth(c.Request.Context(), req.ID, action, req)
	if err != nil {
		common.ErrorResp(c, err, 500)
		return
	}
	common.SuccessResp(c, result)
}

func normalizeSubscription(item *model.Subscription) {
	item.SourceType = strings.ToLower(strings.TrimSpace(item.SourceType))
	if item.SourceType == "" {
		item.SourceType = model.SubscriptionSourceTelegram
	}
	item.TargetRoot = utils.FixAndCleanPath(item.TargetRoot)
	item.MediaType = strings.ToLower(strings.TrimSpace(item.MediaType))
	if item.MediaType != "movie" {
		item.MediaType = "tv"
	}
	item.Category = strings.TrimSpace(item.Category)
	item.TMDBName = strings.TrimSpace(item.TMDBName)
	item.Name = strings.TrimSpace(item.Name)
	if item.CheckIntervalMinutes <= 0 {
		item.CheckIntervalMinutes = 60
	}
	item.Seasons = normalizeSubscriptionSeasons(item.MediaType, item.Seasons, item.Season)
	if item.MediaType == "movie" {
		item.Season = 0
		item.LatestSeasonEpisodeStart = 0
		item.LatestSeasonEpisodeEnd = 0
	} else if len(item.Seasons) > 0 {
		item.Season = item.Seasons[0]
	} else if item.Season <= 0 {
		item.Season = 1
	}
	if item.LastStatus == "" {
		item.LastStatus = model.SubscriptionStatusIdle
	}
}

func filterDisplayedSubscriptionItems(items []model.SubscriptionItem) []model.SubscriptionItem {
	filtered := make([]model.SubscriptionItem, 0, len(items))
	for _, item := range items {
		if item.Status != model.SubscriptionItemStatusTransferred {
			continue
		}
		if strings.TrimSpace(item.FileName) == "" && strings.TrimSpace(item.FilePath) == "" && strings.TrimSpace(item.TargetPath) == "" {
			continue
		}
		filtered = append(filtered, item)
	}
	return filtered
}

func validateSubscriptionEpisodeRange(item *model.Subscription) error {
	if item == nil || item.MediaType == "movie" {
		return nil
	}
	if item.LatestSeasonEpisodeStart < 0 || item.LatestSeasonEpisodeEnd < 0 {
		return errors.New("latest season episode range cannot be negative")
	}
	if item.LatestSeasonEpisodeStart > 0 && item.LatestSeasonEpisodeEnd > 0 && item.LatestSeasonEpisodeEnd < item.LatestSeasonEpisodeStart {
		return errors.New("latest_season_episode_end must be greater than or equal to latest_season_episode_start")
	}
	return nil
}

func normalizeSubscriptionSeasons(mediaType string, seasons []int, legacySeason int) []int {
	if strings.EqualFold(strings.TrimSpace(mediaType), "movie") {
		return nil
	}
	if len(seasons) == 0 && legacySeason > 0 {
		seasons = []int{legacySeason}
	}
	seen := map[int]struct{}{}
	cleaned := make([]int, 0, len(seasons))
	for _, season := range seasons {
		if season <= 0 {
			continue
		}
		if _, ok := seen[season]; ok {
			continue
		}
		seen[season] = struct{}{}
		cleaned = append(cleaned, season)
	}
	sort.Ints(cleaned)
	return cleaned
}
