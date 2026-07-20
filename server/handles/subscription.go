package handles

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/media/tmdb"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/subscription"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/OpenListTeam/OpenList/v4/server/common"
	"github.com/gin-gonic/gin"
)

const (
	tmdbEpisodeRefreshInterval = 24 * time.Hour
	tmdbEpisodeRefreshWorkers  = 4
	subscriptionDeleteTimeout  = 15 * time.Second
)

type listSubscriptionsReq struct {
	model.PageReq
	Keyword       string `form:"keyword" json:"keyword"`
	SourceType    string `form:"source_type" json:"source_type"`
	Active        string `form:"active" json:"active"`
	ArchiveStatus string `form:"archive_status" json:"archive_status"`
}

type listSubscriptionRunsReq struct {
	model.PageReq
	SubscriptionID uint   `form:"subscription_id" json:"subscription_id"`
	View           string `form:"view" json:"view"`
	Keyword        string `form:"keyword" json:"keyword"`
	SourceType     string `form:"source_type" json:"source_type"`
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
	archiveStatus, err := resolveSubscriptionArchiveStatus(req.ArchiveStatus)
	if err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	filter := db.SubscriptionFilter{
		Keyword:    req.Keyword,
		SourceType: strings.TrimSpace(req.SourceType),
		Active:     active,
		Page:       req.Page,
		PerPage:    req.PerPage,
	}
	hydrateSubscriptionEpisodeEnds(c.Request.Context(), filter)
	items, total, err := subscription.ListSubscriptionsWithProgress(filter, archiveStatus, time.Now())
	if err != nil {
		common.ErrorResp(c, err, 500)
		return
	}
	common.SuccessResp(c, common.PageResp{Content: items, Total: total})
}

func resolveSubscriptionArchiveStatus(value string) (string, error) {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return "all", nil
	}
	if err := subscription.ValidateSubscriptionArchiveStatus(value); err != nil {
		return "", err
	}
	return value, nil
}

func hydrateSubscriptionEpisodeEnds(ctx context.Context, filter db.SubscriptionFilter) {
	apiKey := etfArchiveSettingValue(conf.TMDBApiKey)
	if apiKey == "" {
		return
	}
	items, err := db.ListAllSubscriptions(filter)
	if err != nil {
		return
	}
	config := tmdb.Config{
		APIKey:        apiKey,
		BaseURL:       etfArchiveSettingValue(conf.TMDBApiBaseURL),
		Language:      etfArchiveSettingValue(conf.TMDBLanguage),
		CategoryRules: etfArchiveSettingValue(conf.MediaCategoryRules),
	}
	now := time.Now()
	sem := make(chan struct{}, tmdbEpisodeRefreshWorkers)
	var group sync.WaitGroup
	for i := range items {
		item := &items[i]
		if !shouldHydrateSubscriptionEpisodeEnd(item, now) {
			continue
		}
		group.Add(1)
		sem <- struct{}{}
		go func(item *model.Subscription) {
			defer group.Done()
			defer func() { <-sem }()
			query := strings.TrimSpace(item.TMDBName)
			if item.TMDBID > 0 {
				query = strconv.FormatInt(item.TMDBID, 10)
			}
			candidates, err := tmdb.SearchCandidates(ctx, config, query)
			if err != nil {
				return
			}
			episodeEnd := item.LatestSeasonEpisodeEnd
			var discoveredTMDBID *int64
			if candidate := subscriptionTMDBCandidate(item, candidates); candidate != nil {
				if item.TMDBID == 0 && candidate.TMDBID > 0 {
					tmdbID := candidate.TMDBID
					discoveredTMDBID = &tmdbID
				}
				if end := subscriptionEpisodeEndFromTMDBCandidate(item, candidate); end > 0 {
					episodeEnd = end
				}
			}
			checkedAt := now
			_, _ = db.UpdateSubscriptionTMDBEpisodeEnd(item, discoveredTMDBID, episodeEnd, checkedAt)
		}(item)
	}
	group.Wait()
}

func shouldHydrateSubscriptionEpisodeEnd(item *model.Subscription, now time.Time) bool {
	if item == nil || item.MediaType != "tv" {
		return false
	}
	if item.TMDBID <= 0 && strings.TrimSpace(item.TMDBName) == "" {
		return false
	}
	return item.TMDBEpisodeSyncedAt == nil || now.Sub(*item.TMDBEpisodeSyncedAt) >= tmdbEpisodeRefreshInterval
}

func subscriptionEpisodeEndFromTMDBCandidates(item *model.Subscription, candidates []model.ETFArchiveTMDBCandidate) int {
	return subscriptionEpisodeEndFromTMDBCandidate(item, subscriptionTMDBCandidate(item, candidates))
}

func subscriptionTMDBCandidate(item *model.Subscription, candidates []model.ETFArchiveTMDBCandidate) *model.ETFArchiveTMDBCandidate {
	if item == nil {
		return nil
	}
	name := strings.TrimSpace(item.TMDBName)
	if name == "" {
		name = strings.TrimSpace(item.Name)
	}
	for i := range candidates {
		candidate := &candidates[i]
		if candidate.MediaType != "tv" {
			continue
		}
		if item.TMDBID > 0 {
			if candidate.TMDBID == item.TMDBID {
				return candidate
			}
			continue
		}
		if !strings.EqualFold(candidate.Name, name) && !strings.EqualFold(candidate.OriginalName, name) {
			continue
		}
		if item.TMDBYear > 0 && candidate.Year != item.TMDBYear {
			continue
		}
		return candidate
	}
	return nil
}

func subscriptionEpisodeEndFromTMDBCandidate(item *model.Subscription, candidate *model.ETFArchiveTMDBCandidate) int {
	if item == nil || candidate == nil {
		return 0
	}
	latestSeason := item.Season
	for _, season := range item.Seasons {
		if season > latestSeason {
			latestSeason = season
		}
	}
	if latestSeason <= 0 {
		return 0
	}
	if end := candidate.SeasonMap[latestSeason]; end > 0 {
		return end
	}
	for _, season := range candidate.Seasons {
		if season.SeasonNumber == latestSeason && season.EpisodeCount > 0 {
			return season.EpisodeCount
		}
	}
	return 0
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
	subscription.HydrateRealtimeStatus(item, items)
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
	if err := validateSubscriptionPreferredWorkerNodeID(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
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
	subscription.RefreshTelegramRealtimeListener()
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
	if req.Name == "" {
		common.ErrorStrResp(c, "name is required", 400)
		return
	}
	if err := validateSubscriptionPreferredWorkerNodeID(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
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
	subscription.RefreshTelegramRealtimeListener()
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
	ctx, cancel := context.WithTimeout(c.Request.Context(), subscriptionDeleteTimeout)
	defer cancel()
	if err := db.DeleteSubscriptionContext(ctx, req.ID); err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			common.ErrorStrResp(c, "subscription delete timed out; no changes were committed", 503)
			return
		}
		if subscriptionDeleteDatabaseBusy(err) {
			common.ErrorStrResp(c, "subscription delete is temporarily blocked by another database operation; no changes were committed", 503)
			return
		}
		common.ErrorResp(c, err, 500)
		return
	}
	subscription.RefreshTelegramRealtimeListener()
	common.SuccessResp(c)
}

func subscriptionDeleteDatabaseBusy(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "database is locked") || strings.Contains(message, "sqlite_busy")
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
	result, err := subscription.RunForRole(c.Request.Context(), req.ID, req.Transfer, conf.Conf.Cluster.Role)
	if err != nil {
		common.ErrorResp(c, err, 500)
		return
	}
	common.SuccessResp(c, result)
}

func RetryFailedSubscription(c *gin.Context) {
	var req model.SubscriptionCheckReq
	if err := c.ShouldBindJSON(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	if _, err := db.ResetFailedSubscriptionItems(c.Request.Context(), req.ID); err != nil {
		common.ErrorResp(c, err, 500)
		return
	}
	result, err := subscription.RunForRole(c.Request.Context(), req.ID, true, conf.Conf.Cluster.Role)
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
	view, err := resolveSubscriptionRunView(req.View)
	if err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	req.Validate()
	items, total, err := db.ListSubscriptionRuns(db.SubscriptionRunFilter{
		SubscriptionID: req.SubscriptionID,
		View:           view,
		Keyword:        req.Keyword,
		SourceType:     strings.TrimSpace(req.SourceType),
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

func ListSubscriptionBoard(c *gin.Context) {
	var req listSubscriptionRunsReq
	if err := c.ShouldBind(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	if _, err := resolveSubscriptionRunView(req.View); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	board, err := db.GetSubscriptionBoard(db.SubscriptionRunFilter{
		SubscriptionID: req.SubscriptionID,
		Keyword:        req.Keyword,
		SourceType:     strings.TrimSpace(req.SourceType),
		Status:         strings.TrimSpace(req.Status),
	})
	if err != nil {
		common.ErrorResp(c, err, 500)
		return
	}
	common.SuccessResp(c, board)
}

func ListSubscriptionEpisodeSources(c *gin.Context) {
	subscriptionID, err := requiredUintQuery(c, "subscription_id")
	if err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	items, err := db.ListSubscriptionEpisodeSourceDetails(subscriptionID)
	if err != nil {
		common.ErrorResp(c, err, 500)
		return
	}
	common.SuccessResp(c, gin.H{"content": items})
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

func resolveSubscriptionRunView(value string) (string, error) {
	value = strings.ToLower(strings.TrimSpace(value))
	switch value {
	case "":
		return "", nil
	case model.SubscriptionRunViewChanges, model.SubscriptionRunViewFailures:
		return value, nil
	default:
		return "", errors.New("view must be changes or failures")
	}
}

func requiredUintQuery(c *gin.Context, key string) (uint, error) {
	value := strings.TrimSpace(c.Query(key))
	if value == "" {
		return 0, errors.New(key + " is required")
	}
	id, err := strconv.ParseUint(value, 10, 64)
	if err != nil || id == 0 {
		return 0, errors.New(key + " is required")
	}
	return uint(id), nil
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
	common.SuccessResp(c, subscriptionConfigResponse(cfg))
}

func SaveSubscriptionConfig(c *gin.Context) {
	var req model.SubscriptionConfig
	if err := c.ShouldBindJSON(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	current, err := subscription.GetConfig()
	if err != nil {
		common.ErrorResp(c, err, 500)
		return
	}
	mergeSubscriptionConfigSecrets(&req, current)
	cfg, err := subscription.SaveConfig(req)
	if err != nil {
		common.ErrorResp(c, err, 500)
		return
	}
	common.SuccessResp(c, subscriptionConfigResponse(cfg))
}

func subscriptionConfigResponse(cfg model.SubscriptionConfig) model.SubscriptionConfigResponse {
	capabilities := subscription.ResourceSearchSourceCapabilities(cfg)
	configured := map[string]bool{
		"telegram.api_hash":                   cfg.Telegram.APIHash != "",
		"telegram.quark.cookie":               cfg.Telegram.Quark.Cookie != "",
		"telegram.quark.refresh_token":        cfg.Telegram.Quark.RefreshToken != "",
		"telegram.quark.access_token":         cfg.Telegram.Quark.AccessToken != "",
		"telegram.aliyun_drive.cookie":        cfg.Telegram.AliyunDrive.Cookie != "",
		"telegram.aliyun_drive.refresh_token": cfg.Telegram.AliyunDrive.RefreshToken != "",
		"telegram.aliyun_drive.access_token":  cfg.Telegram.AliyunDrive.AccessToken != "",
		"telegram.pan123.cookie":              cfg.Telegram.Pan123.Cookie != "",
		"telegram.pan123.refresh_token":       cfg.Telegram.Pan123.RefreshToken != "",
		"telegram.pan123.access_token":        cfg.Telegram.Pan123.AccessToken != "",
		"telegram.pan115.cookie":              cfg.Telegram.Pan115.Cookie != "",
		"telegram.pan115.refresh_token":       cfg.Telegram.Pan115.RefreshToken != "",
		"telegram.pan115.access_token":        cfg.Telegram.Pan115.AccessToken != "",
	}
	redactSubscriptionConfigSecrets(&cfg)
	return model.SubscriptionConfigResponse{
		SubscriptionConfig: cfg,
		SecretStatus: model.SubscriptionConfigSecretStatus{
			Configured:      configured,
			UnchangedMarker: model.SubscriptionSecretUnchangedMarker,
			ClearMarker:     model.SubscriptionSecretClearMarker,
		},
		SourceCapabilities: capabilities,
	}
}

func mergeSubscriptionConfigSecrets(next *model.SubscriptionConfig, current model.SubscriptionConfig) {
	if next == nil {
		return
	}
	next.Telegram.APIHash = mergeSubscriptionSecret(next.Telegram.APIHash, current.Telegram.APIHash)
	mergeSubscriptionPanSecrets(&next.Telegram.Quark, current.Telegram.Quark)
	mergeSubscriptionPanSecrets(&next.Telegram.AliyunDrive, current.Telegram.AliyunDrive)
	mergeSubscriptionPanSecrets(&next.Telegram.Pan123, current.Telegram.Pan123)
	mergeSubscriptionPanSecrets(&next.Telegram.Pan115, current.Telegram.Pan115)
}

func mergeSubscriptionPanSecrets(next *model.SubscriptionTelegramPanConfig, current model.SubscriptionTelegramPanConfig) {
	next.Cookie = mergeSubscriptionSecret(next.Cookie, current.Cookie)
	next.RefreshToken = mergeSubscriptionSecret(next.RefreshToken, current.RefreshToken)
	next.AccessToken = mergeSubscriptionSecret(next.AccessToken, current.AccessToken)
}

func mergeSubscriptionSecret(next, current string) string {
	switch next {
	case "", model.SubscriptionSecretUnchangedMarker:
		return current
	case model.SubscriptionSecretClearMarker:
		return ""
	default:
		return next
	}
}

func redactSubscriptionConfigSecrets(cfg *model.SubscriptionConfig) {
	if cfg == nil {
		return
	}
	cfg.Telegram.APIHash = ""
	redactSubscriptionPanSecrets(&cfg.Telegram.Quark)
	redactSubscriptionPanSecrets(&cfg.Telegram.AliyunDrive)
	redactSubscriptionPanSecrets(&cfg.Telegram.Pan123)
	redactSubscriptionPanSecrets(&cfg.Telegram.Pan115)
}

func redactSubscriptionPanSecrets(cfg *model.SubscriptionTelegramPanConfig) {
	cfg.Cookie = ""
	cfg.RefreshToken = ""
	cfg.AccessToken = ""
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
	item.TargetRoot = strings.TrimSpace(item.TargetRoot)
	item.PreferredWorkerNodeID = strings.TrimSpace(item.PreferredWorkerNodeID)
	if item.TargetRoot != "" {
		item.TargetRoot = utils.FixAndCleanPath(item.TargetRoot)
	}
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

func validateSubscriptionPreferredWorkerNodeID(item *model.Subscription) error {
	if item != nil && len(item.PreferredWorkerNodeID) > 64 {
		return errors.New("preferred_worker_node_id cannot exceed 64 bytes")
	}
	return nil
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
