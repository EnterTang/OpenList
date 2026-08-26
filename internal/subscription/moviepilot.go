package subscription

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/moviepilotbridge"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"gorm.io/gorm"
)

type MoviePilotBridgeClient interface {
	SearchResources(context.Context, string, moviepilotbridge.ResourceSearchRequest) ([]moviepilotbridge.ResourceSearchResult, error)
	SubmitIntent(context.Context, string, *model.MoviePilotDownloadIntent, moviepilotbridge.DownloadIntentRequest) error
}

var moviePilotBridgeRegistry struct {
	sync.RWMutex
	client MoviePilotBridgeClient
}

func SetMoviePilotBridgeClient(client MoviePilotBridgeClient) {
	moviePilotBridgeRegistry.Lock()
	moviePilotBridgeRegistry.client = client
	moviePilotBridgeRegistry.Unlock()
}

func currentMoviePilotBridgeClient() MoviePilotBridgeClient {
	moviePilotBridgeRegistry.RLock()
	defer moviePilotBridgeRegistry.RUnlock()
	return moviePilotBridgeRegistry.client
}

func moviePilotBridgeAvailable() bool {
	return currentMoviePilotBridgeClient() != nil
}

func SearchMoviePilotResources(ctx context.Context, req model.SubscriptionResourceSearchReq) ([]model.SubscriptionResourceSearchResult, error) {
	query := strings.TrimSpace(req.Query)
	if query == "" {
		return nil, errors.New("query is required")
	}
	bridgeID := strings.TrimSpace(req.BridgeInstanceID)
	if bridgeID == "" {
		return nil, errors.New("bridge_instance_id is required for MoviePilot search")
	}
	if req.TMDBID <= 0 {
		return nil, errors.New("tmdb_id is required for MoviePilot search")
	}
	client := currentMoviePilotBridgeClient()
	if client == nil {
		return nil, errors.New("MoviePilot Bridge is unavailable")
	}
	limit := req.Limit
	if limit <= 0 {
		limit = defaultResourceSearchLimit
	}
	remote, err := client.SearchResources(ctx, bridgeID, moviepilotbridge.ResourceSearchRequest{
		RequestID: shortHash(fmt.Sprintf("search:%s:%d:%s", bridgeID, req.TMDBID, query)),
		Query:     query, MediaSource: "tmdb", MediaID: strconv.FormatInt(req.TMDBID, 10),
		MediaType: strings.TrimSpace(req.MediaType),
	})
	if err != nil {
		return nil, err
	}
	results := make([]model.SubscriptionResourceSearchResult, 0, min(len(remote), limit))
	for _, item := range remote {
		if len(results) >= limit {
			break
		}
		projected := projectMoviePilotResult(bridgeID, bridgeSearchResult{
			ResourceRef: item.ResourceRef, Title: item.Title, Description: item.Description, Site: item.Site, Size: item.Size,
			Seeders: item.Seeders, Leechers: item.Leechers, Grabs: item.Grabs,
			SeasonEpisode: item.SeasonEpisode, EpisodeCount: item.EpisodeCount,
			Promotion: item.Promotion, FreeRemaining: item.FreeRemaining,
			HitAndRun: item.HitAndRun, Labels: item.Labels, PublishedAt: item.PublishedAt,
			SelectedFingerprint: item.SelectedFingerprint,
		})
		if strings.TrimSpace(projected.ExternalRef) == "" || strings.TrimSpace(projected.Title) == "" {
			continue
		}
		results = append(results, projected)
	}
	return results, nil
}

func projectMoviePilotResult(bridgeID string, item bridgeSearchResult) model.SubscriptionResourceSearchResult {
	content := ""
	if item.Size > 0 {
		content = formatResourceSize(item.Size)
	}
	return model.SubscriptionResourceSearchResult{
		SourceType: model.SubscriptionSourceMoviePilot, Provider: strings.TrimSpace(item.Site),
		Title: strings.TrimSpace(item.Title), Content: content, Description: strings.TrimSpace(item.Description),
		ExternalRef: strings.TrimSpace(item.ResourceRef), BridgeInstanceID: strings.TrimSpace(bridgeID),
		TorrentFingerprint: strings.TrimSpace(item.SelectedFingerprint), Size: item.Size,
		Seeders: item.Seeders, Leechers: item.Leechers, Grabs: item.Grabs,
		SeasonEpisode: strings.TrimSpace(item.SeasonEpisode), EpisodeCount: item.EpisodeCount,
		Promotion: strings.TrimSpace(item.Promotion), FreeRemaining: strings.TrimSpace(item.FreeRemaining),
		HitAndRun: item.HitAndRun, Labels: append([]string(nil), item.Labels...), PublishedAt: strings.TrimSpace(item.PublishedAt),
	}
}

type bridgeSearchResult struct {
	ResourceRef         string
	Title               string
	Description         string
	Site                string
	Size                int64
	Seeders             int
	Leechers            int
	Grabs               int
	SeasonEpisode       string
	EpisodeCount        int
	Promotion           string
	FreeRemaining       string
	HitAndRun           bool
	Labels              []string
	PublishedAt         string
	SelectedFingerprint string
	SiteCookie          string
}

func BindMoviePilotResource(ctx context.Context, req model.SubscriptionMoviePilotResourceBindReq) (*model.Subscription, error) {
	if req.SubscriptionID == 0 {
		return nil, errors.New("subscription_id is required")
	}
	if strings.TrimSpace(req.BridgeInstanceID) == "" {
		return nil, errors.New("bridge_instance_id is required")
	}
	if strings.TrimSpace(req.ResourceRef) == "" {
		return nil, errors.New("resource_ref is required")
	}
	if req.Size < 0 {
		return nil, errors.New("resource size must not be negative")
	}
	if err := validateMoviePilotRetentionPolicy(req.RetentionPolicy); err != nil {
		return nil, err
	}
	sub, err := db.GetSubscriptionByID(req.SubscriptionID)
	if err != nil {
		return nil, err
	}
	mediaSource := strings.TrimSpace(req.MediaSource)
	if mediaSource == "" {
		mediaSource = "tmdb"
	}
	mediaID := strings.TrimSpace(req.MediaID)
	if mediaID == "" && sub.TMDBID > 0 {
		mediaID = strconv.FormatInt(sub.TMDBID, 10)
	}
	if mediaID == "" {
		return nil, errors.New("media_id is required")
	}
	mediaType := strings.TrimSpace(req.MediaType)
	if mediaType == "" {
		mediaType = strings.TrimSpace(sub.MediaType)
	}
	bound := &model.SubscriptionBoundTorrent{
		BridgeInstanceID: req.BridgeInstanceID, ResourceRef: req.ResourceRef,
		SelectedFingerprint: strings.TrimSpace(req.SelectedFingerprint), TorrentTitle: strings.TrimSpace(req.TorrentTitle),
		Site: strings.TrimSpace(req.Site), Size: req.Size, MediaSource: mediaSource, MediaID: mediaID, MediaType: mediaType,
		Season: req.Season, Episode: req.Episode, RetentionPolicy: req.RetentionPolicy, BoundAt: time.Now().UTC(),
	}
	sub.BoundTorrent = bound
	sub.SourceType = model.SubscriptionSourceMoviePilot
	if err := db.UpdateSubscription(sub); err != nil {
		return nil, err
	}
	return sub, nil
}

func UnbindMoviePilotResource(ctx context.Context, req model.SubscriptionMoviePilotResourceUnbindReq) (*model.Subscription, error) {
	if req.SubscriptionID == 0 {
		return nil, errors.New("subscription_id is required")
	}
	sub, err := db.GetSubscriptionByID(req.SubscriptionID)
	if err != nil {
		return nil, err
	}
	sub.BoundTorrent = nil
	if sub.BoundShare != nil && strings.TrimSpace(sub.BoundShare.SourceType) != "" {
		sub.SourceType = strings.ToLower(strings.TrimSpace(sub.BoundShare.SourceType))
	} else if sub.SourceType == model.SubscriptionSourceMoviePilot {
		sub.SourceType = model.SubscriptionSourceAuto
	}
	if err := db.UpdateSubscription(sub); err != nil {
		return nil, err
	}
	return sub, nil
}

// UpdateMoviePilotRetention changes the durable policy for the bound torrent
// and all active intent/binding snapshots. A deletion already dispatched to a
// Worker is intentionally not canceled here; callers must update the policy
// before the binding enters deleting state when an extension is required.
func UpdateMoviePilotRetention(ctx context.Context, req model.SubscriptionMoviePilotRetentionUpdateReq) (*model.Subscription, error) {
	if req.SubscriptionID == 0 {
		return nil, errors.New("subscription_id is required")
	}
	if err := validateMoviePilotRetentionPolicy(req.RetentionPolicy); err != nil {
		return nil, err
	}
	sub, err := db.GetSubscriptionByID(req.SubscriptionID)
	if err != nil {
		return nil, err
	}
	if sub.BoundTorrent == nil {
		return nil, errors.New("MoviePilot resource is not bound")
	}
	policyRaw, err := json.Marshal(req.RetentionPolicy)
	if err != nil {
		return nil, err
	}
	policyJSON := string(policyRaw)
	if err := db.GetDb().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		sub.BoundTorrent.RetentionPolicy = req.RetentionPolicy
		if err := tx.Save(sub).Error; err != nil {
			return err
		}
		var intents []model.MoviePilotDownloadIntent
		if err := tx.Where("subscription_id = ? AND status NOT IN ?", sub.ID, []string{model.MoviePilotIntentStatusCancelled}).Find(&intents).Error; err != nil {
			return err
		}
		for _, intent := range intents {
			if err := tx.Model(&model.MoviePilotDownloadIntent{}).Where("id = ?", intent.ID).Update("retention_policy_json", policyJSON).Error; err != nil {
				return err
			}
			var binding model.MoviePilotTorrentBinding
			if err := tx.Where("intent_id = ?", intent.ID).First(&binding).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					continue
				}
				return err
			}
			if binding.Status == model.MoviePilotTorrentStatusDeleting {
				return fmt.Errorf("torrent %s is already deleting; retention can no longer be extended safely", binding.ID)
			}
			if binding.Status == model.MoviePilotTorrentStatusDeleted {
				continue
			}
			retentionStatus := model.MoviePilotRetentionStatusPending
			if req.RetentionPolicy.Permanent || req.RetentionPolicy.ManualHoldUntil != nil && req.RetentionPolicy.ManualHoldUntil.After(time.Now().UTC()) {
				retentionStatus = model.MoviePilotRetentionStatusHeld
			}
			if err := tx.Model(&model.MoviePilotTorrentBinding{}).Where("id = ?", binding.ID).Updates(map[string]any{
				"retention_policy_json": policyJSON, "retention_status": retentionStatus, "retention_eligible_at": nil,
			}).Error; err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return sub, nil
}

func validateMoviePilotRetentionPolicy(policy model.TorrentRetentionPolicy) error {
	if policy.MinSeedSeconds < 0 {
		return errors.New("retention min_seed_seconds must not be negative")
	}
	if policy.MinRatio < 0 || math.IsNaN(policy.MinRatio) || math.IsInf(policy.MinRatio, 0) {
		return errors.New("retention min_ratio must be a finite non-negative number")
	}
	return nil
}

func SubmitMoviePilotIntent(ctx context.Context, sub *model.Subscription) error {
	return submitMoviePilotIntent(ctx, sub, 0)
}

func submitMoviePilotIntent(ctx context.Context, sub *model.Subscription, subscriptionItemID uint) error {
	if sub == nil || sub.BoundTorrent == nil {
		return errors.New("MoviePilot resource is not bound")
	}
	client := currentMoviePilotBridgeClient()
	if client == nil {
		return errors.New("MoviePilot Bridge is unavailable")
	}
	bound := sub.BoundTorrent
	requestID := moviePilotIntentRequestID(sub.ID, bound.ResourceRef, bound.SelectedFingerprint)
	policyRaw, err := json.Marshal(bound.RetentionPolicy)
	if err != nil {
		return err
	}
	var policy map[string]interface{}
	if err := json.Unmarshal(policyRaw, &policy); err != nil {
		return err
	}
	intent := &model.MoviePilotDownloadIntent{
		ID: uuid.NewString(), RequestID: requestID, BridgeInstanceID: bound.BridgeInstanceID,
		SubscriptionID: sub.ID, SubscriptionItemID: subscriptionItemID, MediaSource: bound.MediaSource, MediaID: bound.MediaID,
		TorrentFingerprint: bound.SelectedFingerprint, ResourceRef: bound.ResourceRef, RetentionPolicyJSON: string(policyRaw),
		Status: model.MoviePilotIntentStatusPending,
	}
	payload := moviePilotIntentPayload(sub, bound, requestID, policy)
	return client.SubmitIntent(ctx, bound.BridgeInstanceID, intent, payload)
}

func moviePilotIntentRequestID(subscriptionID uint, resourceRef, fingerprint string) string {
	return shortHash(fmt.Sprintf("subscription:%d:%s:%s", subscriptionID, resourceRef, fingerprint))
}

func moviePilotIntentPayload(sub *model.Subscription, bound *model.SubscriptionBoundTorrent, requestID string, policy map[string]interface{}) moviepilotbridge.DownloadIntentRequest {
	return moviepilotbridge.DownloadIntentRequest{
		RequestID: requestID, SubscriptionID: strconv.FormatUint(uint64(sub.ID), 10),
		Media: moviepilotbridge.MediaIdentity{
			MediaSource: bound.MediaSource, MediaID: bound.MediaID, MediaType: bound.MediaType,
			Season: bound.Season, Episode: bound.Episode,
		},
		Torrent: moviepilotbridge.TorrentResource{
			Title: bound.TorrentTitle, ResourceRef: bound.ResourceRef, Site: bound.Site,
			Size: bound.Size, SelectedFingerprint: bound.SelectedFingerprint,
		},
		DownloaderPolicy: moviepilotbridge.DownloaderPolicy{Mode: "moviepilot_select"},
		RetentionPolicy:  policy,
	}
}

func runMoviePilot(ctx context.Context, sub *model.Subscription, transfer bool) ([]model.SubscriptionItem, string, int, int, int, error) {
	if sub == nil || sub.BoundTorrent == nil {
		return nil, "", 0, 0, 0, errors.New("MoviePilot resource is not bound")
	}
	bound := sub.BoundTorrent
	now := time.Now().UTC()
	fileHash := strings.TrimSpace(bound.SelectedFingerprint)
	if fileHash == "" {
		fileHash = shortHash(bound.ResourceRef)
	}
	item := &model.SubscriptionItem{
		SubscriptionID: sub.ID, SourceKey: "moviepilot:" + shortHash(bound.ResourceRef+"\x00"+bound.SelectedFingerprint),
		SourceProvider: firstNonEmpty(bound.Site, model.SubscriptionSourceMoviePilot), SourceURL: bound.ResourceRef,
		FileName: bound.TorrentTitle, FileHash: fileHash, FileSize: bound.Size, Season: bound.Season, Episode: bound.Episode,
		Status:     map[bool]string{true: model.SubscriptionItemStatusTransferring, false: model.SubscriptionItemStatusPending}[transfer],
		LastSeenAt: now, ProviderData: map[string]string{
			"bridge_instance_id": bound.BridgeInstanceID, "resource_ref": bound.ResourceRef,
			"selected_fingerprint": bound.SelectedFingerprint,
		},
	}
	saved, isNew, err := db.UpsertSubscriptionItem(item)
	if err != nil {
		return nil, sub.LastTreeHash, 0, 0, 0, err
	}
	if transfer {
		if err := submitMoviePilotIntent(ctx, sub, saved.ID); err != nil {
			return nil, sub.LastTreeHash, 0, 0, 0, err
		}
	}
	hash := shortHash(fmt.Sprintf("moviepilot:%d:%s:%s", sub.ID, bound.ResourceRef, bound.SelectedFingerprint))
	return []model.SubscriptionItem{*saved}, hash, boolToInt(isNew), 0, 0, nil
}

func formatResourceSize(size int64) string {
	if size <= 0 {
		return ""
	}
	return fmt.Sprintf("%d bytes", size)
}

func boolToInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
