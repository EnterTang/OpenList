package subscription

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

const (
	externalSubscriptionIdempotencyKeyMaxLength = 191
	externalSubscriptionDeliveryFailedMessage   = "delivery failed"
)

var (
	ErrExternalSubscriptionInvalid  = errors.New("invalid external subscription request")
	ErrExternalSubscriptionConflict = errors.New("external subscription request conflicts with an existing subscription")
	externalSubscriptionRuns        sync.Map
)

type ExternalSubscriptionCreateRequest struct {
	Name            string          `json:"name,omitempty"`
	MediaType       string          `json:"media_type"`
	TMDBID          int64           `json:"tmdb_id"`
	SourceType      string          `json:"source_type,omitempty"`
	SourceConfig    json.RawMessage `json:"source_config,omitempty"`
	SeasonsSelected []int           `json:"seasons_selected,omitempty"`
	EpisodeStart    int             `json:"episode_start,omitempty"`
	EpisodeEnd      int             `json:"episode_end,omitempty"`
	ShareURL        string          `json:"share_url,omitempty"`
	AccessCode      string          `json:"access_code,omitempty"`
	ShareType       string          `json:"share_type,omitempty"`
	SeasonStart     int             `json:"season_start,omitempty"`
}

type ExternalSubscriptionIdentity struct {
	ID                     uint `json:"id"`
	InternalSubscriptionID uint `json:"internal_subscription_id"`
}

type ExternalSubscriptionResponse struct {
	ID                     uint                          `json:"id"`
	SubscriptionID         int64                         `json:"subscription_id"`
	InternalSubscriptionID uint                          `json:"internal_subscription_id"`
	TaskID                 string                        `json:"task_id"`
	Type                   string                        `json:"type"`
	Status                 string                        `json:"status"`
	TaskStatus             string                        `json:"task_status"`
	LastStatus             string                        `json:"last_status"`
	LastMessage            string                        `json:"last_message"`
	ProgressJSON           string                        `json:"progress_json"`
	Progress               model.SubscriptionProgress    `json:"progress"`
	SeasonsJSON            string                        `json:"seasons_json"`
	SeasonsSelected        []int                         `json:"seasons_selected"`
	Completed              bool                          `json:"completed"`
	Subscription           *ExternalSubscriptionIdentity `json:"subscription"`
}

type ExternalSubscriptionLookupResponse struct {
	Exists         bool   `json:"exists"`
	SubscriptionID int64  `json:"subscription_id,omitempty"`
	TaskID         string `json:"task_id,omitempty"`
	TaskStatus     string `json:"task_status,omitempty"`
	Status         string `json:"status,omitempty"`
}

func CreateExternalSubscription(ctx context.Context, input ExternalSubscriptionCreateRequest, idempotencyKey string) (*ExternalSubscriptionResponse, bool, error) {
	normalized, subscription, requestJSON, fingerprint, lookupKey, err := normalizeExternalSubscriptionCreateRequest(input)
	if err != nil {
		return nil, false, err
	}
	idempotencyKey = strings.TrimSpace(idempotencyKey)
	if len(idempotencyKey) > externalSubscriptionIdempotencyKeyMaxLength {
		return nil, false, invalidExternalSubscription("idempotency key cannot exceed %d bytes", externalSubscriptionIdempotencyKeyMaxLength)
	}
	if idempotencyKey == "" {
		idempotencyKey = "fingerprint:" + fingerprint
	}

	if existing, found, err := existingExternalSubscription(ctx, idempotencyKey, lookupKey); err != nil {
		return nil, false, err
	} else if found {
		if existing.RequestFingerprint != fingerprint {
			return nil, false, fmt.Errorf("%w: media_type=%s tmdb_id=%d", ErrExternalSubscriptionConflict, normalized.MediaType, normalized.TMDBID)
		}
		response, err := ProjectExternalSubscription(ctx, existing.ID)
		return response, false, err
	}

	seasonsJSON, err := json.Marshal(subscription.Seasons)
	if err != nil {
		return nil, false, err
	}
	request := &model.ExternalSubscriptionRequest{
		IdempotencyKey:     idempotencyKey,
		LookupKey:          lookupKey,
		RequestFingerprint: fingerprint,
		RequestJSON:        requestJSON,
		LastStatus:         "pending",
		LastMessage:        "queued",
		ProgressJSON:       "{}",
		SeasonsJSON:        string(seasonsJSON),
	}
	if err := db.CreateExternalSubscriptionRequest(ctx, request, subscription); err != nil {
		// A concurrent request may have won either unique key after the preflight.
		if existing, found, lookupErr := existingExternalSubscription(ctx, idempotencyKey, lookupKey); lookupErr == nil && found {
			if existing.RequestFingerprint != fingerprint {
				return nil, false, fmt.Errorf("%w: media_type=%s tmdb_id=%d", ErrExternalSubscriptionConflict, normalized.MediaType, normalized.TMDBID)
			}
			response, projectErr := ProjectExternalSubscription(ctx, existing.ID)
			return response, false, projectErr
		}
		return nil, false, err
	}

	response, err := ProjectExternalSubscription(ctx, request.ID)
	if err != nil {
		return nil, false, err
	}
	persistExternalSubscriptionProjection(ctx, response)
	return response, true, nil
}

func ProjectExternalSubscription(ctx context.Context, externalID uint) (*ExternalSubscriptionResponse, error) {
	request, err := db.GetExternalSubscriptionRequest(ctx, externalID)
	if err != nil {
		return nil, err
	}
	subscription, err := db.GetSubscriptionByID(request.SubscriptionID)
	if err != nil {
		return nil, err
	}
	items, err := db.ListSubscriptionItems(subscription.ID)
	if err != nil {
		return nil, err
	}
	progress := CalculateSubscriptionProgress(subscription, items, time.Now())
	progressJSON, err := json.Marshal(progress)
	if err != nil {
		return nil, err
	}
	seasons := append([]int(nil), subscription.Seasons...)
	if subscription.MediaType == "tv" && len(seasons) == 0 && subscription.Season > 0 {
		seasons = []int{subscription.Season}
	}
	if seasons == nil {
		seasons = []int{}
	}
	seasonsJSON, err := json.Marshal(seasons)
	if err != nil {
		return nil, err
	}
	status, message := projectExternalSubscriptionStatus(request, subscription, items)
	taskID := strconv.FormatUint(uint64(request.ID), 10)
	return &ExternalSubscriptionResponse{
		ID:                     request.ID,
		SubscriptionID:         int64(request.ID),
		InternalSubscriptionID: subscription.ID,
		TaskID:                 taskID,
		Type:                   "subscription",
		Status:                 status,
		TaskStatus:             status,
		LastStatus:             status,
		LastMessage:            message,
		ProgressJSON:           string(progressJSON),
		Progress:               progress,
		SeasonsJSON:            string(seasonsJSON),
		SeasonsSelected:        seasons,
		Completed:              status == "completed",
		Subscription: &ExternalSubscriptionIdentity{
			ID:                     request.ID,
			InternalSubscriptionID: subscription.ID,
		},
	}, nil
}

func LookupExternalSubscription(ctx context.Context, mediaType string, tmdbID int64) (*ExternalSubscriptionLookupResponse, error) {
	mediaType = strings.ToLower(strings.TrimSpace(mediaType))
	if mediaType != "tv" && mediaType != "movie" {
		return nil, invalidExternalSubscription("media_type must be tv or movie")
	}
	if tmdbID <= 0 {
		return nil, invalidExternalSubscription("tmdb_id must be greater than zero")
	}
	request, err := db.GetExternalSubscriptionRequestByLookupKey(ctx, externalSubscriptionLookupKey(mediaType, tmdbID))
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return &ExternalSubscriptionLookupResponse{Exists: false}, nil
		}
		return nil, err
	}
	response, err := ProjectExternalSubscription(ctx, request.ID)
	if err != nil {
		return nil, err
	}
	return &ExternalSubscriptionLookupResponse{
		Exists:         true,
		SubscriptionID: int64(request.ID),
		TaskID:         response.TaskID,
		TaskStatus:     response.TaskStatus,
		Status:         response.Status,
	}, nil
}

func QueueExternalSubscriptionRun(externalID uint) error {
	request, err := db.GetExternalSubscriptionRequest(context.Background(), externalID)
	if err != nil {
		return err
	}
	if _, loaded := externalSubscriptionRuns.LoadOrStore(externalID, struct{}{}); loaded {
		return nil
	}
	if err := db.UpdateExternalSubscriptionRequestState(context.Background(), externalID, "pending", "queued", "", "", "", nil); err != nil {
		externalSubscriptionRuns.Delete(externalID)
		return err
	}
	go runExternalSubscription(request)
	return nil
}

func runExternalSubscription(request *model.ExternalSubscriptionRequest) {
	defer externalSubscriptionRuns.Delete(request.ID)
	ctx := context.Background()
	startedAt := time.Now()
	_ = db.UpdateExternalSubscriptionRequestState(ctx, request.ID, "running", "processing", "", "", "", &startedAt)
	role := ""
	if conf.Conf != nil {
		role = conf.Conf.Cluster.Role
	}
	_, runErr := RunForRole(ctx, request.SubscriptionID, true, role)
	if runErr != nil {
		_ = db.UpdateExternalSubscriptionRequestState(ctx, request.ID, "failed", runErr.Error(), "", "", runErr.Error(), &startedAt)
		log.Errorf("external subscription %d run failed: %+v", request.ID, runErr)
	} else {
		_ = db.UpdateExternalSubscriptionRequestState(ctx, request.ID, "completed", "completed", "", "", "", &startedAt)
	}
	if response, err := ProjectExternalSubscription(ctx, request.ID); err == nil {
		persistExternalSubscriptionProjection(ctx, response)
	}
}

func normalizeExternalSubscriptionCreateRequest(input ExternalSubscriptionCreateRequest) (ExternalSubscriptionCreateRequest, *model.Subscription, string, string, string, error) {
	input.Name = strings.TrimSpace(input.Name)
	input.MediaType = strings.ToLower(strings.TrimSpace(input.MediaType))
	input.SourceType = strings.ToLower(strings.TrimSpace(input.SourceType))
	input.ShareURL = strings.TrimSpace(input.ShareURL)
	input.AccessCode = strings.TrimSpace(input.AccessCode)
	input.ShareType = strings.TrimSpace(input.ShareType)
	if input.MediaType != "tv" && input.MediaType != "movie" {
		return input, nil, "", "", "", invalidExternalSubscription("media_type must be tv or movie")
	}
	if input.TMDBID <= 0 {
		return input, nil, "", "", "", invalidExternalSubscription("tmdb_id must be greater than zero")
	}
	if input.Name == "" && input.ShareURL != "" {
		input.Name = fmt.Sprintf("TMDB %d", input.TMDBID)
	}
	if input.Name == "" {
		return input, nil, "", "", "", invalidExternalSubscription("name is required")
	}
	if input.SourceType == "" {
		if input.ShareURL != "" {
			input.SourceType = model.SubscriptionSourceManual
		} else {
			input.SourceType = model.SubscriptionSourceAuto
		}
	}
	switch input.SourceType {
	case model.SubscriptionSourceManual, model.SubscriptionSourceTelegram, model.SubscriptionSourcePanSou, model.SubscriptionSourceHDHive, model.SubscriptionSourceAuto:
	default:
		return input, nil, "", "", "", invalidExternalSubscription("source_type must be manual, telegram, pansou, hdhive, or auto")
	}

	if input.MediaType == "movie" {
		input.SeasonsSelected = nil
		input.SeasonStart = 0
		input.EpisodeStart = 0
		input.EpisodeEnd = 0
	} else {
		if len(input.SeasonsSelected) == 0 && input.SeasonStart > 0 {
			input.SeasonsSelected = []int{input.SeasonStart}
		}
		if len(input.SeasonsSelected) == 0 {
			input.SeasonsSelected = []int{1}
		}
		seasons, err := normalizeExternalSeasons(input.SeasonsSelected)
		if err != nil {
			return input, nil, "", "", "", err
		}
		input.SeasonsSelected = seasons
		input.SeasonStart = 0
		if input.EpisodeStart < 0 || input.EpisodeEnd < 0 {
			return input, nil, "", "", "", invalidExternalSubscription("episode range cannot be negative")
		}
		if input.EpisodeStart > 0 && input.EpisodeEnd > 0 && input.EpisodeEnd < input.EpisodeStart {
			return input, nil, "", "", "", invalidExternalSubscription("episode_end must be greater than or equal to episode_start")
		}
	}

	sourceConfig, err := normalizeExternalSourceConfig(input)
	if err != nil {
		return input, nil, "", "", "", err
	}
	input.SourceConfig = sourceConfig
	subscription := &model.Subscription{
		Name:                     input.Name,
		TMDBName:                 input.Name,
		TMDBID:                   input.TMDBID,
		MediaType:                input.MediaType,
		SourceType:               input.SourceType,
		SourceConfig:             string(sourceConfig),
		Active:                   true,
		TransferEnabled:          true,
		CheckIntervalMinutes:     60,
		Seasons:                  append([]int(nil), input.SeasonsSelected...),
		LatestSeasonEpisodeStart: input.EpisodeStart,
		LatestSeasonEpisodeEnd:   input.EpisodeEnd,
		LastStatus:               model.SubscriptionStatusIdle,
	}
	if input.MediaType == "tv" && len(subscription.Seasons) > 0 {
		subscription.Season = subscription.Seasons[0]
	}
	if err := ApplyDefaults(subscription); err != nil {
		return input, nil, "", "", "", invalidExternalSubscription("source_config is invalid: %v", err)
	}
	requestBody, err := json.Marshal(input)
	if err != nil {
		return input, nil, "", "", "", err
	}
	digest := sha256.Sum256(requestBody)
	fingerprint := hex.EncodeToString(digest[:])
	return input, subscription, string(requestBody), fingerprint, externalSubscriptionLookupKey(input.MediaType, input.TMDBID), nil
}

func normalizeExternalSourceConfig(input ExternalSubscriptionCreateRequest) (json.RawMessage, error) {
	raw := input.SourceConfig
	if len(raw) == 0 || string(raw) == "null" {
		raw = json.RawMessage(`{}`)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return nil, invalidExternalSubscription("source_config must be a JSON object")
	}
	delete(fields, "_request")
	if input.ShareURL != "" {
		if input.SourceType != model.SubscriptionSourceManual {
			return nil, invalidExternalSubscription("share_url requires source_type manual")
		}
		var manual model.SubscriptionManualSourceConfig
		cleaned, err := json.Marshal(fields)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(cleaned, &manual); err != nil {
			return nil, invalidExternalSubscription("source_config is invalid: %v", err)
		}
		link := normalizeTelegramLinkWithAccessCode(input.ShareURL, input.AccessCode)
		manual.Links = append(manual.Links, link)
		cleaned, err = json.Marshal(manual)
		if err != nil {
			return nil, err
		}
		return cleaned, nil
	}
	cleaned, err := json.Marshal(fields)
	if err != nil {
		return nil, err
	}
	return cleaned, nil
}

func normalizeExternalSeasons(seasons []int) ([]int, error) {
	seen := make(map[int]struct{}, len(seasons))
	result := make([]int, 0, len(seasons))
	for _, season := range seasons {
		if season <= 0 {
			return nil, invalidExternalSubscription("seasons_selected must contain only positive season numbers")
		}
		if _, exists := seen[season]; exists {
			continue
		}
		seen[season] = struct{}{}
		result = append(result, season)
	}
	sort.Ints(result)
	return result, nil
}

func existingExternalSubscription(ctx context.Context, idempotencyKey, lookupKey string) (*model.ExternalSubscriptionRequest, bool, error) {
	request, err := db.GetExternalSubscriptionRequestByIdempotencyKey(ctx, idempotencyKey)
	if err == nil {
		return request, true, nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, false, err
	}
	request, err = db.GetExternalSubscriptionRequestByLookupKey(ctx, lookupKey)
	if err == nil {
		return request, true, nil
	}
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, false, nil
	}
	return nil, false, err
}

func projectExternalSubscriptionStatus(request *model.ExternalSubscriptionRequest, subscription *model.Subscription, items []model.SubscriptionItem) (string, string) {
	hasFailedItem := false
	for _, item := range items {
		if item.Status == model.SubscriptionItemStatusFailed {
			hasFailedItem = true
			if message := strings.TrimSpace(item.LastError); message != "" {
				return "failed", message
			}
		}
	}
	if hasFailedItem {
		return "failed", externalSubscriptionDeliveryFailedMessage
	}
	for _, item := range items {
		switch item.Status {
		case model.SubscriptionItemStatusPending, model.SubscriptionItemStatusNotifying, model.SubscriptionItemStatusTransferring:
			return "running", item.Status
		}
	}
	switch subscription.LastStatus {
	case model.SubscriptionStatusRunning:
		return "running", "processing"
	case model.SubscriptionStatusSuccess:
		return "completed", "completed"
	case model.SubscriptionStatusFailed:
		message := strings.TrimSpace(subscription.LastError)
		if message == "" {
			message = strings.TrimSpace(request.LastError)
		}
		if message == "" {
			message = "failed"
		}
		return "failed", message
	}
	status := strings.ToLower(strings.TrimSpace(request.LastStatus))
	switch status {
	case "running", "failed":
		return status, firstNonEmptyString(request.LastMessage, status)
	case "completed":
		return "pending", firstNonEmptyString(request.LastMessage, "queued")
	default:
		return "pending", firstNonEmptyString(request.LastMessage, "queued")
	}
}

func externalSubscriptionLookupKey(mediaType string, tmdbID int64) string {
	return strings.ToLower(strings.TrimSpace(mediaType)) + ":" + strconv.FormatInt(tmdbID, 10)
}

func invalidExternalSubscription(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrExternalSubscriptionInvalid, fmt.Sprintf(format, args...))
}

func persistExternalSubscriptionResponse(ctx context.Context, response *ExternalSubscriptionResponse) {
	if response == nil {
		return
	}
	raw, err := json.Marshal(response)
	if err != nil {
		return
	}
	_ = db.UpdateExternalSubscriptionResponseJSON(ctx, response.ID, string(raw))
}

func persistExternalSubscriptionProjection(ctx context.Context, response *ExternalSubscriptionResponse) {
	if response == nil {
		return
	}
	lastError := ""
	if response.Status == "failed" {
		lastError = response.LastMessage
	}
	_ = db.UpdateExternalSubscriptionRequestState(
		ctx,
		response.ID,
		response.LastStatus,
		response.LastMessage,
		response.ProgressJSON,
		response.SeasonsJSON,
		lastError,
		nil,
	)
	persistExternalSubscriptionResponse(ctx, response)
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}
