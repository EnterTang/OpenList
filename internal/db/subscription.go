package db

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/pkg/errors"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type SubscriptionFilter struct {
	Keyword    string
	SourceType string
	Active     *bool
	Page       int
	PerPage    int
}

type SubscriptionRunFilter struct {
	SubscriptionID uint
	View           string
	Keyword        string
	SourceType     string
	Status         string
	Page           int
	PerPage        int
}

var ErrStaleSubscriptionTerminalCallback = errors.New("stale subscription terminal callback")

type SubscriptionTerminalItemRequest struct {
	ItemID               uint
	SubscriptionID       uint
	SourceKey            string
	ExpectedFileHash     string
	ExpectedStatus       string
	ExpectedClusterJobID *string
	TerminalStatus       string
	TerminalLastError    string
	TerminalClusterJobID *string
	RecoverySource       *model.SubscriptionEpisodeSource
}

func CreateSubscription(item *model.Subscription) error {
	if item.LastStatus == "" {
		item.LastStatus = model.SubscriptionStatusIdle
	}
	return errors.WithStack(db.Create(item).Error)
}

func UpdateSubscription(item *model.Subscription) error {
	return errors.WithStack(db.Save(item).Error)
}

func UpdateSubscriptionTMDBEpisodeEnd(snapshot *model.Subscription, discoveredTMDBID *int64, episodeEnd int, syncedAt time.Time) (bool, error) {
	if snapshot == nil || snapshot.ID == 0 {
		return false, errors.New("subscription snapshot is required")
	}
	serializedSeasons, err := json.Marshal(snapshot.Seasons)
	if err != nil {
		return false, errors.WithStack(err)
	}
	updates := map[string]any{
		"latest_season_episode_end": episodeEnd,
		"tmdb_episode_synced_at":    syncedAt,
		"updated_at":                syncedAt,
	}
	if discoveredTMDBID != nil {
		updates["tmdb_id"] = *discoveredTMDBID
	}
	query := db.Model(&model.Subscription{}).
		Where(
			"id = ? AND tmdb_id = ? AND tmdb_name = ? AND tmdb_year = ? AND media_type = ? AND season = ? AND latest_season_episode_start = ? AND latest_season_episode_end = ? AND updated_at = ?",
			snapshot.ID,
			snapshot.TMDBID,
			snapshot.TMDBName,
			snapshot.TMDBYear,
			snapshot.MediaType,
			snapshot.Season,
			snapshot.LatestSeasonEpisodeStart,
			snapshot.LatestSeasonEpisodeEnd,
			snapshot.UpdatedAt,
		)
	if snapshot.Seasons == nil {
		query = query.Where("seasons IS NULL")
	} else {
		query = query.Where("seasons = ?", string(serializedSeasons))
	}
	result := query.Updates(updates)
	return result.RowsAffected > 0, errors.WithStack(result.Error)
}

func DeleteSubscription(id uint) error {
	return DeleteSubscriptionContext(context.Background(), id)
}

func DeleteSubscriptionContext(ctx context.Context, id uint) error {
	if ctx == nil {
		ctx = context.Background()
	}
	return errors.WithStack(db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Where(columnName("subscription_id")+" = ?", id).Delete(&model.ExternalSubscriptionRequest{}).Error; err != nil {
			return err
		}
		if err := tx.Where(columnName("subscription_id")+" = ?", id).Delete(&model.SubscriptionTelegramEvent{}).Error; err != nil {
			return err
		}
		if err := tx.Where(columnName("subscription_id")+" = ?", id).Delete(&model.SubscriptionRealtimeCandidate{}).Error; err != nil {
			return err
		}
		if err := tx.Where(columnName("subscription_id")+" = ?", id).Delete(&model.SubscriptionItem{}).Error; err != nil {
			return err
		}
		if err := tx.Where(columnName("subscription_id")+" = ?", id).Delete(&model.SubscriptionEpisodeSource{}).Error; err != nil {
			return err
		}
		if err := tx.Where(columnName("subscription_id")+" = ?", id).Delete(&model.SubscriptionRun{}).Error; err != nil {
			return err
		}
		return tx.Delete(&model.Subscription{}, id).Error
	}))
}

func GetSubscriptionByID(id uint) (*model.Subscription, error) {
	var item model.Subscription
	if err := db.First(&item, id).Error; err != nil {
		return nil, errors.WithStack(err)
	}
	return &item, nil
}

func ListSubscriptions(filter SubscriptionFilter) ([]model.Subscription, int64, error) {
	query := subscriptionListQuery(filter)
	var total int64
	if err := query.Count(&total).Error; err != nil {
		return nil, 0, errors.WithStack(err)
	}
	page := filter.Page
	if page < 1 {
		page = 1
	}
	perPage := filter.PerPage
	if perPage < 1 {
		perPage = 20
	}
	var items []model.Subscription
	if err := query.Order(columnName("updated_at") + " DESC").Offset((page - 1) * perPage).Limit(perPage).Find(&items).Error; err != nil {
		return nil, 0, errors.WithStack(err)
	}
	return items, total, nil
}

func ListAllSubscriptions(filter SubscriptionFilter) ([]model.Subscription, error) {
	var items []model.Subscription
	if err := subscriptionListQuery(filter).Order(columnName("updated_at") + " DESC").Find(&items).Error; err != nil {
		return nil, errors.WithStack(err)
	}
	return items, nil
}

func subscriptionListQuery(filter SubscriptionFilter) *gorm.DB {
	query := db.Model(&model.Subscription{})
	if keyword := strings.TrimSpace(filter.Keyword); keyword != "" {
		like := "%" + keyword + "%"
		query = query.Where(
			columnName("name")+" LIKE ? OR "+columnName("tmdb_name")+" LIKE ? OR "+columnName("target_root")+" LIKE ?",
			like, like, like,
		)
	}
	if sourceType := strings.TrimSpace(filter.SourceType); sourceType != "" {
		query = query.Where(columnName("source_type")+" = ?", sourceType)
	}
	if filter.Active != nil {
		query = query.Where(columnName("active")+" = ?", *filter.Active)
	}
	return query
}

func ListActiveSubscriptions() ([]model.Subscription, error) {
	var items []model.Subscription
	err := db.Where(columnName("active")+" = ?", true).Find(&items).Error
	return items, errors.WithStack(err)
}

func UpsertSubscriptionItem(item *model.SubscriptionItem) (*model.SubscriptionItem, bool, error) {
	return upsertSubscriptionItem(db, item)
}

func upsertSubscriptionItem(tx *gorm.DB, item *model.SubscriptionItem) (*model.SubscriptionItem, bool, error) {
	if item == nil {
		return nil, false, errors.New("subscription item is nil")
	}
	var existing model.SubscriptionItem
	err := tx.Where(columnName("subscription_id")+" = ? AND "+columnName("source_key")+" = ?", item.SubscriptionID, item.SourceKey).First(&existing).Error
	isNew := false
	if errors.Is(err, gorm.ErrRecordNotFound) {
		isNew = true
	} else if err != nil {
		return nil, false, errors.WithStack(err)
	}
	if isNew {
		if item.Status == "" {
			item.Status = model.SubscriptionItemStatusPending
		}
	} else if existing.FileHash != "" && existing.FileHash != item.FileHash {
		item.Status = model.SubscriptionItemStatusPending
	} else if item.Status == model.SubscriptionItemStatusPending &&
		existing.TargetPath != "" &&
		item.TargetPath != "" &&
		existing.TargetPath != item.TargetPath {
		item.LastError = ""
	} else {
		if item.Status == "" || item.Status == model.SubscriptionItemStatusPending {
			item.Status = existing.Status
			item.LastError = existing.LastError
		}
	}
	err = tx.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "subscription_id"}, {Name: "source_key"}},
		DoUpdates: clause.AssignmentColumns([]string{
			"source_provider",
			"source_url",
			"source_message_id",
			"source_message_channel",
			"source_message_url",
			"source_message_text",
			"source_path",
			"file_id",
			"file_path",
			"file_name",
			"file_size",
			"file_hash",
			"season",
			"episode",
			"target_dir",
			"target_name",
			"target_path",
			"status",
			"cluster_job_id",
			"last_seen_at",
			"last_error",
			"updated_at",
		}),
	}).Create(item).Error
	if err != nil {
		return nil, false, errors.WithStack(err)
	}
	saved, err := getSubscriptionItem(tx, item.SubscriptionID, item.SourceKey)
	return saved, isNew, err
}

func GetSubscriptionItem(subscriptionID uint, sourceKey string) (*model.SubscriptionItem, error) {
	return getSubscriptionItem(db, subscriptionID, sourceKey)
}

func getSubscriptionItem(tx *gorm.DB, subscriptionID uint, sourceKey string) (*model.SubscriptionItem, error) {
	var item model.SubscriptionItem
	err := tx.Where(columnName("subscription_id")+" = ? AND "+columnName("source_key")+" = ?", subscriptionID, sourceKey).First(&item).Error
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &item, nil
}

func ListSubscriptionItems(subscriptionID uint) ([]model.SubscriptionItem, error) {
	var items []model.SubscriptionItem
	err := db.Where(columnName("subscription_id")+" = ?", subscriptionID).
		Order(columnName("season") + ", " + columnName("episode") + ", " + columnName("file_path")).
		Find(&items).Error
	return items, errors.WithStack(err)
}

func ListSubscriptionItemsBySubscriptionIDs(subscriptionIDs []uint) ([]model.SubscriptionItem, error) {
	if len(subscriptionIDs) == 0 {
		return nil, nil
	}
	var items []model.SubscriptionItem
	err := db.Where(columnName("subscription_id")+" IN ?", subscriptionIDs).
		Order(columnName("subscription_id") + ", " + columnName("season") + ", " + columnName("episode") + ", " + columnName("file_path")).
		Find(&items).Error
	return items, errors.WithStack(err)
}

func UpsertSubscriptionEpisodeSource(item *model.SubscriptionEpisodeSource) (*model.SubscriptionEpisodeSource, error) {
	return upsertSubscriptionEpisodeSource(db, item)
}

const subscriptionEpisodeClaimTimeout = 5 * time.Minute

// TryClaimSubscriptionEpisodeSource atomically claims one season/episode slot
// before a cluster job is dispatched. The existing snapshot is the durable
// claim, so independent Telegram observations cannot both become active.
func TryClaimSubscriptionEpisodeSource(item *model.SubscriptionEpisodeSource, now time.Time) (bool, *model.SubscriptionEpisodeSource, error) {
	if item == nil {
		return false, nil, errors.New("subscription episode source is nil")
	}
	if now.IsZero() {
		now = time.Now()
	}
	item.SelectedAt = now
	item.Status = model.SubscriptionItemStatusTransferring
	item.ClusterJobID = ""

	var saved model.SubscriptionEpisodeSource
	err := db.Transaction(func(tx *gorm.DB) error {
		updates := map[string]any{
			"source_item_id":  item.SourceItemID,
			"source_type":     item.SourceType,
			"source_provider": item.SourceProvider,
			"share_url":       item.ShareURL,
			"file_name":       item.FileName,
			"file_hash":       item.FileHash,
			"status":          item.Status,
			"cluster_job_id":  "",
			"selected_at":     item.SelectedAt,
			"updated_at":      now,
		}
		staleBefore := now.Add(-subscriptionEpisodeClaimTimeout)
		result := tx.Model(&model.SubscriptionEpisodeSource{}).
			Where(columnName("subscription_id")+" = ? AND "+columnName("season")+" = ? AND "+columnName("episode")+" = ?", item.SubscriptionID, item.Season, item.Episode).
			Where("("+columnName("source_item_id")+" = ? AND "+columnName("file_hash")+" = ?) OR "+columnName("status")+" IN ? OR ("+columnName("cluster_job_id")+" = '' AND "+columnName("selected_at")+" < ?)",
				item.SourceItemID, item.FileHash,
				[]string{model.SubscriptionItemStatusFailed, model.SubscriptionItemStatusSkipped}, staleBefore).
			Updates(updates)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			candidate := *item
			if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&candidate).Error; err != nil {
				return err
			}
		}
		return tx.Where(
			columnName("subscription_id")+" = ? AND "+columnName("season")+" = ? AND "+columnName("episode")+" = ?",
			item.SubscriptionID, item.Season, item.Episode,
		).First(&saved).Error
	})
	if err != nil {
		return false, nil, errors.WithStack(err)
	}
	claimed := saved.SourceItemID == item.SourceItemID && saved.FileHash == item.FileHash
	return claimed, &saved, nil
}

func UpdateClaimedSubscriptionEpisodeSource(item *model.SubscriptionItem, status, clusterJobID string) error {
	if item == nil {
		return errors.New("subscription item is nil")
	}
	now := time.Now()
	updates := map[string]any{"status": status, "updated_at": now}
	if clusterJobID != "" {
		updates["cluster_job_id"] = clusterJobID
	}
	result := db.Model(&model.SubscriptionEpisodeSource{}).
		Where(columnName("subscription_id")+" = ? AND "+columnName("season")+" = ? AND "+columnName("episode")+" = ? AND "+columnName("source_item_id")+" = ? AND "+columnName("file_hash")+" = ?",
			item.SubscriptionID, item.Season, item.Episode, item.ID, item.FileHash).
		Updates(updates)
	if result.Error != nil {
		return errors.WithStack(result.Error)
	}
	if result.RowsAffected == 0 {
		return ErrStaleSubscriptionTerminalCallback
	}
	return nil
}

// ReleaseSubscriptionEpisodeSourceClaim removes an uncommitted pre-dispatch
// claim only when it is still owned by the same subscription item and hash.
// A claim that already carries a cluster job ID belongs to an accepted task
// and must never be removed by a late dispatch error path.
func ReleaseSubscriptionEpisodeSourceClaim(item *model.SubscriptionItem) error {
	if item == nil {
		return errors.New("subscription item is nil")
	}
	result := db.Where(
		columnName("subscription_id")+" = ? AND "+columnName("season")+" = ? AND "+columnName("episode")+" = ? AND "+
			columnName("source_item_id")+" = ? AND "+columnName("file_hash")+" = ? AND "+columnName("cluster_job_id")+" = ''",
		item.SubscriptionID, item.Season, item.Episode, item.ID, item.FileHash,
	).Delete(&model.SubscriptionEpisodeSource{})
	return errors.WithStack(result.Error)
}

func upsertSubscriptionEpisodeSource(tx *gorm.DB, item *model.SubscriptionEpisodeSource) (*model.SubscriptionEpisodeSource, error) {
	if item == nil {
		return nil, errors.New("subscription episode source is nil")
	}
	if item.SelectedAt.IsZero() {
		item.SelectedAt = time.Now()
	}
	err := tx.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "subscription_id"}, {Name: "season"}, {Name: "episode"}},
		DoUpdates: clause.AssignmentColumns([]string{
			"source_item_id",
			"source_type",
			"source_provider",
			"share_url",
			"file_name",
			"file_hash",
			"status",
			"cluster_job_id",
			"selected_at",
			"updated_at",
		}),
	}).Create(item).Error
	if err != nil {
		return nil, errors.WithStack(err)
	}
	var saved model.SubscriptionEpisodeSource
	err = tx.Where(
		columnName("subscription_id")+" = ? AND "+columnName("season")+" = ? AND "+columnName("episode")+" = ?",
		item.SubscriptionID,
		item.Season,
		item.Episode,
	).First(&saved).Error
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &saved, nil
}

func PersistAcceptedSubscriptionItemAndEpisodeSource(item *model.SubscriptionItem, source *model.SubscriptionEpisodeSource) (*model.SubscriptionItem, *model.SubscriptionEpisodeSource, error) {
	if item == nil {
		return nil, nil, errors.New("subscription item is nil")
	}
	if source == nil {
		return nil, nil, errors.New("subscription episode source is nil")
	}
	var savedItem *model.SubscriptionItem
	var savedSource *model.SubscriptionEpisodeSource
	err := db.Transaction(func(tx *gorm.DB) error {
		var err error
		savedItem, _, err = upsertSubscriptionItem(tx, item)
		if err != nil {
			return err
		}
		source.SourceItemID = savedItem.ID
		source.FileHash = savedItem.FileHash
		source.Status = savedItem.Status
		savedSource, err = upsertSubscriptionEpisodeSource(tx, source)
		return err
	})
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	return savedItem, savedSource, nil
}

func PersistSubscriptionTerminalItem(request SubscriptionTerminalItemRequest) (*model.SubscriptionItem, error) {
	if request.ItemID == 0 {
		return nil, errors.New("subscription item id is required")
	}
	if request.SubscriptionID == 0 {
		return nil, errors.New("subscription id is required")
	}
	if request.SourceKey == "" {
		return nil, errors.New("subscription source key is required")
	}
	if request.ExpectedStatus == "" {
		return nil, errors.New("expected subscription item status is required")
	}
	if request.TerminalStatus == "" {
		return nil, errors.New("terminal subscription item status is required")
	}
	var savedItem *model.SubscriptionItem
	err := db.Transaction(func(tx *gorm.DB) error {
		identity := subscriptionTerminalItemIdentityQuery(tx, request)
		if request.RecoverySource != nil {
			var current model.SubscriptionItem
			if err := identity.Clauses(clause.Locking{Strength: "UPDATE"}).First(&current).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return ErrStaleSubscriptionTerminalCallback
				}
				return errors.WithStack(err)
			}
			source := request.RecoverySource
			if source.SelectedAt.IsZero() {
				source.SelectedAt = time.Now()
			}
			source.SourceItemID = request.ItemID
			source.FileHash = request.ExpectedFileHash
			source.Status = request.TerminalStatus
			if err := tx.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "subscription_id"}, {Name: "season"}, {Name: "episode"}},
				DoNothing: true,
			}).Create(source).Error; err != nil {
				return errors.WithStack(err)
			}
		}
		updatedAt := time.Now()
		updates := map[string]any{
			"status":     request.TerminalStatus,
			"last_error": request.TerminalLastError,
			"updated_at": updatedAt,
		}
		if request.TerminalClusterJobID != nil {
			updates["cluster_job_id"] = *request.TerminalClusterJobID
		}
		result := subscriptionTerminalItemIdentityQuery(tx, request).Updates(updates)
		if result.Error != nil {
			return errors.WithStack(result.Error)
		}
		if result.RowsAffected == 0 {
			return ErrStaleSubscriptionTerminalCallback
		}
		if err := tx.Model(&model.SubscriptionEpisodeSource{}).
			Where(
				columnName("source_item_id")+" = ? AND "+columnName("file_hash")+" = ?",
				request.ItemID,
				request.ExpectedFileHash,
			).
			Updates(map[string]any{
				"status":     request.TerminalStatus,
				"updated_at": updatedAt,
			}).Error; err != nil {
			return errors.WithStack(err)
		}
		var err error
		savedItem, err = getSubscriptionItem(tx, request.SubscriptionID, request.SourceKey)
		return err
	})
	if err != nil {
		if errors.Is(err, ErrStaleSubscriptionTerminalCallback) {
			return nil, ErrStaleSubscriptionTerminalCallback
		}
		return nil, errors.WithStack(err)
	}
	return savedItem, nil
}

func subscriptionTerminalItemIdentityQuery(tx *gorm.DB, request SubscriptionTerminalItemRequest) *gorm.DB {
	query := tx.Model(&model.SubscriptionItem{}).Where(
		"id = ? AND "+columnName("subscription_id")+" = ? AND "+columnName("source_key")+" = ? AND "+columnName("file_hash")+" = ? AND "+columnName("status")+" = ?",
		request.ItemID,
		request.SubscriptionID,
		request.SourceKey,
		request.ExpectedFileHash,
		request.ExpectedStatus,
	)
	if request.ExpectedClusterJobID != nil {
		query = query.Where(columnName("cluster_job_id")+" = ?", *request.ExpectedClusterJobID)
	}
	return query
}

func RecoverSubscriptionEpisodeSourceAndTerminalItem(item *model.SubscriptionItem, source *model.SubscriptionEpisodeSource) (*model.SubscriptionItem, error) {
	if item == nil {
		return nil, errors.New("subscription item is nil")
	}
	return PersistSubscriptionTerminalItem(SubscriptionTerminalItemRequest{
		ItemID:            item.ID,
		SubscriptionID:    item.SubscriptionID,
		SourceKey:         item.SourceKey,
		ExpectedFileHash:  item.FileHash,
		ExpectedStatus:    model.SubscriptionItemStatusPending,
		TerminalStatus:    item.Status,
		TerminalLastError: item.LastError,
		RecoverySource:    source,
	})
}

func ListSubscriptionEpisodeSources(subscriptionID uint) ([]model.SubscriptionEpisodeSource, error) {
	var items []model.SubscriptionEpisodeSource
	err := db.Where(columnName("subscription_id")+" = ?", subscriptionID).
		Order(columnName("season") + ", " + columnName("episode")).
		Find(&items).Error
	return items, errors.WithStack(err)
}

func CreateSubscriptionRun(run *model.SubscriptionRun) error {
	return errors.WithStack(db.Create(run).Error)
}

func UpdateSubscriptionRun(run *model.SubscriptionRun) error {
	return errors.WithStack(db.Save(run).Error)
}

func DeleteSubscriptionRun(id uint) error {
	return errors.WithStack(db.Delete(&model.SubscriptionRun{}, id).Error)
}

func ClearFailedSubscriptionRuns() (int64, error) {
	result := db.Where(subscriptionRunFailureCondition(""), model.SubscriptionStatusFailed, "").
		Delete(&model.SubscriptionRun{})
	return result.RowsAffected, errors.WithStack(result.Error)
}

func GetSubscriptionBoard(filter SubscriptionRunFilter) (*model.SubscriptionBoard, error) {
	board := &model.SubscriptionBoard{}
	subscriptionQuery := subscriptionListQuery(SubscriptionFilter{
		Keyword:    filter.Keyword,
		SourceType: filter.SourceType,
	})
	if filter.SubscriptionID > 0 {
		subscriptionQuery = subscriptionQuery.Where(columnName("id")+" = ?", filter.SubscriptionID)
	}
	if err := subscriptionQuery.Count(&board.SubscriptionCount).Error; err != nil {
		return nil, errors.WithStack(err)
	}

	var changedResult struct {
		ChangedRunCount int64
		AddedCount      int64
		ChangedCount    int64
	}
	changedQuery := subscriptionRunBaseQuery(filter).
		Where(subscriptionRunChangesCondition("subscription_runs"), model.SubscriptionStatusSuccess)
	if err := changedQuery.Select(
		"COUNT(*) AS changed_run_count, COALESCE(SUM(added_count), 0) AS added_count, COALESCE(SUM(changed_count), 0) AS changed_count",
	).Scan(&changedResult).Error; err != nil {
		return nil, errors.WithStack(err)
	}
	board.ChangedRunCount = changedResult.ChangedRunCount
	board.AddedCount = changedResult.AddedCount
	board.ChangedCount = changedResult.ChangedCount

	var failureResult struct {
		FailureCount int64
	}
	if err := subscriptionRunBaseQuery(filter).
		Where(subscriptionRunFailureCondition("subscription_runs"), model.SubscriptionStatusFailed, "").
		Select("COUNT(*) AS failure_count").
		Scan(&failureResult).Error; err != nil {
		return nil, errors.WithStack(err)
	}
	board.FailureCount = failureResult.FailureCount
	return board, nil
}

func ListSubscriptionRuns(filter SubscriptionRunFilter) ([]model.SubscriptionRun, int64, error) {
	query := subscriptionRunQuery(filter)
	var total int64
	if err := query.Count(&total).Error; err != nil {
		return nil, 0, errors.WithStack(err)
	}
	page := filter.Page
	if page < 1 {
		page = 1
	}
	perPage := filter.PerPage
	if perPage < 1 {
		perPage = 20
	}
	var items []model.SubscriptionRun
	err := query.Select("subscription_runs.*, subscriptions.name AS subscription_name, subscriptions.source_type AS subscription_source_type").
		Order("subscription_runs.started_at DESC").
		Offset((page - 1) * perPage).
		Limit(perPage).
		Find(&items).Error
	return items, total, errors.WithStack(err)
}

func ListSubscriptionEpisodeSourceDetails(subscriptionID uint) ([]model.SubscriptionEpisodeSourceDetail, error) {
	var items []model.SubscriptionEpisodeSourceDetail
	episodeSourceTable := modelTableName("SubscriptionEpisodeSource")
	subscriptionItemTable := modelTableName("SubscriptionItem")
	clusterJobTable := modelTableName("ClusterJob")
	clusterNodeTable := modelTableName("ClusterNode")
	clusterJobAttemptTable := modelTableName("ClusterJobAttempt")
	clusterJobStageTable := modelTableName("ClusterJobStage")
	subscriptionTable := modelTableName("Subscription")
	etfArchiveTable := modelTableName("ETFArchiveRecord")
	err := db.Table("? AS subscription_episode_sources", clause.Table{Name: episodeSourceTable}).
		Select(strings.Join([]string{
			"subscription_episode_sources.id",
			"subscription_episode_sources.created_at",
			"subscription_episode_sources.updated_at",
			"subscription_episode_sources.subscription_id",
			"subscription_episode_sources.season",
			"subscription_episode_sources.episode",
			"subscription_episode_sources.source_item_id",
			"subscription_episode_sources.source_type",
			"subscription_episode_sources.source_provider",
			"subscription_episode_sources.share_url",
			"subscription_episode_sources.file_name",
			"subscription_episode_sources.cluster_job_id",
			"subscription_episode_sources.selected_at",
			"CASE WHEN subscription_items.id IS NOT NULL THEN subscription_items.status ELSE subscription_episode_sources.status END AS status",
			"CASE WHEN subscription_items.id IS NOT NULL THEN subscription_items.last_error ELSE '' END AS item_last_error",
			"cluster_jobs.status AS job_status",
			"cluster_jobs.notification_status AS job_notification_status",
			"cluster_jobs.current_generation AS job_generation",
			"cluster_jobs.started_at AS job_started_at",
			"cluster_jobs.finished_at AS job_finished_at",
			"cluster_jobs.last_error_code AS job_last_error_code",
			"cluster_jobs.last_error AS job_last_error",
			"current_stage.name AS current_stage",
			"current_stage.status AS current_stage_status",
			"current_stage.retry_count AS current_stage_retry_count",
			"current_stage.error_code AS current_stage_error_code",
			"current_stage.error AS current_stage_error",
			"EXISTS (SELECT 1 FROM ? AS archived_etf JOIN ? AS source_subscription ON source_subscription.id = subscription_episode_sources.subscription_id " +
				"WHERE archived_etf.tmdb_id = source_subscription.tmdb_id AND archived_etf.media_type = source_subscription.media_type " +
				"AND archived_etf.season = subscription_episode_sources.season AND archived_etf.episode = subscription_episode_sources.episode " +
				"AND archived_etf.status IN ('archived', 'corrected')) AS has_archived_etf",
			"CASE " +
				"WHEN subscription_episode_sources.cluster_job_id = '' OR subscription_episode_sources.cluster_job_id IS NULL THEN '本机' " +
				"WHEN assigned_nodes.name IS NOT NULL AND assigned_nodes.name <> '' THEN assigned_nodes.name " +
				"WHEN attempt_nodes.name IS NOT NULL AND attempt_nodes.name <> '' THEN attempt_nodes.name " +
				"ELSE '未指派' END AS worker_name",
		}, ", "), clause.Table{Name: etfArchiveTable}, clause.Table{Name: subscriptionTable}).
		Joins("LEFT JOIN ? AS subscription_items ON subscription_items.id = subscription_episode_sources.source_item_id AND subscription_items.file_hash = subscription_episode_sources.file_hash", clause.Table{Name: subscriptionItemTable}).
		Joins("LEFT JOIN ? AS cluster_jobs ON cluster_jobs.id = subscription_episode_sources.cluster_job_id", clause.Table{Name: clusterJobTable}).
		Joins(
			"LEFT JOIN ? AS current_stage ON current_stage.id = ("+
				"SELECT stage_candidates.id FROM ? AS stage_candidates "+
				"WHERE stage_candidates.job_id = cluster_jobs.id "+
				"AND (cluster_jobs.current_attempt_id = '' OR stage_candidates.attempt_id = cluster_jobs.current_attempt_id) "+
				"ORDER BY CASE stage_candidates.status "+
				"WHEN 'running' THEN 0 WHEN 'permitted' THEN 1 WHEN 'failed' THEN 2 WHEN 'pending' THEN 3 ELSE 4 END, "+
				"stage_candidates.updated_at DESC, stage_candidates.id DESC LIMIT 1"+
				")",
			clause.Table{Name: clusterJobStageTable},
			clause.Table{Name: clusterJobStageTable},
		).
		Joins("LEFT JOIN ? AS assigned_nodes ON assigned_nodes.id = cluster_jobs.assigned_node_id", clause.Table{Name: clusterNodeTable}).
		Joins(
			"LEFT JOIN ? AS latest_attempt ON latest_attempt.id = ("+
				"SELECT attempt_candidates.id FROM ? AS attempt_candidates "+
				"WHERE attempt_candidates.job_id = subscription_episode_sources.cluster_job_id "+
				"ORDER BY attempt_candidates.generation DESC, attempt_candidates.id DESC LIMIT 1"+
				")",
			clause.Table{Name: clusterJobAttemptTable},
			clause.Table{Name: clusterJobAttemptTable},
		).
		Joins("LEFT JOIN ? AS attempt_nodes ON attempt_nodes.id = latest_attempt.node_id", clause.Table{Name: clusterNodeTable}).
		Where("subscription_episode_sources.subscription_id = ?", subscriptionID).
		Order("subscription_episode_sources.season, subscription_episode_sources.episode").
		Scan(&items).Error
	if err == nil {
		activeNotificationJobs, activeErr := activeETFNotificationClusterJobIDs()
		if activeErr != nil {
			return nil, activeErr
		}
		for i := range items {
			items[i].EffectiveStatus = items[i].Status
			if items[i].Status == model.SubscriptionItemStatusFailed && items[i].HasArchivedETF {
				items[i].EffectiveStatus = "historical_succeeded_latest_failed"
			}
			items[i].NotificationDisplayStatus = items[i].JobNotificationStatus
			if isTerminalFailedClusterJobStatus(items[i].JobStatus) &&
				(items[i].JobNotificationStatus == model.ClusterNotificationStatusPending || items[i].JobNotificationStatus == "") {
				if _, active := activeNotificationJobs[items[i].ClusterJobID]; !active {
					items[i].NotificationDisplayStatus = model.ClusterNotificationStatusNotStarted
				}
			}
			if isTerminalFailedClusterJobStatus(items[i].JobStatus) && isNonTerminalClusterStageStatus(items[i].CurrentStageStatus) {
				items[i].CurrentStageStatus = model.ClusterStageStatusFailed
				if items[i].CurrentStageError == "" {
					items[i].CurrentStageError = items[i].JobLastError
				}
			}
		}
	}
	return items, errors.WithStack(err)
}

func activeETFNotificationClusterJobIDs() (map[string]struct{}, error) {
	var jobs []model.ETFSubscriptionJob
	if err := db.Select("cluster_job_ids_json").
		Where("status IN ?", []string{
			model.ETFSubscriptionJobStatusPending,
			model.ETFSubscriptionJobStatusRunning,
			model.ETFSubscriptionJobStatusFailed,
		}).
		Find(&jobs).Error; err != nil {
		return nil, errors.WithStack(err)
	}
	ids := make(map[string]struct{})
	for i := range jobs {
		var jobIDs []string
		if raw := strings.TrimSpace(jobs[i].ClusterJobIDsJSON); raw != "" {
			if err := json.Unmarshal([]byte(raw), &jobIDs); err != nil {
				return nil, errors.Wrap(err, "decode ETF notification cluster job IDs")
			}
		}
		for _, id := range jobIDs {
			if id = strings.TrimSpace(id); id != "" {
				ids[id] = struct{}{}
			}
		}
	}
	return ids, nil
}

func isTerminalFailedClusterJobStatus(status string) bool {
	switch status {
	case model.ClusterJobStatusFailed, model.ClusterJobStatusPartialFailed, model.ClusterJobStatusCancelled, model.ClusterJobStatusDeadLetter:
		return true
	default:
		return false
	}
}

func isNonTerminalClusterStageStatus(status string) bool {
	switch status {
	case model.ClusterStageStatusPending, model.ClusterStageStatusPermitted, model.ClusterStageStatusRunning:
		return true
	default:
		return false
	}
}

func subscriptionRunQuery(filter SubscriptionRunFilter) *gorm.DB {
	query := subscriptionRunBaseQuery(filter)
	switch strings.TrimSpace(filter.View) {
	case model.SubscriptionRunViewChanges:
		return query.Where(subscriptionRunChangesCondition("subscription_runs"), model.SubscriptionStatusSuccess)
	case model.SubscriptionRunViewFailures:
		return query.Where(subscriptionRunFailureCondition("subscription_runs"), model.SubscriptionStatusFailed, "")
	default:
		return query.Where(meaningfulSubscriptionRunCondition("subscription_runs"), model.SubscriptionStatusSuccess, "")
	}
}

func subscriptionRunBaseQuery(filter SubscriptionRunFilter) *gorm.DB {
	subscriptionRunTable := modelTableName("SubscriptionRun")
	subscriptionTable := modelTableName("Subscription")
	query := db.Model(&model.SubscriptionRun{}).
		Table("? AS subscription_runs", clause.Table{Name: subscriptionRunTable}).
		Joins("JOIN ? AS subscriptions ON subscriptions.id = subscription_runs.subscription_id", clause.Table{Name: subscriptionTable})
	if filter.SubscriptionID > 0 {
		query = query.Where("subscription_runs.subscription_id = ?", filter.SubscriptionID)
	}
	if keyword := strings.TrimSpace(filter.Keyword); keyword != "" {
		like := "%" + keyword + "%"
		query = query.Where(
			"subscriptions.name LIKE ? OR subscriptions.tmdb_name LIKE ? OR subscriptions.target_root LIKE ?",
			like, like, like,
		)
	}
	if sourceType := strings.TrimSpace(filter.SourceType); sourceType != "" {
		query = query.Where("subscriptions.source_type = ?", sourceType)
	}
	if status := strings.TrimSpace(filter.Status); status != "" {
		query = query.Where("subscription_runs.status = ?", status)
	}
	return query
}

func meaningfulSubscriptionRunCondition(table string) string {
	return "(" +
		qualifiedColumnName(table, "status") + " <> ? OR " +
		qualifiedColumnName(table, "added_count") + " > 0 OR " +
		qualifiedColumnName(table, "changed_count") + " > 0 OR " +
		qualifiedColumnName(table, "transferred_count") + " > 0 OR " +
		qualifiedColumnName(table, "error") + " <> ?" +
		")"
}

func subscriptionRunChangesCondition(table string) string {
	return qualifiedColumnName(table, "status") + " = ? AND (" + qualifiedColumnName(table, "added_count") + " > 0 OR " + qualifiedColumnName(table, "changed_count") + " > 0)"
}

func subscriptionRunFailureCondition(table string) string {
	return "(" + qualifiedColumnName(table, "status") + " = ? OR " + qualifiedColumnName(table, "error") + " <> ?)"
}

func qualifiedColumnName(table, name string) string {
	if strings.TrimSpace(table) == "" {
		return columnName(name)
	}
	return table + "." + columnName(name)
}

func modelTableName(modelName string) string {
	return db.NamingStrategy.TableName(modelName)
}
