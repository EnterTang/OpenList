package repair

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	appdb "github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/etfauto"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/pkg/errors"
	"gorm.io/gorm"
)

type SubscriptionStatusOptions struct {
	Apply                   bool
	ReconcileUnknown        bool
	DeclareTargetIdempotent bool
	HTTPClient              *http.Client
	Timeout                 time.Duration
	Limit                   int
}

type SubscriptionStatusReport struct {
	Applied                         bool     `json:"applied"`
	NotificationStatusesConverged   int64    `json:"notification_statuses_converged"`
	StagesConverged                 int64    `json:"stages_converged"`
	EpisodeSnapshotsRestored        int64    `json:"episode_snapshots_restored"`
	NoChangeNotificationsCompleted  int64    `json:"no_change_notifications_completed"`
	MaterializedJobsConverged       int64    `json:"materialized_jobs_converged"`
	OrphanNotificationJobsQueued    int      `json:"orphan_notification_jobs_queued"`
	OrphanClusterJobsLinked         int64    `json:"orphan_cluster_jobs_linked"`
	IdempotencyCapabilitiesDeclared int64    `json:"idempotency_capabilities_declared"`
	ReconciledUnknownJobs           int      `json:"reconciled_unknown_jobs"`
	RestoredSubscriptionEpisodeKeys []string `json:"restored_subscription_episode_keys,omitempty"`
}

func RepairSubscriptionStatuses(ctx context.Context, database *gorm.DB, opts SubscriptionStatusOptions) (*SubscriptionStatusReport, error) {
	if database == nil {
		return nil, errors.New("database is not initialized")
	}
	tx := database.WithContext(ctx).Begin()
	if tx.Error != nil {
		return nil, tx.Error
	}
	committed := false
	previousDB := appdb.GetDb()
	defer func() {
		if !committed {
			_ = tx.Rollback().Error
		}
		appdb.UseConnection(previousDB)
	}()
	appdb.UseConnection(tx)

	report := &SubscriptionStatusReport{Applied: opts.Apply}
	if err := convergeTerminalNotificationStatuses(tx, report); err != nil {
		return nil, err
	}
	if err := convergeTerminalStages(tx, report); err != nil {
		return nil, err
	}
	if err := restoreArchivedEpisodeSnapshots(tx, report); err != nil {
		return nil, err
	}
	if opts.DeclareTargetIdempotent {
		count, err := declareTargetIdempotencyTx(tx)
		if err != nil {
			return nil, err
		}
		report.IdempotencyCapabilitiesDeclared = count
	}
	materialized, err := convergeMaterializedJobs(tx)
	if err != nil {
		return nil, err
	}
	report.MaterializedJobsConverged = materialized
	completed, err := etfauto.ReconcileNoChangeBatchNotifications(ctx)
	if err != nil {
		return nil, err
	}
	report.NoChangeNotificationsCompleted = completed
	if opts.DeclareTargetIdempotent {
		queued, linked, err := etfauto.QueueOrphanBatchNotifications(ctx)
		if err != nil {
			return nil, err
		}
		report.OrphanNotificationJobsQueued = queued
		report.OrphanClusterJobsLinked = linked
	}
	if opts.ReconcileUnknown {
		count, err := etfauto.ReconcileUnknownJobs(ctx, etfauto.RunnerOptions{
			HTTPClient: opts.HTTPClient,
			Timeout:    opts.Timeout,
			Limit:      opts.Limit,
			Now:        time.Now().UTC(),
		})
		if err != nil {
			return nil, err
		}
		report.ReconciledUnknownJobs = count
	}
	if !opts.Apply {
		if err := tx.Rollback().Error; err != nil {
			return nil, err
		}
		committed = true
		return report, nil
	}
	if err := tx.Commit().Error; err != nil {
		return nil, err
	}
	committed = true
	return report, nil
}

func convergeMaterializedJobs(tx *gorm.DB) (int64, error) {
	manifestTable, err := tableName(tx, &model.ClusterUploadManifest{})
	if err != nil {
		return 0, err
	}
	jobTable, err := tableName(tx, &model.ClusterJob{})
	if err != nil {
		return 0, err
	}
	result := tx.Model(&model.ClusterJob{}).
		Where("status IN ?", []string{model.ClusterJobStatusRunning, model.ClusterJobStatusLeased}).
		Where("EXISTS (SELECT 1 FROM "+manifestTable+" AS consumed_manifest WHERE consumed_manifest.job_id = "+jobTable+".id AND consumed_manifest.status = ?)", model.ClusterUploadManifestStatusConsumed).
		Updates(map[string]any{"status": model.ClusterJobStatusSucceeded, "last_error_code": "", "last_error": ""})
	return result.RowsAffected, result.Error
}

func declareTargetIdempotencyTx(tx *gorm.DB) (int64, error) {
	rootResult := tx.Model(&model.ETFMediaRoot{}).
		Where("target_base_url <> '' AND target_supports_idempotency = ?", false).
		Update("target_supports_idempotency", true)
	if rootResult.Error != nil {
		return 0, rootResult.Error
	}
	jobResult := tx.Model(&model.ETFSubscriptionJob{}).
		Where("target_base_url <> '' AND target_supports_idempotency = ?", false).
		Update("target_supports_idempotency", true)
	if jobResult.Error != nil {
		return 0, jobResult.Error
	}
	return rootResult.RowsAffected + jobResult.RowsAffected, nil
}

func convergeTerminalNotificationStatuses(tx *gorm.DB, report *SubscriptionStatusReport) error {
	var jobs []model.ClusterJob
	if err := tx.Select("id").
		Where("type = ? AND status IN ? AND (notification_status = ? OR notification_status = '')",
			model.ClusterJobTypeMediaTransfer,
			[]string{model.ClusterJobStatusFailed, model.ClusterJobStatusPartialFailed, model.ClusterJobStatusCancelled, model.ClusterJobStatusDeadLetter},
			model.ClusterNotificationStatusPending,
		).
		Find(&jobs).Error; err != nil {
		return err
	}
	activeIDs, err := activeNotificationClusterJobIDsTx(tx)
	if err != nil {
		return err
	}
	staleIDs := make([]string, 0, len(jobs))
	for i := range jobs {
		if _, active := activeIDs[jobs[i].ID]; !active {
			staleIDs = append(staleIDs, jobs[i].ID)
		}
	}
	if len(staleIDs) == 0 {
		return nil
	}
	result := tx.Model(&model.ClusterJob{}).Where("id IN ?", staleIDs).
		Update("notification_status", model.ClusterNotificationStatusNotStarted)
	if result.Error != nil {
		return result.Error
	}
	report.NotificationStatusesConverged = result.RowsAffected
	return nil
}

func activeNotificationClusterJobIDsTx(tx *gorm.DB) (map[string]struct{}, error) {
	var jobs []model.ETFSubscriptionJob
	if err := tx.Select("cluster_job_ids_json").Where("status IN ?", []string{
		model.ETFSubscriptionJobStatusPending,
		model.ETFSubscriptionJobStatusRunning,
		model.ETFSubscriptionJobStatusFailed,
	}).Find(&jobs).Error; err != nil {
		return nil, err
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

func convergeTerminalStages(tx *gorm.DB, report *SubscriptionStatusReport) error {
	jobTable, err := tableName(tx, &model.ClusterJob{})
	if err != nil {
		return err
	}
	stageTable, err := tableName(tx, &model.ClusterJobStage{})
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	result := tx.Model(&model.ClusterJobStage{}).
		Where("status IN ?", []string{model.ClusterStageStatusPending, model.ClusterStageStatusPermitted, model.ClusterStageStatusRunning}).
		Where("EXISTS (SELECT 1 FROM "+jobTable+" AS terminal_job WHERE terminal_job.id = "+stageTable+".job_id AND terminal_job.current_attempt_id = "+stageTable+".attempt_id AND terminal_job.status IN ?)",
			[]string{model.ClusterJobStatusFailed, model.ClusterJobStatusPartialFailed, model.ClusterJobStatusCancelled, model.ClusterJobStatusDeadLetter}).
		Updates(map[string]any{
			"status":      model.ClusterStageStatusFailed,
			"finished_at": now,
			"error":       gorm.Expr("COALESCE(NULLIF(error, ''), (SELECT last_error FROM " + jobTable + " WHERE id = " + stageTable + ".job_id))"),
		})
	if result.Error != nil {
		return result.Error
	}
	report.StagesConverged = result.RowsAffected
	return nil
}

func restoreArchivedEpisodeSnapshots(tx *gorm.DB, report *SubscriptionStatusReport) error {
	var sources []model.SubscriptionEpisodeSource
	if err := tx.Find(&sources).Error; err != nil {
		return err
	}
	itemTable, err := tableName(tx, &model.SubscriptionItem{})
	if err != nil {
		return err
	}
	manifestTable, err := tableName(tx, &model.ClusterUploadManifest{})
	if err != nil {
		return err
	}
	jobTable, err := tableName(tx, &model.ClusterJob{})
	if err != nil {
		return err
	}
	for i := range sources {
		source := &sources[i]
		var currentItem model.SubscriptionItem
		if err := tx.First(&currentItem, source.SourceItemID).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				continue
			}
			return err
		}
		if currentItem.Status != model.SubscriptionItemStatusFailed {
			continue
		}
		var subscription model.Subscription
		if err := tx.First(&subscription, source.SubscriptionID).Error; err != nil {
			return err
		}
		var archivedCount int64
		if err := tx.Model(&model.ETFArchiveRecord{}).
			Where("tmdb_id = ? AND media_type = ? AND season = ? AND episode = ? AND status IN ?",
				subscription.TMDBID, subscription.MediaType, source.Season, source.Episode,
				[]string{model.ETFArchiveStatusArchived, model.ETFArchiveStatusCorrected, model.ETFArchiveStatusRelocated}).
			Count(&archivedCount).Error; err != nil {
			return err
		}
		if archivedCount == 0 {
			continue
		}
		var candidate model.SubscriptionItem
		err := tx.Table(itemTable+" AS candidate_items").
			Select("candidate_items.*").
			Joins("JOIN "+manifestTable+" AS manifests ON manifests.subscription_item_id = candidate_items.id AND manifests.subscription_id = candidate_items.subscription_id").
			Joins("JOIN "+jobTable+" AS successful_jobs ON successful_jobs.id = manifests.job_id").
			Where("candidate_items.subscription_id = ? AND candidate_items.season = ? AND candidate_items.episode = ?", source.SubscriptionID, source.Season, source.Episode).
			Where("manifests.status = ? AND successful_jobs.status = ?", model.ClusterUploadManifestStatusConsumed, model.ClusterJobStatusSucceeded).
			Order("successful_jobs.finished_at DESC, manifests.id DESC").
			First(&candidate).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		if candidate.ID == source.SourceItemID && candidate.FileHash == source.FileHash {
			continue
		}
		updates := map[string]any{
			"source_item_id":  candidate.ID,
			"source_type":     subscription.SourceType,
			"source_provider": candidate.SourceProvider,
			"share_url":       candidate.SourceURL,
			"file_name":       candidate.FileName,
			"file_hash":       candidate.FileHash,
			"status":          candidate.Status,
			"cluster_job_id":  candidate.ClusterJobID,
			"selected_at":     candidate.UpdatedAt,
		}
		if err := tx.Model(source).Updates(updates).Error; err != nil {
			return err
		}
		report.EpisodeSnapshotsRestored++
		report.RestoredSubscriptionEpisodeKeys = append(report.RestoredSubscriptionEpisodeKeys,
			strings.Join([]string{subscription.Name, seasonEpisodeKey(source.Season, source.Episode)}, " "))
	}
	return nil
}

func seasonEpisodeKey(season, episode int) string {
	return "S" + twoDigits(season) + "E" + twoDigits(episode)
}

func twoDigits(value int) string {
	return fmt.Sprintf("%02d", value)
}

func tableName(database *gorm.DB, value any) (string, error) {
	statement := &gorm.Statement{DB: database}
	if err := statement.Parse(value); err != nil {
		return "", err
	}
	return statement.Schema.Table, nil
}
