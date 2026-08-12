package repair

import (
	"context"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/pkg/errors"
	"gorm.io/gorm"
)

type SourceReadEOFOptions struct {
	Apply        bool
	Limit        int
	AttemptLimit uint64
}

type SourceReadEOFReport struct {
	Applied             bool     `json:"applied"`
	Candidates          int      `json:"candidates"`
	Queued              int      `json:"queued"`
	SkippedManifest     int      `json:"skipped_manifest"`
	SkippedAttemptLimit int      `json:"skipped_attempt_limit"`
	SkippedNonTerminal  int      `json:"skipped_non_terminal"`
	QueuedJobIDs        []string `json:"queued_job_ids,omitempty"`
}

func RepairSourceReadEOF(ctx context.Context, database *gorm.DB, opts SourceReadEOFOptions) (*SourceReadEOFReport, error) {
	if database == nil {
		return nil, errors.New("database is not initialized")
	}
	if opts.Limit <= 0 {
		opts.Limit = 100
	}
	if opts.AttemptLimit == 0 {
		opts.AttemptLimit = 3
	}
	tx := database.WithContext(ctx).Begin()
	if tx.Error != nil {
		return nil, tx.Error
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback().Error
		}
	}()

	report := &SourceReadEOFReport{Applied: opts.Apply}
	var jobs []model.ClusterJob
	if err := tx.Where("type = ? AND status = ? AND source_provider = ?", model.ClusterJobTypeMediaTransfer, model.ClusterJobStatusFailed, "pan123").
		Where("last_error LIKE ? AND last_error LIKE ?", "%failed to read all data%", "%unexpected EOF%").
		Order("created_at ASC").Limit(opts.Limit).Find(&jobs).Error; err != nil {
		return nil, err
	}
	report.Candidates = len(jobs)
	now := time.Now().UTC()
	for i := range jobs {
		job := &jobs[i]
		if job.CurrentGeneration >= opts.AttemptLimit {
			report.SkippedAttemptLimit++
			continue
		}
		var manifestCount int64
		if err := tx.Model(&model.ClusterUploadManifest{}).Where("job_id = ?", job.ID).Count(&manifestCount).Error; err != nil {
			return nil, err
		}
		if manifestCount > 0 {
			report.SkippedManifest++
			continue
		}
		if job.Status != model.ClusterJobStatusFailed {
			report.SkippedNonTerminal++
			continue
		}
		if !opts.Apply {
			report.Queued++
			report.QueuedJobIDs = append(report.QueuedJobIDs, job.ID)
			continue
		}
		updates := map[string]any{
			"status":              model.ClusterJobStatusQueued,
			"available_at":        now,
			"assigned_node_id":    "",
			"current_attempt_id":  "",
			"finished_at":         nil,
			"archived_at":         nil,
			"notification_status": model.ClusterNotificationStatusNotStarted,
			"last_error_code":     "source_unexpected_eof",
		}
		result := tx.Model(&model.ClusterJob{}).Where("id = ? AND status = ?", job.ID, model.ClusterJobStatusFailed).Updates(updates)
		if result.Error != nil {
			return nil, result.Error
		}
		if result.RowsAffected != 1 {
			report.SkippedNonTerminal++
			continue
		}
		if job.SubscriptionItemID != 0 {
			if err := tx.Model(&model.SubscriptionItem{}).
				Where("id = ? AND cluster_job_id = ? AND status = ?", job.SubscriptionItemID, job.ID, model.SubscriptionItemStatusFailed).
				Updates(map[string]any{"status": model.SubscriptionItemStatusTransferring}).Error; err != nil {
				return nil, err
			}
		}
		if err := reconcileRepairParent(tx, job.ParentJobID, now); err != nil {
			return nil, err
		}
		report.Queued++
		report.QueuedJobIDs = append(report.QueuedJobIDs, job.ID)
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

func reconcileRepairParent(tx *gorm.DB, parentJobID string, now time.Time) error {
	parentJobID = strings.TrimSpace(parentJobID)
	if parentJobID == "" {
		return nil
	}
	var parent model.ClusterJob
	if err := tx.Select("expected_items").First(&parent, "id = ?", parentJobID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		return err
	}
	var children []model.ClusterJob
	if err := tx.Select("status").Where("parent_job_id = ?", parentJobID).Find(&children).Error; err != nil {
		return err
	}
	if len(children) == 0 {
		return nil
	}
	succeeded, failed := 0, 0
	for i := range children {
		switch children[i].Status {
		case model.ClusterJobStatusSucceeded:
			succeeded++
		case model.ClusterJobStatusFailed, model.ClusterJobStatusDeadLetter, model.ClusterJobStatusCancelled:
			failed++
		}
	}
	updates := map[string]any{"status": model.ClusterJobStatusRunning, "finished_at": nil}
	expected := parent.ExpectedItems
	if expected <= 0 {
		expected = len(children)
	}
	if succeeded == expected && len(children) == expected {
		updates["status"] = model.ClusterJobStatusSucceeded
		updates["finished_at"] = now
	} else if failed > 0 || len(children) < expected {
		updates["status"] = model.ClusterJobStatusPartialFailed
		if len(children) == expected && succeeded+failed == expected {
			updates["finished_at"] = now
		}
	}
	return tx.Model(&model.ClusterJob{}).Where("id = ?", parentJobID).Updates(updates).Error
}
