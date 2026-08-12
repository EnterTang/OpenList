package subscription

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"gorm.io/gorm"
)

var ErrSubscriptionItemChanged = errors.New("subscription item changed during reconciliation")

// ExecutionReconcileResult is deliberately small and operational: callers
// can expose it in logs/metrics without exposing provider response bodies.
type ExecutionReconcileResult struct {
	SubscriptionID uint
	Inspected      int
	Repaired       int
	Requeued       int
	Succeeded      int
	Failed         int
}

// ReconcileSubscriptionExecution makes the subscription item the durable
// business view and the cluster job the durable execution view agree again.
// It is safe to run repeatedly and is intentionally conservative: an item with
// no job is returned to pending, while a failed item remains failed until an
// explicit retry or a new source observation supplies a fresh dispatch.
func ReconcileSubscriptionExecution(ctx context.Context, subscriptionID uint) (ExecutionReconcileResult, error) {
	result := ExecutionReconcileResult{SubscriptionID: subscriptionID}
	if subscriptionID == 0 {
		return result, errors.New("subscription id is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	maxAttempts := defaultMaxReconcileAttempts
	if cfg, cfgErr := GetConfig(); cfgErr != nil {
		return result, cfgErr
	} else if cfg.MaxReconcileAttempts > 0 {
		maxAttempts = cfg.MaxReconcileAttempts
	}
	var err error
	for reconcileAttempt := 0; reconcileAttempt < 3; reconcileAttempt++ {
		result = ExecutionReconcileResult{SubscriptionID: subscriptionID}
		now := time.Now().UTC()
		err = db.GetDb().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			var sub model.Subscription
			if err := tx.First(&sub, subscriptionID).Error; err != nil {
				return err
			}
			var items []model.SubscriptionItem
			if err := tx.Where("subscription_id = ?", subscriptionID).Order("id ASC").Find(&items).Error; err != nil {
				return err
			}
			var jobs []model.ClusterJob
			if err := tx.Where("subscription_id = ? AND type = ?", subscriptionID, model.ClusterJobTypeMediaTransfer).
				Order("created_at DESC, id DESC").Find(&jobs).Error; err != nil {
				return err
			}
			result.Inspected = len(items) + len(jobs)

			jobsByID := make(map[string]*model.ClusterJob, len(jobs))
			latestByItemID := make(map[uint]*model.ClusterJob)
			for i := range jobs {
				job := &jobs[i]
				jobsByID[strings.TrimSpace(job.ID)] = job
				if job.SubscriptionItemID != 0 {
					if _, exists := latestByItemID[job.SubscriptionItemID]; !exists {
						latestByItemID[job.SubscriptionItemID] = job
					}
				}
			}
			itemsByID := make(map[uint]*model.SubscriptionItem, len(items))
			for i := range items {
				itemsByID[items[i].ID] = &items[i]
			}

			for i := range items {
				item := &items[i]
				job := jobsByID[strings.TrimSpace(item.ClusterJobID)]
				if job == nil {
					job = latestByItemID[item.ID]
				}
				if err := reconcileSubscriptionItemTx(tx, item, job, now, maxAttempts, &result); err != nil {
					return err
				}
			}
			for i := range jobs {
				job := &jobs[i]
				if job.SubscriptionItemID == 0 {
					continue
				}
				if _, exists := itemsByID[job.SubscriptionItemID]; exists {
					continue
				}
				if !subscriptionJobActive(job.Status) {
					continue
				}
				if err := tx.Model(&model.ClusterJob{}).Where("id = ? AND status IN ?", job.ID, subscriptionActiveJobStatuses()).Updates(map[string]any{
					"status": model.ClusterJobStatusCancelled, "finished_at": now,
					"last_error_code": "orphaned_subscription_item",
					"last_error":      "subscription item no longer exists; job cancelled by reconciliation",
				}).Error; err != nil {
					return err
				}
				result.Repaired++
			}

			status := aggregateSubscriptionStatus(items)
			lastError := ""
			if status == model.SubscriptionStatusFailed {
				for i := range items {
					if items[i].Status == model.SubscriptionItemStatusFailed && strings.TrimSpace(items[i].LastError) != "" {
						lastError = items[i].LastError
						break
					}
				}
				if lastError == "" {
					lastError = "one or more subscription items failed; retry is available"
				}
			}
			if err := reconcileLatestSubscriptionRunTx(tx, subscriptionID, items, jobs, now); err != nil {
				return err
			}
			return tx.Model(&model.Subscription{}).Where("id = ?", subscriptionID).Updates(map[string]any{
				"last_status": status, "last_error": lastError, "updated_at": now,
			}).Error
		})
		if !errors.Is(err, ErrSubscriptionItemChanged) || reconcileAttempt == 2 {
			break
		}
		select {
		case <-ctx.Done():
			return result, ctx.Err()
		case <-time.After(time.Duration(reconcileAttempt+1) * 25 * time.Millisecond):
		}
	}
	return result, err
}

func reconcileLatestSubscriptionRunTx(tx *gorm.DB, subscriptionID uint, items []model.SubscriptionItem, jobs []model.ClusterJob, now time.Time) error {
	var run model.SubscriptionRun
	err := tx.Where("subscription_id = ? AND status = ?", subscriptionID, model.SubscriptionStatusRunning).
		Order("started_at DESC, id DESC").First(&run).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil
	}
	if err != nil {
		return err
	}

	dispatchedItems := make(map[uint]struct{}, len(jobs))
	active := false
	for i := range jobs {
		job := &jobs[i]
		if job.SubscriptionItemID != 0 {
			dispatchedItems[job.SubscriptionItemID] = struct{}{}
		}
		if subscriptionJobActive(job.Status) {
			active = true
		}
	}
	projection := projectSubscriptionRun(subscriptionRunProjectionInput{
		Items:              items,
		DiscoveredHint:     len(items),
		DispatchedHint:     len(dispatchedItems),
		HasDiscoveryStage:  true,
		HasDispatchStage:   true,
		DiscoverySucceeded: true,
		DispatchSucceeded:  true,
		TransferRequested:  true,
		ClusterDispatch:    len(dispatchedItems) > 0,
	})
	status := aggregateSubscriptionStatus(items)
	finishedAt := any(now)
	if active {
		status = model.SubscriptionStatusRunning
		finishedAt = nil
	}
	lastError := ""
	if status == model.SubscriptionStatusFailed {
		for i := range items {
			if items[i].Status == model.SubscriptionItemStatusFailed && strings.TrimSpace(items[i].LastError) != "" {
				lastError = items[i].LastError
				break
			}
		}
		if lastError == "" {
			lastError = "one or more subscription items failed; retry is available"
		}
	}
	return tx.Model(&model.SubscriptionRun{}).Where("id = ?", run.ID).Updates(map[string]any{
		"status":            status,
		"finished_at":       finishedAt,
		"discovered_count":  projection.DiscoveredCount,
		"dispatched_count":  projection.DispatchedCount,
		"queued_count":      projection.DispatchedCount,
		"transferred_count": projection.SucceededCount,
		"succeeded_count":   projection.SucceededCount,
		"skipped_count":     projection.SkippedCount,
		"retryable_count":   projection.RetryableCount,
		"blocked_count":     projection.BlockedCount,
		"unknown_count":     projection.UnknownCount,
		"failed_count":      projection.FailedCount,
		"discover_status":   projection.DiscoverStatus,
		"dispatch_status":   projection.DispatchStatus,
		"transfer_status":   projection.TransferStatus,
		"completion_state":  projection.CompletionState,
		"error":             lastError,
		"updated_at":        now,
	}).Error
}

// SubscriptionNeedsExecutionFollowup reports whether a scheduled run should
// bypass its normal discovery interval. It only returns true for retryable
// pending work without an active job; terminal failures remain operator- or
// endpoint-triggered retries and cannot cause an infinite scheduler loop.
func SubscriptionNeedsExecutionFollowup(ctx context.Context, subscriptionID uint) (bool, error) {
	if subscriptionID == 0 {
		return false, errors.New("subscription id is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var sub model.Subscription
	if err := db.GetDb().WithContext(ctx).First(&sub, subscriptionID).Error; err != nil {
		return false, err
	}
	if !sub.TransferEnabled {
		return false, nil
	}
	var items []model.SubscriptionItem
	if err := db.GetDb().WithContext(ctx).Where("subscription_id = ? AND status IN ?", subscriptionID, []string{
		model.SubscriptionItemStatusPending,
		model.SubscriptionItemStatusRetryWait,
		model.SubscriptionItemStatusUnknown,
	}).Where("retry_at IS NULL OR retry_at <= ?", time.Now().UTC()).Find(&items).Error; err != nil {
		return false, err
	}
	if len(items) == 0 {
		return false, nil
	}
	var jobs []model.ClusterJob
	if err := db.GetDb().WithContext(ctx).Where("subscription_id = ? AND type = ?", subscriptionID, model.ClusterJobTypeMediaTransfer).Find(&jobs).Error; err != nil {
		return false, err
	}
	activeByItemID := make(map[uint]struct{}, len(jobs))
	for i := range jobs {
		if subscriptionJobActive(jobs[i].Status) && jobs[i].SubscriptionItemID != 0 {
			activeByItemID[jobs[i].SubscriptionItemID] = struct{}{}
		}
	}
	for i := range items {
		if _, active := activeByItemID[items[i].ID]; active {
			continue
		}
		// A missing worker is a blocked scheduling condition, not a reason to
		// spin the scheduler every tick. The normal subscription interval will
		// retry it after inventory/credential health has had time to refresh.
		if strings.Contains(strings.ToLower(items[i].LastError), "no compatible cluster worker") {
			continue
		}
		if items[i].Status == model.SubscriptionItemStatusRetryWait || items[i].Status == model.SubscriptionItemStatusUnknown || strings.TrimSpace(items[i].LastError) == "" || strings.Contains(items[i].LastError, "no durable cluster job") {
			return true, nil
		}
	}
	return false, nil
}

// ReconcileActiveSubscriptionExecutions is used by the coordinator loop so
// recovery does not depend on the UI or on a successful source scan.
func ReconcileActiveSubscriptionExecutions(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		limit = 100
	}
	subs, err := db.ListActiveSubscriptions()
	if err != nil {
		return 0, err
	}
	if len(subs) > limit {
		subs = subs[:limit]
	}
	reconciled := 0
	var firstErr error
	for i := range subs {
		if _, err := ReconcileSubscriptionExecution(ctx, subs[i].ID); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		reconciled++
	}
	return reconciled, firstErr
}

func reconcileSubscriptionItemTx(tx *gorm.DB, item *model.SubscriptionItem, job *model.ClusterJob, now time.Time, maxAttempts int, result *ExecutionReconcileResult) error {
	if item == nil || result == nil {
		return nil
	}
	if job == nil {
		if item.Status != model.SubscriptionItemStatusTransferring && item.Status != model.SubscriptionItemStatusNotifying {
			return nil
		}
		return updateSubscriptionItemReconcileTx(tx, item, map[string]any{
			"status": model.SubscriptionItemStatusPending, "cluster_job_id": "",
			"last_error": "transfer state had no durable cluster job; returned to pending for compensation",
			"updated_at": now,
		}, model.SubscriptionItemStatusPending, "", result)
	}
	if item.Status == model.SubscriptionItemStatusTransferred && job.Status != model.ClusterJobStatusSucceeded {
		if err := tx.Model(&model.ClusterJob{}).Where("id = ? AND status NOT IN ?", job.ID, []string{model.ClusterJobStatusSucceeded}).Updates(map[string]any{
			"status": model.ClusterJobStatusSucceeded, "finished_at": now,
			"last_error_code": "", "last_error": "",
		}).Error; err != nil {
			return err
		}
		result.Repaired++
		return nil
	}

	switch {
	case job.Status == model.ClusterJobStatusSucceeded:
		if item.Status == model.SubscriptionItemStatusSkipped {
			return nil
		}
		if item.Status != model.SubscriptionItemStatusTransferred {
			return updateSubscriptionItemReconcileTx(tx, item, map[string]any{
				"status": model.SubscriptionItemStatusTransferred, "cluster_job_id": job.ID,
				"last_error": "", "last_error_code": "", "retry_at": nil, "blocked_reason": "", "updated_at": now,
			}, model.SubscriptionItemStatusTransferred, job.ID, result)
		}
		return nil
	case subscriptionJobActive(job.Status):
		if item.Status == model.SubscriptionItemStatusFailed {
			return updateSubscriptionItemReconcileTx(tx, item, map[string]any{
				"status": model.SubscriptionItemStatusTransferring, "cluster_job_id": job.ID,
				"last_error": "active cluster retry is authoritative; waiting for completion", "updated_at": now,
			}, model.SubscriptionItemStatusTransferring, job.ID, result)
		}
		return nil
	case job.Status == model.ClusterJobStatusFailed,
		job.Status == model.ClusterJobStatusPartialFailed,
		job.Status == model.ClusterJobStatusDeadLetter,
		job.Status == model.ClusterJobStatusCancelled:
		if item.Status == model.SubscriptionItemStatusTransferred || item.Status == model.SubscriptionItemStatusSkipped {
			return nil
		}
		lastError := strings.TrimSpace(job.LastError)
		if lastError == "" {
			lastError = strings.TrimSpace(job.LastErrorCode)
		}
		if lastError == "" {
			lastError = "cluster media transfer reached a terminal failure"
		}
		lastErrorCode := strings.TrimSpace(job.LastErrorCode)
		status := model.SubscriptionItemStatusFailed
		updates := map[string]any{
			"status":          status,
			"cluster_job_id":  job.ID,
			"last_error":      lastError,
			"last_error_code": lastErrorCode,
			"updated_at":      now,
		}
		if job.Status != model.ClusterJobStatusDeadLetter {
			switch classifySubscriptionFailure(lastErrorCode) {
			case model.SubscriptionItemStatusRetryWait:
				nextRetryCount := item.RetryCount + 1
				if nextRetryCount >= maxAttempts {
					status = model.SubscriptionItemStatusFailed
					updates["status"] = status
					updates["last_error_code"] = "retry_limit_exceeded"
					updates["retry_at"] = nil
					updates["blocked_reason"] = ""
				} else {
					status = model.SubscriptionItemStatusRetryWait
					retryAt := now.Add(subscriptionRetryDelay)
					updates["status"] = status
					updates["retry_count"] = nextRetryCount
					updates["retry_at"] = retryAt
					updates["blocked_reason"] = ""
				}
			case model.SubscriptionItemStatusUnknown:
				nextRetryCount := item.RetryCount + 1
				if nextRetryCount >= maxAttempts {
					status = model.SubscriptionItemStatusFailed
					updates["status"] = status
					updates["last_error_code"] = "retry_limit_exceeded"
					updates["retry_at"] = nil
					updates["blocked_reason"] = ""
				} else {
					status = model.SubscriptionItemStatusUnknown
					retryAt := now.Add(subscriptionUnknownProbeDelay)
					updates["status"] = status
					updates["retry_count"] = nextRetryCount
					updates["retry_at"] = retryAt
					updates["blocked_reason"] = ""
				}
			case model.SubscriptionItemStatusBlocked:
				status = model.SubscriptionItemStatusBlocked
				updates["status"] = status
				updates["blocked_reason"] = lastErrorCode
				updates["retry_at"] = nil
			}
		}
		return updateSubscriptionItemReconcileTx(tx, item, updates, status, job.ID, result)
	}
	return nil
}

const (
	subscriptionRetryDelay        = 1 * time.Minute
	subscriptionUnknownProbeDelay = 5 * time.Minute
)

func classifySubscriptionFailure(errorCode string) string {
	switch strings.ToLower(strings.TrimSpace(errorCode)) {
	case "share_save_retryable", "share_save_rate_limited", "share_save_transient", "share_save_gateway_response", "source_unexpected_eof", "source_range_failed", "source_link_expired", "network_timeout", "timeout", "rate_limited", "worker_capacity_unavailable", "worker_cleanup_backlog", "worker_journal_unavailable", "worker_start_timeout", "worker_lease_expired", "lease_expired":
		return model.SubscriptionItemStatusRetryWait
	case "share_save_result_unknown", "result_unknown", "request_result_unknown", "operation_result_unknown":
		return model.SubscriptionItemStatusUnknown
	case "no_compatible_worker", "worker_unavailable", "provider_health_stale", "reauthorization_required", "direct_share_reauthorize":
		return model.SubscriptionItemStatusBlocked
	default:
		return model.SubscriptionItemStatusFailed
	}
}

func updateSubscriptionItemReconcileTx(tx *gorm.DB, item *model.SubscriptionItem, updates map[string]any, status, jobID string, result *ExecutionReconcileResult) error {
	if item == nil {
		return errors.New("subscription item is required for reconciliation")
	}
	currentVersion := item.StateVersion
	updates["state_version"] = currentVersion + 1
	updated := tx.Model(&model.SubscriptionItem{}).
		Where("id = ? AND COALESCE(state_version, 0) = ?", item.ID, currentVersion).
		Updates(updates)
	if updated.Error != nil {
		return updated.Error
	}
	if updated.RowsAffected != 1 {
		return ErrSubscriptionItemChanged
	}
	if err := tx.Model(&model.SubscriptionEpisodeSource{}).
		Where("subscription_id = ? AND source_item_id = ? AND file_hash = ?", item.SubscriptionID, item.ID, item.FileHash).
		Updates(map[string]any{"status": status, "cluster_job_id": jobID, "updated_at": time.Now().UTC()}).Error; err != nil {
		return err
	}
	item.Status = status
	item.ClusterJobID = jobID
	item.StateVersion = currentVersion + 1
	if value, ok := updates["last_error"].(string); ok {
		item.LastError = value
	}
	if value, ok := updates["last_error_code"].(string); ok {
		item.LastErrorCode = value
	}
	if value, ok := updates["blocked_reason"].(string); ok {
		item.BlockedReason = value
	}
	if value, ok := updates["retry_count"].(int); ok {
		item.RetryCount = value
	}
	if value, ok := updates["retry_at"].(*time.Time); ok {
		item.RetryAt = value
	}
	result.Repaired++
	switch status {
	case model.SubscriptionItemStatusPending:
		result.Requeued++
	case model.SubscriptionItemStatusTransferred:
		result.Succeeded++
	case model.SubscriptionItemStatusFailed:
		result.Failed++
	}
	return nil
}

func subscriptionActiveJobStatuses() []string {
	return []string{
		model.ClusterJobStatusQueued,
		model.ClusterJobStatusPlanning,
		model.ClusterJobStatusLeased,
		model.ClusterJobStatusRunning,
		model.ClusterJobStatusRetryWait,
		model.ClusterJobStatusCancelRequested,
	}
}

func subscriptionJobActive(status string) bool {
	for _, active := range subscriptionActiveJobStatuses() {
		if status == active {
			return true
		}
	}
	return false
}
