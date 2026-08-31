package db

import (
	"context"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

// ListMoviePilotTaskStatuses returns the Coordinator's redacted, one-row-per-
// intent view. It intentionally includes intents that do not have a torrent
// binding yet, which is the state that is otherwise hardest to diagnose.
func ListMoviePilotTaskStatuses(ctx context.Context, subscriptionID uint, bridgeID string, limit int) ([]model.MoviePilotTaskStatus, error) {
	if db == nil {
		return []model.MoviePilotTaskStatus{}, nil
	}
	if limit <= 0 || limit > 500 {
		limit = 100
	}

	intentQuery := db.WithContext(ctx).Model(&model.MoviePilotDownloadIntent{})
	if subscriptionID > 0 {
		intentQuery = intentQuery.Where("subscription_id = ?", subscriptionID)
	}
	if strings.TrimSpace(bridgeID) != "" {
		intentQuery = intentQuery.Where("bridge_instance_id = ?", strings.TrimSpace(bridgeID))
	}
	var intents []model.MoviePilotDownloadIntent
	if err := intentQuery.Order("updated_at DESC, id DESC").Limit(limit).Find(&intents).Error; err != nil {
		if isMoviePilotMissingTableError(err) {
			return []model.MoviePilotTaskStatus{}, nil
		}
		return nil, err
	}
	if len(intents) == 0 {
		return []model.MoviePilotTaskStatus{}, nil
	}

	intentIDs := make([]string, 0, len(intents))
	itemIDs := make([]uint, 0, len(intents))
	subscriptionIDs := make([]uint, 0, len(intents))
	for _, intent := range intents {
		intentIDs = append(intentIDs, intent.ID)
		if intent.SubscriptionItemID > 0 {
			itemIDs = append(itemIDs, intent.SubscriptionItemID)
		}
		if intent.SubscriptionID > 0 {
			subscriptionIDs = append(subscriptionIDs, intent.SubscriptionID)
		}
	}

	var bindings []model.MoviePilotTorrentBinding
	if err := db.WithContext(ctx).Where("intent_id IN ?", intentIDs).Order("updated_at DESC, id DESC").Find(&bindings).Error; err != nil {
		if isMoviePilotMissingTableError(err) {
			return moviePilotTaskStatusesWithoutBindings(ctx, intents, itemIDs, subscriptionIDs), nil
		}
		return nil, err
	}
	bindingByIntent := make(map[string]model.MoviePilotTorrentBinding, len(bindings))
	bindingIDs := make([]string, 0, len(bindings))
	for _, binding := range bindings {
		if _, exists := bindingByIntent[binding.IntentID]; exists {
			continue
		}
		bindingByIntent[binding.IntentID] = binding
		bindingIDs = append(bindingIDs, binding.ID)
	}

	var deliveries []model.MoviePilotDeliveryFile
	if len(bindingIDs) > 0 {
		if err := db.WithContext(ctx).Where("torrent_binding_id IN ?", bindingIDs).Order("updated_at DESC, id DESC").Find(&deliveries).Error; err != nil {
			if !isMoviePilotMissingTableError(err) {
				return nil, err
			}
		}
	}
	deliveryByBinding := make(map[string][]model.MoviePilotDeliveryFile, len(bindingIDs))
	for _, delivery := range deliveries {
		deliveryByBinding[delivery.TorrentBindingID] = append(deliveryByBinding[delivery.TorrentBindingID], delivery)
	}

	itemsByID := make(map[uint]model.SubscriptionItem, len(itemIDs))
	if len(itemIDs) > 0 {
		var items []model.SubscriptionItem
		if err := db.WithContext(ctx).Where("id IN ?", itemIDs).Find(&items).Error; err != nil {
			return nil, err
		}
		for _, item := range items {
			itemsByID[item.ID] = item
		}
	}
	subscriptionsByID := make(map[uint]model.Subscription, len(subscriptionIDs))
	if len(subscriptionIDs) > 0 {
		var subscriptions []model.Subscription
		if err := db.WithContext(ctx).Where("id IN ?", subscriptionIDs).Find(&subscriptions).Error; err != nil {
			return nil, err
		}
		for _, subscription := range subscriptions {
			subscriptionsByID[subscription.ID] = subscription
		}
	}

	var jobs []model.ClusterJob
	if len(itemIDs) > 0 {
		if err := db.WithContext(ctx).
			Where("subscription_item_id IN ?", itemIDs).
			Order("updated_at DESC, id DESC").Find(&jobs).Error; err != nil {
			if !isMoviePilotMissingTableError(err) {
				return nil, err
			}
		}
	}
	jobByItem := make(map[uint]model.ClusterJob, len(jobs))
	jobIDs := make([]string, 0, len(jobs))
	for _, job := range jobs {
		if _, exists := jobByItem[job.SubscriptionItemID]; exists {
			continue
		}
		jobByItem[job.SubscriptionItemID] = job
		jobIDs = append(jobIDs, job.ID)
	}
	var stages []model.ClusterJobStage
	if len(jobIDs) > 0 {
		if err := db.WithContext(ctx).Where("job_id IN ?", jobIDs).Order("updated_at DESC, id DESC").Find(&stages).Error; err != nil {
			return nil, err
		}
	}
	stageByJob := make(map[string]model.ClusterJobStage, len(stages))
	for _, stage := range stages {
		if _, exists := stageByJob[stage.JobID]; !exists {
			stageByJob[stage.JobID] = stage
		}
	}

	workerStatusByID := make(map[string]string)
	workerIDs := make([]string, 0, len(bindings))
	for _, binding := range bindings {
		if strings.TrimSpace(binding.WorkerNodeID) != "" {
			workerIDs = append(workerIDs, binding.WorkerNodeID)
		}
	}
	if len(workerIDs) > 0 {
		var nodes []model.ClusterNode
		if err := db.WithContext(ctx).Where("id IN ?", workerIDs).Find(&nodes).Error; err == nil {
			for _, node := range nodes {
				workerStatusByID[node.ID] = node.Status
			}
		}
	}

	result := make([]model.MoviePilotTaskStatus, 0, len(intents))
	for _, intent := range intents {
		binding, hasBinding := bindingByIntent[intent.ID]
		item := itemsByID[intent.SubscriptionItemID]
		subscription := subscriptionsByID[intent.SubscriptionID]
		job := jobByItem[intent.SubscriptionItemID]
		stage := stageByJob[job.ID]
		status := model.MoviePilotTaskStatus{
			RequestID: intent.RequestID, SubscriptionID: intent.SubscriptionID,
			SubscriptionItemID: intent.SubscriptionItemID, SubscriptionName: subscription.Name,
			ItemName:     firstNonEmptyMoviePilotStatus(item.FileName, item.TargetName, item.FilePath),
			IntentStatus: intent.Status, BridgeInstanceID: intent.BridgeInstanceID,
			ClusterJobID: job.ID, ClusterJobStatus: job.Status, ClusterJobStage: stage.Name,
			ClusterJobStageStatus: stage.Status, ErrorCode: firstNonEmptyMoviePilotStatus(intent.LastErrorCode, job.LastErrorCode, stage.ErrorCode),
			Error:     firstNonEmptyMoviePilotStatus(intent.LastError, job.LastError, stage.Error),
			UpdatedAt: intent.UpdatedAt,
		}
		if hasBinding {
			status.BindingID = binding.ID
			status.TorrentStatus = binding.Status
			status.WorkerNodeID = binding.WorkerNodeID
			status.WorkerStatus = workerStatusByID[binding.WorkerNodeID]
			status.Downloader = binding.DownloaderAlias
			status.QBClientID = binding.QBClientID
			status.TorrentHash = binding.TorrentHash
			status.DownloadProgress = clampMoviePilotProgress(binding.LastQBProgress)
			if status.Error == "" {
				status.ErrorCode, status.Error = binding.LastErrorCode, binding.LastError
			}
			status.UpdatedAt = binding.UpdatedAt
			if job.UpdatedAt.After(status.UpdatedAt) {
				status.UpdatedAt = job.UpdatedAt
			}
			setMoviePilotDeliveryProgress(&status, deliveryByBinding[binding.ID])
		}
		status.Phase = moviePilotTaskPhase(status, hasBinding, deliveryByBinding[binding.ID])
		result = append(result, status)
	}
	return result, nil
}

func moviePilotTaskStatusesWithoutBindings(ctx context.Context, intents []model.MoviePilotDownloadIntent, itemIDs, subscriptionIDs []uint) []model.MoviePilotTaskStatus {
	itemsByID := make(map[uint]model.SubscriptionItem, len(itemIDs))
	if len(itemIDs) > 0 {
		var items []model.SubscriptionItem
		if db.WithContext(ctx).Where("id IN ?", itemIDs).Find(&items).Error == nil {
			for _, item := range items {
				itemsByID[item.ID] = item
			}
		}
	}
	subscriptionsByID := make(map[uint]model.Subscription, len(subscriptionIDs))
	if len(subscriptionIDs) > 0 {
		var subscriptions []model.Subscription
		if db.WithContext(ctx).Where("id IN ?", subscriptionIDs).Find(&subscriptions).Error == nil {
			for _, subscription := range subscriptions {
				subscriptionsByID[subscription.ID] = subscription
			}
		}
	}
	result := make([]model.MoviePilotTaskStatus, 0, len(intents))
	for _, intent := range intents {
		item := itemsByID[intent.SubscriptionItemID]
		subscription := subscriptionsByID[intent.SubscriptionID]
		result = append(result, model.MoviePilotTaskStatus{
			RequestID: intent.RequestID, SubscriptionID: intent.SubscriptionID,
			SubscriptionItemID: intent.SubscriptionItemID, SubscriptionName: subscription.Name,
			ItemName:     firstNonEmptyMoviePilotStatus(item.FileName, item.TargetName, item.FilePath),
			IntentStatus: intent.Status, BridgeInstanceID: intent.BridgeInstanceID,
			Phase:     moviePilotTaskPhase(model.MoviePilotTaskStatus{IntentStatus: intent.Status, Error: intent.LastError}, false, nil),
			ErrorCode: intent.LastErrorCode, Error: intent.LastError, UpdatedAt: intent.UpdatedAt,
		})
	}
	return result
}

func setMoviePilotDeliveryProgress(status *model.MoviePilotTaskStatus, deliveries []model.MoviePilotDeliveryFile) {
	if status == nil || len(deliveries) == 0 {
		return
	}
	var weightedProgress float64
	var totalBytes int64
	for _, delivery := range deliveries {
		if delivery.Required {
			status.ExpectedFiles++
		}
		if delivery.Status == model.MoviePilotDeliveryStatusMaterialized {
			status.TransferredFiles++
		}
		progress := clampMoviePilotProgress(delivery.UploadProgress)
		if delivery.SourceSize > 0 {
			weightedProgress += float64(delivery.SourceSize) * progress
			totalBytes += delivery.SourceSize
		}
		if delivery.UpdatedAt.After(status.UpdatedAt) {
			status.UpdatedAt = delivery.UpdatedAt
		}
		if status.Error == "" && delivery.LastError != "" {
			status.ErrorCode, status.Error = delivery.LastErrorCode, delivery.LastError
		}
	}
	if totalBytes > 0 {
		status.UploadProgress = clampMoviePilotProgress(weightedProgress / float64(totalBytes))
	} else {
		status.UploadProgress = clampMoviePilotProgress(float64(status.TransferredFiles) / float64(maxMoviePilotStatusInt(status.ExpectedFiles, 1)))
	}
}

func moviePilotTaskPhase(status model.MoviePilotTaskStatus, hasBinding bool, deliveries []model.MoviePilotDeliveryFile) string {
	if status.Error != "" || status.IntentStatus == model.MoviePilotIntentStatusFailed || status.ClusterJobStatus == model.ClusterJobStatusFailed || status.ClusterJobStatus == model.ClusterJobStatusDeadLetter || status.ClusterJobStageStatus == model.ClusterStageStatusFailed {
		return "failed"
	}
	if len(deliveries) > 0 {
		allMaterialized := status.ExpectedFiles > 0 && status.TransferredFiles >= status.ExpectedFiles
		if allMaterialized {
			return "completed"
		}
		for _, delivery := range deliveries {
			if delivery.Status == model.MoviePilotDeliveryStatusUploading || delivery.Status == model.MoviePilotDeliveryStatusStaging {
				return string(delivery.Status)
			}
		}
	}
	if status.ClusterJobStage != "" && status.ClusterJobStatus != model.ClusterJobStatusSucceeded {
		return status.ClusterJobStage
	}
	if !hasBinding {
		if status.IntentStatus == model.MoviePilotIntentStatusAccepted {
			return "waiting_binding"
		}
		return firstNonEmptyMoviePilotStatus(status.IntentStatus, "pending")
	}
	if status.TorrentStatus == model.MoviePilotTorrentStatusSeeding {
		return "seeding"
	}
	if status.TorrentStatus == model.MoviePilotTorrentStatusDownloading {
		return "downloading"
	}
	return firstNonEmptyMoviePilotStatus(status.TorrentStatus, "bound")
}

func clampMoviePilotProgress(value float64) float64 {
	if value < 0 {
		return 0
	}
	if value > 1 {
		return 1
	}
	return value
}

func maxMoviePilotStatusInt(value, fallback int) int {
	if value > fallback {
		return value
	}
	return fallback
}

func firstNonEmptyMoviePilotStatus(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
