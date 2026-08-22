package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/moviepilotbridge"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func isOptionalMoviePilotTableError(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "no such table: movie_pilot_delivery_files")
}

const (
	moviePilotTorrentWorkflow       = "moviepilot-torrent-transfer-v1"
	moviePilotTorrentManifest       = "etf-sha256-v1"
	moviePilotTorrentParentPrefix   = "moviepilot-torrent-parent:"
	moviePilotTorrentObservePrefix  = "moviepilot-torrent-observe:"
	moviePilotTorrentDeliveryPrefix = "moviepilot-torrent-delivery:"
)

var torrentEpisodePattern = regexp.MustCompile(`(?i)(?:^|[^a-z0-9])s(\d{1,3})e(\d{1,4})(?:[^a-z0-9]|$)`)

// TorrentJobDispatchRequest is the narrow coordinator/runtime boundary for
// qB work. The runtime owns leases, inventory matching, and the wire offer;
// the coordinator only supplies an immutable task snapshot.
type TorrentJobDispatchRequest struct {
	JobType              string
	NodeID               string
	IdempotencyKey       string
	Priority             int
	ExpectedBytes        int64
	LeaseDuration        time.Duration
	TaskContext          protocol.TaskContext
	RequiredCapabilities []string
}

type TorrentJobDispatcher interface {
	DispatchTorrentJob(context.Context, TorrentJobDispatchRequest) (*model.ClusterJob, error)
}

func (s *Service) SetTorrentJobDispatcher(dispatcher TorrentJobDispatcher) {
	if s != nil {
		s.torrentDispatcher = dispatcher
	}
}

// HandleMoviePilotEvent is the durable bridge-to-coordinator handoff. The
// event inbox is committed by moviepilotbridge before this method is called.
func (s *Service) HandleMoviePilotEvent(ctx context.Context, bridgeID string, event moviepilotbridge.BridgeEvent) error {
	if s == nil || s.db == nil {
		return errors.New("cluster database is unavailable")
	}
	if err := event.Validate(); err != nil {
		return err
	}
	switch event.Type {
	case moviepilotbridge.EventIntentAccepted:
		return s.updateMoviePilotIntent(ctx, bridgeID, event.RequestID, event.EventID, model.MoviePilotIntentStatusAccepted, "", "")
	case moviepilotbridge.EventTorrentBound:
		return s.handleTorrentBound(ctx, bridgeID, event)
	case moviepilotbridge.EventTorrentStateChanged:
		return s.handleTorrentStateChanged(ctx, bridgeID, event)
	case moviepilotbridge.EventTorrentFailed:
		return s.handleTorrentFailed(ctx, bridgeID, event)
	case moviepilotbridge.EventBridgeHealthChanged:
		return s.db.WithContext(ctx).Model(&model.MoviePilotBridgeInstance{}).Where("id = ?", bridgeID).Updates(map[string]any{
			"last_health": event.Health, "last_seen_at": event.OccurredAt.UTC(), "last_error": "",
		}).Error
	default:
		return fmt.Errorf("unsupported MoviePilot event type %q", event.Type)
	}
}

func (s *Service) updateMoviePilotIntent(ctx context.Context, bridgeID, requestID, eventID, status, errorCode, lastError string) error {
	updates := map[string]any{"status": status, "last_event_id": eventID, "last_error_code": errorCode, "last_error": lastError}
	if status == model.MoviePilotIntentStatusAccepted {
		updates["accepted_at"] = time.Now().UTC()
	}
	return s.db.WithContext(ctx).Model(&model.MoviePilotDownloadIntent{}).
		Where("bridge_instance_id = ? AND request_id = ?", strings.TrimSpace(bridgeID), strings.TrimSpace(requestID)).Updates(updates).Error
}

func (s *Service) handleTorrentBound(ctx context.Context, bridgeID string, event moviepilotbridge.BridgeEvent) error {
	if event.Torrent == nil {
		return errors.New("torrent.bound payload is required")
	}
	var intent model.MoviePilotDownloadIntent
	if err := s.db.WithContext(ctx).Where("bridge_instance_id = ? AND request_id = ?", bridgeID, event.RequestID).First(&intent).Error; err != nil {
		return err
	}
	workerID, qbClientID, err := s.resolveMoviePilotWorkerRoute(ctx, bridgeID, event.Torrent.Downloader)
	if err != nil {
		_ = s.db.WithContext(ctx).Model(&intent).Updates(map[string]any{
			"status": model.MoviePilotIntentStatusWaitingWorker, "last_error_code": "worker_route_unavailable", "last_error": err.Error(),
		}).Error
		return err
	}
	binding, err := db.BindTorrentTx(ctx, s.db, &intent, bridgeID, event.Torrent.Downloader, workerID, qbClientID, event.Torrent.TorrentHash, event.Torrent.ContentPath)
	if err != nil {
		return err
	}
	var item model.SubscriptionItem
	if err := s.loadMoviePilotSubscriptionItem(ctx, &intent, &item); err != nil {
		return err
	}
	var sub model.Subscription
	if err := s.db.WithContext(ctx).First(&sub, "id = ?", intent.SubscriptionID).Error; err != nil {
		return err
	}
	taskContext := moviePilotTorrentTaskContext(&sub, &item, binding, binding.ID, event.Torrent.Media)
	parent, err := s.ensureTorrentParent(ctx, binding, taskContext)
	if err != nil {
		return err
	}
	if err := s.db.WithContext(ctx).Model(&model.MoviePilotTorrentBinding{}).Where("id = ?", binding.ID).Updates(map[string]any{
		"observe_job_id": parent.ID, "status": model.MoviePilotTorrentStatusBound, "last_error_code": "", "last_error": "",
	}).Error; err != nil {
		return err
	}
	taskContext.ParentBatchID = parent.ID
	if err := s.dispatchTorrentJob(ctx, TorrentJobDispatchRequest{
		JobType: model.ClusterJobTypeTorrentObserve, NodeID: binding.WorkerNodeID,
		IdempotencyKey: moviePilotTorrentObservePrefix + binding.ID, ExpectedBytes: event.Torrent.Size, TaskContext: taskContext,
		RequiredCapabilities: []string{"qb.copy", "result.report"},
	}); err != nil {
		return err
	}
	if err := s.db.WithContext(ctx).Model(&model.MoviePilotDownloadIntent{}).Where("id = ?", intent.ID).Updates(map[string]any{
		"status": model.MoviePilotIntentStatusBound, "last_event_id": event.EventID, "last_error_code": "", "last_error": "",
	}).Error; err != nil {
		return err
	}
	_ = s.db.WithContext(ctx).Model(&model.SubscriptionItem{}).Where("id = ?", item.ID).Updates(map[string]any{
		"status": model.SubscriptionItemStatusTransferring, "last_error": "", "last_error_code": "",
	}).Error
	return nil
}

func (s *Service) ensureTorrentParent(ctx context.Context, binding *model.MoviePilotTorrentBinding, taskContext protocol.TaskContext) (*model.ClusterJob, error) {
	if binding == nil {
		return nil, errors.New("torrent binding is required")
	}
	parentID := strings.TrimSpace(binding.ObserveJobID)
	if parentID == "" {
		parentID = uuid.NewString()
	}
	taskContext.ParentBatchID = parentID
	raw, err := json.Marshal(taskContext)
	if err != nil {
		return nil, err
	}
	hash, err := protocol.HashTaskContext(taskContext)
	if err != nil {
		return nil, err
	}
	required, _ := json.Marshal([]string{"qb.copy", "result.report"})
	parent := &model.ClusterJob{
		ID: parentID, Type: model.ClusterJobTypeTorrentObserve, Status: model.ClusterJobStatusPlanning,
		NotificationStatus: model.ClusterNotificationStatusNotRequired, WorkerCleanupStatus: model.ClusterCleanupStatusPending,
		ResultDeliveryStatus: model.ClusterResultDeliveryStatusQueued, IdempotencyKey: moviePilotTorrentParentPrefix + binding.ID,
		WorkflowVersion: moviePilotTorrentWorkflow, SubscriptionID: taskContext.Subscription.SubscriptionID,
		SubscriptionItemID: taskContext.Subscription.SubscriptionItemID, MediaItemID: taskContext.MediaItemID,
		SourceProvider: "qbittorrent", TaskContextJSON: string(raw), TaskContextHash: hash,
		RequiredCapabilitiesJSON: string(required), ExpectedItems: 1,
		AvailableAt: time.Now().UTC(),
	}
	err = s.db.WithContext(ctx).Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "idempotency_key"}}, DoNothing: true}).Create(parent).Error
	if err != nil {
		return nil, err
	}
	if err := s.db.WithContext(ctx).First(parent, "idempotency_key = ?", moviePilotTorrentParentPrefix+binding.ID).Error; err != nil {
		return nil, err
	}
	return parent, nil
}

func (s *Service) dispatchTorrentJob(ctx context.Context, req TorrentJobDispatchRequest) error {
	if s.torrentDispatcher == nil {
		return errors.New("torrent job dispatcher is not configured")
	}
	_, err := s.torrentDispatcher.DispatchTorrentJob(ctx, req)
	return err
}

func (s *Service) handleTorrentStateChanged(ctx context.Context, bridgeID string, event moviepilotbridge.BridgeEvent) error {
	var intent model.MoviePilotDownloadIntent
	if err := s.db.WithContext(ctx).Where("bridge_instance_id = ? AND request_id = ?", bridgeID, event.RequestID).First(&intent).Error; err != nil {
		return err
	}
	var binding model.MoviePilotTorrentBinding
	if err := s.db.WithContext(ctx).Where("intent_id = ?", intent.ID).First(&binding).Error; err != nil {
		return err
	}
	state := strings.ToLower(strings.TrimSpace(event.State.State))
	status := model.MoviePilotTorrentStatusDownloading
	seeding := state == "seeding" || state == "uploading" || state == "stalled_seeding"
	if state == "completed" || state == "complete" || seeding {
		status = model.MoviePilotTorrentStatusDownloadCompleted
	}
	if seeding {
		status = model.MoviePilotTorrentStatusSeeding
	}
	updates := map[string]any{
		"status": status, "last_qb_state": event.State.State, "last_qb_progress": event.State.Progress,
		"last_qb_ratio": event.State.Ratio, "last_qb_seeding_seconds": event.State.SeedingSeconds,
		"last_error_code": "", "last_error": "",
	}
	if event.State.HNRPassed != nil {
		updates["last_qb_hnr_passed"] = *event.State.HNRPassed
	}
	if status == model.MoviePilotTorrentStatusDownloadCompleted || seeding {
		now := event.OccurredAt.UTC()
		if now.IsZero() {
			now = time.Now().UTC()
		}
		updates["download_completed_at"] = now
		if seeding {
			updates["seed_started_at"] = gorm.Expr("COALESCE(seed_started_at, ?)", now)
		}
		var policy model.TorrentRetentionPolicy
		if json.Unmarshal([]byte(binding.RetentionPolicyJSON), &policy) == nil && retentionPolicyEligible(policy, *event.State, now) {
			updates["retention_status"] = model.MoviePilotRetentionStatusEligible
			updates["retention_eligible_at"] = now
		} else if strings.TrimSpace(binding.RetentionPolicyJSON) != "" {
			updates["retention_status"] = model.MoviePilotRetentionStatusHeld
			updates["retention_eligible_at"] = nil
		}
	}
	if err := s.db.WithContext(ctx).Model(&binding).Updates(updates).Error; err != nil {
		return err
	}
	intentStatus := model.MoviePilotIntentStatusDownloading
	if seeding || state == "completed" || state == "complete" {
		intentStatus = model.MoviePilotIntentStatusCompleted
	}
	return s.db.WithContext(ctx).Model(&intent).Updates(map[string]any{"last_event_id": event.EventID, "status": intentStatus}).Error
}

func retentionPolicyEligible(policy model.TorrentRetentionPolicy, state moviepilotbridge.TorrentStatePayload, now time.Time) bool {
	if policy.Permanent || strings.TrimSpace(policy.SiteRuleID) == "" && policy.MinSeedSeconds <= 0 && policy.MinRatio <= 0 && policy.ManualHoldUntil == nil {
		return false
	}
	if policy.ManualHoldUntil != nil && policy.ManualHoldUntil.After(now) {
		return false
	}
	if policy.MinSeedSeconds > 0 && state.SeedingSeconds < policy.MinSeedSeconds {
		return false
	}
	if policy.MinRatio > 0 && state.Ratio < policy.MinRatio {
		return false
	}
	if strings.TrimSpace(policy.SiteRuleID) != "" && (state.HNRPassed == nil || !*state.HNRPassed) {
		return false
	}
	return true
}

func (s *Service) handleTorrentFailed(ctx context.Context, bridgeID string, event moviepilotbridge.BridgeEvent) error {
	var intent model.MoviePilotDownloadIntent
	if err := s.db.WithContext(ctx).Where("bridge_instance_id = ? AND request_id = ?", bridgeID, event.RequestID).First(&intent).Error; err != nil {
		return err
	}
	now := time.Now().UTC()
	if event.OccurredAt.After(time.Time{}) {
		now = event.OccurredAt.UTC()
	}
	if err := s.db.WithContext(ctx).Model(&intent).Updates(map[string]any{
		"status": model.MoviePilotIntentStatusFailed, "last_event_id": event.EventID,
		"last_error_code": event.Failure.Code, "last_error": event.Failure.Message,
	}).Error; err != nil {
		return err
	}
	return s.db.WithContext(ctx).Model(&model.MoviePilotTorrentBinding{}).Where("intent_id = ?", intent.ID).Updates(map[string]any{
		"status": model.MoviePilotTorrentStatusFailed, "last_error_code": event.Failure.Code, "last_error": event.Failure.Message,
		"updated_at": now,
	}).Error
}

func (s *Service) resolveMoviePilotWorkerRoute(ctx context.Context, bridgeID, downloader string) (string, string, error) {
	var inventories []model.ClusterNodeInventory
	if err := s.db.WithContext(ctx).Order("node_id ASC, revision DESC").Find(&inventories).Error; err != nil {
		return "", "", err
	}
	seen := make(map[string]struct{})
	workerID, qbID := "", ""
	for _, inventory := range inventories {
		if _, ok := seen[inventory.NodeID]; ok {
			continue
		}
		seen[inventory.NodeID] = struct{}{}
		var capabilities protocol.NodeCapabilities
		if err := json.Unmarshal([]byte(inventory.CapabilitiesJSON), &capabilities); err != nil {
			continue
		}
		for _, route := range capabilities.MoviePilotRoutes {
			if !strings.EqualFold(strings.TrimSpace(route.BridgeInstanceID), strings.TrimSpace(bridgeID)) || !strings.EqualFold(strings.TrimSpace(route.Downloader), strings.TrimSpace(downloader)) {
				continue
			}
			if health := strings.ToLower(strings.TrimSpace(route.QBHealth)); health != "" && health != "configured" && health != "ready" && health != "healthy" {
				continue
			}
			if workerID != "" && (workerID != inventory.NodeID || qbID != route.QBClientID) {
				return "", "", errors.New("MoviePilot downloader route is advertised by multiple Workers")
			}
			workerID, qbID = inventory.NodeID, route.QBClientID
		}
	}
	if workerID == "" || qbID == "" {
		return "", "", fmt.Errorf("no healthy Worker route is advertised for MoviePilot bridge %q downloader %q", bridgeID, downloader)
	}
	return workerID, qbID, nil
}

func (s *Service) loadMoviePilotSubscriptionItem(ctx context.Context, intent *model.MoviePilotDownloadIntent, item *model.SubscriptionItem) error {
	if intent == nil || item == nil {
		return errors.New("MoviePilot intent and item are required")
	}
	if intent.SubscriptionItemID != 0 {
		if err := s.db.WithContext(ctx).First(item, "id = ? AND subscription_id = ?", intent.SubscriptionItemID, intent.SubscriptionID).Error; err == nil {
			return nil
		}
	}
	query := s.db.WithContext(ctx).Where("subscription_id = ?", intent.SubscriptionID)
	if strings.TrimSpace(intent.MediaID) != "" {
		query = query.Where("provider_data LIKE ?", "%"+strings.TrimSpace(intent.MediaID)+"%")
	}
	if err := query.Order("created_at ASC, id ASC").First(item).Error; err != nil {
		return fmt.Errorf("MoviePilot subscription item is not ready: %w", err)
	}
	return nil
}

func moviePilotTorrentTaskContext(sub *model.Subscription, item *model.SubscriptionItem, binding *model.MoviePilotTorrentBinding, mediaItemID string, media moviepilotbridge.MediaIdentity) protocol.TaskContext {
	if sub == nil {
		sub = &model.Subscription{}
	}
	if item == nil {
		item = &model.SubscriptionItem{}
	}
	mediaType := strings.TrimSpace(media.MediaType)
	if mediaType == "" {
		mediaType = sub.MediaType
	}
	mediaSource := strings.TrimSpace(media.MediaSource)
	if mediaSource == "" {
		mediaSource = item.SourceProvider
	}
	mediaID := strings.TrimSpace(media.MediaID)
	if mediaID == "" {
		mediaID = fmt.Sprint(sub.TMDBID)
	}
	season, episode := media.Season, media.Episode
	if season == 0 {
		season = item.Season
	}
	if episode == 0 {
		episode = item.Episode
	}
	logicalTarget := strings.TrimSpace(item.TargetPath)
	if logicalTarget == "" {
		logicalTarget = path.Join(strings.TrimSpace(item.TargetDir), strings.TrimSpace(item.TargetName))
	}
	if logicalTarget == "." || logicalTarget == "/" || logicalTarget == "" {
		name := strings.TrimSpace(item.FileName)
		if name == "" {
			name = "moviepilot-media.mkv"
		}
		logicalTarget = path.Join("/moviepilot", name)
	}
	logicalRoot := strings.TrimSpace(sub.TargetRoot)
	if logicalRoot == "" || logicalRoot == "/" || !pathWithin(logicalRoot, logicalTarget) {
		logicalRoot = path.Dir(logicalTarget)
	}
	deliveryProvider := strings.TrimSpace(sub.DeliveryTarget.Provider)
	if deliveryProvider == "" {
		deliveryProvider = "yidong139"
	}
	deliveryFolder := strings.TrimSpace(sub.DeliveryTarget.Folder)
	if deliveryFolder == "" {
		deliveryFolder = strings.TrimSpace(sub.TargetRoot)
	}
	return protocol.TaskContext{
		ParentBatchID: parentIDForBinding(binding), MediaItemID: mediaItemID, WorkflowVersion: moviePilotTorrentWorkflow,
		SealedManifestVersion: moviePilotTorrentManifest, TargetProfile: "", DeliveryMode: model.SubscriptionDeliveryModeTransfer,
		Subscription: protocol.SubscriptionTaskContext{SubscriptionID: sub.ID, SubscriptionItemID: item.ID, SubscriptionName: sub.Name,
			PreferredWorkerNodeID: binding.WorkerNodeID, Trigger: "moviepilot", SourceKey: item.SourceKey},
		Media: protocol.MediaTaskContext{MediaType: mediaType, TMDBID: sub.TMDBID, TMDBName: sub.TMDBName,
			Season: season, Episode: episode, LogicalMediaRoot: logicalRoot, LogicalTargetPath: logicalTarget},
		StagingTarget:  protocol.ProviderTargetRequirement{Provider: "moviepilot-qb", RequiredBytes: item.FileSize},
		DeliveryTarget: protocol.ProviderTargetRequirement{Provider: deliveryProvider, Folder: deliveryFolder, NeedUpload: true, RequiredBytes: item.FileSize},
		Torrent: &protocol.TorrentTaskContext{BindingID: binding.ID, WorkerNodeID: binding.WorkerNodeID, BridgeInstanceID: binding.BridgeInstanceID,
			Downloader: binding.DownloaderAlias, QBClientID: binding.QBClientID, TorrentHash: binding.TorrentHash, ContentPath: binding.ContentPath},
	}
}

func parentIDForBinding(binding *model.MoviePilotTorrentBinding) string {
	if binding == nil {
		return ""
	}
	return strings.TrimSpace(binding.ObserveJobID)
}

func pathWithin(root, target string) bool {
	root = path.Clean(strings.TrimSpace(root))
	target = path.Clean(strings.TrimSpace(target))
	return root != "." && target != "." && (target == root || strings.HasPrefix(target, root+"/"))
}

type observedTorrentFile struct {
	Hash         string  `json:"hash"`
	Name         string  `json:"name"`
	QBPath       string  `json:"qb_path"`
	WorkerPath   string  `json:"worker_path"`
	DownloadRoot string  `json:"download_root"`
	Size         int64   `json:"size"`
	Progress     float64 `json:"progress"`
}

// ObserveTorrent consumes the Worker qB observation and creates one durable
// delivery row per media file. Files that cannot map to a subscription item
// are retained as skipped rows for audit, but never dispatched.
func (s *Service) ObserveTorrent(ctx context.Context, jobID string, result map[string]any) error {
	var job model.ClusterJob
	if err := s.db.WithContext(ctx).First(&job, "id = ?", jobID).Error; err != nil {
		return err
	}
	var task protocol.TaskContext
	if err := json.Unmarshal([]byte(job.TaskContextJSON), &task); err != nil {
		return err
	}
	if task.Torrent == nil {
		return errors.New("torrent observation job has no torrent context")
	}
	var files []observedTorrentFile
	raw, err := json.Marshal(result["files"])
	if err != nil {
		return err
	}
	if err := json.Unmarshal(raw, &files); err != nil {
		return err
	}
	if len(files) == 0 {
		return errors.New("torrent observation returned no files")
	}
	var binding model.MoviePilotTorrentBinding
	if err := s.db.WithContext(ctx).First(&binding, "id = ?", task.Torrent.BindingID).Error; err != nil {
		return err
	}
	var intent model.MoviePilotDownloadIntent
	if err := s.db.WithContext(ctx).First(&intent, "id = ?", binding.IntentID).Error; err != nil {
		return err
	}
	var sub model.Subscription
	if err := s.db.WithContext(ctx).First(&sub, "id = ?", intent.SubscriptionID).Error; err != nil {
		return err
	}
	requiredCount := 0
	if err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.First(&binding, "id = ?", task.Torrent.BindingID).Error; err != nil {
			return err
		}
		if err := tx.Model(&binding).Updates(map[string]any{"status": model.MoviePilotTorrentStatusFilesDiscovered, "observed_content_path": task.Torrent.ContentPath, "last_error": "", "last_error_code": ""}).Error; err != nil {
			return err
		}
		for index := range files {
			file := &files[index]
			relative := path.Clean(strings.TrimSpace(file.Name))
			if relative == "." || relative == "/" || strings.HasPrefix(relative, "../") || path.IsAbs(relative) {
				continue
			}
			season, episode, matched := torrentFileEpisode(relative)
			var item model.SubscriptionItem
			itemFound := false
			if matched {
				itemFound = tx.Where("subscription_id = ? AND season = ? AND episode = ? AND status <> ?", intent.SubscriptionID, season, episode, model.SubscriptionItemStatusSkipped).Order("created_at ASC, id ASC").First(&item).Error == nil
			} else if strings.EqualFold(sub.MediaType, "movie") || len(files) == 1 {
				itemFound = tx.Where("id = ? AND subscription_id = ?", intent.SubscriptionItemID, intent.SubscriptionID).First(&item).Error == nil
			}
			required := itemFound
			if !matched && itemFound {
				season, episode = item.Season, item.Episode
			}
			status := model.MoviePilotDeliveryStatusSkipped
			if required {
				status = model.MoviePilotDeliveryStatusPending
				requiredCount++
			}
			sourceKey := "moviepilot:" + binding.ID + ":" + relative
			if itemFound && strings.TrimSpace(item.SourceKey) != "" {
				sourceKey = item.SourceKey
			}
			var delivery model.MoviePilotDeliveryFile
			isNew := false
			lookup := tx.Where("torrent_binding_id = ? AND relative_path = ?", binding.ID, relative).First(&delivery).Error
			if errors.Is(lookup, gorm.ErrRecordNotFound) {
				delivery = model.MoviePilotDeliveryFile{ID: uuid.NewString(), TorrentBindingID: binding.ID, RelativePath: relative}
				isNew = true
			} else if lookup != nil {
				return lookup
			}
			updates := map[string]any{"file_name": path.Base(relative), "source_size": file.Size, "subscription_item_id": item.ID,
				"source_key": sourceKey, "media_source": intent.MediaSource, "media_id": intent.MediaID, "season": season, "episode": episode,
				"required": required, "status": status}
			if !required {
				updates["last_error_code"] = "unmatched_media_file"
				updates["last_error"] = "torrent file does not map to a subscription episode"
			} else {
				updates["last_error_code"] = ""
				updates["last_error"] = ""
			}
			if isNew {
				for key, value := range updates {
					switch key {
					case "file_name":
						delivery.FileName = value.(string)
					case "source_size":
						delivery.SourceSize = value.(int64)
					case "subscription_item_id":
						delivery.SubscriptionItemID = value.(uint)
					case "source_key":
						delivery.SourceKey = value.(string)
					case "media_source":
						delivery.MediaSource = value.(string)
					case "media_id":
						delivery.MediaID = value.(string)
					case "season":
						delivery.Season = value.(int)
					case "episode":
						delivery.Episode = value.(int)
					case "required":
						delivery.Required = value.(bool)
					case "status":
						delivery.Status = value.(string)
					case "last_error_code":
						delivery.LastErrorCode = value.(string)
					case "last_error":
						delivery.LastError = value.(string)
					}
				}
				if err := tx.Create(&delivery).Error; err != nil {
					return err
				}
			} else if delivery.Status != model.MoviePilotDeliveryStatusMaterialized {
				if err := tx.Model(&delivery).Updates(updates).Error; err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}
	if err := s.db.WithContext(ctx).Model(&model.ClusterJob{}).Where("id = ?", task.ParentBatchID).Updates(map[string]any{
		"expected_items": 1 + requiredCount, "status": model.ClusterJobStatusRunning,
	}).Error; err != nil {
		return err
	}
	return s.ReconcileTorrentTransfers(ctx, task.Torrent.BindingID, 100)
}

func torrentFileEpisode(name string) (int, int, bool) {
	match := torrentEpisodePattern.FindStringSubmatch(path.Base(name))
	if len(match) != 3 {
		return 0, 0, false
	}
	var season, episode int
	_, _ = fmt.Sscanf(match[1], "%d", &season)
	_, _ = fmt.Sscanf(match[2], "%d", &episode)
	return season, episode, season > 0 && episode > 0
}

// ReconcileTorrentTransfers is safe to call from the normal coordinator tick;
// it only dispatches pending required files and never bypasses ETF materializing.
func (s *Service) ReconcileTorrentTransfers(ctx context.Context, bindingID string, limit int) error {
	if limit <= 0 {
		limit = 100
	}
	query := s.db.WithContext(ctx).Where("required = ? AND status IN ?", true, []string{model.MoviePilotDeliveryStatusPending, model.MoviePilotDeliveryStatusFailed})
	if strings.TrimSpace(bindingID) != "" {
		query = query.Where("torrent_binding_id = ?", bindingID)
	}
	var deliveries []model.MoviePilotDeliveryFile
	if err := query.Order("created_at ASC, id ASC").Limit(limit).Find(&deliveries).Error; err != nil {
		return err
	}
	for i := range deliveries {
		if err := s.dispatchDeliveryFile(ctx, &deliveries[i]); err != nil {
			_ = s.db.WithContext(ctx).Model(&model.MoviePilotDeliveryFile{}).Where("id = ?", deliveries[i].ID).Updates(map[string]any{
				"status": model.MoviePilotDeliveryStatusPending, "last_error_code": "dispatch_pending", "last_error": err.Error(),
			}).Error
		}
	}
	return nil
}

func (s *Service) dispatchDeliveryFile(ctx context.Context, delivery *model.MoviePilotDeliveryFile) error {
	if delivery == nil {
		return errors.New("delivery file is nil")
	}
	if s.torrentDispatcher == nil {
		return errors.New("torrent job dispatcher is not configured")
	}
	var binding model.MoviePilotTorrentBinding
	if err := s.db.WithContext(ctx).First(&binding, "id = ?", delivery.TorrentBindingID).Error; err != nil {
		return err
	}
	var intent model.MoviePilotDownloadIntent
	if err := s.db.WithContext(ctx).First(&intent, "id = ?", binding.IntentID).Error; err != nil {
		return err
	}
	var item model.SubscriptionItem
	if delivery.SubscriptionItemID == 0 || s.db.WithContext(ctx).First(&item, "id = ?", delivery.SubscriptionItemID).Error != nil {
		return errors.New("MoviePilot delivery is not associated with a subscription item")
	}
	var sub model.Subscription
	if err := s.db.WithContext(ctx).First(&sub, "id = ?", intent.SubscriptionID).Error; err != nil {
		return err
	}
	task := moviePilotTorrentTaskContext(&sub, &item, &binding, parentIDForBinding(&binding), moviepilotbridge.MediaIdentity{MediaSource: intent.MediaSource, MediaID: intent.MediaID, MediaType: sub.MediaType, Season: delivery.Season, Episode: delivery.Episode})
	task.MediaItemID = "moviepilot-delivery:" + delivery.ID
	task.Torrent.RelativePath = delivery.RelativePath
	task.SourceObjects = []protocol.SourceObject{{Provider: "qbittorrent", SourceFileID: "torrent:" + binding.TorrentHash + ":" + delivery.RelativePath, SourceRelativePath: delivery.RelativePath, Size: delivery.SourceSize}}
	request := TorrentJobDispatchRequest{JobType: model.ClusterJobTypeMediaTransfer, NodeID: binding.WorkerNodeID,
		IdempotencyKey: moviePilotTorrentDeliveryPrefix + delivery.ID, ExpectedBytes: delivery.SourceSize,
		TaskContext: task, RequiredCapabilities: []string{"qb.copy", "mobile.upload", "result.report"}}
	job, err := s.torrentDispatcher.DispatchTorrentJob(ctx, request)
	if err != nil {
		return err
	}
	if err := s.db.WithContext(ctx).Model(&model.MoviePilotDeliveryFile{}).Where("id = ? AND status <> ?", delivery.ID, model.MoviePilotDeliveryStatusMaterialized).Updates(map[string]any{
		"status": model.MoviePilotDeliveryStatusUploading, "cluster_job_id": job.ID, "upload_progress": 0, "last_error_code": "", "last_error": "",
	}).Error; err != nil {
		return err
	}
	return s.db.WithContext(ctx).Model(&model.SubscriptionItem{}).Where("id = ?", item.ID).Updates(map[string]any{
		"cluster_job_id": job.ID, "status": model.SubscriptionItemStatusTransferring,
	}).Error
}

// ReconcileTorrentRetention evaluates already-eligible bindings and issues
// hash-scoped qB deletion jobs. A failed or offline Worker leaves the binding
// eligible, so the next Coordinator tick can retry without deleting files
// from another qB instance.
func (s *Service) ReconcileTorrentRetention(ctx context.Context, limit int) error {
	if s.torrentDispatcher == nil {
		return errors.New("torrent job dispatcher is not configured")
	}
	if err := s.refreshRetentionEligibility(ctx, limit); err != nil {
		return err
	}
	candidates, err := db.ListRetentionCandidates(ctx, s.db, time.Now().UTC(), limit)
	if err != nil {
		return err
	}
	for i := range candidates {
		binding := &candidates[i]
		var parent model.ClusterJob
		if err := s.db.WithContext(ctx).First(&parent, "id = ?", binding.ObserveJobID).Error; err != nil {
			continue
		}
		var task protocol.TaskContext
		if err := json.Unmarshal([]byte(parent.TaskContextJSON), &task); err != nil || task.Torrent == nil {
			continue
		}
		task.ParentBatchID = parent.ID
		task.MediaItemID = "moviepilot-retention:" + binding.ID
		task.SourceObjects = nil
		task.Torrent.Action = "delete"
		task.Torrent.RelativePath = ""
		key := moviePilotTorrentDeliveryPrefix + "retention:" + binding.ID
		_, err := s.torrentDispatcher.DispatchTorrentJob(ctx, TorrentJobDispatchRequest{
			JobType: model.ClusterJobTypeTorrentRetention, NodeID: binding.WorkerNodeID, IdempotencyKey: key,
			TaskContext: task, RequiredCapabilities: []string{"qb.control", "result.report"},
		})
		if err != nil {
			continue
		}
		_ = s.db.WithContext(ctx).Model(&model.MoviePilotTorrentBinding{}).Where("id = ? AND retention_status = ?", binding.ID, model.MoviePilotRetentionStatusEligible).Updates(map[string]any{
			"status": model.MoviePilotTorrentStatusDeleting, "retention_status": model.MoviePilotRetentionStatusDeleting,
			"deleting_at": time.Now().UTC(), "last_error_code": "", "last_error": "",
		}).Error
	}
	return nil
}

func (s *Service) refreshRetentionEligibility(ctx context.Context, limit int) error {
	if limit <= 0 {
		limit = 100
	}
	var bindings []model.MoviePilotTorrentBinding
	if err := s.db.WithContext(ctx).Where("status = ? AND retention_status IN ?", model.MoviePilotTorrentStatusSeeding, []string{model.MoviePilotRetentionStatusPending, model.MoviePilotRetentionStatusHeld}).Limit(limit).Find(&bindings).Error; err != nil {
		return err
	}
	now := time.Now().UTC()
	for _, binding := range bindings {
		var policy model.TorrentRetentionPolicy
		if json.Unmarshal([]byte(binding.RetentionPolicyJSON), &policy) != nil {
			continue
		}
		state := moviepilotbridge.TorrentStatePayload{State: "seeding", Ratio: binding.LastQBRatio, SeedingSeconds: binding.LastQBSeedingSeconds}
		state.HNRPassed = &binding.LastQBHNRPassed
		if !retentionPolicyEligible(policy, state, now) {
			continue
		}
		_ = s.db.WithContext(ctx).Model(&model.MoviePilotTorrentBinding{}).Where("id = ? AND status = ?", binding.ID, model.MoviePilotTorrentStatusSeeding).Updates(map[string]any{
			"retention_status": model.MoviePilotRetentionStatusEligible, "retention_eligible_at": now,
		}).Error
	}
	return nil
}

func (s *Service) completeTorrentRetention(ctx context.Context, jobID string, result protocol.JobResult) error {
	var job model.ClusterJob
	if err := s.db.WithContext(ctx).First(&job, "id = ?", jobID).Error; err != nil {
		return err
	}
	var task protocol.TaskContext
	if err := json.Unmarshal([]byte(job.TaskContextJSON), &task); err != nil || task.Torrent == nil {
		return errors.New("torrent retention job has no torrent context")
	}
	if result.Status == "succeeded" && strings.EqualFold(strings.TrimSpace(task.Torrent.Action), "delete") {
		now := result.FinishedAt.UTC()
		if now.IsZero() {
			now = time.Now().UTC()
		}
		if err := s.db.WithContext(ctx).Model(&model.MoviePilotTorrentBinding{}).Where("id = ?", task.Torrent.BindingID).Updates(map[string]any{
			"status": model.MoviePilotTorrentStatusDeleted, "retention_status": model.MoviePilotRetentionStatusDeleted,
			"deleted_at": now, "last_error_code": "", "last_error": "",
		}).Error; err != nil {
			return err
		}
		var binding model.MoviePilotTorrentBinding
		if err := s.db.WithContext(ctx).First(&binding, "id = ?", task.Torrent.BindingID).Error; err != nil {
			return err
		}
		return s.db.WithContext(ctx).Model(&model.MoviePilotDownloadIntent{}).Where("id = ?", binding.IntentID).Update("status", model.MoviePilotIntentStatusCompleted).Error
	}
	return s.db.WithContext(ctx).Model(&model.MoviePilotTorrentBinding{}).Where("id = ?", task.Torrent.BindingID).Updates(map[string]any{
		"status": model.MoviePilotTorrentStatusSeeding, "retention_status": model.MoviePilotRetentionStatusManualReview,
		"last_error_code": result.ErrorCode, "last_error": result.Error,
	}).Error
}
