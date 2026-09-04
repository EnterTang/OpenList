package db

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func CreateIntentTx(ctx context.Context, database *gorm.DB, intent *model.MoviePilotDownloadIntent) error {
	if database == nil {
		return errors.New("database is required")
	}
	if intent == nil || strings.TrimSpace(intent.ID) == "" || strings.TrimSpace(intent.RequestID) == "" {
		return errors.New("intent id and request id are required")
	}
	if strings.TrimSpace(intent.Status) == "" {
		intent.Status = model.MoviePilotIntentStatusPending
	}
	return database.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var existing model.MoviePilotDownloadIntent
		err := tx.Where("request_id = ?", intent.RequestID).First(&existing).Error
		if err == nil {
			if existing.BridgeInstanceID != intent.BridgeInstanceID ||
				existing.SubscriptionID != intent.SubscriptionID ||
				existing.MediaSource != intent.MediaSource ||
				existing.MediaID != intent.MediaID ||
				existing.ResourceRef != intent.ResourceRef ||
				existing.TorrentFingerprint != intent.TorrentFingerprint ||
				existing.RetentionPolicyJSON != intent.RetentionPolicyJSON {
				return fmt.Errorf("request id %q already belongs to a different intent", intent.RequestID)
			}
			// SubscriptionItemID is Coordinator-local bookkeeping, not part of
			// the Bridge intent identity. Older rows may have been created before
			// the item was persisted, so allow a later retry to attach that item.
			if existing.SubscriptionItemID == 0 && intent.SubscriptionItemID != 0 {
				if err := tx.Model(&existing).Update("subscription_item_id", intent.SubscriptionItemID).Error; err != nil {
					return err
				}
				existing.SubscriptionItemID = intent.SubscriptionItemID
			}
			// Scheduling metadata is deliberately mutable while an intent is
			// waiting for capacity or being retried. Once a torrent is bound,
			// the binding remains authoritative and a late retry cannot move it.
			if existing.Status != model.MoviePilotIntentStatusBound && existing.Status != model.MoviePilotIntentStatusCompleted && existing.Status != model.MoviePilotIntentStatusCancelled {
				updates := map[string]any{}
				if strings.TrimSpace(intent.DownloaderPolicyJSON) != "" {
					// The policy JSON is the complete scheduling projection. Clear
					// old route/reservation fields when preferred mode falls back to
					// MoviePilot selection on a later retry.
					updates["downloader_policy_json"] = intent.DownloaderPolicyJSON
					updates["downloader_policy_mode"] = intent.DownloaderPolicyMode
					updates["selected_downloader"] = intent.SelectedDownloader
					updates["selected_route_id"] = intent.SelectedRouteID
					updates["selected_worker_node_id"] = intent.SelectedWorkerNodeID
					updates["selected_qb_client_id"] = intent.SelectedQBClientID
					updates["reservation_id"] = intent.ReservationID
					updates["reservation_expires_at"] = intent.ReservationExpiresAt
					// A successful retry must clear the delivery error that caused
					// the intent to wait. Leaving it behind makes the reservation
					// reaper mistake a newly selected route for a stale failed one.
					updates["last_error_code"] = intent.LastErrorCode
					updates["last_error"] = intent.LastError
				}
				if intent.Status != "" && (intent.Status != model.MoviePilotIntentStatusPending || existing.Status == model.MoviePilotIntentStatusWaitingCapacity || existing.Status == model.MoviePilotIntentStatusFailed) {
					updates["status"] = intent.Status
				}
				if len(updates) > 0 {
					if err := tx.Model(&existing).Updates(updates).Error; err != nil {
						return err
					}
					if err := tx.First(&existing, "id = ?", existing.ID).Error; err != nil {
						return err
					}
				}
			}
			intent.ID = existing.ID
			*intent = existing
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		return tx.Create(intent).Error
	})
}

func BindTorrentTx(ctx context.Context, database *gorm.DB, intent *model.MoviePilotDownloadIntent, bridgeID, downloader, workerID, qbClientID, torrentHash, contentPath string) (*model.MoviePilotTorrentBinding, error) {
	if database == nil {
		return nil, errors.New("database is required")
	}
	if intent == nil || strings.TrimSpace(intent.ID) == "" {
		return nil, errors.New("intent is required")
	}
	if err := validateTorrentHash(torrentHash); err != nil {
		return nil, err
	}
	if strings.TrimSpace(bridgeID) == "" || strings.TrimSpace(downloader) == "" || strings.TrimSpace(workerID) == "" || strings.TrimSpace(qbClientID) == "" {
		return nil, errors.New("bridge, downloader, worker, and qB client are required")
	}

	binding := &model.MoviePilotTorrentBinding{}
	err := database.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var lockedIntent model.MoviePilotDownloadIntent
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&lockedIntent, "id = ?", intent.ID).Error; err != nil {
			return err
		}
		var existing model.MoviePilotTorrentBinding
		byIntentErr := tx.Where("intent_id = ?", lockedIntent.ID).First(&existing).Error
		if byIntentErr == nil {
			if existing.TorrentHash == torrentHash && existing.WorkerNodeID != strings.TrimSpace(workerID) {
				return fmt.Errorf("torrent hash is already bound to %s", existing.WorkerNodeID)
			}
			if existing.BridgeInstanceID != strings.TrimSpace(bridgeID) ||
				existing.DownloaderAlias != strings.TrimSpace(downloader) ||
				existing.WorkerNodeID != strings.TrimSpace(workerID) ||
				existing.QBClientID != strings.TrimSpace(qbClientID) ||
				existing.TorrentHash != torrentHash ||
				existing.ContentPath != strings.TrimSpace(contentPath) {
				return fmt.Errorf("request id %q already has a different immutable torrent binding", lockedIntent.RequestID)
			}
			*binding = existing
			return nil
		}
		if !errors.Is(byIntentErr, gorm.ErrRecordNotFound) {
			return byIntentErr
		}
		var byHash model.MoviePilotTorrentBinding
		byHashErr := tx.Where("bridge_instance_id = ? AND torrent_hash = ?", strings.TrimSpace(bridgeID), torrentHash).First(&byHash).Error
		if byHashErr == nil {
			return fmt.Errorf("torrent hash is already bound to %s", byHash.WorkerNodeID)
		}
		if !errors.Is(byHashErr, gorm.ErrRecordNotFound) {
			return byHashErr
		}
		now := time.Now().UTC()
		retentionPolicy := strings.TrimSpace(lockedIntent.RetentionPolicyJSON)
		*binding = model.MoviePilotTorrentBinding{
			ID:                  uuid.NewString(),
			IntentID:            lockedIntent.ID,
			BridgeInstanceID:    strings.TrimSpace(bridgeID),
			DownloaderAlias:     strings.TrimSpace(downloader),
			WorkerNodeID:        strings.TrimSpace(workerID),
			QBClientID:          strings.TrimSpace(qbClientID),
			TorrentHash:         torrentHash,
			ContentPath:         strings.TrimSpace(contentPath),
			RetentionPolicyJSON: retentionPolicy,
			Status:              model.MoviePilotTorrentStatusBound,
			RetentionStatus:     model.MoviePilotRetentionStatusPending,
		}
		if err := tx.Create(binding).Error; err != nil {
			return err
		}
		updates := map[string]interface{}{"status": model.MoviePilotIntentStatusBound, "bound_at": now}
		return tx.Model(&lockedIntent).Updates(updates).Error
	})
	if err != nil {
		return nil, err
	}
	return binding, nil
}

// NormalizeMoviePilotTorrentBindingIndex removes the pre-release global hash
// uniqueness constraint. qB torrent hashes are unique within one MoviePilot
// instance, but the same public torrent may legitimately be present in two
// independent instances.
func NormalizeMoviePilotTorrentBindingIndex(database *gorm.DB) error {
	if database == nil {
		return errors.New("database is required")
	}
	if !database.Migrator().HasTable(&model.MoviePilotTorrentBinding{}) {
		return nil
	}
	const legacyIndex = "idx_moviepilot_torrent_hash"
	if database.Migrator().HasIndex(&model.MoviePilotTorrentBinding{}, legacyIndex) {
		if err := database.Migrator().DropIndex(&model.MoviePilotTorrentBinding{}, legacyIndex); err != nil {
			return fmt.Errorf("drop legacy MoviePilot torrent hash index: %w", err)
		}
	}
	const scopedIndex = "idx_moviepilot_bridge_torrent_hash"
	if !database.Migrator().HasIndex(&model.MoviePilotTorrentBinding{}, scopedIndex) {
		if err := database.Migrator().CreateIndex(&model.MoviePilotTorrentBinding{}, scopedIndex); err != nil {
			return fmt.Errorf("create scoped MoviePilot torrent hash index: %w", err)
		}
	}
	return nil
}

func ListRetentionCandidates(ctx context.Context, database *gorm.DB, now time.Time, limit int) ([]model.MoviePilotTorrentBinding, error) {
	if database == nil {
		return nil, errors.New("database is required")
	}
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	var rows []model.MoviePilotTorrentBinding
	err := database.WithContext(ctx).
		Where("status = ? AND retention_status = ? AND retention_eligible_at IS NOT NULL AND retention_eligible_at <= ?", model.MoviePilotTorrentStatusSeeding, model.MoviePilotRetentionStatusEligible, now).
		Order("created_at ASC").Limit(limit).Find(&rows).Error
	return rows, err
}

// ListMoviePilotProgressBySubscriptionIDs returns a compact, read-only
// projection for subscription cards. Missing MoviePilot tables are treated as
// an empty projection so rolling upgrades keep the existing subscription API
// usable before AutoMigrate has run.
func ListMoviePilotProgressBySubscriptionIDs(subscriptionIDs []uint) (map[uint]model.MoviePilotProgressSnapshot, error) {
	result := make(map[uint]model.MoviePilotProgressSnapshot)
	if len(subscriptionIDs) == 0 {
		return result, nil
	}
	var intents []model.MoviePilotDownloadIntent
	if err := db.Where("subscription_id IN ?", subscriptionIDs).Find(&intents).Error; err != nil {
		if isMoviePilotMissingTableError(err) {
			return result, nil
		}
		return nil, err
	}
	intentIDs := make([]string, 0, len(intents))
	intentSubscription := make(map[string]uint, len(intents))
	for _, intent := range intents {
		intentIDs = append(intentIDs, intent.ID)
		intentSubscription[intent.ID] = intent.SubscriptionID
	}
	if len(intentIDs) == 0 {
		return result, nil
	}
	var bindings []model.MoviePilotTorrentBinding
	if err := db.Where("intent_id IN ?", intentIDs).Order("updated_at DESC, id DESC").Find(&bindings).Error; err != nil {
		return nil, err
	}
	bindingIDs := make([]string, 0, len(bindings))
	seenSubscription := make(map[uint]struct{}, len(bindings))
	selectedBinding := make(map[uint]string, len(bindings))
	for _, binding := range bindings {
		subscriptionID := intentSubscription[binding.IntentID]
		if subscriptionID == 0 {
			continue
		}
		if _, exists := seenSubscription[subscriptionID]; exists {
			continue
		}
		seenSubscription[subscriptionID] = struct{}{}
		selectedBinding[subscriptionID] = binding.ID
		result[subscriptionID] = model.MoviePilotProgressSnapshot{TorrentStatus: binding.Status, DownloadProgress: binding.LastQBProgress, SeedElapsed: binding.LastQBSeedingSeconds, RetentionStatus: binding.RetentionStatus}
		bindingIDs = append(bindingIDs, binding.ID)
	}
	if len(bindingIDs) == 0 {
		return result, nil
	}
	var deliveries []model.MoviePilotDeliveryFile
	if err := db.Where("torrent_binding_id IN ? AND required = ?", bindingIDs, true).Find(&deliveries).Error; err != nil {
		if isMoviePilotMissingTableError(err) {
			return result, nil
		}
		return nil, err
	}
	counts := make(map[string][3]float64, len(bindingIDs))
	for _, delivery := range deliveries {
		value := counts[delivery.TorrentBindingID]
		value[0]++
		value[1] += delivery.UploadProgress
		if delivery.Status == model.MoviePilotDeliveryStatusMaterialized {
			value[2]++
		}
		counts[delivery.TorrentBindingID] = value
	}
	for _, binding := range bindings {
		subscriptionID := intentSubscription[binding.IntentID]
		if subscriptionID == 0 {
			continue
		}
		if selectedBinding[subscriptionID] != binding.ID {
			continue
		}
		current, ok := result[subscriptionID]
		if !ok {
			continue
		}
		value := counts[binding.ID]
		current.ExpectedFiles = int(value[0])
		current.TransferredFiles = int(value[2])
		if value[0] > 0 {
			current.UploadProgress = value[1] / value[0]
		}
		result[subscriptionID] = current
	}
	return result, nil
}

func ListMoviePilotTransferViews(ctx context.Context, subscriptionID uint, bindingID string) ([]model.MoviePilotTransferView, error) {
	if subscriptionID == 0 {
		return nil, errors.New("subscription id is required")
	}
	var rows []model.MoviePilotTransferView
	query := db.WithContext(ctx).Table("movie_pilot_delivery_files AS f").
		Select("f.id AS delivery_id, i.subscription_id, f.torrent_binding_id, f.relative_path, f.file_name, f.source_size, f.subscription_item_id, f.source_key, f.season, f.episode, f.required, f.status, f.cluster_job_id, f.manifest_id, f.upload_progress, f.last_error_code, f.last_error, f.finished_at, b.worker_node_id, b.downloader_alias, b.qb_client_id, b.torrent_hash, b.status AS torrent_status, b.retention_status").
		Joins("JOIN movie_pilot_torrent_bindings AS b ON b.id = f.torrent_binding_id").
		Joins("JOIN movie_pilot_download_intents AS i ON i.id = b.intent_id").
		Where("i.subscription_id = ?", subscriptionID)
	if strings.TrimSpace(bindingID) != "" {
		query = query.Where("f.torrent_binding_id = ?", strings.TrimSpace(bindingID))
	}
	if err := query.Order("f.created_at ASC, f.id ASC").Scan(&rows).Error; err != nil {
		if isMoviePilotMissingTableError(err) {
			return []model.MoviePilotTransferView{}, nil
		}
		return nil, err
	}
	return rows, nil
}

func isMoviePilotMissingTableError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "no such table: movie_pilot_") || strings.Contains(message, "doesn't exist") && strings.Contains(message, "movie_pilot_")
}

func validateTorrentHash(value string) error {
	value = strings.TrimSpace(value)
	if len(value) != 40 && len(value) != 64 {
		return errors.New("torrent hash must contain 40 or 64 hexadecimal characters")
	}
	if value != strings.ToLower(value) {
		return errors.New("torrent hash must be lowercase")
	}
	if _, err := hex.DecodeString(value); err != nil {
		return errors.New("torrent hash must contain hexadecimal characters")
	}
	return nil
}
