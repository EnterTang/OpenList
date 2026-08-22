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
			if existing.BridgeInstanceID != intent.BridgeInstanceID || existing.ResourceRef != intent.ResourceRef || existing.TorrentFingerprint != intent.TorrentFingerprint {
				return fmt.Errorf("request id %q already belongs to a different intent", intent.RequestID)
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
			if existing.TorrentHash != torrentHash {
				return fmt.Errorf("request id %q is already bound to a different torrent hash", lockedIntent.RequestID)
			}
			if existing.WorkerNodeID != workerID {
				return fmt.Errorf("torrent hash is already bound to %s", existing.WorkerNodeID)
			}
			*binding = existing
			return nil
		}
		if !errors.Is(byIntentErr, gorm.ErrRecordNotFound) {
			return byIntentErr
		}
		var byHash model.MoviePilotTorrentBinding
		byHashErr := tx.Where("torrent_hash = ?", torrentHash).First(&byHash).Error
		if byHashErr == nil {
			return fmt.Errorf("torrent hash is already bound to %s", byHash.WorkerNodeID)
		}
		if !errors.Is(byHashErr, gorm.ErrRecordNotFound) {
			return byHashErr
		}
		now := time.Now().UTC()
		*binding = model.MoviePilotTorrentBinding{
			ID:               uuid.NewString(),
			IntentID:         lockedIntent.ID,
			BridgeInstanceID: strings.TrimSpace(bridgeID),
			DownloaderAlias:  strings.TrimSpace(downloader),
			WorkerNodeID:     strings.TrimSpace(workerID),
			QBClientID:       strings.TrimSpace(qbClientID),
			TorrentHash:      torrentHash,
			ContentPath:      strings.TrimSpace(contentPath),
			Status:           model.MoviePilotTorrentStatusBound,
			RetentionStatus:  model.MoviePilotRetentionStatusPending,
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
		Where("status = ? AND retention_status IN ? AND (retention_eligible_at IS NULL OR retention_eligible_at <= ?)", model.MoviePilotTorrentStatusSeeding, []string{model.MoviePilotRetentionStatusPending, model.MoviePilotRetentionStatusEligible}, now).
		Order("created_at ASC").Limit(limit).Find(&rows).Error
	return rows, err
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
