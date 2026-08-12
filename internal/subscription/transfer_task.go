package subscription

import (
	"context"
	stdpath "path"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/etfmeta"
	"github.com/OpenListTeam/OpenList/v4/internal/fs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/task_group"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const standaloneTransferRecoveryAge = 5 * time.Minute

func RecoverStaleStandaloneTransfers(ctx context.Context, subscriptionID uint) (int, error) {
	var items []model.SubscriptionItem
	var err error
	if subscriptionID > 0 {
		items, err = db.ListSubscriptionItems(subscriptionID)
	} else {
		subscriptions, listErr := db.ListActiveSubscriptions()
		if listErr != nil {
			return 0, listErr
		}
		ids := make([]uint, 0, len(subscriptions))
		for i := range subscriptions {
			ids = append(ids, subscriptions[i].ID)
		}
		items, err = db.ListSubscriptionItemsBySubscriptionIDs(ids)
	}
	if err != nil {
		return 0, err
	}
	cutoff := time.Now().Add(-standaloneTransferRecoveryAge)
	recovered := 0
	for i := range items {
		item := &items[i]
		if item.Status != model.SubscriptionItemStatusTransferring || strings.TrimSpace(item.ClusterJobID) != "" || item.UpdatedAt.After(cutoff) {
			continue
		}
		if standaloneMoveTaskIsActive(item) {
			continue
		}
		exists, probeErr := standaloneTransferTargetExists(ctx, item)
		if probeErr != nil {
			logrus.WithError(probeErr).WithField("subscription_item_id", item.ID).Warn("skip stale standalone transfer recovery because target probe failed")
			continue
		}
		if exists {
			if err := persistStandaloneTerminalSubscriptionItem(item, model.SubscriptionItemStatusTransferred, ""); err != nil {
				return recovered, err
			}
		} else {
			changed, err := db.ResetStandaloneSubscriptionTransfer(item, "standalone transfer was recovered after restart; retry is required")
			if err != nil {
				return recovered, err
			}
			if !changed {
				continue
			}
		}
		recovered++
	}
	return recovered, nil
}

func standaloneMoveTaskIsActive(item *model.SubscriptionItem) bool {
	if item == nil || fs.MoveTaskManager == nil {
		return false
	}
	sourcePath := utils.FixAndCleanPath(item.SourcePath)
	targetDir := utils.FixAndCleanPath(item.TargetDir)
	for _, task := range fs.MoveTaskManager.GetAll() {
		if task == nil || task.ClusterBinding != nil {
			continue
		}
		taskSourcePath := utils.FixAndCleanPath(stdpath.Join(task.SrcStorageMp, task.SrcActualPath))
		taskTargetDir := utils.FixAndCleanPath(stdpath.Join(task.DstStorageMp, task.DstActualPath))
		if sourcePath != "" && sourcePath == taskSourcePath && targetDir == taskTargetDir {
			return true
		}
	}
	return false
}

func standaloneTransferTargetExists(ctx context.Context, item *model.SubscriptionItem) (bool, error) {
	if item == nil {
		return false, nil
	}
	if targetPath := strings.TrimSpace(item.TargetPath); targetPath != "" {
		obj, err := fs.Get(ctx, targetPath, &fs.GetArgs{NoLog: true})
		if err == nil && obj != nil && !obj.IsDir() {
			return true, nil
		}
		if err != nil && !errs.IsObjectNotFound(err) {
			return false, err
		}
	}
	payload := TransferFinalizePayload{
		TargetDir:  item.TargetDir,
		FileName:   item.FileName,
		TargetName: item.TargetName,
	}
	return generatedETFExists(ctx, payload), nil
}

type TransferFinalizePayload = task_group.TransferFinalizePayload

func RegisterTransferTaskHooks() {
	task_group.RegisterPayloadHandler(handleTransferPayload)
}

func handleTransferPayload(ctx context.Context, success bool, payload any) {
	item, ok := payload.(TransferFinalizePayload)
	if !ok {
		return
	}
	if success {
		finalizeSubscriptionTransfer(ctx, item)
		return
	}
	markSubscriptionTransferFailed(item, errors.New("transfer task failed"))
}

func finalizeSubscriptionTransfer(ctx context.Context, payload TransferFinalizePayload) {
	copiedPath := utils.FixAndCleanPath(stdpath.Join(payload.TargetDir, payload.FileName))
	if payload.TargetName != "" && payload.TargetName != payload.FileName {
		if sourceDeletedAfterETFGeneration(ctx, payload, copiedPath) {
			markSubscriptionTransferSucceeded(payload)
			return
		}
		if err := fs.Rename(ctx, copiedPath, payload.TargetName, true); err != nil {
			if errs.IsObjectNotFound(err) && generatedETFExists(ctx, payload) {
				markSubscriptionTransferSucceeded(payload)
				return
			}
			markSubscriptionTransferFailed(payload, err)
			return
		}
	}
	markSubscriptionTransferSucceeded(payload)
}

func sourceDeletedAfterETFGeneration(ctx context.Context, payload TransferFinalizePayload, copiedPath string) bool {
	if _, err := fs.Get(ctx, copiedPath, &fs.GetArgs{NoLog: true}); !errs.IsObjectNotFound(err) {
		return false
	}
	return generatedETFExists(ctx, payload)
}

func generatedETFExists(ctx context.Context, payload TransferFinalizePayload) bool {
	candidates := []string{
		utils.FixAndCleanPath(stdpath.Join(payload.TargetDir, etfmeta.FileName(payload.FileName))),
	}
	if payload.TargetName != "" && payload.TargetName != payload.FileName {
		candidates = append(candidates, utils.FixAndCleanPath(stdpath.Join(payload.TargetDir, etfmeta.FileName(payload.TargetName))))
	}
	for _, candidate := range candidates {
		obj, err := fs.Get(ctx, candidate, &fs.GetArgs{NoLog: true})
		if err == nil && obj != nil && !obj.IsDir() {
			return true
		}
	}
	return false
}

func markSubscriptionTransferSucceeded(payload TransferFinalizePayload) {
	item, err := db.GetSubscriptionItem(payload.SubscriptionID, payload.SourceKey)
	if err != nil || item == nil {
		return
	}
	if !transferPayloadMatchesItem(payload, item) {
		return
	}
	if err := persistStandaloneTerminalSubscriptionItem(item, model.SubscriptionItemStatusTransferred, ""); err != nil && !errors.Is(err, db.ErrStaleSubscriptionTerminalCallback) {
		logrus.WithError(err).WithField("subscription_item_id", item.ID).Error("failed to persist subscription transfer completion")
	}
}

func markSubscriptionTransferFailed(payload TransferFinalizePayload, err error) {
	item, getErr := db.GetSubscriptionItem(payload.SubscriptionID, payload.SourceKey)
	if getErr != nil || item == nil {
		return
	}
	if !transferPayloadMatchesItem(payload, item) {
		return
	}
	lastError := item.LastError
	if err != nil {
		lastError = err.Error()
	}
	if persistErr := persistStandaloneTerminalSubscriptionItem(item, model.SubscriptionItemStatusFailed, lastError); persistErr != nil && !errors.Is(persistErr, db.ErrStaleSubscriptionTerminalCallback) {
		logrus.WithError(persistErr).WithField("subscription_item_id", item.ID).Error("failed to persist subscription transfer failure")
	}
}

func transferItem(ctx context.Context, item *model.SubscriptionItem, deleteSourceAfter bool) error {
	if item == nil {
		return errors.New("subscription item is nil")
	}
	targetDir := utils.FixAndCleanPath(item.TargetDir)
	if targetDir == "" || targetDir == "/" {
		return errors.New("target dir is empty")
	}
	if err := ensureDir(ctx, targetDir); err != nil {
		return err
	}
	payload := TransferFinalizePayload{
		SubscriptionID:     item.SubscriptionID,
		SubscriptionItemID: item.ID,
		SourceKey:          item.SourceKey,
		FileHash:           item.FileHash,
		TargetDir:          targetDir,
		FileName:           item.FileName,
		TargetName:         item.TargetName,
	}
	taskCtx := context.WithValue(ctx, conf.ForceTaskKey, struct{}{})
	taskCtx = context.WithValue(taskCtx, conf.TransferTaskPayloadKey, payload)
	var err error
	if deleteSourceAfter {
		_, err = fs.Move(taskCtx, item.SourcePath, targetDir, true)
	} else {
		_, err = fs.Copy(taskCtx, item.SourcePath, targetDir, true)
	}
	if err != nil {
		return err
	}
	return nil
}

func subscriptionEpisodeSourceSnapshot(sourceSub *model.Subscription, item *model.SubscriptionItem) (*model.SubscriptionEpisodeSource, error) {
	if sourceSub == nil {
		return nil, errors.New("source subscription is nil")
	}
	if item == nil {
		return nil, errors.New("subscription item is nil")
	}
	season, episode := item.Season, item.Episode
	if normalizeMediaType(sourceSub.MediaType) == "movie" {
		season, episode = 0, 0
	}
	return &model.SubscriptionEpisodeSource{
		SubscriptionID: item.SubscriptionID,
		Season:         season,
		Episode:        episode,
		SourceItemID:   item.ID,
		SourceType:     sourceSub.SourceType,
		SourceProvider: item.SourceProvider,
		ShareURL:       item.SourceURL,
		FileName:       item.FileName,
		FileHash:       item.FileHash,
		Status:         item.Status,
		ClusterJobID:   item.ClusterJobID,
		SelectedAt:     time.Now(),
	}, nil
}

var persistAcceptedSubscriptionItemAndEpisodeSourceSnapshot = func(sourceSub *model.Subscription, item *model.SubscriptionItem) error {
	source, err := subscriptionEpisodeSourceSnapshot(sourceSub, item)
	if err != nil {
		return err
	}
	_, _, err = db.PersistAcceptedSubscriptionItemAndEpisodeSource(item, source)
	return err
}

var persistSubscriptionTerminalItem = db.PersistSubscriptionTerminalItem

func persistStandaloneTerminalSubscriptionItem(item *model.SubscriptionItem, status, lastError string) error {
	request := db.SubscriptionTerminalItemRequest{
		ItemID:            item.ID,
		SubscriptionID:    item.SubscriptionID,
		SourceKey:         item.SourceKey,
		ExpectedFileHash:  item.FileHash,
		ExpectedStatus:    item.Status,
		TerminalStatus:    status,
		TerminalLastError: lastError,
	}
	if item.Status == model.SubscriptionItemStatusPending {
		subscription, err := db.GetSubscriptionByID(item.SubscriptionID)
		if err != nil {
			return err
		}
		source, err := subscriptionEpisodeSourceSnapshot(subscription, item)
		if err != nil {
			return err
		}
		request.RecoverySource = source
	}
	_, err := persistSubscriptionTerminalItem(request)
	return err
}

func transferPayloadMatchesItem(payload TransferFinalizePayload, item *model.SubscriptionItem) bool {
	return item != nil &&
		payload.SubscriptionItemID != 0 &&
		payload.SubscriptionItemID == item.ID &&
		payload.FileHash == item.FileHash &&
		utils.FixAndCleanPath(payload.TargetDir) == utils.FixAndCleanPath(item.TargetDir) &&
		payload.FileName == item.FileName &&
		payload.TargetName == item.TargetName
}

func applyItemTransfer(ctx context.Context, sourceSub *model.Subscription, stored *model.SubscriptionItem, deleteSourceAfter bool, onAccepted func(*model.Subscription, *model.SubscriptionItem) error) (*model.SubscriptionItem, int, error) {
	if stored == nil {
		return nil, 0, errors.New("subscription item is nil")
	}
	if err := transferItem(ctx, stored, deleteSourceAfter); err != nil {
		stored.Status = model.SubscriptionItemStatusFailed
		stored.LastError = err.Error()
		updated, _, err := db.UpsertSubscriptionItem(stored)
		return updated, 0, err
	}
	stored.Status = model.SubscriptionItemStatusTransferring
	stored.LastError = ""
	if onAccepted == nil {
		return stored, 0, errors.New("accepted subscription item persister is nil")
	}
	if err := onAccepted(sourceSub, stored); err != nil {
		return stored, 0, err
	}
	return stored, 1, nil
}
