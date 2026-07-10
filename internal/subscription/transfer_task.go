package subscription

import (
	"context"
	stdpath "path"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/etfmeta"
	"github.com/OpenListTeam/OpenList/v4/internal/fs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/task_group"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/pkg/errors"
)

type TransferFinalizePayload struct {
	SubscriptionID uint
	SourceKey      string
	TargetDir      string
	FileName       string
	TargetName     string
}

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
	item.Status = model.SubscriptionItemStatusTransferred
	item.LastError = ""
	_, _, _ = db.UpsertSubscriptionItem(item)
}

func markSubscriptionTransferFailed(payload TransferFinalizePayload, err error) {
	item, getErr := db.GetSubscriptionItem(payload.SubscriptionID, payload.SourceKey)
	if getErr != nil || item == nil {
		return
	}
	item.Status = model.SubscriptionItemStatusFailed
	if err != nil {
		item.LastError = err.Error()
	}
	_, _, _ = db.UpsertSubscriptionItem(item)
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
		SubscriptionID: item.SubscriptionID,
		SourceKey:      item.SourceKey,
		TargetDir:      targetDir,
		FileName:       item.FileName,
		TargetName:     item.TargetName,
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

func applyItemTransfer(ctx context.Context, stored *model.SubscriptionItem, deleteSourceAfter bool) (*model.SubscriptionItem, int, error) {
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
	updated, _, err := db.UpsertSubscriptionItem(stored)
	if err != nil {
		return updated, 0, err
	}
	return updated, 1, nil
}
