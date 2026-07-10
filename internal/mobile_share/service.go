package mobileshare

import (
	"context"
	"fmt"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/pkg/errors"
)

type Creator interface {
	CreateMobileShare(context.Context, model.Obj, model.MobileShareCreateArgs) (*model.MobileShareLink, error)
}

type ShareRequest struct {
	StorageID        uint
	StorageMountPath string
	Object           model.Obj
	SourcePath       string
	PeriodUnit       int
	Force            bool
}

func CreateOrReuseShareByPath(ctx context.Context, storage driver.Driver, actualPath string, periodUnit int, force bool) (*model.MobileShareCreateResult, error) {
	if storage == nil || storage.GetStorage() == nil {
		return nil, errors.New("storage is nil")
	}
	creator, ok := storage.(Creator)
	if !ok {
		return nil, errors.New("storage does not support mobile share creation")
	}
	obj, err := op.Get(ctx, storage, actualPath)
	if err != nil {
		return nil, err
	}
	sourcePath := utils.GetFullPath(storage.GetStorage().MountPath, actualPath)
	return CreateOrReuseShare(ctx, creator, ShareRequest{
		StorageID:        storage.GetStorage().ID,
		StorageMountPath: storage.GetStorage().MountPath,
		Object:           obj,
		SourcePath:       sourcePath,
		PeriodUnit:       periodUnit,
		Force:            force,
	})
}

func CreateOrReuseShare(ctx context.Context, creator Creator, req ShareRequest) (*model.MobileShareCreateResult, error) {
	if creator == nil {
		return nil, errors.New("mobile share creator is nil")
	}
	if req.Object == nil {
		return nil, errors.New("mobile share source object is nil")
	}
	sourceFileID := strings.TrimSpace(req.Object.GetID())
	if sourceFileID == "" {
		return nil, errors.New("source object id is empty")
	}
	driveID := DriveID(req.StorageMountPath)
	existing, err := db.GetMobileShareRecordBySource(driveID, sourceFileID)
	if err != nil {
		return nil, err
	}
	if existing != nil && existing.IsValid && !req.Force {
		return &model.MobileShareCreateResult{
			Record:          existing,
			Created:         false,
			Existing:        true,
			RequiresConfirm: true,
		}, nil
	}
	periodUnit := req.PeriodUnit
	if periodUnit <= 0 {
		periodUnit = 1
	}
	link, err := creator.CreateMobileShare(ctx, req.Object, model.MobileShareCreateArgs{
		SourcePath: req.SourcePath,
		PeriodUnit: periodUnit,
	})
	if err != nil {
		return nil, err
	}
	if link == nil || strings.TrimSpace(link.ShareURL) == "" {
		return nil, fmt.Errorf("mobile share response missing share url")
	}
	sourceType := "file"
	if req.Object.IsDir() {
		sourceType = "folder"
	}
	record, err := db.UpsertMobileShareRecord(&model.MobileShareRecord{
		StorageID:        req.StorageID,
		StorageMountPath: utils.FixAndCleanPath(req.StorageMountPath),
		DriveID:          driveID,
		SourceFileID:     sourceFileID,
		SourcePath:       utils.FixAndCleanPath(req.SourcePath),
		SourceName:       req.Object.GetName(),
		SourceType:       sourceType,
		PeriodUnit:       periodUnit,
		LinkID:           strings.TrimSpace(link.LinkID),
		ShareURL:         strings.TrimSpace(link.ShareURL),
		ExtractCode:      strings.TrimSpace(link.ExtractCode),
		IsValid:          true,
		LastError:        "",
	})
	if err != nil {
		return nil, err
	}
	return &model.MobileShareCreateResult{
		Record:          record,
		Created:         true,
		Existing:        existing != nil,
		RequiresConfirm: false,
	}, nil
}

func DriveID(mountPath string) string {
	return utils.FixAndCleanPath(mountPath)
}
