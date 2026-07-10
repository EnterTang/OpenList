package handles

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	mobileshare "github.com/OpenListTeam/OpenList/v4/internal/mobile_share"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/OpenListTeam/OpenList/v4/server/common"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type mobileShareDeleter interface {
	DeleteMobileShare(context.Context, model.MobileShareDeleteArgs) error
}

type createMobileShareReq struct {
	Path       string `json:"path" binding:"required"`
	Force      bool   `json:"force"`
	PeriodUnit int    `json:"period_unit"`
}

type listMobileShareRecordsReq struct {
	model.PageReq
	Keyword          string `form:"keyword" json:"keyword"`
	StorageMountPath string `form:"storage_mount_path" json:"storage_mount_path"`
	SourceType       string `form:"source_type" json:"source_type"`
	IsValid          string `form:"is_valid" json:"is_valid"`
}

type deleteMobileShareReq struct {
	ID  uint   `json:"id"`
	IDs []uint `json:"ids"`
}

func CreateMobileShare(c *gin.Context) {
	var req createMobileShareReq
	if err := c.ShouldBindJSON(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	user := c.Request.Context().Value(conf.UserKey).(*model.User)
	reqPath, err := user.JoinPath(req.Path)
	if err != nil {
		common.ErrorResp(c, err, 403)
		return
	}
	storage, actualPath, err := op.GetStorageAndActualPath(reqPath)
	if err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	if _, ok := storage.(mobileshare.Creator); !ok {
		common.ErrorStrResp(c, "storage does not support mobile share creation", 400)
		return
	}
	obj, err := op.Get(c.Request.Context(), storage, actualPath)
	if err != nil {
		common.ErrorResp(c, err, 404)
		return
	}
	result, err := mobileshare.CreateOrReuseShare(c.Request.Context(), storage.(mobileshare.Creator), mobileshare.ShareRequest{
		StorageID:        storage.GetStorage().ID,
		StorageMountPath: storage.GetStorage().MountPath,
		Object:           obj,
		SourcePath:       reqPath,
		PeriodUnit:       req.PeriodUnit,
		Force:            req.Force,
	})
	if err != nil {
		common.ErrorResp(c, err, 500)
		return
	}
	common.SuccessResp(c, result)
}

func ListMobileShareRecords(c *gin.Context) {
	var req listMobileShareRecordsReq
	if err := c.ShouldBind(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	req.Validate()
	var isValid *bool
	if req.IsValid != "" {
		value, err := strconv.ParseBool(req.IsValid)
		if err != nil {
			common.ErrorResp(c, err, 400)
			return
		}
		isValid = &value
	}
	storageMountPath := strings.TrimSpace(req.StorageMountPath)
	if storageMountPath != "" {
		storageMountPath = utils.FixAndCleanPath(storageMountPath)
	}
	records, total, err := db.ListMobileShareRecords(db.MobileShareRecordFilter{
		Keyword:          req.Keyword,
		StorageMountPath: storageMountPath,
		SourceType:       req.SourceType,
		IsValid:          isValid,
		Page:             req.Page,
		PerPage:          req.PerPage,
	})
	if err != nil {
		common.ErrorResp(c, err, 500)
		return
	}
	common.SuccessResp(c, common.PageResp{
		Content: records,
		Total:   total,
	})
}

func DeleteMobileShare(c *gin.Context) {
	var req deleteMobileShareReq
	if err := c.ShouldBindJSON(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	ids := mobileShareDeleteRecordIDs(req)
	if len(ids) == 0 {
		common.ErrorStrResp(c, "mobile share record id is required", 400)
		return
	}

	records := make([]*model.MobileShareRecord, 0, len(ids))
	recordsByMount := make(map[string][]*model.MobileShareRecord)
	deleted := 0
	for _, id := range ids {
		record, err := db.GetMobileShareRecordByID(id)
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				common.ErrorResp(c, err, 404)
				return
			}
			common.ErrorResp(c, err, 500)
			return
		}
		records = append(records, record)
		if !record.IsValid {
			continue
		}
		if strings.TrimSpace(record.LinkID) == "" {
			record.IsValid = false
			record.LastError = ""
			if err := db.UpdateMobileShareRecord(record); err != nil {
				common.ErrorResp(c, err, 500)
				return
			}
			deleted++
			continue
		}
		mountPath := utils.FixAndCleanPath(record.StorageMountPath)
		recordsByMount[mountPath] = append(recordsByMount[mountPath], record)
	}

	for mountPath, group := range recordsByMount {
		storage, err := op.GetStorageByMountPath(mountPath)
		if err != nil {
			_ = updateMobileShareRecordsError(group, err)
			common.ErrorResp(c, err, 400)
			return
		}
		deleter, ok := storage.(mobileShareDeleter)
		if !ok {
			err := errors.New("storage does not support mobile share deletion")
			_ = updateMobileShareRecordsError(group, err)
			common.ErrorResp(c, err, 400)
			return
		}
		if err := deleter.DeleteMobileShare(c.Request.Context(), model.MobileShareDeleteArgs{
			LinkIDs: mobileShareRecordLinkIDs(group),
		}); err != nil {
			_ = updateMobileShareRecordsError(group, err)
			common.ErrorResp(c, err, 500)
			return
		}
		for _, record := range group {
			record.IsValid = false
			record.LastError = ""
			if err := db.UpdateMobileShareRecord(record); err != nil {
				common.ErrorResp(c, err, 500)
				return
			}
			deleted++
		}
	}

	common.SuccessResp(c, model.MobileShareDeleteResult{
		Records: mobileShareRecordValues(records),
		Deleted: deleted,
	})
}

func mobileShareDriveID(mountPath string) string {
	return mobileshare.DriveID(mountPath)
}

func mobileShareDeleteRecordIDs(req deleteMobileShareReq) []uint {
	seen := make(map[uint]struct{}, len(req.IDs)+1)
	ids := make([]uint, 0, len(req.IDs)+1)
	add := func(id uint) {
		if id == 0 {
			return
		}
		if _, ok := seen[id]; ok {
			return
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	add(req.ID)
	for _, id := range req.IDs {
		add(id)
	}
	return ids
}

func mobileShareRecordLinkIDs(records []*model.MobileShareRecord) []string {
	seen := make(map[string]struct{}, len(records))
	linkIDs := make([]string, 0, len(records))
	for _, record := range records {
		linkID := strings.TrimSpace(record.LinkID)
		if linkID == "" {
			continue
		}
		if _, ok := seen[linkID]; ok {
			continue
		}
		seen[linkID] = struct{}{}
		linkIDs = append(linkIDs, linkID)
	}
	return linkIDs
}

func updateMobileShareRecordsError(records []*model.MobileShareRecord, err error) error {
	if err == nil {
		return nil
	}
	for _, record := range records {
		record.LastError = err.Error()
		if updateErr := db.UpdateMobileShareRecord(record); updateErr != nil {
			return updateErr
		}
	}
	return nil
}

func mobileShareRecordValues(records []*model.MobileShareRecord) []model.MobileShareRecord {
	values := make([]model.MobileShareRecord, 0, len(records))
	for _, record := range records {
		values = append(values, *record)
	}
	return values
}
