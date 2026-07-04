package handles

import (
	"context"
	"strconv"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/OpenListTeam/OpenList/v4/server/common"
	"github.com/gin-gonic/gin"
)

type mobileShareCreator interface {
	CreateMobileShare(context.Context, model.Obj, model.MobileShareCreateArgs) (*model.MobileShareLink, error)
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
	creator, ok := storage.(mobileShareCreator)
	if !ok {
		common.ErrorStrResp(c, "storage does not support mobile share creation", 400)
		return
	}
	obj, err := op.Get(c.Request.Context(), storage, actualPath)
	if err != nil {
		common.ErrorResp(c, err, 404)
		return
	}
	sourceFileID := strings.TrimSpace(obj.GetID())
	if sourceFileID == "" {
		common.ErrorStrResp(c, "source object id is empty", 400)
		return
	}
	driveID := mobileShareDriveID(storage.GetStorage().MountPath)
	existing, err := db.GetMobileShareRecordBySource(driveID, sourceFileID)
	if err != nil {
		common.ErrorResp(c, err, 500)
		return
	}
	if existing != nil && !req.Force {
		common.SuccessResp(c, model.MobileShareCreateResult{
			Record:          existing,
			Created:         false,
			Existing:        true,
			RequiresConfirm: true,
		})
		return
	}
	link, err := creator.CreateMobileShare(c.Request.Context(), obj, model.MobileShareCreateArgs{
		SourcePath: reqPath,
		PeriodUnit: req.PeriodUnit,
	})
	if err != nil {
		common.ErrorResp(c, err, 500)
		return
	}
	sourceType := "file"
	if obj.IsDir() {
		sourceType = "folder"
	}
	periodUnit := req.PeriodUnit
	if periodUnit <= 0 {
		periodUnit = 1
	}
	record, err := db.UpsertMobileShareRecord(&model.MobileShareRecord{
		StorageID:        storage.GetStorage().ID,
		StorageMountPath: utils.FixAndCleanPath(storage.GetStorage().MountPath),
		DriveID:          driveID,
		SourceFileID:     sourceFileID,
		SourcePath:       utils.FixAndCleanPath(reqPath),
		SourceName:       obj.GetName(),
		SourceType:       sourceType,
		PeriodUnit:       periodUnit,
		LinkID:           link.LinkID,
		ShareURL:         link.ShareURL,
		ExtractCode:      link.ExtractCode,
		IsValid:          true,
		LastError:        "",
	})
	if err != nil {
		common.ErrorResp(c, err, 500)
		return
	}
	common.SuccessResp(c, model.MobileShareCreateResult{
		Record:          record,
		Created:         true,
		Existing:        existing != nil,
		RequiresConfirm: false,
	})
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

func mobileShareDriveID(mountPath string) string {
	return utils.FixAndCleanPath(mountPath)
}
