package handles

import (
	"context"
	"strconv"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/server/common"
	"github.com/gin-gonic/gin"
)

type etfArchiveCorrector interface {
	CorrectETFArchive(context.Context, *model.ETFArchiveRecord, model.ETFArchiveCorrection) (*model.ETFArchiveRecord, error)
}

type listETFArchiveRecordsReq struct {
	model.PageReq
	Keyword     string `form:"keyword" json:"keyword"`
	TMDBID      int64  `form:"tmdb_id" json:"tmdb_id"`
	TMDBMatched string `form:"tmdb_matched" json:"tmdb_matched"`
}

type correctETFArchiveRecordReq struct {
	ID uint `json:"id" binding:"required"`
	model.ETFArchiveCorrection
}

func ListETFArchiveRecords(c *gin.Context) {
	var req listETFArchiveRecordsReq
	if err := c.ShouldBind(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	req.Validate()
	var matched *bool
	if req.TMDBMatched != "" {
		value, err := strconv.ParseBool(req.TMDBMatched)
		if err != nil {
			common.ErrorResp(c, err, 400)
			return
		}
		matched = &value
	}
	records, total, err := db.ListETFArchiveRecords(db.ETFArchiveRecordFilter{
		Keyword:     req.Keyword,
		TMDBID:      req.TMDBID,
		TMDBMatched: matched,
		Page:        req.Page,
		PerPage:     req.PerPage,
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

func CorrectETFArchiveRecord(c *gin.Context) {
	var req correctETFArchiveRecordReq
	if err := c.ShouldBindJSON(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	record, err := db.GetETFArchiveRecordByID(req.ID)
	if err != nil {
		common.ErrorResp(c, err, 404)
		return
	}
	storage, err := op.GetStorageByMountPath(record.StorageMountPath)
	if err != nil {
		common.ErrorResp(c, err, 500)
		return
	}
	corrector, ok := storage.(etfArchiveCorrector)
	if !ok {
		common.ErrorStrResp(c, "storage does not support ETF archive correction", 400)
		return
	}
	updated, err := corrector.CorrectETFArchive(c.Request.Context(), record, req.ETFArchiveCorrection)
	if err != nil {
		common.ErrorResp(c, err, 500)
		return
	}
	common.SuccessResp(c, updated)
}
