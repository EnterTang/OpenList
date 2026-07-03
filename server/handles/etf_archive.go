package handles

import (
	"context"
	"strconv"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/media/tmdb"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/server/common"
	"github.com/gin-gonic/gin"
)

type etfArchiveCorrector interface {
	CorrectETFArchive(context.Context, *model.ETFArchiveRecord, model.ETFArchiveCorrection) (*model.ETFArchiveRecord, error)
}

type etfManualArchiver interface {
	PreviewManualETFArchive(context.Context, string, model.ETFManualArchiveMetadata) (*model.ETFManualArchivePreview, error)
	ApplyManualETFArchive(context.Context, string, model.ETFManualArchiveMetadata, []model.ETFManualArchiveItem) (*model.ETFManualArchivePreview, error)
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

func SearchETFArchiveTMDB(c *gin.Context) {
	query := strings.TrimSpace(c.Query("query"))
	if query == "" {
		common.ErrorStrResp(c, "query is required", 400)
		return
	}
	candidates, err := tmdb.SearchCandidates(c.Request.Context(), tmdb.Config{
		APIKey:        etfArchiveSettingValue(conf.TMDBApiKey),
		BaseURL:       etfArchiveSettingValue(conf.TMDBApiBaseURL),
		Language:      etfArchiveSettingValue(conf.TMDBLanguage),
		CategoryRules: etfArchiveSettingValue(conf.MediaCategoryRules),
	}, query)
	if err != nil {
		common.ErrorResp(c, err, 500)
		return
	}
	common.SuccessResp(c, candidates)
}

func PreviewManualETFArchive(c *gin.Context) {
	var req model.ETFManualArchivePreviewReq
	if err := c.ShouldBindJSON(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	storage, actualPath, err := op.GetStorageAndActualPath(req.Path)
	if err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	archiver, ok := storage.(etfManualArchiver)
	if !ok {
		common.ErrorStrResp(c, "storage does not support manual ETF archive", 400)
		return
	}
	preview, err := archiver.PreviewManualETFArchive(c.Request.Context(), actualPath, req.Metadata)
	if err != nil {
		common.ErrorResp(c, err, 500)
		return
	}
	common.SuccessResp(c, preview)
}

func ApplyManualETFArchive(c *gin.Context) {
	var req model.ETFManualArchiveApplyReq
	if err := c.ShouldBindJSON(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	storage, actualPath, err := op.GetStorageAndActualPath(req.Path)
	if err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	archiver, ok := storage.(etfManualArchiver)
	if !ok {
		common.ErrorStrResp(c, "storage does not support manual ETF archive", 400)
		return
	}
	preview, err := archiver.ApplyManualETFArchive(c.Request.Context(), actualPath, req.Metadata, req.Items)
	if err != nil {
		common.ErrorResp(c, err, 500)
		return
	}
	common.SuccessResp(c, preview)
}

func etfArchiveSettingValue(key string) string {
	item, err := op.GetSettingItemByKey(key)
	if err != nil || item == nil {
		return ""
	}
	return item.Value
}
