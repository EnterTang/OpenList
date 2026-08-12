package handles

import (
	"strconv"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/etfauto"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/server/common"
	"github.com/gin-gonic/gin"
)

type listETFAutoMediaRootsReq struct {
	model.PageReq
	Keyword string `form:"keyword" json:"keyword"`
	Status  string `form:"status" json:"status"`
}

type triggerETFAutoCheckReq struct {
	ID uint `json:"id" binding:"required"`
}

type listETFAutoJobsReq struct {
	Type   string `form:"type"`
	Status string `form:"status"`
}

func ListETFAutoMediaRoots(c *gin.Context) {
	var req listETFAutoMediaRootsReq
	if err := c.ShouldBind(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	req.Validate()
	roots, total, err := etfauto.ListMediaRoots(c.Request.Context(), etfauto.MediaRootFilter{
		Keyword: req.Keyword,
		Status:  req.Status,
		Page:    req.Page,
		PerPage: req.PerPage,
	})
	if err != nil {
		common.ErrorResp(c, err, 500)
		return
	}
	common.SuccessResp(c, common.PageResp{Content: roots, Total: total})
}

func TriggerETFAutoSubscriptionCheck(c *gin.Context) {
	var req triggerETFAutoCheckReq
	if err := c.ShouldBindJSON(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	result, err := etfauto.RequestManualCheck(c.Request.Context(), req.ID, time.Now())
	if err != nil {
		common.ErrorResp(c, err, 500)
		return
	}
	common.SuccessResp(c, result)
}

func ProcessETFAutoSubscriptionJobs(c *gin.Context) {
	result, err := etfauto.ProcessOnce(c.Request.Context(), etfauto.RunnerOptions{})
	if err != nil {
		common.ErrorResp(c, err, 500)
		return
	}
	common.SuccessResp(c, result)
}

func ListETFAutoJobs(c *gin.Context) {
	var req listETFAutoJobsReq
	if err := c.ShouldBindQuery(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	jobs, err := etfauto.ListJobs(c.Request.Context(), etfauto.JobFilter{Type: req.Type, Status: req.Status})
	if err != nil {
		common.ErrorResp(c, err, 500)
		return
	}
	common.SuccessResp(c, jobs)
}

func RetryUnknownETFAutoJob(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil || id == 0 {
		common.ErrorStrResp(c, "invalid ETF notification job id", 400)
		return
	}
	if err := etfauto.RetryUnknownJob(c.Request.Context(), uint(id)); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	common.SuccessResp(c, gin.H{"queued": true})
}
