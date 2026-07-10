package handles

import (
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
