package handles

import (
	"strconv"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster"
	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/server/common"
	"github.com/gin-gonic/gin"
)

func clusterControlActor(c *gin.Context) cluster.ControlActor {
	actor := cluster.ControlActor{RemoteIP: c.ClientIP(), RequestID: c.GetHeader("X-Request-ID")}
	if user, ok := c.Request.Context().Value(conf.UserKey).(*model.User); ok && user != nil {
		actor.Name = user.Username
	}
	return actor
}

func GetClusterConfig(c *gin.Context) {
	cfg, err := cluster.GetAdminConfig()
	if err != nil {
		common.ErrorResp(c, err, 500)
		return
	}
	common.SuccessResp(c, cfg)
}

func SaveClusterConfig(c *gin.Context) {
	var req cluster.AdminConfigUpdate
	if err := c.ShouldBindJSON(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	cfg, err := cluster.SaveAdminConfig(req)
	if err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	common.SuccessResp(c, cfg)
}

func ListClusterNodes(c *gin.Context) {
	service := cluster.CoordinatorService()
	if service == nil {
		common.ErrorStrResp(c, "cluster coordinator is disabled", 400)
		return
	}
	nodes, err := service.ListNodes(c.Request.Context())
	if err != nil {
		common.ErrorResp(c, err, 500)
		return
	}
	common.SuccessResp(c, nodes)
}

func ListClusterUploadResults(c *gin.Context) {
	service := cluster.CoordinatorService()
	if service == nil {
		common.ErrorStrResp(c, "cluster coordinator is disabled", 400)
		return
	}
	limit, _ := strconv.Atoi(c.Query("limit"))
	items, err := service.ListUploadManifests(c.Request.Context(), limit)
	if err != nil {
		common.ErrorResp(c, err, 500)
		return
	}
	common.SuccessResp(c, items)
}

func ListClusterJobs(c *gin.Context) {
	service := cluster.CoordinatorService()
	if service == nil {
		common.ErrorStrResp(c, "cluster coordinator is disabled", 400)
		return
	}
	limit, _ := strconv.Atoi(c.Query("limit"))
	jobs, err := service.ListJobs(c.Request.Context(), c.Query("status"), c.Query("include_archived") == "true", limit)
	if err != nil {
		common.ErrorResp(c, err, 500)
		return
	}
	common.SuccessResp(c, jobs)
}

func RetryClusterJob(c *gin.Context) {
	service := cluster.CoordinatorService()
	if service == nil {
		common.ErrorStrResp(c, "cluster coordinator is disabled", 400)
		return
	}
	if err := service.RetryJob(c.Request.Context(), c.Param("id")); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	common.SuccessResp(c, gin.H{"queued": true})
}

func ArchiveFailedClusterJobs(c *gin.Context) {
	service := cluster.CoordinatorService()
	if service == nil {
		common.ErrorStrResp(c, "cluster coordinator is disabled", 400)
		return
	}
	count, err := service.ArchiveFailedJobs(c.Request.Context())
	if err != nil {
		common.ErrorResp(c, err, 500)
		return
	}
	common.SuccessResp(c, gin.H{"archived": count})
}

func QueryClusterNodeInventory(c *gin.Context) {
	if err := cluster.QueryNodeInventory(c.Request.Context(), c.Param("id")); err != nil {
		common.ErrorResp(c, err, 503)
		return
	}
	common.SuccessResp(c, gin.H{"requested": true})
}

func SetClusterNodeState(c *gin.Context) {
	var req struct {
		State string `json:"state" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	if err := cluster.SetNodeState(c.Request.Context(), c.Param("id"), req.State); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	common.SuccessResp(c, gin.H{"updated": true})
}

func GetClusterResultQueueStats(c *gin.Context) {
	service := cluster.WorkerService()
	if service == nil {
		common.ErrorStrResp(c, "cluster worker is disabled", 400)
		return
	}
	stats, err := service.QueueStats(c.Request.Context())
	if err != nil {
		common.ErrorResp(c, err, 503)
		return
	}
	common.SuccessResp(c, stats)
}

func EnqueueClusterUploadResult(c *gin.Context) {
	service := cluster.WorkerService()
	if service == nil {
		common.ErrorStrResp(c, "cluster worker is disabled", 400)
		return
	}
	var manifest protocol.UploadETFManifest
	if err := c.ShouldBindJSON(&manifest); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	id, err := service.EnqueueUploadResult(c.Request.Context(), manifest)
	if err != nil {
		common.ErrorResp(c, err, 503)
		return
	}
	common.SuccessResp(c, gin.H{"stream_id": id, "media_delete_allowed": true})
}

func DispatchClusterMediaJob(c *gin.Context) {
	var req cluster.DispatchMediaJobRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	job, err := cluster.DispatchMediaJob(c.Request.Context(), req)
	if err != nil {
		common.ErrorResp(c, err, 500)
		return
	}
	common.SuccessResp(c, job)
}

func DispatchClusterMediaBatch(c *gin.Context) {
	var req cluster.DispatchMediaBatchRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	result, err := cluster.DispatchMediaBatch(c.Request.Context(), req)
	if err != nil && result == nil {
		common.ErrorResp(c, err, 500)
		return
	}
	common.SuccessResp(c, result)
}

func ListClusterSecrets(c *gin.Context) {
	items, err := cluster.ListSecrets(c.Request.Context())
	if err != nil {
		common.ErrorResp(c, err, 500)
		return
	}
	common.SuccessResp(c, items)
}

func WriteClusterSecret(c *gin.Context) {
	var req cluster.SecretWriteRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	item, err := cluster.WriteSecret(c.Request.Context(), req, clusterControlActor(c))
	if err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	common.SuccessResp(c, item)
}

func RevokeClusterSecret(c *gin.Context) {
	if err := cluster.RevokeSecret(c.Request.Context(), c.Param("id"), clusterControlActor(c)); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	common.SuccessResp(c, gin.H{"revoked": true})
}

func ApplyClusterNodeConfig(c *gin.Context) {
	var desired protocol.WorkerDesiredConfig
	if err := c.ShouldBindJSON(&desired); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	state, err := cluster.ApplyNodeConfig(c.Request.Context(), c.Param("id"), desired, clusterControlActor(c))
	if err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	common.SuccessResp(c, state)
}

func ListClusterStorageProfiles(c *gin.Context) {
	items, err := cluster.ListStorageProfiles(c.Request.Context())
	if err != nil {
		common.ErrorResp(c, err, 500)
		return
	}
	common.SuccessResp(c, items)
}

func ApplyClusterStorageProfile(c *gin.Context) {
	var req cluster.StorageProfileWriteRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	item, err := cluster.ApplyStorageProfile(c.Request.Context(), req, clusterControlActor(c))
	if err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	common.SuccessResp(c, item)
}

func ListClusterControlAudit(c *gin.Context) {
	limit, _ := strconv.Atoi(c.Query("limit"))
	items, err := cluster.ListControlAudit(c.Request.Context(), limit)
	if err != nil {
		common.ErrorResp(c, err, 500)
		return
	}
	common.SuccessResp(c, items)
}
