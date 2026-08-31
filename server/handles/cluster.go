package handles

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster"
	"github.com/OpenListTeam/OpenList/v4/internal/cluster/coordinator"
	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/server/common"
	"github.com/OpenListTeam/OpenList/v4/server/middlewares"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
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

func GetClusterNodeConfig(c *gin.Context) {
	view, err := cluster.GetNodeConfig(c.Request.Context(), c.Param("id"))
	if errors.Is(err, gorm.ErrRecordNotFound) {
		common.ErrorResp(c, err, http.StatusNotFound)
		return
	}
	if err != nil {
		common.ErrorResp(c, err, http.StatusInternalServerError)
		return
	}
	common.SuccessResp(c, view)
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
	includeStale := c.Query("include_stale") == "true"
	nodes, err := service.ListNodes(c.Request.Context(), includeStale, time.Now().UTC())
	if err != nil {
		common.ErrorResp(c, err, 500)
		return
	}
	common.SuccessResp(c, nodes)
}

func DeleteClusterNode(c *gin.Context) {
	service := cluster.CoordinatorService()
	if service == nil {
		common.ErrorStrResp(c, "cluster coordinator is disabled", 400)
		return
	}
	if err := service.DeleteNode(c.Request.Context(), c.Param("id"), time.Now().UTC()); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	common.SuccessResp(c, gin.H{"deleted": true})
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

// ListMoviePilotTaskStatuses serves both Coordinator and Worker panels. A
// Worker returns only its local accepted/running registry; a Coordinator
// returns the durable cross-node projection.
func ListMoviePilotTaskStatuses(c *gin.Context) {
	var subscriptionID uint
	if raw := c.Query("subscription_id"); raw != "" {
		parsed, err := strconv.ParseUint(raw, 10, 64)
		if err != nil || parsed == 0 {
			common.ErrorStrResp(c, "subscription_id must be a positive integer", http.StatusBadRequest)
			return
		}
		subscriptionID = uint(parsed)
	}
	limit, _ := strconv.Atoi(c.Query("limit"))
	if service := cluster.CoordinatorService(); service != nil {
		items, err := db.ListMoviePilotTaskStatuses(c.Request.Context(), subscriptionID, c.Query("bridge_instance_id"), limit)
		if err != nil {
			common.ErrorResp(c, err, http.StatusInternalServerError)
			return
		}
		common.SuccessResp(c, items)
		return
	}
	if service := cluster.WorkerService(); service != nil {
		items := service.ListMoviePilotTaskStatuses()
		if subscriptionID > 0 {
			filtered := items[:0]
			for _, item := range items {
				if item.SubscriptionID == subscriptionID {
					filtered = append(filtered, item)
				}
			}
			items = filtered
		}
		common.SuccessResp(c, items)
		return
	}
	common.ErrorStrResp(c, "cluster Coordinator and Worker are disabled", http.StatusBadRequest)
}

// ListMoviePilotBridgeTaskStatuses is the HMAC-authenticated read endpoint
// used by the Bridge plugin. The verified bridge instance is the filter, so a
// plugin cannot inspect another MoviePilot instance's tasks.
func ListMoviePilotBridgeTaskStatuses(c *gin.Context) {
	bridge, ok := c.Get(middlewares.MoviePilotBridgeInstanceContextKey)
	if !ok || bridge == nil {
		common.ErrorStrResp(c, "MoviePilot Bridge instance is unavailable", http.StatusUnauthorized)
		return
	}
	instance, ok := bridge.(*model.MoviePilotBridgeInstance)
	if !ok || instance == nil {
		common.ErrorStrResp(c, "MoviePilot Bridge instance is invalid", http.StatusUnauthorized)
		return
	}
	limit, _ := strconv.Atoi(c.Query("limit"))
	items, err := db.ListMoviePilotTaskStatuses(c.Request.Context(), 0, instance.ID, limit)
	if err != nil {
		common.ErrorResp(c, err, http.StatusInternalServerError)
		return
	}
	common.SuccessResp(c, items)
}

func ListMoviePilotTransfers(c *gin.Context) {
	subscriptionID, err := requiredUintQuery(c, "subscription_id")
	if err != nil {
		common.ErrorResp(c, err, http.StatusBadRequest)
		return
	}
	items, err := db.ListMoviePilotTransferViews(c.Request.Context(), subscriptionID, c.Query("binding_id"))
	if err != nil {
		common.ErrorResp(c, err, http.StatusInternalServerError)
		return
	}
	common.SuccessResp(c, gin.H{"content": items})
}

// AdoptCompletedQBTorrent starts the normal Worker observation/upload
// workflow for a torrent that was already completed outside MoviePilot.
func AdoptCompletedQBTorrent(c *gin.Context) {
	service := cluster.CoordinatorService()
	if service == nil {
		common.ErrorStrResp(c, "cluster coordinator is disabled", http.StatusBadRequest)
		return
	}
	var req coordinator.ManualQBTorrentAdoptionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		common.ErrorResp(c, err, http.StatusBadRequest)
		return
	}
	result, err := service.AdoptCompletedQBTorrent(c.Request.Context(), req)
	if err != nil {
		common.ErrorResp(c, err, http.StatusBadRequest)
		return
	}
	common.SuccessResp(c, result)
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

func MigrateClusterSecrets(c *gin.Context) {
	result, err := cluster.MigrateSecrets(c.Request.Context(), clusterControlActor(c))
	if err != nil {
		common.ErrorResp(c, err, http.StatusBadRequest)
		return
	}
	common.SuccessResp(c, result)
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
