package handles

import (
	"encoding/json"
	"math"
	"strconv"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster"
	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/task"

	"github.com/OpenListTeam/OpenList/v4/internal/fs"
	"github.com/OpenListTeam/OpenList/v4/internal/offline_download/tool"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/OpenListTeam/OpenList/v4/server/common"
	"github.com/OpenListTeam/tache"
	"github.com/gin-gonic/gin"
)

type TaskInfo struct {
	ID          string      `json:"id"`
	Name        string      `json:"name"`
	Creator     string      `json:"creator"`
	CreatorRole int         `json:"creator_role"`
	State       tache.State `json:"state"`
	Status      string      `json:"status"`
	Progress    float64     `json:"progress"`
	StartTime   *time.Time  `json:"start_time"`
	EndTime     *time.Time  `json:"end_time"`
	TotalBytes  int64       `json:"total_bytes"`
	Error       string      `json:"error"`
}

// QueueItem is the common read model for native tasks and durable cluster
// jobs. Native task APIs remain unchanged; this view gives clients one queue
// to render without exposing cluster task context or attempt credentials.
type QueueItem struct {
	ID                 string      `json:"id"`
	Source             string      `json:"source"`
	Kind               string      `json:"kind"`
	Name               string      `json:"name"`
	State              tache.State `json:"state"`
	Status             string      `json:"status"`
	Progress           float64     `json:"progress"`
	StartTime          *time.Time  `json:"start_time,omitempty"`
	EndTime            *time.Time  `json:"end_time,omitempty"`
	CreatedAt          *time.Time  `json:"created_at,omitempty"`
	UpdatedAt          *time.Time  `json:"updated_at,omitempty"`
	TotalBytes         int64       `json:"total_bytes"`
	Error              string      `json:"error,omitempty"`
	ErrorCode          string      `json:"error_code,omitempty"`
	ClusterJobID       string      `json:"cluster_job_id,omitempty"`
	SubscriptionID     uint        `json:"subscription_id,omitempty"`
	SubscriptionItemID uint        `json:"subscription_item_id,omitempty"`
	Provider           string      `json:"provider,omitempty"`
	Worker             string      `json:"worker,omitempty"`
	Stage              string      `json:"stage,omitempty"`
	StageStatus        string      `json:"stage_status,omitempty"`
}

func getTaskInfo[T task.TaskExtensionInfo](task T) TaskInfo {
	errMsg := ""
	if task.GetErr() != nil {
		errMsg = task.GetErr().Error()
	}
	progress := task.GetProgress()
	// if progress is NaN, set it to 100
	if math.IsNaN(progress) {
		progress = 100
	}
	creatorName := ""
	creatorRole := -1
	if task.GetCreator() != nil {
		creatorName = task.GetCreator().Username
		creatorRole = task.GetCreator().Role
	}
	return TaskInfo{
		ID:          task.GetID(),
		Name:        task.GetName(),
		Creator:     creatorName,
		CreatorRole: creatorRole,
		State:       task.GetState(),
		Status:      task.GetStatus(),
		Progress:    progress,
		StartTime:   task.GetStartTime(),
		EndTime:     task.GetEndTime(),
		TotalBytes:  task.GetTotalBytes(),
		Error:       errMsg,
	}
}

func getTaskInfos[T task.TaskExtensionInfo](tasks []T) []TaskInfo {
	return utils.MustSliceConvert(tasks, getTaskInfo[T])
}

func argsContains[T comparable](v T, slice ...T) bool {
	return utils.SliceContains(slice, v)
}

func getUserInfo(c *gin.Context) (bool, uint, bool) {
	if user, ok := c.Request.Context().Value(conf.UserKey).(*model.User); ok {
		return user.IsAdmin(), user.ID, true
	} else {
		return false, 0, false
	}
}

func getTargetedHandler[T task.TaskExtensionInfo](manager task.Manager[T], callback func(c *gin.Context, task T)) gin.HandlerFunc {
	return func(c *gin.Context) {
		isAdmin, uid, ok := getUserInfo(c)
		if !ok {
			// if there is no bug, here is unreachable
			common.ErrorStrResp(c, "user invalid", 401)
			return
		}
		t, ok := manager.GetByID(c.Query("tid"))
		if !ok {
			common.ErrorStrResp(c, "task not found", 404)
			return
		}
		if !isAdmin && uid != t.GetCreator().ID {
			// to avoid an attacker using error messages to guess valid TID, return a 404 rather than a 403
			common.ErrorStrResp(c, "task not found", 404)
			return
		}
		callback(c, t)
	}
}

func getBatchHandler[T task.TaskExtensionInfo](manager task.Manager[T], callback func(task T)) gin.HandlerFunc {
	return func(c *gin.Context) {
		isAdmin, uid, ok := getUserInfo(c)
		if !ok {
			common.ErrorStrResp(c, "user invalid", 401)
			return
		}
		var tids []string
		if err := c.ShouldBind(&tids); err != nil {
			common.ErrorStrResp(c, "invalid request format", 400)
			return
		}
		retErrs := make(map[string]string)
		for _, tid := range tids {
			t, ok := manager.GetByID(tid)
			if !ok || (!isAdmin && uid != t.GetCreator().ID) {
				retErrs[tid] = "task not found"
				continue
			}
			callback(t)
		}
		common.SuccessResp(c, retErrs)
	}
}

func taskRoute[T task.TaskExtensionInfo](g *gin.RouterGroup, manager task.Manager[T]) {
	g.GET("/undone", func(c *gin.Context) {
		isAdmin, uid, ok := getUserInfo(c)
		if !ok {
			// if there is no bug, here is unreachable
			common.ErrorStrResp(c, "user invalid", 401)
			return
		}
		common.SuccessResp(c, getTaskInfos(manager.GetByCondition(func(task T) bool {
			// avoid directly passing the user object into the function to reduce closure size
			return (isAdmin || uid == task.GetCreator().ID) &&
				argsContains(task.GetState(), tache.StatePending, tache.StateRunning, tache.StateCanceling,
					tache.StateErrored, tache.StateFailing, tache.StateWaitingRetry, tache.StateBeforeRetry)
		})))
	})
	g.GET("/done", func(c *gin.Context) {
		isAdmin, uid, ok := getUserInfo(c)
		if !ok {
			// if there is no bug, here is unreachable
			common.ErrorStrResp(c, "user invalid", 401)
			return
		}
		common.SuccessResp(c, getTaskInfos(manager.GetByCondition(func(task T) bool {
			return (isAdmin || uid == task.GetCreator().ID) &&
				argsContains(task.GetState(), tache.StateCanceled, tache.StateFailed, tache.StateSucceeded)
		})))
	})
	g.POST("/info", getTargetedHandler(manager, func(c *gin.Context, task T) {
		common.SuccessResp(c, getTaskInfo(task))
	}))
	g.POST("/cancel", getTargetedHandler(manager, func(c *gin.Context, task T) {
		manager.Cancel(task.GetID())
		common.SuccessResp(c)
	}))
	g.POST("/delete", getTargetedHandler(manager, func(c *gin.Context, task T) {
		manager.Remove(task.GetID())
		common.SuccessResp(c)
	}))
	g.POST("/retry", getTargetedHandler(manager, func(c *gin.Context, task T) {
		manager.Retry(task.GetID())
		common.SuccessResp(c)
	}))
	g.POST("/cancel_some", getBatchHandler(manager, func(task T) {
		manager.Cancel(task.GetID())
	}))
	g.POST("/delete_some", getBatchHandler(manager, func(task T) {
		manager.Remove(task.GetID())
	}))
	g.POST("/retry_some", getBatchHandler(manager, func(task T) {
		manager.Retry(task.GetID())
	}))
	g.POST("/clear_done", func(c *gin.Context) {
		isAdmin, uid, ok := getUserInfo(c)
		if !ok {
			// if there is no bug, here is unreachable
			common.ErrorStrResp(c, "user invalid", 401)
			return
		}
		manager.RemoveByCondition(func(task T) bool {
			return (isAdmin || uid == task.GetCreator().ID) &&
				argsContains(task.GetState(), tache.StateCanceled, tache.StateFailed, tache.StateSucceeded)
		})
		common.SuccessResp(c)
	})
	g.POST("/clear_succeeded", func(c *gin.Context) {
		isAdmin, uid, ok := getUserInfo(c)
		if !ok {
			// if there is no bug, here is unreachable
			common.ErrorStrResp(c, "user invalid", 401)
			return
		}
		manager.RemoveByCondition(func(task T) bool {
			return (isAdmin || uid == task.GetCreator().ID) && task.GetState() == tache.StateSucceeded
		})
		common.SuccessResp(c)
	})
	g.POST("/retry_failed", func(c *gin.Context) {
		isAdmin, uid, ok := getUserInfo(c)
		if !ok {
			// if there is no bug, here is unreachable
			common.ErrorStrResp(c, "user invalid", 401)
			return
		}
		tasks := manager.GetByCondition(func(task T) bool {
			return (isAdmin || uid == task.GetCreator().ID) && task.GetState() == tache.StateFailed
		})
		for _, t := range tasks {
			manager.Retry(t.GetID())
		}
		common.SuccessResp(c)
	})
}

func getTaskQueue(c *gin.Context) {
	isAdmin, uid, ok := getUserInfo(c)
	if !ok {
		common.ErrorStrResp(c, "user invalid", 401)
		return
	}
	includeDone, _ := strconv.ParseBool(c.Query("include_done"))
	items := make([]QueueItem, 0)
	appendNativeQueueItems(&items, "copy", fs.CopyTaskManager, isAdmin, uid, includeDone)
	appendNativeQueueItems(&items, "move", fs.MoveTaskManager, isAdmin, uid, includeDone)
	appendNativeQueueItems(&items, "download", tool.DownloadTaskManager, isAdmin, uid, includeDone)
	appendNativeQueueItems(&items, "transfer", tool.TransferTaskManager, isAdmin, uid, includeDone)

	// Cluster jobs contain immutable task context and may reference provider
	// URLs, so expose their operational projection only to administrators.
	if isAdmin {
		if service := cluster.CoordinatorService(); service != nil {
			limit := 100
			if value, err := strconv.Atoi(c.Query("limit")); err == nil && value > 0 {
				limit = value
			}
			if limit > 500 {
				limit = 500
			}
			clusterStatus := c.Query("cluster_status")
			if clusterStatus == "" && !includeDone {
				clusterStatus = "active"
			}
			jobs, err := service.ListJobs(c.Request.Context(), clusterStatus, c.Query("include_archived") == "true", limit)
			if err != nil {
				common.ErrorResp(c, err, 500)
				return
			}
			for i := range jobs {
				if !includeDone && !isActiveClusterQueueStatus(jobs[i].Status) {
					continue
				}
				items = append(items, clusterQueueItem(&jobs[i]))
			}
		}
	}
	common.SuccessResp(c, gin.H{"items": items, "total": len(items)})
}

func appendNativeQueueItems[T task.TaskExtensionInfo](items *[]QueueItem, kind string, manager task.Manager[T], isAdmin bool, uid uint, includeDone bool) {
	if manager == nil {
		return
	}
	for _, current := range manager.GetAll() {
		creator := current.GetCreator()
		if !isAdmin && (creator == nil || creator.ID != uid) {
			continue
		}
		if !includeDone && !isActiveTaskState(current.GetState()) {
			continue
		}
		info := getTaskInfo(current)
		*items = append(*items, QueueItem{
			ID:         info.ID,
			Source:     "native",
			Kind:       kind,
			Name:       info.Name,
			State:      info.State,
			Status:     info.Status,
			Progress:   info.Progress,
			StartTime:  info.StartTime,
			EndTime:    info.EndTime,
			TotalBytes: info.TotalBytes,
			Error:      info.Error,
		})
	}
}

func isActiveTaskState(state tache.State) bool {
	return argsContains(state, tache.StatePending, tache.StateRunning, tache.StateCanceling,
		tache.StateErrored, tache.StateFailing, tache.StateWaitingRetry, tache.StateBeforeRetry)
}

func isActiveClusterQueueStatus(status string) bool {
	return argsContains(status, model.ClusterJobStatusQueued, model.ClusterJobStatusPlanning,
		model.ClusterJobStatusLeased, model.ClusterJobStatusRunning, model.ClusterJobStatusRetryWait,
		model.ClusterJobStatusCancelRequested)
}

func clusterQueueItem(job *model.ClusterJob) QueueItem {
	if job == nil {
		return QueueItem{}
	}
	item := QueueItem{
		ID:                 job.ID,
		Source:             "cluster",
		Kind:               job.Type,
		Name:               job.ID,
		Status:             job.Status,
		StartTime:          job.StartedAt,
		EndTime:            job.FinishedAt,
		TotalBytes:         job.ExpectedBytes,
		Error:              job.LastError,
		ErrorCode:          job.LastErrorCode,
		ClusterJobID:       job.ID,
		SubscriptionID:     job.SubscriptionID,
		SubscriptionItemID: job.SubscriptionItemID,
		Provider:           job.SourceProvider,
		Worker:             job.AssignedNodeID,
		CreatedAt:          &job.CreatedAt,
		UpdatedAt:          &job.UpdatedAt,
	}
	item.State = clusterQueueState(job.Status)
	if len(job.Stages) > 0 {
		stage := job.Stages[len(job.Stages)-1]
		item.Stage = stage.Name
		item.StageStatus = stage.Status
		var progress struct {
			Progress float64 `json:"progress"`
		}
		if json.Unmarshal([]byte(stage.ProgressJSON), &progress) == nil && progress.Progress >= 0 {
			item.Progress = progress.Progress
		}
	}
	return item
}

func clusterQueueState(status string) tache.State {
	switch status {
	case model.ClusterJobStatusQueued, model.ClusterJobStatusPlanning, model.ClusterJobStatusRetryWait:
		return tache.StatePending
	case model.ClusterJobStatusLeased, model.ClusterJobStatusRunning:
		return tache.StateRunning
	case model.ClusterJobStatusCancelRequested:
		return tache.StateCanceling
	case model.ClusterJobStatusSucceeded:
		return tache.StateSucceeded
	case model.ClusterJobStatusCancelled:
		return tache.StateCanceled
	default:
		return tache.StateFailed
	}
}

func SetupTaskRoute(g *gin.RouterGroup) {
	g.GET("/queue", getTaskQueue)
	taskRoute(g.Group("/upload"), fs.UploadTaskManager)
	taskRoute(g.Group("/copy"), fs.CopyTaskManager)
	taskRoute(g.Group("/move"), fs.MoveTaskManager)
	taskRoute(g.Group("/offline_download"), tool.DownloadTaskManager)
	taskRoute(g.Group("/offline_download_transfer"), tool.TransferTaskManager)
	taskRoute(g.Group("/decompress"), fs.ArchiveDownloadTaskManager)
	taskRoute(g.Group("/decompress_upload"), fs.ArchiveContentUploadTaskManager)
}
