package worker

import (
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

// ListMoviePilotTaskStatuses exposes only the Worker-local part of a
// MoviePilot transfer. The Coordinator remains authoritative for the final
// upload receipt, while this view answers whether this Worker has actually
// accepted and is executing the task.
func (s *Service) ListMoviePilotTaskStatuses() []model.MoviePilotTaskStatus {
	if s == nil {
		return []model.MoviePilotTaskStatus{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]model.MoviePilotTaskStatus, 0, len(s.active)+len(s.moviePilotTorrents))
	activeBindings := make(map[string]struct{}, len(s.active))
	now := time.Now().UTC()
	for _, task := range s.active {
		if task == nil || task.offer.TaskContext.Torrent == nil {
			continue
		}
		torrent := task.offer.TaskContext.Torrent
		activeBindings[moviePilotStatusBindingKey(torrent)] = struct{}{}
		phase := workerMoviePilotTaskPhase(task.stage, task.stageStatus, task.completedBytes, task.totalBytes, task.progressMessage)
		torrentStatus := "worker_running"
		if task.stage == model.ClusterStageQBObserving {
			torrentStatus = firstWorkerStatusValue(task.progressMessage, torrentStatus)
		}
		status := model.MoviePilotTaskStatus{
			SubscriptionID:     task.offer.TaskContext.Subscription.SubscriptionID,
			SubscriptionItemID: task.offer.TaskContext.Subscription.SubscriptionItemID,
			SubscriptionName:   task.offer.TaskContext.Subscription.SubscriptionName,
			ItemName:           task.offer.TaskContext.Media.LogicalTargetPath,
			Phase:              phase, IntentStatus: model.MoviePilotIntentStatusBound,
			TorrentStatus: torrentStatus, BindingID: torrent.BindingID,
			BridgeInstanceID: torrent.BridgeInstanceID, WorkerNodeID: firstWorkerStatusValue(s.controlNodeID, torrent.WorkerNodeID),
			Downloader: torrent.Downloader, QBClientID: torrent.QBClientID, TorrentHash: torrent.TorrentHash,
			ClusterJobID: task.offer.JobID, ClusterJobStatus: model.ClusterJobStatusRunning,
			ClusterJobStage: task.stage, ClusterJobStageStatus: task.stageStatus,
			UpdatedAt: firstWorkerStatusTime(task.progressAt, now),
		}
		if task.totalBytes > 0 {
			progress := clampWorkerStatusProgress(float64(task.completedBytes) / float64(task.totalBytes))
			if task.stage == model.ClusterStageQBObserving {
				status.DownloadProgress = progress
			} else {
				status.UploadProgress = progress
			}
		}
		result = append(result, status)
	}
	for _, entry := range s.moviePilotTorrents {
		torrent := entry.Torrent
		if _, active := activeBindings[moviePilotStatusBindingKey(&torrent)]; active {
			continue
		}
		result = append(result, model.MoviePilotTaskStatus{
			SubscriptionID:     entry.Subscription.SubscriptionID,
			SubscriptionItemID: entry.Subscription.SubscriptionItemID,
			SubscriptionName:   entry.Subscription.SubscriptionName,
			ItemName:           firstWorkerStatusValue(entry.Torrent.RelativePath, entry.Torrent.ContentPath),
			Phase:              firstWorkerStatusValue(entry.Phase, model.MoviePilotTaskPhaseBound),
			IntentStatus:       firstWorkerStatusValue(entry.IntentStatus, model.MoviePilotIntentStatusBound),
			TorrentStatus:      firstWorkerStatusValue(entry.TorrentStatus, "registered"),
			DownloadProgress:   clampWorkerStatusProgress(entry.DownloadProgress),
			UploadProgress:     clampWorkerStatusProgress(entry.UploadProgress),
			BindingID:          torrent.BindingID, BridgeInstanceID: torrent.BridgeInstanceID,
			WorkerNodeID: firstWorkerStatusValue(s.controlNodeID, torrent.WorkerNodeID),
			Downloader:   torrent.Downloader, QBClientID: torrent.QBClientID, TorrentHash: torrent.TorrentHash,
			ClusterJobID: entry.ClusterJobID, ClusterJobStatus: entry.ClusterJobStatus,
			ClusterJobStage: entry.ClusterJobStage, ClusterJobStageStatus: entry.ClusterStageStatus,
			ErrorCode: entry.ErrorCode, Error: entry.Error,
			UpdatedAt: firstWorkerStatusTime(entry.UpdatedAt, now),
		})
	}
	return result
}

func moviePilotStatusBindingKey(torrent *protocol.TorrentTaskContext) string {
	if torrent == nil {
		return ""
	}
	if bindingID := strings.TrimSpace(torrent.BindingID); bindingID != "" {
		return bindingID
	}
	return moviePilotTorrentRegistryKey(torrent)
}

func clampWorkerStatusProgress(value float64) float64 {
	if value < 0 {
		return 0
	}
	if value > 1 {
		return 1
	}
	return value
}

func firstWorkerStatusValue(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func firstWorkerStatusTime(values ...time.Time) time.Time {
	for _, value := range values {
		if !value.IsZero() {
			return value
		}
	}
	return time.Now().UTC()
}
