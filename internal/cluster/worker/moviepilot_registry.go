package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm/clause"
)

const moviePilotTorrentRegistryType = "moviepilot_qb_torrent"

type moviePilotTorrentRegistryEntry struct {
	Torrent            protocol.TorrentTaskContext      `json:"torrent"`
	Subscription       protocol.SubscriptionTaskContext `json:"subscription,omitempty"`
	PausedByDisconnect bool                             `json:"paused_by_disconnect"`
	PausedByCapacity   bool                             `json:"paused_by_capacity"`
	Phase              string                           `json:"phase,omitempty"`
	IntentStatus       string                           `json:"intent_status,omitempty"`
	TorrentStatus      string                           `json:"torrent_status,omitempty"`
	DownloadProgress   float64                          `json:"download_progress,omitempty"`
	UploadProgress     float64                          `json:"upload_progress,omitempty"`
	ClusterJobID       string                           `json:"cluster_job_id,omitempty"`
	ClusterJobStatus   string                           `json:"cluster_job_status,omitempty"`
	ClusterJobStage    string                           `json:"cluster_job_stage,omitempty"`
	ClusterStageStatus string                           `json:"cluster_job_stage_status,omitempty"`
	ErrorCode          string                           `json:"error_code,omitempty"`
	Error              string                           `json:"error,omitempty"`
	UpdatedAt          time.Time                        `json:"updated_at,omitempty"`
}

func moviePilotTorrentRegistryKey(torrent *protocol.TorrentTaskContext) string {
	if torrent == nil {
		return ""
	}
	return strings.Join([]string{
		strings.TrimSpace(torrent.BridgeInstanceID), strings.TrimSpace(torrent.QBClientID), strings.TrimSpace(torrent.TorrentHash),
	}, ":")
}

func moviePilotTorrentRegistryID(torrent *protocol.TorrentTaskContext) string {
	return moviePilotTorrentRegistryType + ":" + moviePilotTorrentRegistryKey(torrent)
}

// rememberMoviePilotTorrent makes the qB binding durable before a Worker
// acknowledges the job. This lets a restarted Worker pause known incomplete
// torrents on the next transport loss instead of relying on active memory.
func (s *Service) rememberMoviePilotTorrent(ctx context.Context, torrent *protocol.TorrentTaskContext) error {
	return s.rememberMoviePilotTorrentWithSubscription(ctx, torrent, protocol.SubscriptionTaskContext{})
}

func (s *Service) rememberMoviePilotTorrentWithSubscription(ctx context.Context, torrent *protocol.TorrentTaskContext, subscription protocol.SubscriptionTaskContext) error {
	if torrent == nil || strings.TrimSpace(torrent.TorrentHash) == "" {
		return nil
	}
	s.mu.Lock()
	previous := s.moviePilotTorrents[moviePilotTorrentRegistryKey(torrent)]
	s.mu.Unlock()
	entry := previous
	entry.Torrent = *torrent
	if subscription.SubscriptionID != 0 || strings.TrimSpace(subscription.SourceKey) != "" || strings.TrimSpace(subscription.SubscriptionName) != "" {
		entry.Subscription = subscription
	} else {
		subscription = previous.Subscription
	}
	entry.Subscription = subscription
	if entry.Phase == "" {
		entry.Phase = model.MoviePilotTaskPhaseBound
	}
	if entry.IntentStatus == "" {
		entry.IntentStatus = model.MoviePilotIntentStatusBound
	}
	if entry.TorrentStatus == "" {
		entry.TorrentStatus = "registered"
	}
	if entry.UpdatedAt.IsZero() {
		entry.UpdatedAt = time.Now().UTC()
	}
	return s.storeMoviePilotTorrentRegistryEntry(ctx, entry)
}

func (s *Service) setMoviePilotTorrentDisconnectPaused(ctx context.Context, torrent *protocol.TorrentTaskContext, paused bool) error {
	if torrent == nil || strings.TrimSpace(torrent.TorrentHash) == "" {
		return nil
	}
	s.mu.Lock()
	previous := s.moviePilotTorrents[moviePilotTorrentRegistryKey(torrent)]
	s.mu.Unlock()
	previous.Torrent = *torrent
	previous.PausedByDisconnect = paused
	return s.storeMoviePilotTorrentRegistryEntry(ctx, previous)
}

func (s *Service) setMoviePilotTorrentCapacityPaused(ctx context.Context, torrent *protocol.TorrentTaskContext, paused bool) error {
	if torrent == nil || strings.TrimSpace(torrent.TorrentHash) == "" {
		return nil
	}
	s.mu.Lock()
	previous := s.moviePilotTorrents[moviePilotTorrentRegistryKey(torrent)]
	s.mu.Unlock()
	previous.Torrent = *torrent
	previous.PausedByCapacity = paused
	return s.storeMoviePilotTorrentRegistryEntry(ctx, previous)
}

func (s *Service) storeMoviePilotTorrentRegistryEntry(ctx context.Context, entry moviePilotTorrentRegistryEntry) error {
	torrent := &entry.Torrent
	raw, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("encode MoviePilot qB registry entry: %w", err)
	}
	// Keep the live safeguard state even when the local database is temporarily
	// unavailable. A later persistence failure must not make a torrent that was
	// just paused for a transport loss eligible for an unsafe automatic resume.
	s.mu.Lock()
	if s.moviePilotTorrents == nil {
		s.moviePilotTorrents = make(map[string]moviePilotTorrentRegistryEntry)
	}
	s.moviePilotTorrents[moviePilotTorrentRegistryKey(torrent)] = entry
	s.mu.Unlock()
	if database := db.GetDb(); database != nil {
		state := model.ClusterWorkerObservedState{
			ID: moviePilotTorrentRegistryID(torrent), ResourceType: moviePilotTorrentRegistryType,
			ResourceKey: moviePilotTorrentRegistryKey(torrent), Hash: strings.TrimSpace(torrent.TorrentHash), PayloadJSON: string(raw),
		}
		if err := database.WithContext(ctx).Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "id"}},
			DoUpdates: clause.AssignmentColumns([]string{"updated_at", "resource_key", "hash", "payload_json"}),
		}).Create(&state).Error; err != nil {
			return fmt.Errorf("persist MoviePilot qB registry entry: %w", err)
		}
	}
	return nil
}

// syncMoviePilotTorrentStatus keeps the Worker-local status useful after the
// active job is removed. Progress is always reflected in memory for the local
// status endpoint; only stage/phase transitions and completed progress are
// written to the durable observed-state row to avoid turning every upload
// progress tick into a database write.
func (s *Service) syncMoviePilotTorrentStatus(ctx context.Context, torrent *protocol.TorrentTaskContext, jobID, stage, stageStatus string, completedBytes, totalBytes int64, message, errorCode, taskError, phaseOverride string, forcePersist bool) {
	if torrent == nil || strings.TrimSpace(torrent.TorrentHash) == "" {
		return
	}
	key := moviePilotTorrentRegistryKey(torrent)
	now := time.Now().UTC()
	phase := strings.TrimSpace(phaseOverride)
	if phase == "" {
		phase = workerMoviePilotTaskPhase(stage, stageStatus, completedBytes, totalBytes, message)
	}

	s.mu.Lock()
	entry, ok := s.moviePilotTorrents[key]
	if !ok {
		s.mu.Unlock()
		return
	}
	previous := entry
	entry.Torrent = *torrent
	entry.Phase = phase
	entry.IntentStatus = model.MoviePilotIntentStatusBound
	if stage == model.ClusterStageQBObserving && strings.TrimSpace(message) != "" {
		entry.TorrentStatus = strings.TrimSpace(message)
	}
	entry.TorrentStatus = firstWorkerStatusValue(entry.TorrentStatus, "registered")
	entry.ClusterJobID = firstWorkerStatusValue(jobID, entry.ClusterJobID)
	if stageStatus == model.ClusterStageStatusSucceeded {
		entry.ClusterJobStatus = model.ClusterJobStatusSucceeded
	} else if stageStatus == model.ClusterStageStatusRunning {
		entry.ClusterJobStatus = model.ClusterJobStatusRunning
	}
	entry.ClusterJobStage = firstWorkerStatusValue(stage, entry.ClusterJobStage)
	entry.ClusterStageStatus = firstWorkerStatusValue(stageStatus, entry.ClusterStageStatus)
	entry.ErrorCode = strings.TrimSpace(errorCode)
	entry.Error = strings.TrimSpace(taskError)
	entry.UpdatedAt = now
	if totalBytes > 0 {
		progress := clampWorkerStatusProgress(float64(completedBytes) / float64(totalBytes))
		if stage == model.ClusterStageQBObserving {
			entry.DownloadProgress = progress
		} else if stage == model.ClusterStageUploadingMobile {
			entry.UploadProgress = progress
		}
	}
	if phase == model.MoviePilotTaskPhaseDownloadComplete {
		entry.DownloadProgress = 1
	}
	if phase == model.MoviePilotTaskPhaseCompleted {
		entry.UploadProgress = 1
	}
	s.moviePilotTorrents[key] = entry
	persist := forcePersist || previous.Phase != entry.Phase || previous.ClusterJobID != entry.ClusterJobID || previous.ClusterJobStage != entry.ClusterJobStage || previous.ClusterStageStatus != entry.ClusterStageStatus || previous.ErrorCode != entry.ErrorCode || previous.Error != entry.Error
	s.mu.Unlock()
	if persist {
		if err := s.storeMoviePilotTorrentRegistryEntry(ctx, entry); err != nil {
			log.Warnf("persist MoviePilot qB task status for %s: %v", torrent.TorrentHash, err)
		}
	}
}

func workerMoviePilotTaskPhase(stage, stageStatus string, completedBytes, totalBytes int64, message string) string {
	if stageStatus == model.ClusterStageStatusFailed {
		return model.MoviePilotTaskPhaseFailed
	}
	if stage == model.ClusterStageQBObserving {
		if stageStatus == model.ClusterStageStatusSucceeded || totalBytes > 0 && completedBytes >= totalBytes || workerMoviePilotDownloadComplete(message) {
			return model.MoviePilotTaskPhaseDownloadComplete
		}
		return model.MoviePilotTaskPhaseDownloading
	}
	switch stage {
	case model.ClusterStageQBCopying:
		return model.MoviePilotTaskPhaseStaging
	case model.ClusterStageUploadingMobile:
		return model.MoviePilotTaskPhaseUploading
	case model.ClusterStageRetentionCheck:
		return model.MoviePilotTaskPhaseSeeding
	default:
		return model.MoviePilotTaskPhaseBound
	}
}

func workerMoviePilotDownloadComplete(message string) bool {
	switch strings.ToLower(strings.TrimSpace(message)) {
	case "completed", "uploading", "stalledup", "queuedup", "checkingup", "forcedup", "pausedup":
		return true
	default:
		return false
	}
}

func decodeMoviePilotTorrentRegistryEntry(raw string) (moviePilotTorrentRegistryEntry, bool) {
	var entry moviePilotTorrentRegistryEntry
	if json.Unmarshal([]byte(raw), &entry) == nil && strings.TrimSpace(entry.Torrent.TorrentHash) != "" {
		return entry, true
	}
	// The initial registry implementation persisted the TorrentTaskContext
	// directly. Retain those rows during the rolling upgrade.
	var torrent protocol.TorrentTaskContext
	if json.Unmarshal([]byte(raw), &torrent) != nil || strings.TrimSpace(torrent.TorrentHash) == "" {
		return moviePilotTorrentRegistryEntry{}, false
	}
	return moviePilotTorrentRegistryEntry{Torrent: torrent}, true
}

func (s *Service) forgetMoviePilotTorrent(ctx context.Context, torrent *protocol.TorrentTaskContext) {
	if torrent == nil {
		return
	}
	// The torrent has already been deleted from qB when this is called. Remove
	// the in-memory record first so a transient database failure cannot make the
	// running Worker act on a non-existent torrent again.
	s.mu.Lock()
	delete(s.moviePilotTorrents, moviePilotTorrentRegistryKey(torrent))
	s.mu.Unlock()
	if database := db.GetDb(); database != nil {
		if err := database.WithContext(ctx).Where("id = ?", moviePilotTorrentRegistryID(torrent)).Delete(&model.ClusterWorkerObservedState{}).Error; err != nil {
			return
		}
	}
}
