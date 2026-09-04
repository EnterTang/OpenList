package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/qbittorrent"
	"github.com/shirou/gopsutil/v4/disk"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm/clause"
)

const qbCapacityPauseType = "qb_capacity_pause"

type qbCapacityPauseEntry struct {
	QBClientID   string    `json:"qb_client_id"`
	TorrentHash  string    `json:"torrent_hash"`
	DownloadRoot string    `json:"download_root"`
	UpdatedAt    time.Time `json:"updated_at"`
}

func qbCapacityPauseKey(clientID, hash string) string {
	return strings.TrimSpace(clientID) + ":" + strings.ToLower(strings.TrimSpace(hash))
}

func qbCapacityPauseID(entry qbCapacityPauseEntry) string {
	return qbCapacityPauseType + ":" + qbCapacityPauseKey(entry.QBClientID, entry.TorrentHash)
}

func (s *Service) storeQBCapacityPause(ctx context.Context, entry qbCapacityPauseEntry) error {
	entry.QBClientID = strings.TrimSpace(entry.QBClientID)
	entry.TorrentHash = strings.ToLower(strings.TrimSpace(entry.TorrentHash))
	entry.DownloadRoot = filepath.Clean(strings.TrimSpace(entry.DownloadRoot))
	entry.UpdatedAt = time.Now().UTC()
	if entry.QBClientID == "" || entry.TorrentHash == "" || entry.DownloadRoot == "." || entry.DownloadRoot == "" {
		return errors.New("qB capacity pause entry is incomplete")
	}
	raw, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("encode qB capacity pause: %w", err)
	}
	s.mu.Lock()
	if s.capacityPausedTorrents == nil {
		s.capacityPausedTorrents = make(map[string]qbCapacityPauseEntry)
	}
	s.capacityPausedTorrents[qbCapacityPauseKey(entry.QBClientID, entry.TorrentHash)] = entry
	s.mu.Unlock()
	if database := db.GetDb(); database != nil {
		state := model.ClusterWorkerObservedState{
			ID: qbCapacityPauseID(entry), ResourceType: qbCapacityPauseType,
			ResourceKey: qbCapacityPauseKey(entry.QBClientID, entry.TorrentHash), Hash: entry.TorrentHash, PayloadJSON: string(raw),
		}
		if err := database.WithContext(ctx).Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "id"}},
			DoUpdates: clause.AssignmentColumns([]string{"updated_at", "resource_key", "hash", "payload_json"}),
		}).Create(&state).Error; err != nil {
			return fmt.Errorf("persist qB capacity pause: %w", err)
		}
	}
	return nil
}

func (s *Service) clearQBCapacityPause(ctx context.Context, entry qbCapacityPauseEntry) {
	key := qbCapacityPauseKey(entry.QBClientID, entry.TorrentHash)
	s.mu.Lock()
	delete(s.capacityPausedTorrents, key)
	s.mu.Unlock()
	if database := db.GetDb(); database != nil {
		if err := database.WithContext(ctx).Where("id = ?", qbCapacityPauseID(entry)).Delete(&model.ClusterWorkerObservedState{}).Error; err != nil {
			log.Warnf("remove qB capacity pause %s: %v", key, err)
		}
	}
}

func (s *Service) qbClientForCapacity(config protocol.QBClientConfig) (qbittorrent.Client, error) {
	s.mu.Lock()
	factory := s.qbClientFactory
	secret := s.qbSecrets[strings.TrimSpace(config.ID)]
	s.mu.Unlock()
	if factory != nil {
		client, err := factory(config)
		if err != nil {
			return nil, err
		}
		return client, nil
	}
	if secret == nil {
		return nil, errors.New("qB credentials are unavailable")
	}
	return newWorkerQBClientWithSecret(config, secret)
}

func downloadWatermarkBytes(staging protocol.StagingConfig) (uint64, uint64, bool) {
	low := staging.DownloadDiskPauseWatermarkGB
	high := staging.DownloadDiskResumeWatermarkGB
	if low <= 0 || high <= 0 {
		return 0, 0, false
	}
	maxInt64 := uint64(^uint64(0) >> 1)
	if uint64(high) > maxInt64/uint64(bytesPerGB) {
		return 0, 0, false
	}
	return uint64(low) * uint64(bytesPerGB), uint64(high) * uint64(bytesPerGB), true
}

func downloadWatermarkLowBytes(staging protocol.StagingConfig) int64 {
	low, _, enabled := downloadWatermarkBytes(staging)
	if !enabled {
		return 0
	}
	return clampUint64ToInt64(low)
}

func (s *Service) downloadSpaceUsage(ctx context.Context, root string) (uint64, error) {
	s.mu.Lock()
	usage := s.downloadFreeSpace
	s.mu.Unlock()
	if usage != nil {
		return usage(ctx, root)
	}
	value, err := disk.UsageWithContext(ctx, root)
	if err != nil {
		return 0, err
	}
	return value.Free, nil
}

// downloadSpaceUsageForQB prefers qBittorrent's path-aware capacity API. That
// keeps the capacity decision on the machine that owns the qB files and avoids
// treating a container/host path mapping as if it were a local filesystem.
// Older qBittorrent versions do not expose this endpoint. When there is only
// one mapped qB path, their global default-save-path capacity is a bounded
// compatibility fallback; otherwise the Worker-local fallback avoids
// attributing one qB volume's capacity to another.
func (s *Service) downloadSpaceUsageForQB(ctx context.Context, client qbittorrent.Client, qbPath, workerPath string, allowGlobal bool) (uint64, error) {
	if strings.TrimSpace(qbPath) != "" && normalizeQBPath(qbPath) != "." {
		freeSpaceClient, ok := client.(qbittorrent.FreeSpaceClient)
		if !ok {
			return s.downloadSpaceUsage(ctx, workerPath)
		}
		free, err := freeSpaceClient.GetFreeSpaceAtPath(ctx, qbPath)
		if err == nil {
			return free, nil
		}
		if !errors.Is(err, qbittorrent.ErrFreeSpaceAtPathUnsupported) {
			return 0, fmt.Errorf("query qB free space at path %q: %w", qbPath, err)
		}
		if allowGlobal {
			if globalFreeSpaceClient, ok := client.(qbittorrent.GlobalFreeSpaceClient); ok {
				free, globalErr := globalFreeSpaceClient.GetFreeSpace(ctx)
				if globalErr != nil {
					return 0, fmt.Errorf("query qB global free space: %w", globalErr)
				}
				return free, nil
			}
		}
	}
	return s.downloadSpaceUsage(ctx, workerPath)
}

func qBGlobalFreeSpaceAllowed(config protocol.QBClientConfig) bool {
	var mappedPath string
	for _, mapping := range config.PathMappings {
		path := normalizeQBPath(mapping.QBPath)
		if path == "." {
			continue
		}
		if mappedPath == "" {
			mappedPath = path
			continue
		}
		if path != mappedPath {
			return false
		}
	}
	return mappedPath != ""
}

// downloadRootCapacity returns the least free space among the configured qB
// path mappings. The minimum is intentional: one qB client may span several
// volumes, and a route must not advertise more capacity than its tightest
// mapped volume can provide.
func (s *Service) downloadRootCapacity(ctx context.Context, config protocol.QBClientConfig) (int64, bool) {
	client, clientErr := s.qbClientForCapacity(config)
	allowGlobal := qBGlobalFreeSpaceAllowed(config)
	seen := make(map[string]struct{}, len(config.PathMappings))
	var free uint64
	known := false
	for _, mapping := range config.PathMappings {
		root := filepath.Clean(strings.TrimSpace(mapping.WorkerPath))
		if root == "." || root == "" {
			continue
		}
		if _, ok := seen[root]; ok {
			continue
		}
		seen[root] = struct{}{}
		value, err := s.downloadSpaceUsage(ctx, root)
		if clientErr == nil {
			value, err = s.downloadSpaceUsageForQB(ctx, client, normalizeQBPath(mapping.QBPath), root, allowGlobal)
		}
		if err != nil {
			continue
		}
		if !known || value < free {
			free = value
		}
		known = true
	}
	if !known {
		return 0, false
	}
	return clampUint64ToInt64(free), true
}

func isIncompleteTorrent(info qbittorrent.TorrentInfo) bool {
	return info.Progress < 0.999999 || info.AmountLeft > 0
}

func isPausedDownload(info qbittorrent.TorrentInfo) bool {
	return info.State == qbittorrent.PAUSEDDL || info.State == qbittorrent.CHECKINGRESUMEDATA
}

func (s *Service) moviePilotCapacityPauseState(clientID, hash string) (protocol.TorrentTaskContext, bool, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, entry := range s.moviePilotTorrents {
		if strings.EqualFold(strings.TrimSpace(entry.Torrent.QBClientID), strings.TrimSpace(clientID)) && strings.EqualFold(strings.TrimSpace(entry.Torrent.TorrentHash), strings.TrimSpace(hash)) {
			return entry.Torrent, entry.PausedByDisconnect, true
		}
	}
	return protocol.TorrentTaskContext{}, false, false
}

func (s *Service) syncMoviePilotCapacityPause(ctx context.Context, clientID, hash string, paused bool) {
	torrent, _, ok := s.moviePilotCapacityPauseState(clientID, hash)
	if !ok {
		return
	}
	if err := s.setMoviePilotTorrentCapacityPaused(ctx, &torrent, paused); err != nil {
		log.Warnf("persist MoviePilot qB torrent %s download capacity pause=%t: %v", hash, paused, err)
	}
}

func (s *Service) configuredQBClientConfigs() []protocol.QBClientConfig {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]protocol.QBClientConfig(nil), s.desiredConfig.QBClients...)
}

// ReconcileQBDiskCapacity enforces the download-volume low/high watermark on
// every configured qB client, including torrents that were not created by
// MoviePilot. Only tasks paused by this policy are resumed later; manually
// paused torrents are never touched.
func (s *Service) ReconcileQBDiskCapacity(ctx context.Context) {
	s.mu.Lock()
	staging := s.desiredConfig.Staging
	paused := make([]qbCapacityPauseEntry, 0, len(s.capacityPausedTorrents))
	for _, entry := range s.capacityPausedTorrents {
		paused = append(paused, entry)
	}
	s.mu.Unlock()
	low, high, enabled := downloadWatermarkBytes(staging)
	if !enabled {
		s.resumeQBCapacityPausedWhenDisabled(ctx, paused)
		return
	}

	configs := s.configuredQBClientConfigs()
	type clientState struct {
		config   protocol.QBClientConfig
		client   qbittorrent.Client
		torrents []qbittorrent.TorrentInfo
		roots    map[string]uint64
	}
	states := make(map[string]clientState, len(configs))
	for _, config := range configs {
		client, err := s.qbClientForCapacity(config)
		if err != nil {
			s.recordQBHealth(config.ID, "unhealthy")
			log.Warnf("inspect qB client %s for download capacity: %v", config.ID, err)
			continue
		}
		torrents, err := client.GetTorrents(ctx)
		if err != nil {
			s.recordQBHealth(config.ID, "unhealthy")
			log.Warnf("list qB torrents for download capacity on %s: %v", config.ID, err)
			continue
		}
		s.recordQBHealth(config.ID, "healthy")
		states[strings.TrimSpace(config.ID)] = clientState{config: config, client: client, torrents: torrents, roots: make(map[string]uint64)}
	}

	for clientID, state := range states {
		for _, info := range state.torrents {
			root, err := ResolveQBPath(state.config, info.SavePath)
			if err != nil {
				if info.ContentPath == "" {
					continue
				}
				root, err = ResolveQBPath(state.config, info.ContentPath)
			}
			if err != nil {
				continue
			}
			qbPath, _, mappingErr := resolveQBPathMapping(state.config, info.SavePath)
			if mappingErr != nil {
				qbPath = normalizeQBPath(info.ContentPath)
			}
			capacityKey := qbPath
			if capacityKey == "." {
				capacityKey = root
			}
			if _, ok := state.roots[capacityKey]; !ok {
				free, usageErr := s.downloadSpaceUsageForQB(ctx, state.client, qbPath, root, qBGlobalFreeSpaceAllowed(state.config))
				if usageErr != nil {
					log.Warnf("inspect qB download volume %s: %v", qbPath, usageErr)
					continue
				}
				state.roots[capacityKey] = free
			}
			if state.roots[capacityKey] > low || !isIncompleteTorrent(info) || isPausedDownload(info) {
				continue
			}
			if err := state.client.StopByHash(ctx, info.Hash); err != nil {
				log.Warnf("pause qB torrent %s for low disk space: %v", info.Hash, err)
				continue
			}
			if err := s.storeQBCapacityPause(ctx, qbCapacityPauseEntry{QBClientID: clientID, TorrentHash: info.Hash, DownloadRoot: root}); err != nil {
				log.Warnf("persist qB capacity pause %s: %v", info.Hash, err)
			}
			s.syncMoviePilotCapacityPause(ctx, clientID, info.Hash, true)
			log.Warnf("paused qB torrent %s on %s: free disk space is %d bytes, low watermark is %d bytes", info.Hash, clientID, state.roots[capacityKey], low)
		}
	}

	for _, entry := range paused {
		state, ok := states[entry.QBClientID]
		if !ok {
			continue
		}
		var info *qbittorrent.TorrentInfo
		for i := range state.torrents {
			if strings.EqualFold(strings.TrimSpace(state.torrents[i].Hash), entry.TorrentHash) {
				info = &state.torrents[i]
				break
			}
		}
		if info == nil {
			s.clearQBCapacityPause(ctx, entry)
			s.syncMoviePilotCapacityPause(ctx, entry.QBClientID, entry.TorrentHash, false)
			continue
		}
		if !isIncompleteTorrent(*info) {
			s.clearQBCapacityPause(ctx, entry)
			s.syncMoviePilotCapacityPause(ctx, entry.QBClientID, entry.TorrentHash, false)
			continue
		}
		if _, pausedByDisconnect, isMoviePilot := s.moviePilotCapacityPauseState(entry.QBClientID, entry.TorrentHash); isMoviePilot && pausedByDisconnect {
			continue
		}
		root, err := ResolveQBPath(state.config, info.SavePath)
		if err != nil {
			root = entry.DownloadRoot
		}
		qbPath, _, mappingErr := resolveQBPathMapping(state.config, info.SavePath)
		if mappingErr != nil {
			qbPath = normalizeQBPath(info.ContentPath)
		}
		free, err := s.downloadSpaceUsageForQB(ctx, state.client, qbPath, root, qBGlobalFreeSpaceAllowed(state.config))
		if err != nil || free < high {
			continue
		}
		if err := state.client.StartByHash(ctx, info.Hash); err != nil {
			log.Warnf("resume qB torrent %s after disk recovery: %v", info.Hash, err)
			continue
		}
		s.clearQBCapacityPause(ctx, entry)
		s.syncMoviePilotCapacityPause(ctx, entry.QBClientID, entry.TorrentHash, false)
		log.Infof("resumed qB torrent %s on %s: free disk space recovered to %d bytes", info.Hash, entry.QBClientID, free)
	}
}

func (s *Service) resumeQBCapacityPausedWhenDisabled(ctx context.Context, paused []qbCapacityPauseEntry) {
	if len(paused) == 0 {
		return
	}
	clients := make(map[string]qbittorrent.Client, len(paused))
	for _, config := range s.configuredQBClientConfigs() {
		client, err := s.qbClientForCapacity(config)
		if err != nil {
			continue
		}
		clients[strings.TrimSpace(config.ID)] = client
	}
	for _, entry := range paused {
		client := clients[entry.QBClientID]
		if client == nil {
			continue
		}
		info, err := client.GetTorrentByHash(ctx, entry.TorrentHash)
		if err != nil {
			if isMoviePilotTorrentMissing(err) {
				s.clearQBCapacityPause(ctx, entry)
				s.syncMoviePilotCapacityPause(ctx, entry.QBClientID, entry.TorrentHash, false)
			}
			continue
		}
		if !isIncompleteTorrent(info) {
			s.clearQBCapacityPause(ctx, entry)
			s.syncMoviePilotCapacityPause(ctx, entry.QBClientID, entry.TorrentHash, false)
			continue
		}
		if _, pausedByDisconnect, isMoviePilot := s.moviePilotCapacityPauseState(entry.QBClientID, entry.TorrentHash); isMoviePilot && pausedByDisconnect {
			continue
		}
		if err := client.StartByHash(ctx, entry.TorrentHash); err != nil {
			log.Warnf("resume qB torrent %s after disabling disk capacity policy: %v", entry.TorrentHash, err)
			continue
		}
		s.clearQBCapacityPause(ctx, entry)
		s.syncMoviePilotCapacityPause(ctx, entry.QBClientID, entry.TorrentHash, false)
	}
}
