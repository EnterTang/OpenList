package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"gorm.io/gorm/clause"
)

const moviePilotTorrentRegistryType = "moviepilot_qb_torrent"

type moviePilotTorrentRegistryEntry struct {
	Torrent            protocol.TorrentTaskContext      `json:"torrent"`
	Subscription       protocol.SubscriptionTaskContext `json:"subscription,omitempty"`
	PausedByDisconnect bool                             `json:"paused_by_disconnect"`
	PausedByCapacity   bool                             `json:"paused_by_capacity"`
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
	if subscription.SubscriptionID == 0 {
		subscription = previous.Subscription
	}
	return s.storeMoviePilotTorrentRegistryEntry(ctx, moviePilotTorrentRegistryEntry{Torrent: *torrent, Subscription: subscription, PausedByDisconnect: previous.PausedByDisconnect, PausedByCapacity: previous.PausedByCapacity})
}

func (s *Service) setMoviePilotTorrentDisconnectPaused(ctx context.Context, torrent *protocol.TorrentTaskContext, paused bool) error {
	if torrent == nil || strings.TrimSpace(torrent.TorrentHash) == "" {
		return nil
	}
	s.mu.Lock()
	previous := s.moviePilotTorrents[moviePilotTorrentRegistryKey(torrent)]
	s.mu.Unlock()
	return s.storeMoviePilotTorrentRegistryEntry(ctx, moviePilotTorrentRegistryEntry{Torrent: *torrent, Subscription: previous.Subscription, PausedByDisconnect: paused, PausedByCapacity: previous.PausedByCapacity})
}

func (s *Service) setMoviePilotTorrentCapacityPaused(ctx context.Context, torrent *protocol.TorrentTaskContext, paused bool) error {
	if torrent == nil || strings.TrimSpace(torrent.TorrentHash) == "" {
		return nil
	}
	s.mu.Lock()
	previous := s.moviePilotTorrents[moviePilotTorrentRegistryKey(torrent)]
	s.mu.Unlock()
	return s.storeMoviePilotTorrentRegistryEntry(ctx, moviePilotTorrentRegistryEntry{Torrent: *torrent, Subscription: previous.Subscription, PausedByDisconnect: previous.PausedByDisconnect, PausedByCapacity: paused})
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
