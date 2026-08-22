package moviepilotbridge

import (
	"encoding/hex"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"
)

const (
	EventIntentAccepted      = "intent.accepted"
	EventTorrentBound        = "torrent.bound"
	EventTorrentStateChanged = "torrent.state_changed"
	EventTorrentFailed       = "torrent.failed"
	EventBridgeHealthChanged = "bridge.health_changed"

	BridgeSearchPath = "/api/v1/openlist/search"
)

type ResourceSearchRequest struct {
	RequestID   string `json:"request_id"`
	Query       string `json:"query"`
	MediaSource string `json:"media_source"`
	MediaID     string `json:"media_id"`
	MediaType   string `json:"media_type,omitempty"`
	Season      int    `json:"season,omitempty"`
	Episode     int    `json:"episode,omitempty"`
}

type ResourceSearchResult struct {
	ResourceRef         string `json:"resource_ref"`
	Title               string `json:"title"`
	Site                string `json:"site,omitempty"`
	Size                int64  `json:"size,omitempty"`
	Seeders             int    `json:"seeders,omitempty"`
	Leechers            int    `json:"leechers,omitempty"`
	SelectedFingerprint string `json:"selected_fingerprint,omitempty"`
}

type ResourceSearchResponse struct {
	Results []ResourceSearchResult `json:"results"`
}

type DownloadIntentRequest struct {
	RequestID          string                 `json:"request_id"`
	SubscriptionID     string                 `json:"subscription_id,omitempty"`
	SubscriptionItemID string                 `json:"subscription_item_id,omitempty"`
	Media              MediaIdentity          `json:"media"`
	Torrent            TorrentResource        `json:"torrent"`
	DownloaderPolicy   DownloaderPolicy       `json:"downloader_policy"`
	RetentionPolicy    map[string]interface{} `json:"retention_policy,omitempty"`
}

type MediaIdentity struct {
	MediaSource string `json:"media_source"`
	MediaID     string `json:"media_id"`
	MediaType   string `json:"media_type,omitempty"`
	Season      int    `json:"season,omitempty"`
	Episode     int    `json:"episode,omitempty"`
}

type TorrentResource struct {
	Title               string `json:"title,omitempty"`
	ResourceRef         string `json:"resource_ref,omitempty"`
	Enclosure           string `json:"enclosure,omitempty"`
	Site                string `json:"site,omitempty"`
	Size                int64  `json:"size,omitempty"`
	SelectedFingerprint string `json:"selected_fingerprint,omitempty"`
}

type DownloaderPolicy struct {
	Mode    string   `json:"mode"`
	Allowed []string `json:"allowed,omitempty"`
}

type BridgeEvent struct {
	EventID    string               `json:"event_id"`
	RequestID  string               `json:"request_id"`
	Type       string               `json:"type"`
	OccurredAt time.Time            `json:"occurred_at"`
	Torrent    *TorrentBoundPayload `json:"torrent,omitempty"`
	State      *TorrentStatePayload `json:"state,omitempty"`
	Failure    *TorrentFailure      `json:"failure,omitempty"`
	Health     string               `json:"health,omitempty"`
}

type TorrentBoundPayload struct {
	Downloader  string        `json:"downloader"`
	TorrentHash string        `json:"torrent_hash"`
	ContentPath string        `json:"content_path"`
	Media       MediaIdentity `json:"media,omitempty"`
}

type TorrentStatePayload struct {
	State    string  `json:"state"`
	Progress float64 `json:"progress"`
	LeftTime int64   `json:"left_time,omitempty"`
}

type TorrentFailure struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e BridgeEvent) Validate() error {
	if strings.TrimSpace(e.EventID) == "" {
		return errors.New("event_id is required")
	}
	if strings.TrimSpace(e.RequestID) == "" {
		return errors.New("request_id is required")
	}
	switch e.Type {
	case EventIntentAccepted, EventBridgeHealthChanged:
		return nil
	case EventTorrentBound:
		if e.Torrent == nil {
			return errors.New("torrent.bound torrent is required")
		}
		if strings.TrimSpace(e.Torrent.Downloader) == "" {
			return errors.New("torrent.bound downloader is required")
		}
		if err := validateTorrentHash(e.Torrent.TorrentHash); err != nil {
			return fmt.Errorf("torrent.bound torrent hash: %w", err)
		}
		contentPath := path.Clean(strings.TrimSpace(e.Torrent.ContentPath))
		if contentPath == "." || contentPath == "/" || !path.IsAbs(contentPath) {
			return errors.New("torrent.bound content path must be an absolute non-root path")
		}
	case EventTorrentStateChanged:
		if e.State == nil {
			return errors.New("torrent.state_changed state is required")
		}
		if e.State.Progress < 0 || e.State.Progress > 1 {
			return errors.New("torrent.state_changed progress must be between 0 and 1")
		}
	case EventTorrentFailed:
		if e.Failure == nil || strings.TrimSpace(e.Failure.Code) == "" || strings.TrimSpace(e.Failure.Message) == "" {
			return errors.New("torrent.failed failure code and message are required")
		}
	default:
		return fmt.Errorf("unsupported bridge event type %q", e.Type)
	}
	return nil
}

func (r DownloadIntentRequest) Validate() error {
	if strings.TrimSpace(r.RequestID) == "" {
		return errors.New("request_id is required")
	}
	if strings.TrimSpace(r.Media.MediaSource) == "" || strings.TrimSpace(r.Media.MediaID) == "" {
		return errors.New("media_source and media_id are required")
	}
	if strings.TrimSpace(r.DownloaderPolicy.Mode) != "moviepilot_select" {
		return errors.New("downloader policy mode must be moviepilot_select")
	}
	if strings.TrimSpace(r.Torrent.ResourceRef) == "" && strings.TrimSpace(r.Torrent.Enclosure) == "" {
		return errors.New("torrent resource_ref or enclosure is required")
	}
	return nil
}

func validateTorrentHash(value string) error {
	value = strings.TrimSpace(value)
	if len(value) != 40 && len(value) != 64 {
		return errors.New("must contain 40 or 64 hexadecimal characters")
	}
	if value != strings.ToLower(value) {
		return errors.New("must be lowercase")
	}
	if _, err := hex.DecodeString(value); err != nil {
		return errors.New("must contain hexadecimal characters")
	}
	return nil
}
