package moviepilotbridge

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"
)

var forbiddenBridgeFieldNames = map[string]struct{}{
	"site_cookie": {}, "qb_password": {}, "qb_url": {}, "local_path": {}, "enclosure": {},
}

func validateNoForbiddenBridgeFields(body []byte) error {
	var payload any
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	decoder.UseNumber()
	if err := decoder.Decode(&payload); err != nil {
		return fmt.Errorf("decode bridge payload for secret-field validation: %w", err)
	}
	var walk func(any) error
	walk = func(value any) error {
		switch typed := value.(type) {
		case map[string]any:
			for key, child := range typed {
				if _, forbidden := forbiddenBridgeFieldNames[strings.ToLower(strings.TrimSpace(key))]; forbidden {
					return fmt.Errorf("bridge payload contains forbidden field %q", key)
				}
				if err := walk(child); err != nil {
					return err
				}
			}
		case []any:
			for _, child := range typed {
				if err := walk(child); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return walk(payload)
}

const (
	EventIntentAccepted      = "intent.accepted"
	EventTorrentBound        = "torrent.bound"
	EventTorrentStateChanged = "torrent.state_changed"
	EventTorrentFailed       = "torrent.failed"
	EventBridgeHealthChanged = "bridge.health_changed"

	BridgeSearchPath  = "/api/v1/plugin/OpenListBridge/search"
	BridgeControlPath = "/api/v1/plugin/OpenListBridge/control"
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
	ResourceRef         string   `json:"resource_ref"`
	Title               string   `json:"title"`
	Description         string   `json:"description,omitempty"`
	Site                string   `json:"site,omitempty"`
	Size                int64    `json:"size,omitempty"`
	Seeders             int      `json:"seeders,omitempty"`
	Leechers            int      `json:"leechers,omitempty"`
	Grabs               int      `json:"grabs,omitempty"`
	SeasonEpisode       string   `json:"season_episode,omitempty"`
	EpisodeCount        int      `json:"episode_count,omitempty"`
	Promotion           string   `json:"promotion,omitempty"`
	FreeRemaining       string   `json:"free_remaining,omitempty"`
	HitAndRun           bool     `json:"hit_and_run,omitempty"`
	Labels              []string `json:"labels,omitempty"`
	PublishedAt         string   `json:"published_at,omitempty"`
	SelectedFingerprint string   `json:"selected_fingerprint,omitempty"`
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
	Mode           string   `json:"mode"`
	Downloader     string   `json:"downloader,omitempty"`
	RouteID        string   `json:"route_id,omitempty"`
	ReservationID  string   `json:"reservation_id,omitempty"`
	FallbackReason string   `json:"fallback_reason,omitempty"`
	Allowed        []string `json:"allowed,omitempty"`
}

const (
	DownloaderPolicyMoviePilotSelect     = "moviepilot_select"
	DownloaderPolicyCoordinatorSelect    = "coordinator_select"
	DownloaderPolicyCoordinatorPreferred = "coordinator_preferred"
)

// ErrDownloaderCapacityUnavailable is shared by the Coordinator scheduler
// and subscription runner so an admission failure can be persisted as a
// retryable waiting state instead of being mistaken for a terminal failure.
var ErrDownloaderCapacityUnavailable = errors.New("no MoviePilot downloader route satisfies current capacity")

type TorrentControlRequest struct {
	RequestID   string `json:"request_id"`
	Downloader  string `json:"downloader"`
	TorrentHash string `json:"torrent_hash"`
	Action      string `json:"action"`
	Reason      string `json:"reason,omitempty"`
}

func (r TorrentControlRequest) Validate() error {
	if strings.TrimSpace(r.RequestID) == "" || strings.TrimSpace(r.Downloader) == "" {
		return errors.New("request_id and downloader are required")
	}
	if err := validateTorrentHash(r.TorrentHash); err != nil {
		return fmt.Errorf("torrent_hash: %w", err)
	}
	action := strings.ToLower(strings.TrimSpace(r.Action))
	if action != "pause" && action != "resume" {
		return errors.New("torrent control action must be pause or resume")
	}
	return nil
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
	Size        int64         `json:"size,omitempty"`
	Media       MediaIdentity `json:"media,omitempty"`
}

type TorrentStatePayload struct {
	State          string  `json:"state"`
	Progress       float64 `json:"progress"`
	LeftTime       int64   `json:"left_time,omitempty"`
	Ratio          float64 `json:"ratio,omitempty"`
	SeedingSeconds int64   `json:"seeding_seconds,omitempty"`
	HNRPassed      *bool   `json:"hnr_passed,omitempty"`
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
		if !isPortableNonRootContentPath(e.Torrent.ContentPath) {
			return errors.New("torrent.bound content path must be an absolute non-root path")
		}
		if e.Torrent.Size < 0 {
			return errors.New("torrent.bound size must not be negative")
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

// isPortableNonRootContentPath validates the qB path reported by the Bridge,
// not a path local to the Coordinator. The two hosts can run different
// operating systems, so filepath.IsAbs would reject a valid Windows path
// when the Coordinator runs on Linux or macOS.
func isPortableNonRootContentPath(value string) bool {
	normalized := path.Clean(strings.ReplaceAll(strings.TrimSpace(value), `\`, "/"))
	if normalized == "." || normalized == "/" {
		return false
	}
	return path.IsAbs(normalized) || isWindowsDriveAbsoluteContentPath(normalized)
}

func isWindowsDriveAbsoluteContentPath(value string) bool {
	return len(value) >= 3 &&
		((value[0] >= 'a' && value[0] <= 'z') || (value[0] >= 'A' && value[0] <= 'Z')) &&
		value[1] == ':' && value[2] == '/'
}

func (r DownloadIntentRequest) Validate() error {
	if strings.TrimSpace(r.RequestID) == "" {
		return errors.New("request_id is required")
	}
	if strings.TrimSpace(r.Media.MediaSource) == "" || strings.TrimSpace(r.Media.MediaID) == "" {
		return errors.New("media_source and media_id are required")
	}
	mode := strings.TrimSpace(r.DownloaderPolicy.Mode)
	switch mode {
	case DownloaderPolicyMoviePilotSelect:
		if strings.TrimSpace(r.DownloaderPolicy.Downloader) != "" || strings.TrimSpace(r.DownloaderPolicy.RouteID) != "" || strings.TrimSpace(r.DownloaderPolicy.ReservationID) != "" {
			return errors.New("moviepilot_select must not include a Coordinator downloader, route, or reservation")
		}
	case DownloaderPolicyCoordinatorSelect:
		if strings.TrimSpace(r.DownloaderPolicy.Downloader) == "" {
			return errors.New("coordinator_select requires a downloader")
		}
		if strings.TrimSpace(r.DownloaderPolicy.RouteID) == "" || strings.TrimSpace(r.DownloaderPolicy.ReservationID) == "" {
			return errors.New("coordinator_select requires route_id and reservation_id")
		}
	case DownloaderPolicyCoordinatorPreferred:
		if strings.TrimSpace(r.DownloaderPolicy.Downloader) != "" && (strings.TrimSpace(r.DownloaderPolicy.RouteID) == "" || strings.TrimSpace(r.DownloaderPolicy.ReservationID) == "") {
			return errors.New("coordinator_preferred requires route_id and reservation_id when a downloader is selected")
		}
	default:
		return fmt.Errorf("unsupported downloader policy mode %q", r.DownloaderPolicy.Mode)
	}
	if strings.TrimSpace(r.Torrent.Enclosure) != "" {
		return errors.New("torrent enclosure is forbidden; use the opaque resource_ref returned by MoviePilot search")
	}
	if err := validateOpaqueResourceRef(r.Torrent.ResourceRef); err != nil {
		return err
	}
	if r.Torrent.Size < 0 {
		return errors.New("torrent size must not be negative")
	}
	return nil
}

func validateOpaqueResourceRef(value string) error {
	value = strings.TrimSpace(value)
	lower := strings.ToLower(value)
	if value == "" {
		return errors.New("torrent resource_ref is required")
	}
	if len(value) > 2048 || strings.Contains(value, "://") || strings.HasPrefix(lower, "magnet:") || strings.ContainsAny(value, "\r\n\x00") {
		return errors.New("torrent resource_ref must be an opaque Bridge reference, not a download URL")
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
