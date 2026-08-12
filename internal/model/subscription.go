package model

import "time"

const (
	SubscriptionSourceManual   = "manual"
	SubscriptionSourceTelegram = "telegram"
	SubscriptionSourcePanSou   = "pansou"
	SubscriptionSourceHDHive   = "hdhive"
	SubscriptionSourceAuto     = "auto"

	SubscriptionStatusIdle    = "idle"
	SubscriptionStatusRunning = "running"
	SubscriptionStatusSuccess = "success"
	SubscriptionStatusFailed  = "failed"

	SubscriptionItemStatusRetryWait = "retry_wait"
	SubscriptionItemStatusBlocked   = "blocked"
	SubscriptionItemStatusUnknown   = "unknown"

	SubscriptionDeliveryModeTransfer       = "transfer"
	SubscriptionDeliveryModeDirectDownload = "direct_download"

	SubscriptionRunViewChanges  = "changes"
	SubscriptionRunViewFailures = "failures"

	SubscriptionItemStatusPending      = "pending"
	SubscriptionItemStatusNotifying    = "notifying"
	SubscriptionItemStatusTransferring = "transferring"
	SubscriptionItemStatusTransferred  = "transferred"
	SubscriptionItemStatusSkipped      = "skipped"
	SubscriptionItemStatusFailed       = "failed"

	// Subscription secret markers let configuration clients distinguish an
	// unchanged credential from an explicit clear without echoing stored values.
	SubscriptionSecretUnchangedMarker = "__OPENLIST_SECRET_UNCHANGED__"
	SubscriptionSecretClearMarker     = "__OPENLIST_SECRET_CLEAR__"

	SubscriptionArchiveStatusOngoing   = "ongoing"
	SubscriptionArchiveStatusCompleted = "completed"
	SubscriptionArchiveStatusStalled   = "stalled"
)

type Subscription struct {
	ID                       uint                       `json:"id" gorm:"primarykey"`
	CreatedAt                time.Time                  `json:"created_at"`
	UpdatedAt                time.Time                  `json:"updated_at"`
	Name                     string                     `json:"name" gorm:"index"`
	SourceType               string                     `json:"source_type" gorm:"index"`
	SourceConfig             string                     `json:"source_config" gorm:"type:text"`
	Active                   bool                       `json:"active" gorm:"index"`
	CheckIntervalMinutes     int                        `json:"check_interval_minutes"`
	TargetRoot               string                     `json:"target_root,omitempty"`
	TempTarget               SubscriptionStorageTarget  `json:"temp_target,omitempty" gorm:"serializer:json"`
	DeliveryTarget           SubscriptionStorageTarget  `json:"delivery_target,omitempty" gorm:"serializer:json"`
	PreferredWorkerNodeID    string                     `json:"preferred_worker_node_id,omitempty" gorm:"size:64"`
	TransferEnabled          bool                       `json:"transfer_enabled"`
	TMDBID                   int64                      `json:"tmdb_id" gorm:"index"`
	TMDBName                 string                     `json:"tmdb_name"`
	TMDBYear                 int                        `json:"tmdb_year"`
	MediaType                string                     `json:"media_type" gorm:"index"`
	Category                 string                     `json:"category"`
	Season                   int                        `json:"season"`
	Seasons                  []int                      `json:"seasons" gorm:"serializer:json"`
	LatestSeasonEpisodeStart int                        `json:"latest_season_episode_start"`
	LatestSeasonEpisodeEnd   int                        `json:"latest_season_episode_end"`
	TMDBEpisodeSyncedAt      *time.Time                 `json:"tmdb_episode_synced_at,omitempty"`
	LastCheckedAt            *time.Time                 `json:"last_checked_at"`
	LastCursor               string                     `json:"last_cursor"`
	LastTreeHash             string                     `json:"last_tree_hash"`
	LastStatus               string                     `json:"last_status" gorm:"index"`
	LastError                string                     `json:"last_error" gorm:"type:text"`
	Progress                 SubscriptionProgress       `json:"progress" gorm:"-"`
	RealtimeStatus           SubscriptionRealtimeStatus `json:"realtime_status" gorm:"-"`
	BoundShare               *SubscriptionBoundShare    `json:"bound_share,omitempty" gorm:"serializer:json"`
}

type SubscriptionBoundShare struct {
	SourceType     string    `json:"source_type,omitempty"`
	Provider       string    `json:"provider,omitempty"`
	ShareURL       string    `json:"share_url,omitempty"`
	AccessCode     string    `json:"access_code,omitempty"`
	ResourceURL    string    `json:"resource_url,omitempty"`
	ResourceSlug   string    `json:"resource_slug,omitempty"`
	RequiresUnlock bool      `json:"requires_unlock,omitempty"`
	UnlockPoints   *int      `json:"unlock_points,omitempty"`
	BoundAt        time.Time `json:"bound_at,omitempty"`
}

type ExternalSubscriptionRequest struct {
	ID                 uint       `json:"id" gorm:"primarykey"`
	CreatedAt          time.Time  `json:"created_at"`
	UpdatedAt          time.Time  `json:"updated_at"`
	IdempotencyKey     string     `json:"idempotency_key" gorm:"size:191;uniqueIndex"`
	LookupKey          string     `json:"lookup_key" gorm:"size:191;uniqueIndex"`
	RequestFingerprint string     `json:"request_fingerprint" gorm:"size:64;index"`
	SubscriptionID     uint       `json:"subscription_id" gorm:"uniqueIndex"`
	RequestJSON        string     `json:"request_json" gorm:"type:text"`
	ResponseJSON       string     `json:"response_json" gorm:"type:text"`
	LastStatus         string     `json:"last_status" gorm:"size:32;index"`
	LastMessage        string     `json:"last_message" gorm:"type:text"`
	ProgressJSON       string     `json:"progress_json" gorm:"type:text"`
	SeasonsJSON        string     `json:"seasons_json" gorm:"type:text"`
	LastRunAt          *time.Time `json:"last_run_at"`
	LastError          string     `json:"last_error" gorm:"type:text"`
}

// SubscriptionProgress is calculated from subscription items when a
// subscription is listed. It is deliberately not persisted: a late-arriving
// episode should make a previously stalled subscription visible again.
type SubscriptionProgress struct {
	ArchiveStatus      string     `json:"archive_status"`
	LatestSeason       int        `json:"latest_season,omitempty"`
	LatestEpisode      int        `json:"latest_episode,omitempty"`
	MissingEpisodes    []int      `json:"missing_episodes"`
	CompletedEpisodes  int        `json:"completed_episodes"`
	ExpectedEpisodes   int        `json:"expected_episodes,omitempty"`
	LastEpisodeAddedAt *time.Time `json:"last_episode_added_at,omitempty"`
}

type SubscriptionItem struct {
	ID                   uint              `json:"id" gorm:"primarykey"`
	CreatedAt            time.Time         `json:"created_at"`
	UpdatedAt            time.Time         `json:"updated_at"`
	SubscriptionID       uint              `json:"subscription_id" gorm:"uniqueIndex:idx_subscription_item_source;index"`
	SourceKey            string            `json:"source_key" gorm:"uniqueIndex:idx_subscription_item_source"`
	SourceProvider       string            `json:"source_provider" gorm:"index"`
	SourceURL            string            `json:"source_url" gorm:"type:text"`
	SourceMessageID      string            `json:"source_message_id" gorm:"index"`
	SourceMessageChannel string            `json:"source_message_channel" gorm:"index"`
	SourceMessageURL     string            `json:"source_message_url" gorm:"type:text"`
	SourceMessageText    string            `json:"source_message_text" gorm:"type:text"`
	SourcePath           string            `json:"source_path" gorm:"index"`
	FileID               string            `json:"file_id" gorm:"index"`
	FilePath             string            `json:"file_path" gorm:"index"`
	FileName             string            `json:"file_name"`
	FileSize             int64             `json:"file_size"`
	FileHash             string            `json:"file_hash" gorm:"index"`
	ProviderData         map[string]string `json:"provider_data,omitempty" gorm:"serializer:json"`
	Season               int               `json:"season" gorm:"index"`
	Episode              int               `json:"episode" gorm:"index"`
	TargetDir            string            `json:"target_dir"`
	TargetName           string            `json:"target_name"`
	TargetPath           string            `json:"target_path"`
	Status               string            `json:"status" gorm:"index"`
	ClusterJobID         string            `json:"cluster_job_id" gorm:"size:64;index"`
	LastSeenAt           time.Time         `json:"last_seen_at" gorm:"index"`
	LastError            string            `json:"last_error" gorm:"type:text"`
	LastErrorCode        string            `json:"last_error_code" gorm:"size:64;index"`
	RetryCount           int               `json:"retry_count"`
	RetryAt              *time.Time        `json:"retry_at,omitempty" gorm:"index"`
	BlockedReason        string            `json:"blocked_reason,omitempty" gorm:"size:128"`
	OperationKey         string            `json:"operation_key,omitempty" gorm:"size:191;index"`
	StateVersion         uint64            `json:"state_version" gorm:"not null;default:0"`
	DeliveryMode         string            `json:"delivery_mode,omitempty" gorm:"size:32;index"`
	FallbackReason       string            `json:"fallback_reason,omitempty" gorm:"size:128"`
}

type SubscriptionEpisodeSource struct {
	ID             uint      `json:"id" gorm:"primarykey"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
	SubscriptionID uint      `json:"subscription_id" gorm:"uniqueIndex:idx_subscription_episode_source;index"`
	Season         int       `json:"season" gorm:"uniqueIndex:idx_subscription_episode_source;index"`
	Episode        int       `json:"episode" gorm:"uniqueIndex:idx_subscription_episode_source;index"`
	SourceItemID   uint      `json:"source_item_id" gorm:"index"`
	SourceType     string    `json:"source_type" gorm:"index"`
	SourceProvider string    `json:"source_provider" gorm:"index"`
	ShareURL       string    `json:"share_url" gorm:"type:text"`
	FileName       string    `json:"file_name"`
	FileHash       string    `json:"-" gorm:"index"`
	Status         string    `json:"status" gorm:"index"`
	ClusterJobID   string    `json:"cluster_job_id" gorm:"size:64;index"`
	SelectedAt     time.Time `json:"selected_at" gorm:"index"`
}

type SubscriptionRun struct {
	ID                     uint       `json:"id" gorm:"primarykey"`
	CreatedAt              time.Time  `json:"created_at"`
	UpdatedAt              time.Time  `json:"updated_at"`
	SubscriptionID         uint       `json:"subscription_id" gorm:"index"`
	StartedAt              time.Time  `json:"started_at"`
	FinishedAt             *time.Time `json:"finished_at"`
	Status                 string     `json:"status" gorm:"index"`
	PreviousTreeHash       string     `json:"previous_tree_hash"`
	CurrentTreeHash        string     `json:"current_tree_hash"`
	AddedCount             int        `json:"added_count"`
	ChangedCount           int        `json:"changed_count"`
	TransferredCount       int        `json:"transferred_count"`
	QueuedCount            int        `json:"queued_count,omitempty"`
	DiscoveredCount        int        `json:"discovered_count,omitempty"`
	DispatchedCount        int        `json:"dispatched_count,omitempty"`
	SucceededCount         int        `json:"succeeded_count,omitempty"`
	SkippedCount           int        `json:"skipped_count,omitempty"`
	RetryableCount         int        `json:"retryable_count,omitempty"`
	BlockedCount           int        `json:"blocked_count,omitempty"`
	UnknownCount           int        `json:"unknown_count,omitempty"`
	FailedCount            int        `json:"failed_count,omitempty"`
	DiscoverStatus         string     `json:"discover_status,omitempty" gorm:"size:32"`
	DispatchStatus         string     `json:"dispatch_status,omitempty" gorm:"size:32"`
	TransferStatus         string     `json:"transfer_status,omitempty" gorm:"size:32"`
	CompletionState        string     `json:"completion_state,omitempty" gorm:"size:32;index"`
	Error                  string     `json:"error" gorm:"type:text"`
	SubscriptionName       string     `json:"subscription_name" gorm:"->;-:migration;column:subscription_name"`
	SubscriptionSourceType string     `json:"subscription_source_type" gorm:"->;-:migration;column:subscription_source_type"`
}

type SubscriptionBoard struct {
	SubscriptionCount int64 `json:"subscription_count"`
	ChangedRunCount   int64 `json:"changed_run_count"`
	AddedCount        int64 `json:"added_count"`
	ChangedCount      int64 `json:"changed_count"`
	FailureCount      int64 `json:"failure_count"`
}

type SubscriptionEpisodeSourceDetail struct {
	ID                        uint       `json:"id"`
	CreatedAt                 time.Time  `json:"created_at"`
	UpdatedAt                 time.Time  `json:"updated_at"`
	SubscriptionID            uint       `json:"subscription_id"`
	Season                    int        `json:"season"`
	Episode                   int        `json:"episode"`
	SourceItemID              uint       `json:"source_item_id"`
	SourceType                string     `json:"source_type"`
	SourceProvider            string     `json:"source_provider"`
	ShareURL                  string     `json:"share_url"`
	FileName                  string     `json:"file_name"`
	ClusterJobID              string     `json:"cluster_job_id"`
	SelectedAt                time.Time  `json:"selected_at"`
	Status                    string     `json:"status"`
	ItemLastError             string     `json:"item_last_error,omitempty"`
	WorkerName                string     `json:"worker_name"`
	JobStatus                 string     `json:"job_status,omitempty"`
	JobNotificationStatus     string     `json:"job_notification_status,omitempty"`
	JobGeneration             uint64     `json:"job_generation,omitempty"`
	JobStartedAt              *time.Time `json:"job_started_at,omitempty"`
	JobFinishedAt             *time.Time `json:"job_finished_at,omitempty"`
	JobLastErrorCode          string     `json:"job_last_error_code,omitempty"`
	JobLastError              string     `json:"job_last_error,omitempty"`
	CurrentStage              string     `json:"current_stage,omitempty"`
	CurrentStageStatus        string     `json:"current_stage_status,omitempty"`
	CurrentStageRetryCount    int        `json:"current_stage_retry_count,omitempty"`
	CurrentStageErrorCode     string     `json:"current_stage_error_code,omitempty"`
	CurrentStageError         string     `json:"current_stage_error,omitempty"`
	EffectiveStatus           string     `json:"effective_status,omitempty"`
	NotificationDisplayStatus string     `json:"notification_display_status,omitempty"`
	HasArchivedETF            bool       `json:"has_archived_etf,omitempty"`
}

type SubscriptionStorageTarget struct {
	Provider string `json:"provider,omitempty"`
	Folder   string `json:"folder,omitempty"`
}

type SubscriptionManualSourceConfig struct {
	Paths       []string `json:"paths"`
	Links       []string `json:"links"`
	ImportsText string   `json:"imports_text"`
}

type SubscriptionTelegramSourceConfig struct {
	APIID                        int                              `json:"api_id"`
	APIHash                      string                           `json:"api_hash"`
	SessionFile                  string                           `json:"session_file"`
	Channels                     []string                         `json:"channels"`
	QuarkChannels                []string                         `json:"quark_channels,omitempty"`
	AliyunDriveChannels          []string                         `json:"aliyun_drive_channels,omitempty"`
	Pan123Channels               []string                         `json:"pan123_channels,omitempty"`
	Pan115Channels               []string                         `json:"pan115_channels,omitempty"`
	GuangYaPanChannels           []string                         `json:"guangyapan_channels,omitempty"`
	Quark                        SubscriptionTelegramPanConfig    `json:"quark"`
	AliyunDrive                  SubscriptionTelegramPanConfig    `json:"aliyun_drive"`
	Pan123                       SubscriptionTelegramPanConfig    `json:"pan123"`
	Pan115                       SubscriptionTelegramPanConfig    `json:"pan115"`
	GuangYaPan                   SubscriptionTelegramPanConfig    `json:"guangyapan"`
	HDHive                       SubscriptionTelegramHDHiveConfig `json:"hdhive,omitempty"`
	SearchCommand                []string                         `json:"search_command"`
	AuthCommand                  []string                         `json:"auth_command"`
	CommandEnv                   []string                         `json:"command_env"`
	CommandTimeoutSeconds        int64                            `json:"command_timeout_seconds"`
	Limit                        int                              `json:"limit"`
	TransferPriority             []string                         `json:"transfer_priority,omitempty"`
	RealtimeEnabled              bool                             `json:"realtime_enabled,omitempty"`
	RealtimeGroups               []string                         `json:"realtime_groups,omitempty"`
	RealtimeCandidateWaitSeconds *int                             `json:"realtime_candidate_wait_seconds,omitempty"`
	RealtimeExpectedProviders    []string                         `json:"realtime_expected_providers,omitempty"`
}

// SubscriptionTelegramHDHiveConfig configures the optional HDHive resource
// unlocker used by Telegram-backed subscriptions. Proxy credentials are
// redacted by the subscription config API like pan provider secrets.
type SubscriptionTelegramHDHiveConfig struct {
	Enabled         bool   `json:"enabled,omitempty"`
	BaseURL         string `json:"base_url,omitempty"`
	UserID          string `json:"user_id,omitempty"`
	ProxyUserKey    string `json:"proxy_user_key,omitempty"`
	ProxySecret     string `json:"proxy_secret,omitempty"`
	TimeoutSeconds  int    `json:"timeout_seconds,omitempty"`
	MaxUnlockPoints int    `json:"max_unlock_points,omitempty"`
}

// SubscriptionRealtimeStatus is a card-ready projection derived from durable
// Telegram events, pending candidate selections, and subscription items.
type SubscriptionRealtimeStatus struct {
	Enabled            bool       `json:"enabled"`
	ListenerState      string     `json:"listener_state"`
	DeliveryStatus     string     `json:"delivery_status"`
	ActiveJobCount     int64      `json:"active_job_count"`
	LastEventAt        *time.Time `json:"last_event_at,omitempty"`
	LastCompletedAt    *time.Time `json:"last_completed_at,omitempty"`
	LastMessageChannel string     `json:"last_message_channel,omitempty"`
	LastMessageID      string     `json:"last_message_id,omitempty"`
	LastError          string     `json:"last_error,omitempty"`
	RetryAt            *time.Time `json:"retry_at,omitempty"`
}

type SubscriptionTelegramPanConfig struct {
	Channels           []string                  `json:"channels"`
	TempTransferRoot   string                    `json:"temp_transfer_root"`
	TempTransferTarget SubscriptionStorageTarget `json:"temp_transfer_target,omitempty"`
	DeleteSourceAfter  bool                      `json:"delete_source_after"`
	Cookie             string                    `json:"cookie,omitempty"`
	RefreshToken       string                    `json:"refresh_token,omitempty"`
	AccessToken        string                    `json:"access_token,omitempty"`
	DriveID            string                    `json:"-"`
	DriveType          string                    `json:"drive_type,omitempty"`
}

type SubscriptionPanSouSourceConfig struct {
	BaseURL               string   `json:"base_url"`
	SearchCommand         []string `json:"search_command"`
	CommandEnv            []string `json:"command_env"`
	CommandTimeoutSeconds int64    `json:"command_timeout_seconds"`
	Limit                 int      `json:"limit"`
	Query                 string   `json:"query"`
}

type SubscriptionHDHiveSourceConfig struct {
	CloudType string `json:"cloud_type,omitempty"`
	Limit     int    `json:"limit,omitempty"`
}

type SubscriptionConfig struct {
	DefaultTargetRoot           string                           `json:"default_target_root,omitempty"`
	DefaultTarget               SubscriptionStorageTarget        `json:"default_target,omitempty"`
	DefaultCheckIntervalMinutes int                              `json:"default_check_interval_minutes,omitempty"`
	DefaultTransferEnabled      bool                             `json:"default_transfer_enabled,omitempty"`
	ResultConfirmationEnabled   bool                             `json:"result_confirmation_enabled,omitempty"`
	ProviderHealthRequired      bool                             `json:"provider_health_required,omitempty"`
	DirectShareLinkEnabled      bool                             `json:"direct_share_link_enabled,omitempty"`
	DirectDownloadFirstEnabled  bool                             `json:"direct_download_first_enabled,omitempty"`
	MaxReconcileAttempts        int                              `json:"max_reconcile_attempts,omitempty"`
	DefaultMediaType            string                           `json:"default_media_type,omitempty"`
	DefaultCategory             string                           `json:"default_category,omitempty"`
	EpisodeEarlyCloseMinBytes   *int64                           `json:"episode_early_close_min_bytes,omitempty"`
	MovieEarlyCloseMinBytes     *int64                           `json:"movie_early_close_min_bytes,omitempty"`
	Telegram                    SubscriptionTelegramSourceConfig `json:"telegram"`
	PanSou                      SubscriptionPanSouSourceConfig   `json:"pansou"`
}

type SubscriptionResourceSearchReq struct {
	Query     string   `json:"query" form:"query"`
	Sources   []string `json:"sources" form:"sources"`
	Limit     int      `json:"limit" form:"limit"`
	TMDBID    int64    `json:"tmdb_id" form:"tmdb_id"`
	MediaType string   `json:"media_type" form:"media_type"`
	CloudType string   `json:"cloud_type" form:"cloud_type"`
}

type SubscriptionResourceSearchResp struct {
	Query              string                                        `json:"query"`
	Sources            []string                                      `json:"sources"`
	Results            []SubscriptionResourceSearchResult            `json:"results"`
	SourceErrors       map[string]string                             `json:"source_errors,omitempty"`
	SourceCapabilities map[string]SubscriptionSearchSourceCapability `json:"source_capabilities"`
}

type SubscriptionSearchSourceCapability struct {
	Configured        bool   `json:"configured"`
	Available         bool   `json:"available"`
	UnavailableReason string `json:"unavailable_reason,omitempty"`
}

type SubscriptionConfigSecretStatus struct {
	Configured      map[string]bool `json:"configured"`
	UnchangedMarker string          `json:"unchanged_marker"`
	ClearMarker     string          `json:"clear_marker"`
}

// SubscriptionConfigResponse embeds the legacy configuration shape so older
// clients keep working while newer clients can handle redacted credentials and
// source availability explicitly.
type SubscriptionConfigResponse struct {
	SubscriptionConfig
	SecretStatus       SubscriptionConfigSecretStatus                `json:"secret_status"`
	SourceCapabilities map[string]SubscriptionSearchSourceCapability `json:"source_capabilities"`
}

type SubscriptionResourceSearchResult struct {
	SourceType string                           `json:"source_type"`
	Provider   string                           `json:"provider,omitempty"`
	Title      string                           `json:"title"`
	Content    string                           `json:"content,omitempty"`
	Channel    string                           `json:"channel,omitempty"`
	MessageURL string                           `json:"message_url,omitempty"`
	Date       string                           `json:"date,omitempty"`
	Links      []SubscriptionResourceSearchLink `json:"links,omitempty"`
}

type SubscriptionResourceSearchLink struct {
	URL          string `json:"url"`
	Provider     string `json:"provider,omitempty"`
	Unlockable   bool   `json:"unlockable,omitempty"`
	Unlocked     bool   `json:"unlocked,omitempty"`
	UnlockPoints *int   `json:"unlock_points,omitempty"`
	AccessCode   string `json:"access_code,omitempty"`
}

type SubscriptionResourceUnlockReq struct {
	URL string `json:"url" binding:"required"`
}

type SubscriptionResourceUnlockResp struct {
	URL         string `json:"url"`
	AccessCode  string `json:"access_code,omitempty"`
	FromCache   bool   `json:"from_cache,omitempty"`
	PointsSpent *int   `json:"points_spent,omitempty"`
}

type SubscriptionResourceBindReq struct {
	SubscriptionID uint   `json:"subscription_id" binding:"required"`
	SourceType     string `json:"source_type,omitempty"`
	Provider       string `json:"provider,omitempty"`
	ShareURL       string `json:"share_url" binding:"required"`
	AccessCode     string `json:"access_code,omitempty"`
	ResourceURL    string `json:"resource_url,omitempty"`
	RequiresUnlock bool   `json:"requires_unlock,omitempty"`
	UnlockPoints   *int   `json:"unlock_points,omitempty"`
}

type SubscriptionResourceUnbindReq struct {
	SubscriptionID uint `json:"subscription_id" binding:"required"`
}

type SubscriptionPreviewReq struct {
	ID uint `json:"id" binding:"required"`
}

type SubscriptionCheckReq struct {
	ID       uint `json:"id" binding:"required"`
	Transfer bool `json:"transfer"`
}

type SubscriptionRunResult struct {
	Subscription *Subscription      `json:"subscription"`
	Run          *SubscriptionRun   `json:"run"`
	Items        []SubscriptionItem `json:"items"`
}

type SubscriptionTelegramAuthReq struct {
	ID            uint   `json:"id"`
	Phone         string `json:"phone"`
	Code          string `json:"code"`
	PhoneCodeHash string `json:"phone_code_hash"`
}

type SubscriptionTelegramAuthResp struct {
	OK            bool           `json:"ok,omitempty"`
	Authorized    bool           `json:"authorized"`
	User          map[string]any `json:"user,omitempty"`
	PhoneCodeHash string         `json:"phone_code_hash,omitempty"`
	Error         string         `json:"error,omitempty"`
}
