package model

import "time"

const (
	SubscriptionSourceManual   = "manual"
	SubscriptionSourceTelegram = "telegram"
	SubscriptionSourcePanSou   = "pansou"

	SubscriptionStatusIdle    = "idle"
	SubscriptionStatusRunning = "running"
	SubscriptionStatusSuccess = "success"
	SubscriptionStatusFailed  = "failed"

	SubscriptionItemStatusPending      = "pending"
	SubscriptionItemStatusTransferring = "transferring"
	SubscriptionItemStatusTransferred  = "transferred"
	SubscriptionItemStatusSkipped      = "skipped"
	SubscriptionItemStatusFailed       = "failed"
)

type Subscription struct {
	ID                   uint       `json:"id" gorm:"primarykey"`
	CreatedAt            time.Time  `json:"created_at"`
	UpdatedAt            time.Time  `json:"updated_at"`
	Name                 string     `json:"name" gorm:"index"`
	SourceType           string     `json:"source_type" gorm:"index"`
	SourceConfig         string     `json:"source_config" gorm:"type:text"`
	Active               bool       `json:"active" gorm:"index"`
	CheckIntervalMinutes int        `json:"check_interval_minutes"`
	TargetRoot           string     `json:"target_root"`
	TransferEnabled      bool       `json:"transfer_enabled"`
	TMDBID               int64      `json:"tmdb_id" gorm:"index"`
	TMDBName             string     `json:"tmdb_name"`
	TMDBYear             int        `json:"tmdb_year"`
	MediaType            string     `json:"media_type" gorm:"index"`
	Category             string     `json:"category"`
	Season               int        `json:"season"`
	Seasons              []int      `json:"seasons" gorm:"serializer:json"`
	LastCheckedAt        *time.Time `json:"last_checked_at"`
	LastCursor           string     `json:"last_cursor"`
	LastTreeHash         string     `json:"last_tree_hash"`
	LastStatus           string     `json:"last_status" gorm:"index"`
	LastError            string     `json:"last_error" gorm:"type:text"`
}

type SubscriptionItem struct {
	ID             uint      `json:"id" gorm:"primarykey"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
	SubscriptionID uint      `json:"subscription_id" gorm:"uniqueIndex:idx_subscription_item_source;index"`
	SourceKey      string    `json:"source_key" gorm:"uniqueIndex:idx_subscription_item_source"`
	SourceURL      string    `json:"source_url" gorm:"type:text"`
	SourcePath     string    `json:"source_path" gorm:"index"`
	FileID         string    `json:"file_id" gorm:"index"`
	FilePath       string    `json:"file_path" gorm:"index"`
	FileName       string    `json:"file_name"`
	FileSize       int64     `json:"file_size"`
	FileHash       string    `json:"file_hash" gorm:"index"`
	Season         int       `json:"season" gorm:"index"`
	Episode        int       `json:"episode" gorm:"index"`
	TargetDir      string    `json:"target_dir"`
	TargetName     string    `json:"target_name"`
	TargetPath     string    `json:"target_path"`
	Status         string    `json:"status" gorm:"index"`
	LastSeenAt     time.Time `json:"last_seen_at" gorm:"index"`
	LastError      string    `json:"last_error" gorm:"type:text"`
}

type SubscriptionRun struct {
	ID               uint       `json:"id" gorm:"primarykey"`
	CreatedAt        time.Time  `json:"created_at"`
	UpdatedAt        time.Time  `json:"updated_at"`
	SubscriptionID   uint       `json:"subscription_id" gorm:"index"`
	StartedAt        time.Time  `json:"started_at"`
	FinishedAt       *time.Time `json:"finished_at"`
	Status           string     `json:"status" gorm:"index"`
	PreviousTreeHash string     `json:"previous_tree_hash"`
	CurrentTreeHash  string     `json:"current_tree_hash"`
	AddedCount       int        `json:"added_count"`
	ChangedCount     int        `json:"changed_count"`
	TransferredCount int        `json:"transferred_count"`
	Error            string     `json:"error" gorm:"type:text"`
}

type SubscriptionManualSourceConfig struct {
	Paths       []string `json:"paths"`
	Links       []string `json:"links"`
	ImportsText string   `json:"imports_text"`
}

type SubscriptionTelegramSourceConfig struct {
	APIID                 int                           `json:"api_id"`
	APIHash               string                        `json:"api_hash"`
	SessionFile           string                        `json:"session_file"`
	Channels              []string                      `json:"channels"`
	QuarkChannels         []string                      `json:"quark_channels,omitempty"`
	AliyunDriveChannels   []string                      `json:"aliyun_drive_channels,omitempty"`
	Pan123Channels        []string                      `json:"pan123_channels,omitempty"`
	Pan115Channels        []string                      `json:"pan115_channels,omitempty"`
	Quark                 SubscriptionTelegramPanConfig `json:"quark"`
	AliyunDrive           SubscriptionTelegramPanConfig `json:"aliyun_drive"`
	Pan123                SubscriptionTelegramPanConfig `json:"pan123"`
	Pan115                SubscriptionTelegramPanConfig `json:"pan115"`
	SearchCommand         []string                      `json:"search_command"`
	AuthCommand           []string                      `json:"auth_command"`
	CommandEnv            []string                      `json:"command_env"`
	CommandTimeoutSeconds int64                         `json:"command_timeout_seconds"`
	Limit                 int                           `json:"limit"`
}

type SubscriptionTelegramPanConfig struct {
	Channels          []string `json:"channels"`
	TempTransferRoot  string   `json:"temp_transfer_root"`
	DeleteSourceAfter bool     `json:"delete_source_after"`
	Cookie            string   `json:"cookie,omitempty"`
	RefreshToken      string   `json:"refresh_token,omitempty"`
	AccessToken       string   `json:"access_token,omitempty"`
	DriveID           string   `json:"-"`
	DriveType         string   `json:"drive_type,omitempty"`
}

type SubscriptionPanSouSourceConfig struct {
	BaseURL               string   `json:"base_url"`
	SearchCommand         []string `json:"search_command"`
	CommandEnv            []string `json:"command_env"`
	CommandTimeoutSeconds int64    `json:"command_timeout_seconds"`
	Limit                 int      `json:"limit"`
	Query                 string   `json:"query"`
}

type SubscriptionConfig struct {
	DefaultTargetRoot           string                           `json:"default_target_root,omitempty"`
	DefaultCheckIntervalMinutes int                              `json:"default_check_interval_minutes,omitempty"`
	DefaultTransferEnabled      bool                             `json:"default_transfer_enabled,omitempty"`
	DefaultMediaType            string                           `json:"default_media_type,omitempty"`
	DefaultCategory             string                           `json:"default_category,omitempty"`
	Telegram                    SubscriptionTelegramSourceConfig `json:"telegram"`
	PanSou                      SubscriptionPanSouSourceConfig   `json:"pansou"`
}

type SubscriptionResourceSearchReq struct {
	Query   string   `json:"query" form:"query"`
	Sources []string `json:"sources" form:"sources"`
	Limit   int      `json:"limit" form:"limit"`
}

type SubscriptionResourceSearchResp struct {
	Query        string                             `json:"query"`
	Sources      []string                           `json:"sources"`
	Results      []SubscriptionResourceSearchResult `json:"results"`
	SourceErrors map[string]string                  `json:"source_errors,omitempty"`
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
	URL      string `json:"url"`
	Provider string `json:"provider,omitempty"`
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
