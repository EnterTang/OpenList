package model

import "time"

const (
	MoviePilotIntentStatusPending       = "pending"
	MoviePilotIntentStatusAccepted      = "accepted"
	MoviePilotIntentStatusBound         = "bound"
	MoviePilotIntentStatusWaitingWorker = "waiting_worker"
	MoviePilotIntentStatusDownloading   = "downloading"
	MoviePilotIntentStatusCompleted     = "completed"
	MoviePilotIntentStatusFailed        = "failed"
	MoviePilotIntentStatusCancelled     = "cancelled"

	MoviePilotTorrentStatusBound             = "bound"
	MoviePilotTorrentStatusDownloading       = "downloading"
	MoviePilotTorrentStatusDownloadCompleted = "download_completed"
	MoviePilotTorrentStatusFilesDiscovered   = "files_discovered"
	MoviePilotTorrentStatusTransferring      = "transferring"
	MoviePilotTorrentStatusSeeding           = "seeding"
	MoviePilotTorrentStatusRetentionReview   = "retention_review"
	MoviePilotTorrentStatusDeleting          = "deleting"
	MoviePilotTorrentStatusDeleted           = "deleted"
	MoviePilotTorrentStatusFailed            = "failed"

	MoviePilotDeliveryStatusPending      = "pending"
	MoviePilotDeliveryStatusStaging      = "staging"
	MoviePilotDeliveryStatusUploading    = "uploading"
	MoviePilotDeliveryStatusMaterialized = "materialized"
	MoviePilotDeliveryStatusSkipped      = "skipped"
	MoviePilotDeliveryStatusFailed       = "failed"

	MoviePilotRetentionStatusPending      = "pending"
	MoviePilotRetentionStatusHeld         = "held"
	MoviePilotRetentionStatusEligible     = "eligible"
	MoviePilotRetentionStatusManualReview = "manual_review"
	MoviePilotRetentionStatusDeleting     = "deleting"
	MoviePilotRetentionStatusDeleted      = "deleted"
)

type MoviePilotBridgeInstance struct {
	ID                string     `json:"id" gorm:"primaryKey;size:64"`
	CreatedAt         time.Time  `json:"created_at"`
	UpdatedAt         time.Time  `json:"updated_at"`
	Name              string     `json:"name" gorm:"size:128;index"`
	BaseURL           string     `json:"base_url" gorm:"type:text"`
	SecretRef         string     `json:"secret_ref" gorm:"size:64;index"`
	SecretFingerprint string     `json:"secret_fingerprint" gorm:"size:64"`
	Enabled           bool       `json:"enabled" gorm:"index"`
	LastHealth        string     `json:"last_health" gorm:"size:32;index"`
	LastSeenAt        *time.Time `json:"last_seen_at,omitempty"`
	LastError         string     `json:"last_error" gorm:"type:text"`
}

type MoviePilotDownloadIntent struct {
	ID                 string     `json:"id" gorm:"primaryKey;size:64"`
	CreatedAt          time.Time  `json:"created_at"`
	UpdatedAt          time.Time  `json:"updated_at"`
	RequestID          string     `json:"request_id" gorm:"size:64;uniqueIndex"`
	BridgeInstanceID   string     `json:"bridge_instance_id" gorm:"size:64;index"`
	SubscriptionID     uint       `json:"subscription_id" gorm:"index"`
	SubscriptionItemID uint       `json:"subscription_item_id" gorm:"index"`
	MediaSource        string     `json:"media_source" gorm:"size:64"`
	MediaID            string     `json:"media_id" gorm:"size:128"`
	TorrentFingerprint string     `json:"torrent_fingerprint" gorm:"size:128"`
	ResourceRef        string     `json:"resource_ref" gorm:"type:text"`
	Status             string     `json:"status" gorm:"size:32;index"`
	LastEventID        string     `json:"last_event_id" gorm:"size:64;index"`
	LastErrorCode      string     `json:"last_error_code" gorm:"size:128"`
	LastError          string     `json:"last_error" gorm:"type:text"`
	AcceptedAt         *time.Time `json:"accepted_at,omitempty"`
	BoundAt            *time.Time `json:"bound_at,omitempty"`
	FinishedAt         *time.Time `json:"finished_at,omitempty"`
}

type TorrentRetentionPolicy struct {
	MinSeedSeconds  int64      `json:"min_seed_seconds,omitempty"`
	MinRatio        float64    `json:"min_ratio,omitempty"`
	SiteRuleID      string     `json:"site_rule_id,omitempty"`
	ManualHoldUntil *time.Time `json:"manual_hold_until,omitempty"`
	Permanent       bool       `json:"permanent,omitempty"`
}

type MoviePilotTorrentBinding struct {
	ID                   string     `json:"id" gorm:"primaryKey;size:64"`
	CreatedAt            time.Time  `json:"created_at"`
	UpdatedAt            time.Time  `json:"updated_at"`
	IntentID             string     `json:"intent_id" gorm:"size:64;uniqueIndex"`
	BridgeInstanceID     string     `json:"bridge_instance_id" gorm:"size:64;index"`
	DownloaderAlias      string     `json:"downloader_alias" gorm:"size:128;index"`
	WorkerNodeID         string     `json:"worker_node_id" gorm:"size:64;index"`
	QBClientID           string     `json:"qb_client_id" gorm:"size:128"`
	TorrentHash          string     `json:"torrent_hash" gorm:"size:64;uniqueIndex:idx_moviepilot_torrent_hash"`
	ContentPath          string     `json:"content_path" gorm:"type:text"`
	ObservedContentPath  string     `json:"observed_content_path" gorm:"type:text"`
	RetentionPolicyJSON  string     `json:"retention_policy_json" gorm:"type:text"`
	Status               string     `json:"status" gorm:"size:32;index"`
	RetentionStatus      string     `json:"retention_status" gorm:"size:32;index"`
	LastQBState          string     `json:"last_qb_state" gorm:"size:64"`
	LastQBProgress       float64    `json:"last_qb_progress"`
	LastQBRatio          float64    `json:"last_qb_ratio"`
	LastQBSeedingSeconds int64      `json:"last_qb_seeding_seconds"`
	DownloadCompletedAt  *time.Time `json:"download_completed_at,omitempty"`
	SeedStartedAt        *time.Time `json:"seed_started_at,omitempty"`
	RetentionEligibleAt  *time.Time `json:"retention_eligible_at,omitempty"`
	DeletingAt           *time.Time `json:"deleting_at,omitempty"`
	DeletedAt            *time.Time `json:"deleted_at,omitempty"`
	LastErrorCode        string     `json:"last_error_code" gorm:"size:128"`
	LastError            string     `json:"last_error" gorm:"type:text"`
}

type MoviePilotDeliveryFile struct {
	ID               string     `json:"id" gorm:"primaryKey;size:64"`
	CreatedAt        time.Time  `json:"created_at"`
	UpdatedAt        time.Time  `json:"updated_at"`
	TorrentBindingID string     `json:"torrent_binding_id" gorm:"size:64;index;uniqueIndex:idx_moviepilot_delivery_path"`
	RelativePath     string     `json:"relative_path" gorm:"type:text;uniqueIndex:idx_moviepilot_delivery_path"`
	FileName         string     `json:"file_name"`
	SourceSize       int64      `json:"source_size"`
	MediaSource      string     `json:"media_source" gorm:"size:64"`
	MediaID          string     `json:"media_id" gorm:"size:128"`
	Season           int        `json:"season"`
	Episode          int        `json:"episode"`
	Required         bool       `json:"required"`
	Status           string     `json:"status" gorm:"size:32;index"`
	ClusterJobID     string     `json:"cluster_job_id" gorm:"size:64;index"`
	ManifestID       string     `json:"manifest_id" gorm:"size:64;index"`
	UploadProgress   float64    `json:"upload_progress"`
	LastErrorCode    string     `json:"last_error_code" gorm:"size:128"`
	LastError        string     `json:"last_error" gorm:"type:text"`
	FinishedAt       *time.Time `json:"finished_at,omitempty"`
}

type MoviePilotBridgeNonce struct {
	ID        string    `json:"id" gorm:"primaryKey;size:64"`
	CreatedAt time.Time `json:"created_at"`
	BridgeID  string    `json:"bridge_id" gorm:"size:64;uniqueIndex:idx_moviepilot_bridge_nonce"`
	Nonce     string    `json:"nonce" gorm:"size:128;uniqueIndex:idx_moviepilot_bridge_nonce"`
	UsedAt    time.Time `json:"used_at" gorm:"index"`
}

type MoviePilotBridgeOutbox struct {
	ID           string    `json:"id" gorm:"primaryKey;size:64"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
	BridgeID     string    `json:"bridge_id" gorm:"size:64;index"`
	RequestID    string    `json:"request_id" gorm:"size:64;index"`
	EventID      string    `json:"event_id" gorm:"size:64;uniqueIndex"`
	PayloadJSON  string    `json:"payload_json" gorm:"type:text"`
	Status       string    `json:"status" gorm:"size:32;index"`
	AttemptCount int       `json:"attempt_count"`
	AvailableAt  time.Time `json:"available_at" gorm:"index"`
	LastError    string    `json:"last_error" gorm:"type:text"`
}

type MoviePilotBridgeInbox struct {
	EventID     string     `json:"event_id" gorm:"primaryKey;size:64"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
	BridgeID    string     `json:"bridge_id" gorm:"size:64;index"`
	RequestID   string     `json:"request_id" gorm:"size:64;index"`
	EventType   string     `json:"event_type" gorm:"size:64;index"`
	PayloadJSON string     `json:"payload_json" gorm:"type:text"`
	Status      string     `json:"status" gorm:"size:32;index"`
	ProcessedAt *time.Time `json:"processed_at,omitempty"`
	LastError   string     `json:"last_error" gorm:"type:text"`
}

type SubscriptionBoundTorrent struct {
	BridgeInstanceID    string    `json:"bridge_instance_id,omitempty"`
	ResourceRef         string    `json:"resource_ref,omitempty"`
	SelectedFingerprint string    `json:"selected_fingerprint,omitempty"`
	MediaSource         string    `json:"media_source,omitempty"`
	MediaID             string    `json:"media_id,omitempty"`
	MediaType           string    `json:"media_type,omitempty"`
	Season              int       `json:"season,omitempty"`
	Episode             int       `json:"episode,omitempty"`
	BoundAt             time.Time `json:"bound_at,omitempty"`
}
