package model

import "time"

const (
	// MoviePilot task phases are user-facing lifecycle states shared by the
	// Coordinator API, Worker API, and the MoviePilot Bridge UI. Technical
	// cluster stages remain available in ClusterJobStage for diagnostics.
	MoviePilotTaskPhasePending          = "pending"
	MoviePilotTaskPhaseAccepted         = "accepted"
	MoviePilotTaskPhaseWaitingCapacity  = "waiting_capacity"
	MoviePilotTaskPhaseWaitingBinding   = "waiting_binding"
	MoviePilotTaskPhaseBound            = "bound"
	MoviePilotTaskPhaseDownloading      = "downloading"
	MoviePilotTaskPhaseDownloadComplete = "download_completed"
	MoviePilotTaskPhaseStaging          = "staging"
	MoviePilotTaskPhaseUploading        = "uploading"
	MoviePilotTaskPhaseSeeding          = "seeding"
	MoviePilotTaskPhaseCompleted        = "completed"
	MoviePilotTaskPhaseFailed           = "failed"

	MoviePilotIntentStatusPending         = "pending"
	MoviePilotIntentStatusWaitingCapacity = "waiting_capacity"
	MoviePilotIntentStatusAccepted        = "accepted"
	MoviePilotIntentStatusBound           = "bound"
	MoviePilotIntentStatusWaitingWorker   = "waiting_worker"
	MoviePilotIntentStatusDownloading     = "downloading"
	MoviePilotIntentStatusCompleted       = "completed"
	MoviePilotIntentStatusFailed          = "failed"
	MoviePilotIntentStatusCancelled       = "cancelled"

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

	MoviePilotReservationStatusReserved = "reserved"
	MoviePilotReservationStatusBound    = "bound"
	MoviePilotReservationStatusReleased = "released"
	MoviePilotReservationStatusExpired  = "expired"
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
	ID                   string     `json:"id" gorm:"primaryKey;size:64"`
	CreatedAt            time.Time  `json:"created_at"`
	UpdatedAt            time.Time  `json:"updated_at"`
	RequestID            string     `json:"request_id" gorm:"size:64;uniqueIndex"`
	BridgeInstanceID     string     `json:"bridge_instance_id" gorm:"size:64;index"`
	SubscriptionID       uint       `json:"subscription_id" gorm:"index"`
	SubscriptionItemID   uint       `json:"subscription_item_id" gorm:"index"`
	MediaSource          string     `json:"media_source" gorm:"size:64"`
	MediaID              string     `json:"media_id" gorm:"size:128"`
	TorrentFingerprint   string     `json:"torrent_fingerprint" gorm:"size:128"`
	ResourceRef          string     `json:"resource_ref" gorm:"type:text"`
	RetentionPolicyJSON  string     `json:"retention_policy_json" gorm:"type:text"`
	DownloaderPolicyJSON string     `json:"downloader_policy_json" gorm:"type:text"`
	DownloaderPolicyMode string     `json:"downloader_policy_mode" gorm:"size:32;index"`
	SelectedDownloader   string     `json:"selected_downloader" gorm:"size:128;index"`
	SelectedRouteID      string     `json:"selected_route_id" gorm:"size:128;index"`
	SelectedWorkerNodeID string     `json:"selected_worker_node_id" gorm:"size:64;index"`
	SelectedQBClientID   string     `json:"selected_qb_client_id" gorm:"size:128;index"`
	ReservationID        string     `json:"reservation_id" gorm:"size:64;index"`
	ReservationExpiresAt *time.Time `json:"reservation_expires_at,omitempty"`
	Status               string     `json:"status" gorm:"size:32;index"`
	LastEventID          string     `json:"last_event_id" gorm:"size:64;index"`
	LastErrorCode        string     `json:"last_error_code" gorm:"size:128"`
	LastError            string     `json:"last_error" gorm:"type:text"`
	AcceptedAt           *time.Time `json:"accepted_at,omitempty"`
	BoundAt              *time.Time `json:"bound_at,omitempty"`
	FinishedAt           *time.Time `json:"finished_at,omitempty"`
}

// MoviePilotDownloaderReservation is a short-lived Coordinator admission
// record. It reserves qB download capacity before MoviePilot creates a task;
// it never contains qB credentials or Worker-local paths.
type MoviePilotDownloaderReservation struct {
	ID               string     `json:"id" gorm:"primaryKey;size:64"`
	CreatedAt        time.Time  `json:"created_at"`
	UpdatedAt        time.Time  `json:"updated_at"`
	RequestID        string     `json:"request_id" gorm:"size:64;uniqueIndex"`
	BridgeInstanceID string     `json:"bridge_instance_id" gorm:"size:64;index"`
	PolicyMode       string     `json:"policy_mode" gorm:"size:32;index"`
	RouteID          string     `json:"route_id" gorm:"size:128;index"`
	WorkerNodeID     string     `json:"worker_node_id" gorm:"size:64;index"`
	Downloader       string     `json:"downloader" gorm:"size:128;index"`
	QBClientID       string     `json:"qb_client_id" gorm:"size:128"`
	ExpectedBytes    int64      `json:"expected_bytes"`
	Status           string     `json:"status" gorm:"size:32;index"`
	ExpiresAt        time.Time  `json:"expires_at" gorm:"index"`
	BoundAt          *time.Time `json:"bound_at,omitempty"`
	LastError        string     `json:"last_error" gorm:"type:text"`
}

type TorrentRetentionPolicy struct {
	MinSeedSeconds  int64      `json:"min_seed_seconds,omitempty"`
	MinRatio        float64    `json:"min_ratio,omitempty"`
	SiteRuleID      string     `json:"site_rule_id,omitempty"`
	ManualHoldUntil *time.Time `json:"manual_hold_until,omitempty"`
	Permanent       bool       `json:"permanent,omitempty"`
}

type MoviePilotTorrentBinding struct {
	ID                     string     `json:"id" gorm:"primaryKey;size:64"`
	CreatedAt              time.Time  `json:"created_at"`
	UpdatedAt              time.Time  `json:"updated_at"`
	IntentID               string     `json:"intent_id" gorm:"size:64;uniqueIndex"`
	BridgeInstanceID       string     `json:"bridge_instance_id" gorm:"size:64;index;uniqueIndex:idx_moviepilot_bridge_torrent_hash"`
	DownloaderAlias        string     `json:"downloader_alias" gorm:"size:128;index"`
	WorkerNodeID           string     `json:"worker_node_id" gorm:"size:64;index"`
	QBClientID             string     `json:"qb_client_id" gorm:"size:128"`
	TorrentHash            string     `json:"torrent_hash" gorm:"size:64;uniqueIndex:idx_moviepilot_bridge_torrent_hash"`
	ContentPath            string     `json:"content_path" gorm:"type:text"`
	ObserveJobID           string     `json:"observe_job_id" gorm:"size:64;index"`
	ObservedContentPath    string     `json:"observed_content_path" gorm:"type:text"`
	RetentionPolicyJSON    string     `json:"retention_policy_json" gorm:"type:text"`
	Status                 string     `json:"status" gorm:"size:32;index"`
	RetentionStatus        string     `json:"retention_status" gorm:"size:32;index"`
	LastQBState            string     `json:"last_qb_state" gorm:"size:64"`
	LastQBProgress         float64    `json:"last_qb_progress"`
	LastQBRatio            float64    `json:"last_qb_ratio"`
	LastQBSeedingSeconds   int64      `json:"last_qb_seeding_seconds"`
	LastQBHNRPassed        bool       `json:"last_qb_hnr_passed" gorm:"column:last_qb_hnr_passed"`
	LastQBHNRKnown         bool       `json:"last_qb_hnr_known" gorm:"column:last_qb_hnr_known"`
	PausedForWorkerOffline bool       `json:"paused_for_worker_offline" gorm:"index"`
	WorkerOfflinePausedAt  *time.Time `json:"worker_offline_paused_at,omitempty"`
	DownloadCompletedAt    *time.Time `json:"download_completed_at,omitempty"`
	SeedStartedAt          *time.Time `json:"seed_started_at,omitempty"`
	RetentionEligibleAt    *time.Time `json:"retention_eligible_at,omitempty"`
	DeletingAt             *time.Time `json:"deleting_at,omitempty"`
	DeletedAt              *time.Time `json:"deleted_at,omitempty"`
	LastErrorCode          string     `json:"last_error_code" gorm:"size:128"`
	LastError              string     `json:"last_error" gorm:"type:text"`
}

type MoviePilotDeliveryFile struct {
	ID                 string     `json:"id" gorm:"primaryKey;size:64"`
	CreatedAt          time.Time  `json:"created_at"`
	UpdatedAt          time.Time  `json:"updated_at"`
	TorrentBindingID   string     `json:"torrent_binding_id" gorm:"size:64;index;uniqueIndex:idx_moviepilot_delivery_path"`
	RelativePath       string     `json:"relative_path" gorm:"type:text;uniqueIndex:idx_moviepilot_delivery_path"`
	FileName           string     `json:"file_name"`
	SourceSize         int64      `json:"source_size"`
	SubscriptionItemID uint       `json:"subscription_item_id" gorm:"index"`
	SourceKey          string     `json:"source_key" gorm:"size:191;index"`
	MediaSource        string     `json:"media_source" gorm:"size:64"`
	MediaID            string     `json:"media_id" gorm:"size:128"`
	Season             int        `json:"season"`
	Episode            int        `json:"episode"`
	Required           bool       `json:"required"`
	Status             string     `json:"status" gorm:"size:32;index"`
	ClusterJobID       string     `json:"cluster_job_id" gorm:"size:64;index"`
	ManifestID         string     `json:"manifest_id" gorm:"size:64;index"`
	UploadProgress     float64    `json:"upload_progress"`
	LastErrorCode      string     `json:"last_error_code" gorm:"size:128"`
	LastError          string     `json:"last_error" gorm:"type:text"`
	FinishedAt         *time.Time `json:"finished_at,omitempty"`
}

type MoviePilotTransferView struct {
	DeliveryID         string     `json:"delivery_id"`
	SubscriptionID     uint       `json:"subscription_id"`
	TorrentBindingID   string     `json:"torrent_binding_id"`
	RelativePath       string     `json:"relative_path"`
	FileName           string     `json:"file_name"`
	SourceSize         int64      `json:"source_size"`
	SubscriptionItemID uint       `json:"subscription_item_id"`
	SourceKey          string     `json:"source_key"`
	Season             int        `json:"season"`
	Episode            int        `json:"episode"`
	Required           bool       `json:"required"`
	Status             string     `json:"status"`
	ClusterJobID       string     `json:"cluster_job_id,omitempty"`
	ManifestID         string     `json:"manifest_id,omitempty"`
	UploadProgress     float64    `json:"upload_progress"`
	LastErrorCode      string     `json:"last_error_code,omitempty"`
	LastError          string     `json:"last_error,omitempty"`
	FinishedAt         *time.Time `json:"finished_at,omitempty"`
	WorkerNodeID       string     `json:"worker_node_id"`
	DownloaderAlias    string     `json:"downloader"`
	QBClientID         string     `json:"qb_client_id"`
	TorrentHash        string     `json:"torrent_hash"`
	TorrentStatus      string     `json:"torrent_status"`
	RetentionStatus    string     `json:"retention_status"`
}

// MoviePilotTaskStatus is the shared, redacted status projection used by the
// Coordinator admin UI and the Bridge status page. It deliberately contains
// identifiers and progress only; qB credentials and worker-local paths never
// cross this boundary.
type MoviePilotTaskStatus struct {
	RequestID             string     `json:"request_id"`
	SubscriptionID        uint       `json:"subscription_id"`
	SubscriptionItemID    uint       `json:"subscription_item_id"`
	SubscriptionName      string     `json:"subscription_name,omitempty"`
	ItemName              string     `json:"item_name,omitempty"`
	Phase                 string     `json:"phase"`
	IntentStatus          string     `json:"intent_status,omitempty"`
	DownloaderPolicyMode  string     `json:"downloader_policy_mode,omitempty"`
	SelectedRouteID       string     `json:"selected_route_id,omitempty"`
	ReservationID         string     `json:"reservation_id,omitempty"`
	ReservationStatus     string     `json:"reservation_status,omitempty"`
	ReservationExpiresAt  *time.Time `json:"reservation_expires_at,omitempty"`
	TorrentStatus         string     `json:"torrent_status,omitempty"`
	DownloadProgress      float64    `json:"download_progress,omitempty"`
	UploadProgress        float64    `json:"upload_progress,omitempty"`
	TransferredFiles      int        `json:"transferred_files,omitempty"`
	ExpectedFiles         int        `json:"expected_files,omitempty"`
	BindingID             string     `json:"binding_id,omitempty"`
	BridgeInstanceID      string     `json:"bridge_instance_id,omitempty"`
	WorkerNodeID          string     `json:"worker_node_id,omitempty"`
	WorkerStatus          string     `json:"worker_status,omitempty"`
	Downloader            string     `json:"downloader,omitempty"`
	QBClientID            string     `json:"qb_client_id,omitempty"`
	TorrentHash           string     `json:"torrent_hash,omitempty"`
	ClusterJobID          string     `json:"cluster_job_id,omitempty"`
	ClusterJobStatus      string     `json:"cluster_job_status,omitempty"`
	ClusterJobStage       string     `json:"cluster_job_stage,omitempty"`
	ClusterJobStageStatus string     `json:"cluster_job_stage_status,omitempty"`
	ErrorCode             string     `json:"error_code,omitempty"`
	Error                 string     `json:"error,omitempty"`
	UpdatedAt             time.Time  `json:"updated_at"`
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
	BridgeID     string    `json:"bridge_id" gorm:"size:64;index;uniqueIndex:idx_moviepilot_bridge_outbox_request"`
	RequestID    string    `json:"request_id" gorm:"size:64;index;uniqueIndex:idx_moviepilot_bridge_outbox_request"`
	EventID      string    `json:"event_id" gorm:"size:64;uniqueIndex"`
	PayloadJSON  string    `json:"payload_json" gorm:"type:text"`
	Status       string    `json:"status" gorm:"size:32;index"`
	AttemptCount int       `json:"attempt_count"`
	AvailableAt  time.Time `json:"available_at" gorm:"index"`
	LastError    string    `json:"last_error" gorm:"type:text"`
}

type MoviePilotBridgeInbox struct {
	EventID      string     `json:"event_id" gorm:"primaryKey;size:64"`
	CreatedAt    time.Time  `json:"created_at"`
	UpdatedAt    time.Time  `json:"updated_at"`
	BridgeID     string     `json:"bridge_id" gorm:"size:64;index"`
	RequestID    string     `json:"request_id" gorm:"size:64;index"`
	EventType    string     `json:"event_type" gorm:"size:64;index"`
	PayloadJSON  string     `json:"payload_json" gorm:"type:text"`
	Status       string     `json:"status" gorm:"size:32;index"`
	AttemptCount int        `json:"attempt_count" gorm:"not null;default:0"`
	ProcessedAt  *time.Time `json:"processed_at,omitempty"`
	LastError    string     `json:"last_error" gorm:"type:text"`
}

type SubscriptionBoundTorrent struct {
	BridgeInstanceID    string                 `json:"bridge_instance_id,omitempty"`
	ResourceRef         string                 `json:"resource_ref,omitempty"`
	SelectedFingerprint string                 `json:"selected_fingerprint,omitempty"`
	TorrentTitle        string                 `json:"torrent_title,omitempty"`
	Site                string                 `json:"site,omitempty"`
	Size                int64                  `json:"size,omitempty"`
	MediaSource         string                 `json:"media_source,omitempty"`
	MediaID             string                 `json:"media_id,omitempty"`
	MediaType           string                 `json:"media_type,omitempty"`
	Season              int                    `json:"season,omitempty"`
	Episode             int                    `json:"episode,omitempty"`
	RetentionPolicy     TorrentRetentionPolicy `json:"retention_policy,omitempty"`
	BoundAt             time.Time              `json:"bound_at,omitempty"`
}
