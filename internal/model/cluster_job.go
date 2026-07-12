package model

import "time"

const (
	ClusterJobTypeShareInspect  = "share.inspect"
	ClusterJobTypeShareBatch    = "share.batch"
	ClusterJobTypeMediaTransfer = "media.transfer"

	ClusterJobStatusQueued          = "queued"
	ClusterJobStatusPlanning        = "planning"
	ClusterJobStatusLeased          = "leased"
	ClusterJobStatusRunning         = "running"
	ClusterJobStatusRetryWait       = "retry_wait"
	ClusterJobStatusPartialFailed   = "partial_failed"
	ClusterJobStatusCancelRequested = "cancel_requested"
	ClusterJobStatusSucceeded       = "succeeded"
	ClusterJobStatusFailed          = "failed"
	ClusterJobStatusCancelled       = "cancelled"
	ClusterJobStatusDeadLetter      = "dead_letter"

	ClusterAttemptStatusOffered   = "offered"
	ClusterAttemptStatusAccepted  = "accepted"
	ClusterAttemptStatusRunning   = "running"
	ClusterAttemptStatusSucceeded = "succeeded"
	ClusterAttemptStatusFailed    = "failed"
	ClusterAttemptStatusLost      = "lost"
	ClusterAttemptStatusCancelled = "cancelled"
	ClusterAttemptStatusRejected  = "rejected"

	ClusterStageStatusPending   = "pending"
	ClusterStageStatusPermitted = "permitted"
	ClusterStageStatusRunning   = "running"
	ClusterStageStatusSucceeded = "succeeded"
	ClusterStageStatusFailed    = "failed"
	ClusterStageStatusSkipped   = "skipped"
	ClusterStageStatusUnknown   = "unknown"

	ClusterStageSavingShare        = "saving_share"
	ClusterStageDiscoveringFiles   = "discovering_files"
	ClusterStageDownloading        = "downloading"
	ClusterStageSourceCleanup      = "source_cleanup"
	ClusterStageUploadingMobile    = "uploading_mobile"
	ClusterStageResultPersisting   = "result_persisting"
	ClusterStageWorkerMediaCleanup = "worker_media_cleanup"
	ClusterStageResultReporting    = "result_reporting"
	ClusterStageETFMaterializing   = "etf_materializing"
	ClusterStageETFArchiving       = "etf_archiving"
	ClusterStageTargetNotifying    = "target_notifying"

	ClusterNotificationStatusPending     = "pending"
	ClusterNotificationStatusSending     = "sending"
	ClusterNotificationStatusSucceeded   = "succeeded"
	ClusterNotificationStatusUnknown     = "unknown"
	ClusterNotificationStatusFailed      = "failed"
	ClusterNotificationStatusNotRequired = "not_required"

	ClusterCleanupStatusPending   = "pending"
	ClusterCleanupStatusRunning   = "running"
	ClusterCleanupStatusSucceeded = "succeeded"
	ClusterCleanupStatusFailed    = "failed"

	ClusterResultDeliveryStatusQueued     = "queued"
	ClusterResultDeliveryStatusSending    = "sending"
	ClusterResultDeliveryStatusConsumed   = "consumed"
	ClusterResultDeliveryStatusDeadLetter = "dead_letter"

	ClusterUploadManifestStatusReceived     = "received"
	ClusterUploadManifestStatusAccepted     = "accepted"
	ClusterUploadManifestStatusDuplicate    = "duplicate"
	ClusterUploadManifestStatusAdopted      = "adopted"
	ClusterUploadManifestStatusConflict     = "conflict"
	ClusterUploadManifestStatusContextError = "context_mismatch"
	ClusterUploadManifestStatusConsumed     = "consumed"

	ClusterMessageStatusPending    = "pending"
	ClusterMessageStatusSending    = "sending"
	ClusterMessageStatusAcked      = "acked"
	ClusterMessageStatusProcessed  = "processed"
	ClusterMessageStatusFailed     = "failed"
	ClusterMessageStatusDeadLetter = "dead_letter"
)

// ClusterJob is the durable coordinator-side business task. TaskContextJSON is
// an immutable snapshot whose hash is copied into every worker result.
type ClusterJob struct {
	ID                       string     `json:"id" gorm:"primaryKey;size:64"`
	CreatedAt                time.Time  `json:"created_at"`
	UpdatedAt                time.Time  `json:"updated_at"`
	ParentJobID              string     `json:"parent_job_id" gorm:"size:64;index"`
	Type                     string     `json:"type" gorm:"size:64;index"`
	Status                   string     `json:"status" gorm:"size:32;index"`
	NotificationStatus       string     `json:"notification_status" gorm:"size:32;index"`
	WorkerCleanupStatus      string     `json:"worker_cleanup_status" gorm:"size:32;index"`
	ResultDeliveryStatus     string     `json:"result_delivery_status" gorm:"size:32;index"`
	IdempotencyKey           string     `json:"idempotency_key" gorm:"size:255;uniqueIndex"`
	WorkflowVersion          string     `json:"workflow_version" gorm:"size:64"`
	Priority                 int        `json:"priority" gorm:"index"`
	SubscriptionID           uint       `json:"subscription_id" gorm:"index"`
	SubscriptionItemID       uint       `json:"subscription_item_id" gorm:"index"`
	MediaItemID              string     `json:"media_item_id" gorm:"size:128;index"`
	SourceProvider           string     `json:"source_provider" gorm:"size:64;index"`
	SourceURL                string     `json:"source_url" gorm:"type:text"`
	TaskContextJSON          string     `json:"task_context_json" gorm:"type:text"`
	TaskContextHash          string     `json:"task_context_hash" gorm:"size:64;index"`
	RequiredCapabilitiesJSON string     `json:"required_capabilities_json" gorm:"type:text"`
	ExpectedBytes            int64      `json:"expected_bytes"`
	ExpectedItems            int        `json:"expected_items"`
	AssignedNodeID           string     `json:"assigned_node_id" gorm:"size:64;index"`
	CurrentAttemptID         string     `json:"current_attempt_id" gorm:"size:64;index"`
	CurrentGeneration        uint64     `json:"current_generation"`
	AvailableAt              time.Time  `json:"available_at" gorm:"index"`
	StartedAt                *time.Time `json:"started_at"`
	FinishedAt               *time.Time `json:"finished_at"`
	ArchivedAt               *time.Time `json:"archived_at" gorm:"index"`
	LastErrorCode            string     `json:"last_error_code" gorm:"size:128"`
	LastError                string     `json:"last_error" gorm:"type:text"`
}

// ClusterJobAttempt represents one leased execution of a job. Generation is a
// fencing counter and must increase whenever a job is reassigned.
type ClusterJobAttempt struct {
	ID             string     `json:"id" gorm:"primaryKey;size:64"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
	JobID          string     `json:"job_id" gorm:"size:64;uniqueIndex:idx_cluster_attempt_generation;index"`
	NodeID         string     `json:"node_id" gorm:"size:64;index"`
	Generation     uint64     `json:"generation" gorm:"uniqueIndex:idx_cluster_attempt_generation"`
	Status         string     `json:"status" gorm:"size:32;index"`
	LeaseTokenHash string     `json:"lease_token_hash" gorm:"size:64"`
	LeaseUntil     time.Time  `json:"lease_until" gorm:"index"`
	OfferedAt      time.Time  `json:"offered_at"`
	AcceptedAt     *time.Time `json:"accepted_at"`
	StartedAt      *time.Time `json:"started_at"`
	FinishedAt     *time.Time `json:"finished_at"`
	LastEventSeq   uint64     `json:"last_event_seq"`
	ResultHash     string     `json:"result_hash" gorm:"size:64;index"`
	ErrorCode      string     `json:"error_code" gorm:"size:128"`
	Error          string     `json:"error" gorm:"type:text"`
}

// ClusterJobStage captures resumable progress and the permit for an external
// side effect such as saving a share or submitting an upload.
type ClusterJobStage struct {
	ID              string     `json:"id" gorm:"primaryKey;size:64"`
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
	JobID           string     `json:"job_id" gorm:"size:64;index"`
	AttemptID       string     `json:"attempt_id" gorm:"size:64;uniqueIndex:idx_cluster_attempt_stage;index"`
	Name            string     `json:"name" gorm:"size:64;uniqueIndex:idx_cluster_attempt_stage"`
	Status          string     `json:"status" gorm:"size:32;index"`
	OperationKey    string     `json:"operation_key" gorm:"size:255;index"`
	PermitTokenHash string     `json:"permit_token_hash" gorm:"size:64"`
	PermitExpiresAt *time.Time `json:"permit_expires_at" gorm:"index"`
	CheckpointJSON  string     `json:"checkpoint_json" gorm:"type:text"`
	ProgressJSON    string     `json:"progress_json" gorm:"type:text"`
	RetryCount      int        `json:"retry_count"`
	StartedAt       *time.Time `json:"started_at"`
	FinishedAt      *time.Time `json:"finished_at"`
	ErrorCode       string     `json:"error_code" gorm:"size:128"`
	Error           string     `json:"error" gorm:"type:text"`
}

// ClusterUploadManifest is the coordinator-side durable copy of a worker's ETF
// upload result. PayloadJSON preserves the signed wire payload for audit.
type ClusterUploadManifest struct {
	ID                   string     `json:"id" gorm:"primaryKey;size:64"`
	CreatedAt            time.Time  `json:"created_at"`
	UpdatedAt            time.Time  `json:"updated_at"`
	JobID                string     `json:"job_id" gorm:"size:64;uniqueIndex:idx_cluster_upload_result;index"`
	ParentBatchID        string     `json:"parent_batch_id" gorm:"size:64;index"`
	MediaItemID          string     `json:"media_item_id" gorm:"size:128;uniqueIndex:idx_cluster_upload_result;index"`
	AttemptID            string     `json:"attempt_id" gorm:"size:64;index"`
	NodeID               string     `json:"node_id" gorm:"size:64;index"`
	Generation           uint64     `json:"generation"`
	OperationKey         string     `json:"operation_key" gorm:"size:255;index"`
	TaskContextHash      string     `json:"task_context_hash" gorm:"size:64;index"`
	WorkflowVersion      string     `json:"workflow_version" gorm:"size:64"`
	SubscriptionID       uint       `json:"subscription_id" gorm:"index"`
	SubscriptionItemID   uint       `json:"subscription_item_id" gorm:"index"`
	MediaType            string     `json:"media_type" gorm:"size:32;index"`
	TMDBID               int64      `json:"tmdb_id" gorm:"index"`
	Season               int        `json:"season" gorm:"index"`
	Episode              int        `json:"episode" gorm:"index"`
	LogicalTargetPath    string     `json:"logical_target_path" gorm:"type:text"`
	MobileAccountBinding string     `json:"mobile_account_binding" gorm:"size:128;index"`
	RemoteFileID         string     `json:"remote_file_id" gorm:"size:255"`
	RemoteParentID       string     `json:"remote_parent_id" gorm:"size:255"`
	RemotePath           string     `json:"remote_path" gorm:"type:text"`
	Name                 string     `json:"name" gorm:"type:text"`
	Size                 int64      `json:"size"`
	SHA256               string     `json:"sha256" gorm:"size:64;index"`
	HashSource           string     `json:"hash_source" gorm:"size:64"`
	UploadReceipt        string     `json:"upload_receipt" gorm:"type:text"`
	SourceObjectsJSON    string     `json:"source_objects_json" gorm:"type:text"`
	PayloadJSON          string     `json:"payload_json" gorm:"type:text"`
	PayloadHash          string     `json:"payload_hash" gorm:"size:64;uniqueIndex:idx_cluster_upload_result"`
	Status               string     `json:"status" gorm:"size:32;index"`
	AckOutcome           string     `json:"ack_outcome" gorm:"size:32;index"`
	ReceivedAt           time.Time  `json:"received_at" gorm:"index"`
	ConsumedAt           *time.Time `json:"consumed_at"`
	LastError            string     `json:"last_error" gorm:"type:text"`
}

// ClusterOutbox is a durable, replayable message queued for one cluster peer.
type ClusterOutbox struct {
	ID            string     `json:"id" gorm:"primaryKey;size:64"`
	CreatedAt     time.Time  `json:"created_at"`
	UpdatedAt     time.Time  `json:"updated_at"`
	MessageID     string     `json:"message_id" gorm:"size:64;uniqueIndex"`
	PeerNodeID    string     `json:"peer_node_id" gorm:"size:64;uniqueIndex:idx_cluster_outbox_seq;index"`
	SessionID     string     `json:"session_id" gorm:"size:64;index"`
	Seq           uint64     `json:"seq" gorm:"uniqueIndex:idx_cluster_outbox_seq"`
	CorrelationID string     `json:"correlation_id" gorm:"size:64;index"`
	MessageType   string     `json:"message_type" gorm:"size:64;index"`
	PayloadJSON   string     `json:"payload_json" gorm:"type:text"`
	PayloadHash   string     `json:"payload_hash" gorm:"size:64"`
	Status        string     `json:"status" gorm:"size:32;index"`
	AttemptCount  int        `json:"attempt_count"`
	AvailableAt   time.Time  `json:"available_at" gorm:"index"`
	LastSentAt    *time.Time `json:"last_sent_at"`
	AckedAt       *time.Time `json:"acked_at"`
	RetainUntil   *time.Time `json:"retain_until" gorm:"index"`
	LastError     string     `json:"last_error" gorm:"type:text"`
}

// ClusterInbox provides message-id and sequence based deduplication before a
// received command is applied.
type ClusterInbox struct {
	ID            string     `json:"id" gorm:"primaryKey;size:64"`
	CreatedAt     time.Time  `json:"created_at"`
	UpdatedAt     time.Time  `json:"updated_at"`
	MessageID     string     `json:"message_id" gorm:"size:64;uniqueIndex"`
	PeerNodeID    string     `json:"peer_node_id" gorm:"size:64;uniqueIndex:idx_cluster_inbox_session_seq;index"`
	SessionID     string     `json:"session_id" gorm:"size:64;uniqueIndex:idx_cluster_inbox_session_seq;index"`
	Seq           uint64     `json:"seq" gorm:"uniqueIndex:idx_cluster_inbox_session_seq"`
	CorrelationID string     `json:"correlation_id" gorm:"size:64;index"`
	MessageType   string     `json:"message_type" gorm:"size:64;index"`
	PayloadHash   string     `json:"payload_hash" gorm:"size:64"`
	Status        string     `json:"status" gorm:"size:32;index"`
	ReceivedAt    time.Time  `json:"received_at" gorm:"index"`
	ProcessedAt   *time.Time `json:"processed_at"`
	Error         string     `json:"error" gorm:"type:text"`
}
