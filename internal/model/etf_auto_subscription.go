package model

import "time"

const (
	ETFMediaRootStatusCollecting = "collecting"
	ETFMediaRootStatusSubscribed = "subscribed"
	ETFMediaRootStatusDirty      = "dirty"

	ETFMediaRootBatchStatusCollecting = "collecting"
	ETFMediaRootBatchStatusClosed     = "closed"

	ETFMediaRootBatchReasonInitialCreate  = "initial_create"
	ETFMediaRootBatchReasonContentChanged = "content_changed"

	ETFSubscriptionJobTypeCreate      = "create_subscription"
	ETFSubscriptionJobTypeManualCheck = "manual_check"

	ETFSubscriptionJobStatusPending    = "pending"
	ETFSubscriptionJobStatusRunning    = "running"
	ETFSubscriptionJobStatusSucceeded  = "succeeded"
	ETFSubscriptionJobStatusFailed     = "failed"
	ETFSubscriptionJobStatusUnknown    = "unknown"
	ETFSubscriptionJobStatusDeadLetter = "dead_letter"
)

type ETFMediaRoot struct {
	ID                        uint       `json:"id" gorm:"primarykey"`
	CreatedAt                 time.Time  `json:"created_at"`
	UpdatedAt                 time.Time  `json:"updated_at"`
	RootKey                   string     `json:"root_key" gorm:"uniqueIndex"`
	StorageID                 uint       `json:"storage_id" gorm:"index"`
	StorageMountPath          string     `json:"storage_mount_path" gorm:"index"`
	DriveID                   string     `json:"drive_id" gorm:"index"`
	MediaRootFileID           string     `json:"media_root_file_id" gorm:"index"`
	MediaRootPath             string     `json:"media_root_path" gorm:"index;type:text"`
	ActualMediaRootPath       string     `json:"actual_media_root_path" gorm:"type:text"`
	ShareRiskCanonicalTitle   string     `json:"share_risk_canonical_title"`
	MediaType                 string     `json:"media_type" gorm:"index"`
	TMDBID                    int64      `json:"tmdb_id" gorm:"index"`
	TMDBName                  string     `json:"tmdb_name"`
	TMDBYear                  int        `json:"tmdb_year"`
	Category                  string     `json:"category" gorm:"index"`
	TargetBaseURL             string     `json:"target_base_url"`
	TargetAPIToken            string     `json:"target_api_token"`
	TargetSupportsIdempotency bool       `json:"target_supports_idempotency"`
	ShareType                 string     `json:"share_type"`
	SharePeriodUnit           int        `json:"share_period_unit"`
	MobileShareRecordID       uint       `json:"mobile_share_record_id" gorm:"index"`
	ShareURL                  string     `json:"share_url" gorm:"type:text"`
	AccessCode                string     `json:"access_code"`
	TargetSubscriptionID      int64      `json:"target_subscription_id" gorm:"index"`
	LastCreateTaskID          string     `json:"last_create_task_id"`
	LastCheckTaskID           string     `json:"last_check_task_id"`
	CurrentFingerprint        string     `json:"current_fingerprint" gorm:"index"`
	LastNotifiedFingerprint   string     `json:"last_notified_fingerprint" gorm:"index"`
	DirtySince                *time.Time `json:"dirty_since"`
	PendingChangeCount        int        `json:"pending_change_count"`
	Status                    string     `json:"status" gorm:"index"`
	LastError                 string     `json:"last_error" gorm:"type:text"`
}

type ETFMediaRootBatch struct {
	ID                    uint      `json:"id" gorm:"primarykey"`
	CreatedAt             time.Time `json:"created_at"`
	UpdatedAt             time.Time `json:"updated_at"`
	BatchKey              string    `json:"batch_key" gorm:"uniqueIndex"`
	MediaRootID           uint      `json:"media_root_id" gorm:"index"`
	Status                string    `json:"status" gorm:"index"`
	Reason                string    `json:"reason" gorm:"index"`
	ETFCount              int       `json:"etf_count"`
	FirstEventAt          time.Time `json:"first_event_at" gorm:"index"`
	LastEventAt           time.Time `json:"last_event_at" gorm:"index"`
	QuietUntil            time.Time `json:"quiet_until" gorm:"index"`
	MediaRootCreated      bool      `json:"media_root_created" gorm:"index"`
	FingerprintAfterBatch string    `json:"fingerprint_after_batch" gorm:"index"`
	ClusterJobIDsJSON     string    `json:"cluster_job_ids_json" gorm:"type:text"`
}

type ETFSubscriptionJob struct {
	ID                        uint       `json:"id" gorm:"primarykey"`
	CreatedAt                 time.Time  `json:"created_at"`
	UpdatedAt                 time.Time  `json:"updated_at"`
	JobKey                    string     `json:"job_key" gorm:"uniqueIndex"`
	MediaRootID               uint       `json:"media_root_id" gorm:"index"`
	BatchID                   uint       `json:"batch_id" gorm:"index"`
	Type                      string     `json:"type" gorm:"index"`
	Status                    string     `json:"status" gorm:"index"`
	TargetBaseURL             string     `json:"target_base_url"`
	TargetAPIToken            string     `json:"target_api_token"`
	TargetSupportsIdempotency bool       `json:"target_supports_idempotency"`
	ShareType                 string     `json:"share_type"`
	Attempts                  int        `json:"attempts"`
	NextRetryAt               *time.Time `json:"next_retry_at" gorm:"index"`
	MobileShareRecordID       uint       `json:"mobile_share_record_id" gorm:"index"`
	ShareURL                  string     `json:"share_url" gorm:"type:text"`
	AccessCode                string     `json:"access_code"`
	TargetSubscriptionID      int64      `json:"target_subscription_id" gorm:"index"`
	TargetTaskID              string     `json:"target_task_id"`
	RequestPayloadJSON        string     `json:"request_payload_json" gorm:"type:text"`
	ResponseJSON              string     `json:"response_json" gorm:"type:text"`
	Fingerprint               string     `json:"fingerprint" gorm:"index"`
	ClusterJobIDsJSON         string     `json:"cluster_job_ids_json" gorm:"type:text"`
	LastError                 string     `json:"last_error" gorm:"type:text"`
}
