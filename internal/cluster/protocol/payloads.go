package protocol

import "time"

const (
	ManifestAckAccepted        = "accepted"
	ManifestAckDuplicate       = "duplicate"
	ManifestAckAdopted         = "adopted"
	ManifestAckConflict        = "conflict"
	ManifestAckContextMismatch = "context_mismatch"
)

type Hello struct {
	NodeID            string            `json:"node_id,omitempty"`
	NodeName          string            `json:"node_name"`
	AgentVersion      string            `json:"agent_version"`
	SupportedVersions []string          `json:"supported_versions"`
	Role              string            `json:"role"`
	ResumeSessionID   string            `json:"resume_session_id,omitempty"`
	LastReceivedSeq   uint64            `json:"last_received_seq,omitempty"`
	EnrollmentToken   string            `json:"enrollment_token,omitempty"`
	Labels            map[string]string `json:"labels,omitempty"`
	KeyAgreement      *NodeKeyAgreement `json:"key_agreement,omitempty"`
	ObservedRevision  uint64            `json:"observed_revision,omitempty"`
}

type Welcome struct {
	CoordinatorID    string    `json:"coordinator_id"`
	NodeID           string    `json:"node_id"`
	SessionID        string    `json:"session_id"`
	ConnectionEpoch  uint64    `json:"connection_epoch"`
	ProtocolVersion  string    `json:"protocol_version"`
	HeartbeatSeconds int       `json:"heartbeat_seconds"`
	LeaseSeconds     int       `json:"lease_seconds"`
	ResumeAccepted   bool      `json:"resume_accepted"`
	ReplayFromSeq    uint64    `json:"replay_from_seq,omitempty"`
	ServerTime       time.Time `json:"server_time"`
}

type Heartbeat struct {
	ObservedAt       time.Time        `json:"observed_at"`
	ActiveJobCount   int              `json:"active_job_count"`
	DownloadBytesSec int64            `json:"download_bytes_sec"`
	UploadBytesSec   int64            `json:"upload_bytes_sec"`
	FreeDiskBytes    int64            `json:"free_disk_bytes"`
	ResultQueue      ResultQueueStats `json:"result_queue"`
}

type ResultQueueStats struct {
	PendingCount     int64  `json:"pending_count"`
	PendingBytes     int64  `json:"pending_bytes"`
	InFlightCount    int64  `json:"in_flight_count"`
	DeadLetterCount  int64  `json:"dead_letter_count"`
	OldestAgeSeconds int64  `json:"oldest_age_seconds"`
	DurabilityReady  bool   `json:"durability_ready"`
	AOFStatus        string `json:"aof_status,omitempty"`
}

type InventoryQuery struct {
	RequestID string `json:"request_id"`
}

type InventoryReport struct {
	RequestID        string            `json:"request_id,omitempty"`
	Revision         uint64            `json:"revision"`
	CollectedAt      time.Time         `json:"collected_at"`
	InventoryHash    string            `json:"inventory_hash"`
	Capabilities     NodeCapabilities  `json:"capabilities"`
	Mounts           []MountInventory  `json:"mounts"`
	RecentError      string            `json:"recent_error,omitempty"`
	KeyAgreement     *NodeKeyAgreement `json:"key_agreement,omitempty"`
	ObservedRevision uint64            `json:"observed_revision,omitempty"`
}

type NodeCapabilities struct {
	DownloadTools        []string `json:"download_tools,omitempty"`
	SupportedProviders   []string `json:"supported_providers,omitempty"`
	SupportedOperations  []string `json:"supported_operations,omitempty"`
	DownloadConcurrency  int      `json:"download_concurrency"`
	UploadConcurrency    int      `json:"upload_concurrency"`
	LocalScratchRoot     string   `json:"local_scratch_root,omitempty"`
	FreeLocalBytes       int64    `json:"free_local_bytes"`
	RedisDurabilityReady bool     `json:"redis_durability_ready"`
}

type MountInventory struct {
	NodeMountID        string `json:"node_mount_id"`
	Driver             string `json:"driver"`
	Provider           string `json:"provider"`
	MountPath          string `json:"mount_path"`
	AccountAlias       string `json:"account_alias,omitempty"`
	AccountFingerprint string `json:"account_fingerprint,omitempty"`
	Status             string `json:"status"`
	ReadOnly           bool   `json:"read_only"`
	CanUpload          bool   `json:"can_upload"`
	CanShare           bool   `json:"can_share"`
	SupportsETF        bool   `json:"supports_etf"`
	TotalBytes         int64  `json:"total_bytes,omitempty"`
	FreeBytes          int64  `json:"free_bytes,omitempty"`
	DriverVersion      string `json:"driver_version,omitempty"`
	ConfigSchemaHash   string `json:"config_schema_hash,omitempty"`
}

type ConfigApply struct {
	Revision      uint64               `json:"revision"`
	DesiredHash   string               `json:"desired_hash"`
	ConfigJSON    string               `json:"config_json,omitempty"`
	DesiredConfig *WorkerDesiredConfig `json:"desired_config,omitempty"`
}

type StorageApply struct {
	Revision       uint64         `json:"revision"`
	DesiredHash    string         `json:"desired_hash"`
	NodeMountID    string         `json:"node_mount_id"`
	Driver         string         `json:"driver"`
	SchemaVersion  string         `json:"schema_version"`
	MountPath      string         `json:"mount_path"`
	Parameters     map[string]any `json:"parameters,omitempty"`
	CredentialRef  string         `json:"credential_ref,omitempty"`
	SecretEnvelope string         `json:"secret_envelope,omitempty"`
	Operation      string         `json:"operation,omitempty"`
	Remark         string         `json:"remark,omitempty"`
	Disabled       bool           `json:"disabled,omitempty"`
}

type AttemptRef struct {
	JobID      string `json:"job_id"`
	AttemptID  string `json:"attempt_id"`
	Generation uint64 `json:"generation"`
	LeaseToken string `json:"lease_token,omitempty"`
}

type JobOffer struct {
	AttemptRef
	IdempotencyKey       string        `json:"idempotency_key"`
	JobType              string        `json:"job_type"`
	LeaseUntil           time.Time     `json:"lease_until"`
	RequiredCapabilities []string      `json:"required_capabilities,omitempty"`
	TaskContext          TaskContext   `json:"task_context"`
	TaskContextHash      string        `json:"task_context_hash"`
	StagePermits         []StagePermit `json:"stage_permits,omitempty"`
}

type JobAccept struct {
	AttemptRef
	AcceptedAt time.Time `json:"accepted_at"`
}

type JobReject struct {
	AttemptRef
	Code      string `json:"code"`
	Reason    string `json:"reason,omitempty"`
	Retryable bool   `json:"retryable"`
}

type JobProgress struct {
	AttemptRef
	Stage          string    `json:"stage"`
	EventSeq       uint64    `json:"event_seq"`
	CompletedBytes int64     `json:"completed_bytes,omitempty"`
	TotalBytes     int64     `json:"total_bytes,omitempty"`
	BytesPerSecond int64     `json:"bytes_per_second,omitempty"`
	ObservedAt     time.Time `json:"observed_at"`
	Message        string    `json:"message,omitempty"`
}

type JobCheckpoint struct {
	AttemptRef
	Stage          string         `json:"stage"`
	EventSeq       uint64         `json:"event_seq"`
	Checkpoint     map[string]any `json:"checkpoint"`
	CheckpointHash string         `json:"checkpoint_hash"`
	ObservedAt     time.Time      `json:"observed_at"`
}

type JobResult struct {
	AttemptRef
	Status     string         `json:"status"`
	ResultHash string         `json:"result_hash,omitempty"`
	Result     map[string]any `json:"result,omitempty"`
	ErrorCode  string         `json:"error_code,omitempty"`
	Error      string         `json:"error,omitempty"`
	FinishedAt time.Time      `json:"finished_at"`
}

// ShareInspectManifest is the sealed, canonical view of a share returned by a
// Worker. It contains metadata only; inspecting a share never saves or
// transfers any object.
type ShareInspectManifest struct {
	Version      string           `json:"version"`
	Share        ShareTaskContext `json:"share"`
	CanonicalRef string           `json:"canonical_ref"`
	Objects      []SourceObject   `json:"objects"`
	ObjectHash   string           `json:"object_hash"`
	InspectedAt  time.Time        `json:"inspected_at"`
}

type JobCancel struct {
	AttemptRef
	Reason      string    `json:"reason,omitempty"`
	RequestedAt time.Time `json:"requested_at"`
}

type LeaseRenew struct {
	AttemptRef
	RequestedUntil time.Time `json:"requested_until"`
	LastEventSeq   uint64    `json:"last_event_seq"`
}

type StagePermit struct {
	AttemptRef
	Stage           string    `json:"stage"`
	OperationKey    string    `json:"operation_key"`
	PermitToken     string    `json:"permit_token"`
	PermitExpiresAt time.Time `json:"permit_expires_at"`
}

type StagePermitRequest struct {
	AttemptRef
	Stage        string `json:"stage"`
	OperationKey string `json:"operation_key"`
}

type TaskContext struct {
	ParentBatchID         string                  `json:"parent_batch_id"`
	MediaItemID           string                  `json:"media_item_id"`
	WorkflowVersion       string                  `json:"workflow_version"`
	SealedManifestVersion string                  `json:"sealed_manifest_version"`
	Subscription          SubscriptionTaskContext `json:"subscription"`
	Share                 ShareTaskContext        `json:"share"`
	Media                 MediaTaskContext        `json:"media"`
	SourceObjects         []SourceObject          `json:"source_objects"`
	TargetProfile         string                  `json:"target_profile"`
}

type ShareTaskContext struct {
	Provider string `json:"provider"`
	URL      string `json:"url"`
	Passcode string `json:"passcode,omitempty"`
}

type SubscriptionTaskContext struct {
	SubscriptionID       uint   `json:"subscription_id"`
	SubscriptionItemID   uint   `json:"subscription_item_id"`
	SubscriptionName     string `json:"subscription_name"`
	SourceKey            string `json:"source_key"`
	SourceMessageID      string `json:"source_message_id,omitempty"`
	SourceMessageChannel string `json:"source_message_channel,omitempty"`
	SourceMessageURL     string `json:"source_message_url,omitempty"`
	SourceMessageText    string `json:"source_message_text,omitempty"`
	ShareRefFingerprint  string `json:"share_ref_fingerprint"`
}

type MediaTaskContext struct {
	MediaType         string `json:"media_type"`
	TMDBID            int64  `json:"tmdb_id"`
	Season            int    `json:"season"`
	Episode           int    `json:"episode"`
	LogicalMediaRoot  string `json:"logical_media_root"`
	LogicalTargetPath string `json:"logical_target_path"`
}

type SourceObject struct {
	Provider           string    `json:"provider"`
	SourceFileID       string    `json:"source_file_id"`
	SourceRelativePath string    `json:"source_relative_path"`
	Size               int64     `json:"size,omitempty"`
	Hash               string    `json:"hash,omitempty"`
	ModifiedAt         time.Time `json:"modified_at,omitempty"`
}

type UploadETFManifest struct {
	AttemptRef
	ParentBatchID         string                  `json:"parent_batch_id"`
	MediaItemID           string                  `json:"media_item_id"`
	OperationKey          string                  `json:"operation_key"`
	StagePermitToken      string                  `json:"stage_permit_token"`
	TaskContextHash       string                  `json:"task_context_hash"`
	WorkflowVersion       string                  `json:"workflow_version"`
	SealedManifestVersion string                  `json:"sealed_manifest_version"`
	TargetProfile         string                  `json:"target_profile"`
	WorkerTargetRoot      string                  `json:"worker_target_root,omitempty"`
	Subscription          SubscriptionTaskContext `json:"subscription"`
	Share                 ShareTaskContext        `json:"share"`
	Media                 MediaTaskContext        `json:"media"`
	SourceObjects         []SourceObject          `json:"source_objects"`
	MobileAccountBinding  string                  `json:"mobile_account_binding"`
	RemoteFileID          string                  `json:"remote_file_id"`
	RemoteParentID        string                  `json:"remote_parent_id,omitempty"`
	RemotePath            string                  `json:"remote_path"`
	Name                  string                  `json:"name"`
	Size                  int64                   `json:"size"`
	SHA256                string                  `json:"sha256"`
	HashSource            string                  `json:"hash_source"`
	UploadReceipt         string                  `json:"upload_receipt,omitempty"`
}

type UploadETFManifestAck struct {
	JobID       string    `json:"job_id"`
	AttemptID   string    `json:"attempt_id"`
	MediaItemID string    `json:"media_item_id"`
	PayloadHash string    `json:"payload_hash"`
	Outcome     string    `json:"outcome"`
	ManifestID  string    `json:"manifest_id,omitempty"`
	ErrorCode   string    `json:"error_code,omitempty"`
	Error       string    `json:"error,omitempty"`
	ConsumedAt  time.Time `json:"consumed_at"`
}

type Ack struct {
	MessageID string `json:"message_id,omitempty"`
	AckSeq    uint64 `json:"ack_seq"`
	Outcome   string `json:"outcome,omitempty"`
}

type Nack struct {
	MessageID string `json:"message_id,omitempty"`
	AckSeq    uint64 `json:"ack_seq"`
	Code      string `json:"code"`
	Error     string `json:"error,omitempty"`
	Retryable bool   `json:"retryable"`
}
