package model

import "time"

const (
	ClusterDesiredStatusPending  = "pending"
	ClusterDesiredStatusSending  = "sending"
	ClusterDesiredStatusApplied  = "applied"
	ClusterDesiredStatusRejected = "rejected"
	ClusterDesiredStatusFailed   = "failed"
)

// ClusterSecret stores only Coordinator-master-key ciphertext. Plaintext is
// decrypted in memory solely while producing a node-specific secret envelope.
type ClusterSecret struct {
	ID          string     `json:"id" gorm:"primaryKey;size:64"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
	Alias       string     `json:"alias" gorm:"size:128;uniqueIndex"`
	Kind        string     `json:"kind" gorm:"size:64;index"`
	Ciphertext  string     `json:"-" gorm:"type:text"`
	Nonce       string     `json:"-" gorm:"type:text"`
	Fingerprint string     `json:"fingerprint" gorm:"size:64;index"`
	Version     uint64     `json:"version"`
	RotatedAt   time.Time  `json:"rotated_at"`
	RevokedAt   *time.Time `json:"revoked_at,omitempty" gorm:"index"`
}

type ClusterNodeDesiredConfig struct {
	NodeID           string     `json:"node_id" gorm:"primaryKey;size:64"`
	CreatedAt        time.Time  `json:"created_at"`
	UpdatedAt        time.Time  `json:"updated_at"`
	Revision         uint64     `json:"revision" gorm:"index"`
	DesiredHash      string     `json:"desired_hash" gorm:"size:64"`
	ConfigJSON       string     `json:"config_json" gorm:"type:text"`
	Status           string     `json:"status" gorm:"size:32;index"`
	ObservedRevision uint64     `json:"observed_revision"`
	ObservedHash     string     `json:"observed_hash" gorm:"size:64"`
	ObservedAt       *time.Time `json:"observed_at,omitempty"`
	LastError        string     `json:"last_error" gorm:"type:text"`
}

type ClusterStorageProfile struct {
	ID                string     `json:"id" gorm:"primaryKey;size:64"`
	CreatedAt         time.Time  `json:"created_at"`
	UpdatedAt         time.Time  `json:"updated_at"`
	NodeID            string     `json:"node_id" gorm:"size:64;uniqueIndex:idx_cluster_node_path;index"`
	NodeMountID       string     `json:"node_mount_id" gorm:"size:128;index"`
	Revision          uint64     `json:"revision" gorm:"index"`
	DesiredHash       string     `json:"desired_hash" gorm:"size:64"`
	Driver            string     `json:"driver" gorm:"size:128"`
	SchemaVersion     string     `json:"schema_version" gorm:"size:64"`
	MountPath         string     `json:"mount_path" gorm:"type:text;uniqueIndex:idx_cluster_node_path"`
	ParametersJSON    string     `json:"parameters_json" gorm:"type:text"`
	CredentialRef     string     `json:"credential_ref" gorm:"size:64;index"`
	Status            string     `json:"status" gorm:"size:32;index"`
	ObservedRevision  uint64     `json:"observed_revision"`
	ObservedHash      string     `json:"observed_hash" gorm:"size:64"`
	ObservedStorageID uint       `json:"observed_storage_id"`
	ObservedAt        *time.Time `json:"observed_at,omitempty"`
	LastError         string     `json:"last_error" gorm:"type:text"`
}

type ClusterControlAudit struct {
	ID           string    `json:"id" gorm:"primaryKey;size:64"`
	CreatedAt    time.Time `json:"created_at" gorm:"index"`
	NodeID       string    `json:"node_id" gorm:"size:64;index"`
	Actor        string    `json:"actor" gorm:"size:128;index"`
	RemoteIP     string    `json:"remote_ip,omitempty" gorm:"size:64"`
	RequestID    string    `json:"request_id,omitempty" gorm:"size:128;index"`
	Action       string    `json:"action" gorm:"size:64;index"`
	ResourceType string    `json:"resource_type" gorm:"size:64;index"`
	ResourceID   string    `json:"resource_id" gorm:"size:128;index"`
	Revision     uint64    `json:"revision"`
	Outcome      string    `json:"outcome" gorm:"size:32;index"`
	Detail       string    `json:"detail" gorm:"type:text"`
}

// ClusterWorkerObservedState is local Worker state. It contains only
// non-secret desired configuration and revision/hash journals needed to make
// control commands idempotent across restarts.
type ClusterWorkerObservedState struct {
	ID           string    `json:"id" gorm:"primaryKey;size:255"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
	ResourceType string    `json:"resource_type" gorm:"size:32;index"`
	ResourceKey  string    `json:"resource_key" gorm:"size:255;index"`
	Revision     uint64    `json:"revision" gorm:"index"`
	Hash         string    `json:"hash" gorm:"size:64"`
	PayloadJSON  string    `json:"payload_json" gorm:"type:text"`
}
