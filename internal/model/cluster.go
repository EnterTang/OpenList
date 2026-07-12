package model

import "time"

const (
	ClusterRoleStandalone  = "standalone"
	ClusterRoleCoordinator = "coordinator"
	ClusterRoleWorker      = "worker"
	ClusterRoleHybrid      = "hybrid"

	ClusterNodeStatusPending  = "pending"
	ClusterNodeStatusOnline   = "online"
	ClusterNodeStatusOffline  = "offline"
	ClusterNodeStatusDraining = "draining"
	ClusterNodeStatusDisabled = "disabled"
	ClusterNodeStatusRevoked  = "revoked"

	ClusterSessionStatusConnected    = "connected"
	ClusterSessionStatusDisconnected = "disconnected"
	ClusterSessionStatusSuperseded   = "superseded"
)

// ClusterNode is the coordinator-owned identity and scheduling summary for a
// connected OpenList instance. Secrets and complete driver configurations must
// not be stored in this record.
type ClusterNode struct {
	ID                string     `json:"id" gorm:"primaryKey;size:64"`
	CreatedAt         time.Time  `json:"created_at"`
	UpdatedAt         time.Time  `json:"updated_at"`
	Name              string     `json:"name" gorm:"size:255;index"`
	Role              string     `json:"role" gorm:"size:32;index"`
	Status            string     `json:"status" gorm:"size:32;index"`
	ProtocolVersion   string     `json:"protocol_version" gorm:"size:32"`
	AgentVersion      string     `json:"agent_version" gorm:"size:64"`
	LabelsJSON        string     `json:"labels_json" gorm:"type:text"`
	Weight            int        `json:"weight"`
	Drain             bool       `json:"drain" gorm:"index"`
	Disabled          bool       `json:"disabled" gorm:"index"`
	LastSessionID     string     `json:"last_session_id" gorm:"size:64;index"`
	LastHeartbeatAt   *time.Time `json:"last_heartbeat_at" gorm:"index"`
	LastInventoryHash string     `json:"last_inventory_hash" gorm:"size:64"`
	KeyAlgorithm      string     `json:"key_algorithm,omitempty" gorm:"size:32"`
	KeyID             string     `json:"key_id,omitempty" gorm:"size:64;index"`
	KeyPublic         string     `json:"-" gorm:"type:text"`
	LastError         string     `json:"last_error" gorm:"type:text"`
}

// ClusterNodeSession stores durable WebSocket sequence watermarks so a node
// can resume delivery after either side reconnects.
type ClusterNodeSession struct {
	ID              string     `json:"id" gorm:"primaryKey;size:64"`
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
	NodeID          string     `json:"node_id" gorm:"size:64;index"`
	Status          string     `json:"status" gorm:"size:32;index"`
	ProtocolVersion string     `json:"protocol_version" gorm:"size:32"`
	RemoteAddr      string     `json:"remote_addr" gorm:"size:255"`
	ConnectedAt     time.Time  `json:"connected_at" gorm:"index"`
	DisconnectedAt  *time.Time `json:"disconnected_at"`
	LastSentSeq     uint64     `json:"last_sent_seq"`
	LastReceivedSeq uint64     `json:"last_received_seq"`
	LastAckedSeq    uint64     `json:"last_acked_seq"`
	ConnectionEpoch uint64     `json:"connection_epoch"`
	DisconnectCode  int        `json:"disconnect_code"`
	DisconnectError string     `json:"disconnect_error" gorm:"type:text"`
}

// ClusterNodeInventory stores an immutable, non-secret capability snapshot.
// MountsJSON must contain only redacted inventory fields, never Storage.Addition.
type ClusterNodeInventory struct {
	ID               string    `json:"id" gorm:"primaryKey;size:64"`
	CreatedAt        time.Time `json:"created_at"`
	NodeID           string    `json:"node_id" gorm:"size:64;uniqueIndex:idx_cluster_inventory_revision"`
	Revision         uint64    `json:"revision" gorm:"uniqueIndex:idx_cluster_inventory_revision"`
	CollectedAt      time.Time `json:"collected_at" gorm:"index"`
	InventoryHash    string    `json:"inventory_hash" gorm:"size:64;index"`
	CapabilitiesJSON string    `json:"capabilities_json" gorm:"type:text"`
	MountsJSON       string    `json:"mounts_json" gorm:"type:text"`
	MetricsJSON      string    `json:"metrics_json" gorm:"type:text"`
	RedisHealthJSON  string    `json:"redis_health_json" gorm:"type:text"`
}

// ClusterCoordinatorLease prevents two Coordinator processes sharing one
// database from advancing subscriptions and notifications concurrently.
type ClusterCoordinatorLease struct {
	Name       string    `json:"name" gorm:"primaryKey;size:64"`
	OwnerID    string    `json:"owner_id" gorm:"size:128;index"`
	LeaseUntil time.Time `json:"lease_until" gorm:"index"`
	UpdatedAt  time.Time `json:"updated_at"`
}
