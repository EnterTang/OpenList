package model

import "time"

const (
	ClusterShareInspectStatusPending  = "pending"
	ClusterShareInspectStatusConsumed = "consumed"
)

// ClusterShareInspectManifest is the Coordinator's durable sealed copy of a
// Worker's metadata-only share inspection. PayloadJSON is retained so a
// subscription consumer can diff the exact canonical object set after a
// Coordinator restart without asking the Worker to repeat the inspection.
type ClusterShareInspectManifest struct {
	ID             string     `json:"id" gorm:"primaryKey;size:64"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
	JobID          string     `json:"job_id" gorm:"size:64;uniqueIndex"`
	AttemptID      string     `json:"attempt_id" gorm:"size:64;index"`
	NodeID         string     `json:"node_id" gorm:"size:64;index"`
	Generation     uint64     `json:"generation"`
	SubscriptionID uint       `json:"subscription_id" gorm:"index"`
	Version        string     `json:"version" gorm:"size:64;index"`
	CanonicalRef   string     `json:"canonical_ref" gorm:"size:255;index"`
	ObjectHash     string     `json:"object_hash" gorm:"size:64;index"`
	PayloadJSON    string     `json:"payload_json" gorm:"type:text"`
	PayloadHash    string     `json:"payload_hash" gorm:"size:64;index"`
	Status         string     `json:"status" gorm:"size:32;index"`
	InspectedAt    time.Time  `json:"inspected_at" gorm:"index"`
	ConsumedAt     *time.Time `json:"consumed_at"`
	LastError      string     `json:"last_error" gorm:"type:text"`
}
